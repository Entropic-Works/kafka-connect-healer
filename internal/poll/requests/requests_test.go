package requests

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
)

func TestNewConnectAPICreatesIndependentClients(t *testing.T) {
	first := NewConnectAPI(connectcluster.DefaultConfiguration(), time.Second)
	second := NewConnectAPI(connectcluster.DefaultConfiguration(), 2*time.Second)
	if first.HTTPClient == second.HTTPClient {
		t.Fatal("clusters share an HTTP client")
	}
	firstTransport := first.HTTPClient.Transport.(*http.Transport)
	secondTransport := second.HTTPClient.Transport.(*http.Transport)
	if firstTransport == secondTransport || firstTransport == http.DefaultTransport || firstTransport.TLSClientConfig == secondTransport.TLSClientConfig {
		t.Fatal("clusters share a transport or TLS configuration")
	}
	firstTransport.TLSClientConfig.InsecureSkipVerify = true
	if secondTransport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("changing one cluster's TLS policy changed another cluster")
	}
	first.HTTPClient.Timeout = 3 * time.Second
	if second.HTTPClient.Timeout != 2*time.Second {
		t.Fatal("changing one cluster's timeout changed another cluster's client")
	}
}

func TestConnectAPIVerifiesTLSUnlessExplicitlyDisabled(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	address, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name          string
		trust         bool
		wrongHostname bool
		skipVerify    bool
		wantError     bool
	}{
		{name: "untrusted certificate", wantError: true},
		{name: "explicit bypass", skipVerify: true},
		{name: "trusted certificate", trust: true},
		{name: "wrong hostname", trust: true, wrongHostname: true, wantError: true},
		{name: "explicit hostname bypass", trust: true, wrongHostname: true, skipVerify: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			connect := NewConnectAPI(connectcluster.Configuration{
				Host: address.Hostname(), Port: address.Port(),
				TLS: connectcluster.TLSConfiguration{HTTPS: true, InsecureSkipVerify: test.skipVerify},
			}, time.Second)
			t.Cleanup(connect.HTTPClient.CloseIdleConnections)
			transport := connect.HTTPClient.Transport.(*http.Transport)
			if test.trust {
				roots := x509.NewCertPool()
				roots.AddCert(server.Certificate())
				transport.TLSClientConfig.RootCAs = roots
			}
			if test.wrongHostname {
				transport.TLSClientConfig.ServerName = "wrong.example.test"
			}
			request, err := connect.NewRequest(t.Context(), http.MethodGet, connect.BaseURL)
			if err != nil {
				t.Fatal(err)
			}
			response, err := connect.HTTPClient.Do(request)
			if response != nil {
				response.Body.Close()
			}
			if (err != nil) != test.wantError {
				t.Fatalf("request error = %v, want error = %t", err, test.wantError)
			}
			if !test.wantError && response.StatusCode != http.StatusNoContent {
				t.Fatalf("unexpected status: %d", response.StatusCode)
			}
		})
	}
}

func TestConnectAPIReusesClientAfterTimeoutChange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	connect := NewConnectAPI(connectcluster.Configuration{
		Host: serverURL.Hostname(), Port: serverURL.Port(),
	}, time.Second)
	client := connect.HTTPClient
	request, err := connect.NewRequest(t.Context(), http.MethodGet, connect.BaseURL)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()

	connect.HTTPClient.Timeout = 30 * time.Millisecond
	request, err = connect.NewRequest(t.Context(), http.MethodGet, connect.BaseURL+"/slow")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Do(request)
	if timeout, ok := errors.AsType[net.Error](err); !ok || !timeout.Timeout() {
		t.Fatalf("slow request error = %v, want a timeout", err)
	}
	if connect.HTTPClient != client {
		t.Fatal("timeout update replaced the HTTP client")
	}
}

func TestNewConnectAPIBuildsBaseURL(t *testing.T) {
	tests := []struct {
		name          string
		configuration connectcluster.Configuration
		wantURL       string
	}{
		{
			name:          "HTTP",
			configuration: connectcluster.Configuration{Host: "localhost", Port: "8083"},
			wantURL:       "http://localhost:8083",
		},
		{
			name:          "HTTPS",
			configuration: connectcluster.Configuration{Host: "connect.example.test", Port: "8443", TLS: connectcluster.TLSConfiguration{HTTPS: true}},
			wantURL:       "https://connect.example.test:8443",
		},
		{
			name:          "IPv6",
			configuration: connectcluster.Configuration{Host: "2001:db8::1", Port: "8083"},
			wantURL:       "http://[2001:db8::1]:8083",
		},
		{
			name:          "scoped IPv6",
			configuration: connectcluster.Configuration{Host: "fe80::1%eth0", Port: "8083"},
			wantURL:       "http://[fe80::1%25eth0]:8083",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connect := NewConnectAPI(test.configuration, time.Second)
			if connect.BaseURL != test.wantURL {
				t.Fatalf("BaseURL = %q, want %q", connect.BaseURL, test.wantURL)
			}
		})
	}
}

func TestConnectAPIDoesNotFollowRedirects(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			t.Run(method+"/"+strconv.Itoa(code), func(t *testing.T) {
				var targetRequests atomic.Int32
				target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					targetRequests.Add(1)
					w.WriteHeader(http.StatusOK)
				}))
				defer target.Close()
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Location", target.URL)
					w.WriteHeader(code)
					io.WriteString(w, "original redirect response")
				}))
				defer server.Close()
				serverURL, err := url.Parse(server.URL)
				if err != nil {
					t.Fatal(err)
				}
				connect := NewConnectAPI(connectcluster.Configuration{
					Host: serverURL.Hostname(), Port: serverURL.Port(),
				}, 2*time.Second)
				if connect.HTTPClient.Timeout != 2*time.Second {
					t.Fatal("Connect client did not use the configured timeout")
				}
				request, err := connect.NewRequest(context.Background(), method, connect.BaseURL)
				if err != nil {
					t.Fatal(err)
				}
				response, err := connect.HTTPClient.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				if err != nil {
					t.Fatal(err)
				}
				if response.StatusCode != code || string(body) != "original redirect response" {
					t.Fatalf("response = (%d, %q), want original redirect response", response.StatusCode, body)
				}
				if targetRequests.Load() != 0 {
					t.Fatal("Connect client followed the redirect")
				}
			})
		}
	}
}

func TestConnectAPINewRequestBasicAuth(t *testing.T) {
	tests := []struct {
		name     string
		auth     connectcluster.AuthConfiguration
		expected bool
	}{
		{
			name:     "disabled authentication omits credentials",
			expected: false,
		},
		{
			name: "enabled authentication adds credentials",
			auth: connectcluster.AuthConfiguration{
				Enabled:  true,
				Username: "connect-user",
				Password: "connect-password",
			},
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connect := ConnectAPI{Auth: test.auth}
			request, err := connect.NewRequest(context.Background(), http.MethodGet, "https://connect.example.test/connectors")
			if err != nil {
				t.Fatalf("NewRequest() error = %v", err)
			}

			username, password, ok := request.BasicAuth()
			if ok != test.expected {
				t.Fatalf("request.BasicAuth() found credentials = %t, want %t", ok, test.expected)
			}
			if test.expected && (username != test.auth.Username || password != test.auth.Password) {
				t.Fatalf("request.BasicAuth() = (%q, %q), want (%q, %q)", username, password, test.auth.Username, test.auth.Password)
			}
		})
	}
}
