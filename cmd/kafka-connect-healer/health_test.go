package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/api"
	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
	"github.com/Entropic-Works/kafka-connect-healer/internal/health"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll"
)

func TestHealthProbes(t *testing.T) {
	for _, authEnabled := range []bool{false, true} {
		for _, state := range []string{"ready without clusters", "canceled", "closed"} {
			name := state
			if authEnabled {
				name += " with authentication"
			}
			t.Run(name, func(t *testing.T) {
				configuration := config.DefaultConfiguration()
				configuration.APIConfig.AuthConfig = config.AuthConfiguration{Enabled: authEnabled, Username: "user", Password: "secret"}
				configurationManager := config.NewManager(configuration)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				clusterManager, err := poll.NewClusterManager(ctx, configurationManager, nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(clusterManager.Close)
				controlServer := api.NewServer(api.Components{ConfigurationManager: configurationManager, ClusterManager: clusterManager})
				server := health.NewServer(clusterManager.Ready)
				if controlServer.Addr != ":8080" || server.Addr != ":8081" {
					t.Fatalf("unexpected server addresses: control %q, health %q", controlServer.Addr, server.Addr)
				}
				for _, path := range []string{"/livez", "/readyz"} {
					request := httptest.NewRequest(http.MethodGet, path, nil)
					request.SetBasicAuth("user", "secret")
					response := httptest.NewRecorder()
					controlServer.Handler.ServeHTTP(response, request)
					if response.Code != http.StatusNotFound {
						t.Fatalf("control API serves %s: %d", path, response.Code)
					}
				}
				for _, path := range []string{"/", "/config", "/clusters"} {
					response := httptest.NewRecorder()
					server.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
					if response.Code != http.StatusNotFound {
						t.Fatalf("health API serves control route %s: %d", path, response.Code)
					}
				}
				httpServer := httptest.NewServer(server.Handler)
				t.Cleanup(httpServer.Close)
				baseURL := httpServer.URL
				client := &http.Client{Timeout: 5 * time.Second}
				t.Cleanup(client.CloseIdleConnections)
				readyStatus, readyBody := http.StatusOK, "ok\n"
				switch state {
				case "canceled":
					cancel()
					readyStatus, readyBody = http.StatusServiceUnavailable, "not ready\n"
				case "closed":
					clusterManager.Close()
					readyStatus, readyBody = http.StatusServiceUnavailable, "not ready\n"
				}
				for _, probe := range []struct {
					path   string
					status int
					body   string
				}{
					{"/livez", http.StatusOK, "ok\n"},
					{"/readyz", readyStatus, readyBody},
				} {
					// Neither missing nor invalid credentials may block a kubelet probe.
					for _, authorization := range []string{"", "Basic invalid"} {
						request, err := http.NewRequest(http.MethodGet, baseURL+probe.path, nil)
						if err != nil {
							t.Fatal(err)
						}
						request.Header.Set("Authorization", authorization)
						response, err := client.Do(request)
						if err != nil {
							t.Fatal(err)
						}
						body, err := io.ReadAll(response.Body)
						response.Body.Close()
						if err != nil {
							t.Fatal(err)
						}
						if response.StatusCode != probe.status || string(body) != probe.body {
							t.Fatalf("%s = %d %q, want %d %q", probe.path, response.StatusCode, body, probe.status, probe.body)
						}
						if response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("WWW-Authenticate") != "" {
							t.Fatalf("unexpected probe headers: %v", response.Header)
						}
					}
				}
			})
		}
	}
}

func TestHealthProbesRemainHealthyDuringConnectFailure(t *testing.T) {
	failedRequest := make(chan struct{}, 1)
	connect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		select {
		case failedRequest <- struct{}{}:
		default:
		}
	}))
	t.Cleanup(connect.Close)
	endpoint, err := url.Parse(connect.URL)
	if err != nil {
		t.Fatal(err)
	}
	configuration := config.DefaultConfiguration()
	configuration.PollingBehavior.Interval = config.Duration(5 * time.Millisecond)
	configurationManager := config.NewManager(configuration)
	clusterManager, err := poll.NewClusterManager(t.Context(), configurationManager,
		map[string]connectcluster.Configuration{"unavailable": {Host: endpoint.Hostname(), Port: endpoint.Port()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(clusterManager.Close)
	server := health.NewServer(clusterManager.Ready)
	select {
	case <-failedRequest:
	case <-time.After(5 * time.Second):
		t.Fatal("poller did not request Connect status")
	}
	for _, path := range []string{"/livez", "/readyz"} {
		response := httptest.NewRecorder()
		server.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s during Connect failure = %d", path, response.Code)
		}
	}
}
