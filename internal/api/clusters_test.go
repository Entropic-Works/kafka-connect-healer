package api

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll"
)

func TestClusterRoutesOverHTTP(t *testing.T) {
	configuration := config.DefaultConfiguration()
	configuration.PollingBehavior.Interval = config.Duration(time.Hour)
	server := newTestServer(t, config.NewManager(configuration))
	baseURL := startTestServer(t, server)
	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	clusterPath := "/clusters/" + url.PathEscape("production/eu west")
	for _, test := range []struct {
		method   string
		path     string
		body     string
		status   int
		location string
	}{
		{method: http.MethodGet, path: "/clusters", status: http.StatusOK},
		{method: http.MethodPut, path: clusterPath, body: `{"host":"localhost","port":"8083"}`, status: http.StatusCreated, location: clusterPath},
		{method: http.MethodPut, path: clusterPath, body: `{"host":"localhost","port":"8083"}`, status: http.StatusNoContent},
		{method: http.MethodPut, path: clusterPath + "/start", status: http.StatusNoContent},
		{method: http.MethodPut, path: clusterPath + "/stop", status: http.StatusNoContent},
		{method: http.MethodPut, path: clusterPath + "/stop", status: http.StatusNoContent},
		{method: http.MethodGet, path: "/clusters", status: http.StatusOK},
		{method: http.MethodPut, path: clusterPath, body: `{"host":"replacement.local","port":"8084"}`, status: http.StatusNoContent},
		{method: http.MethodPut, path: clusterPath + "/start", status: http.StatusNoContent},
		{method: http.MethodPut, path: clusterPath + "/start", status: http.StatusNoContent},
		{method: http.MethodPut, path: clusterPath + "/stop", status: http.StatusNoContent},
		{method: http.MethodDelete, path: clusterPath, status: http.StatusNoContent},
		{method: http.MethodDelete, path: clusterPath, status: http.StatusNotFound},
		{method: http.MethodPut, path: clusterPath + "/start", status: http.StatusNotFound},
		{method: http.MethodPut, path: clusterPath + "/stop", status: http.StatusNotFound},
		{method: http.MethodPost, path: clusterPath + "/start", status: http.StatusMethodNotAllowed},
		{method: http.MethodGet, path: clusterPath + "/stop", status: http.StatusMethodNotAllowed},
		{method: http.MethodPut, path: clusterPath + "/pause", status: http.StatusNotFound},
		{method: http.MethodPost, path: "/clusters", status: http.StatusMethodNotAllowed},
		{method: http.MethodGet, path: clusterPath, status: http.StatusMethodNotAllowed},
	} {
		request, err := http.NewRequest(test.method, baseURL+test.path, strings.NewReader(test.body))
		if err != nil {
			t.Fatal(err)
		}
		if test.body != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, readError := io.ReadAll(response.Body)
		response.Body.Close()
		if readError != nil {
			t.Fatal(readError)
		}
		if response.StatusCode != test.status || response.Header.Get("Location") != test.location {
			t.Fatalf("%s %s = %d, Location %q, body %s", test.method, test.path, response.StatusCode, response.Header.Get("Location"), body)
		}
		if test.method == http.MethodGet && test.status == http.StatusOK {
			var definitions map[string]connectcluster.Configuration
			if err := json.Unmarshal(body, &definitions); err != nil {
				t.Fatal(err)
			}
			if response.Header.Get("Cache-Control") != "no-store" {
				t.Fatal("cluster definitions can be cached")
			}
		}
	}
}

func TestClusterRouteAuthenticationPreventsChanges(t *testing.T) {
	for _, operation := range []struct{ method, path string }{
		{http.MethodGet, "/clusters"},
		{http.MethodPut, "/clusters/production"},
		{http.MethodDelete, "/clusters/production"},
		{http.MethodPut, "/clusters/production/start"},
		{http.MethodPut, "/clusters/production/stop"},
	} {
		t.Run(operation.method+" "+operation.path, func(t *testing.T) {
			configuration := config.DefaultConfiguration()
			configuration.PollingBehavior.Interval = config.Duration(time.Hour)
			configuration.APIConfig.AuthConfig = config.AuthConfiguration{Enabled: true, Username: "user", Password: "secret"}
			configurationManager := config.NewManager(configuration)
			clusterManager, err := poll.NewClusterManager(t.Context(), configurationManager,
				map[string]connectcluster.Configuration{"production": connectcluster.DefaultConfiguration()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(clusterManager.Close)
			server := NewServer(Components{ConfigurationManager: configurationManager, ClusterManager: clusterManager})
			for _, authenticated := range []bool{false, true} {
				request := httptest.NewRequest(operation.method, operation.path, strings.NewReader(`{"host":"replacement.local","port":"8084"}`))
				request.Header.Set("Content-Type", "application/json")
				if authenticated {
					request.SetBasicAuth("user", "secret")
				}
				response := httptest.NewRecorder()
				server.Handler.ServeHTTP(response, request)
				if !authenticated {
					if response.Code != http.StatusUnauthorized || clusterManager.GetClusters()["production"] != connectcluster.DefaultConfiguration() {
						t.Fatal("unauthenticated cluster request was allowed or changed configuration")
					}
				} else if response.Code != http.StatusOK && response.Code != http.StatusNoContent {
					t.Fatalf("authenticated %s = %d: %s", operation.method, response.Code, response.Body.String())
				}
			}
		})
	}
}
