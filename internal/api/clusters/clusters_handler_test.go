package clusters

import (
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll"
)

func newTestHandler(t *testing.T) (*ClustersHandler, *poll.ClusterManager) {
	t.Helper()
	configuration := config.DefaultConfiguration()
	configuration.PollingBehavior.Interval = config.Duration(time.Hour)
	manager, err := poll.NewClusterManager(t.Context(), config.NewManager(configuration), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.Close)
	return NewClustersHandler(manager), manager
}

func putRequest(body string) *http.Request {
	request := httptest.NewRequest(http.MethodPut, "/clusters/production", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.SetPathValue("name", "production")
	return request
}

func TestGetClustersRedactsPasswordsWithoutChangingStoredValues(t *testing.T) {
	handler, manager := newTestHandler(t)
	configuration := connectcluster.DefaultConfiguration()
	configuration.HTTPS = true
	configuration.AuthConfig = connectcluster.AuthConfiguration{Enabled: true, Username: "user", Password: "secret"}
	if _, err := manager.PutCluster("production", configuration); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.GetClusters(response, httptest.NewRequest(http.MethodGet, "/clusters", nil))
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/json" ||
		response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("unexpected GET response: status %d, headers %v", response.Code, response.Header())
	}
	// Decode the configuration normally, then inspect the actual JSON for an
	// omitted password rather than merely a password with an empty value.
	var definitions map[string]connectcluster.Configuration
	if err := json.Unmarshal(response.Body.Bytes(), &definitions); err != nil {
		t.Fatal(err)
	}
	if len(definitions) != 1 || definitions["production"].AuthConfig.Password != "" ||
		strings.Contains(response.Body.String(), `"password"`) || strings.Contains(response.Body.String(), "secret") {
		t.Fatal("GET exposed the cluster password")
	}
	if manager.GetClusters()["production"] != configuration {
		t.Fatal("GET modified stored cluster configuration")
	}
}

func TestGetClustersReturnsEmptyObject(t *testing.T) {
	handler, _ := newTestHandler(t)
	response := httptest.NewRecorder()
	handler.GetClusters(response, httptest.NewRequest(http.MethodGet, "/clusters", nil))
	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != "{}" {
		t.Fatalf("GET empty clusters = %d %q", response.Code, response.Body.String())
	}
}

func TestPutClusterCreatesAndCompletelyReplacesDefinition(t *testing.T) {
	handler, manager := newTestHandler(t)
	response := httptest.NewRecorder()
	request := putRequest(`{"host":"localhost","port":"8083","https":false,"authConfig":{"enabled":true,"username":"user","password":"secret"}}`)
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	handler.PutCluster(response, request)
	if response.Code != http.StatusCreated || response.Header().Get("Location") != "/clusters/production" {
		t.Fatalf("create = %d, Location = %q, body = %s", response.Code, response.Header().Get("Location"), response.Body.String())
	}
	created := manager.GetClusters()["production"]
	if created.HTTPS || !created.AuthConfig.Enabled || created.AuthConfig.Username != "user" || created.AuthConfig.Password != "secret" {
		t.Fatal("HTTP cluster did not retain enabled authentication and credentials")
	}
	response = httptest.NewRecorder()
	handler.PutCluster(response, putRequest(`{"host":"replacement.local","port":"8084"}`))
	if response.Code != http.StatusNoContent || response.Body.Len() != 0 || response.Header().Get("Location") != "" {
		t.Fatalf("replace = %d %q", response.Code, response.Body.String())
	}
	expected := connectcluster.Configuration{Host: "replacement.local", Port: "8084"}
	if manager.GetClusters()["production"] != expected {
		t.Fatal("PUT retained omitted values from the previous definition")
	}
}

func TestPutClusterRejectsInvalidDefinitionsWithoutChangingStoredValues(t *testing.T) {
	for _, body := range []string{
		"", "null", "[]", "{", "{} {}", "{} true", "{}", `{"host":"localhost"}`,
		`{"host":"http://localhost","port":"8083"}`,
		`{"host":"localhost","port":"0"}`,
		`{"host":"localhost","port":8083}`,
		`{"host":"localhost","port":"8083","unknown":"private-secret"}`,
		`{"host":"localhost","host":"other","port":"8083"}`,
		`{"host":"localhost","\u0068ost":"other","port":"8083"}`,
		`{"host":"localhost","port":"8083","https":null}`,
		`{"host":"localhost","port":"8083","authConfig":{"password":null}}`,
		`{"host":"localhost","port":"8083","authConfig":{"password":[]}}`,
		`{"host":"localhost","port":"8083","authConfig":{"password":"one","password":"two"}}`,
		`{"host":"localhost","port":"8083","authConfig":{"enabled":true,"password":"private-secret"}}`,
		`{"host":"localhost","port":"8083","authConfig":{"enabled":true,"username":"user"}}`,
		`{"host":"localhost","port":"8083","https":true,"authConfig":{"enabled":true,"username":"user"}}`,
		"{\"host\":\"\xff\",\"port\":\"8083\"}",
	} {
		t.Run(body, func(t *testing.T) {
			handler, manager := newTestHandler(t)
			original := connectcluster.DefaultConfiguration()
			if _, err := manager.PutCluster("production", original); err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			handler.PutCluster(response, putRequest(body))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body = %s", response.Code, response.Body.String())
			}
			if len(manager.GetClusters()) != 1 || manager.GetClusters()["production"] != original {
				t.Fatal("invalid PUT changed existing cluster configuration")
			}
			if strings.Contains(response.Body.String(), "private-secret") {
				t.Fatal("error response exposed supplied credentials")
			}
		})
	}
}

func TestPutClusterContentTypeAndSizeLimits(t *testing.T) {
	for _, test := range []struct {
		name        string
		contentType string
		body        string
		status      int
	}{
		{name: "missing", body: "{}", status: http.StatusUnsupportedMediaType},
		{name: "unsupported", contentType: "text/plain", body: "{}", status: http.StatusUnsupportedMediaType},
		{name: "malformed", contentType: "application/json; invalid", body: "{}", status: http.StatusUnsupportedMediaType},
		{name: "oversized", contentType: "application/json", body: strings.Repeat(" ", 64<<10) + "{}", status: http.StatusRequestEntityTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, manager := newTestHandler(t)
			request := putRequest(test.body)
			request.Header.Set("Content-Type", test.contentType)
			response := httptest.NewRecorder()
			handler.PutCluster(response, request)
			if response.Code != test.status || len(manager.GetClusters()) != 0 {
				t.Fatalf("status = %d, want %d; invalid request created a cluster", response.Code, test.status)
			}
		})
	}
}

type failingBody struct {
	err    error
	closed bool
}

func (body *failingBody) Read([]byte) (int, error) { return 0, body.err }
func (body *failingBody) Close() error             { body.closed = true; return nil }

type readTimeoutError struct{}

func (readTimeoutError) Error() string   { return "read timed out" }
func (readTimeoutError) Timeout() bool   { return true }
func (readTimeoutError) Temporary() bool { return false }

func TestPutClusterReadErrorsCloseBodyAndDoNotCreateCluster(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		status int
	}{
		{name: "read error", err: errors.New("private-secret"), status: http.StatusBadRequest},
		{name: "timeout", err: readTimeoutError{}, status: http.StatusRequestTimeout},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, manager := newTestHandler(t)
			body := &failingBody{err: test.err}
			request := putRequest("")
			request.Body = body
			response := httptest.NewRecorder()
			handler.PutCluster(response, request)
			if response.Code != test.status || !body.closed || len(manager.GetClusters()) != 0 ||
				strings.Contains(response.Body.String(), "private-secret") {
				t.Fatalf("unsafe read-error handling: status %d, closed %t", response.Code, body.closed)
			}
		})
	}
}

func TestDeleteClusterAndClosedManagerResponses(t *testing.T) {
	handler, manager := newTestHandler(t)
	if _, err := manager.PutCluster("production", connectcluster.DefaultConfiguration()); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodDelete, "/clusters/production", nil)
	request.SetPathValue("name", "production")
	for _, status := range []int{http.StatusNoContent, http.StatusNotFound} {
		response := httptest.NewRecorder()
		handler.DeleteCluster(response, request)
		if response.Code != status || len(manager.GetClusters()) != 0 {
			t.Fatalf("DELETE = %d, want %d", response.Code, status)
		}
	}
	manager.Close()
	response := httptest.NewRecorder()
	handler.DeleteCluster(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("DELETE closed manager = %d, want 503", response.Code)
	}
	response = httptest.NewRecorder()
	handler.PutCluster(response, putRequest(`{"host":"localhost","port":"8083"}`))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("PUT closed manager = %d, want 503", response.Code)
	}
}
