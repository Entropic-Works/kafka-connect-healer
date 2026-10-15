package integration

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func testAPI(t *testing.T, s *suite) {
	s.resetAfter(t)
	t.Run("separate health API", func(t *testing.T) {
		healthURL := endpoint(t, s.healer, "8081/tcp", "http")
		for _, path := range []string{"/livez", "/readyz"} {
			code, body, _ := request(s.client, http.MethodGet, healthURL+path, nil, "", "", "")
			if code != http.StatusOK || string(body) != "ok\n" {
				t.Fatalf("health %s: %d %s", path, code, body)
			}
			s.apiRequest(t, http.MethodGet, path, "", http.StatusNotFound)
		}
		for _, path := range []string{"/", "/config", "/clusters"} {
			code, body, _ := request(s.client, http.MethodGet, healthURL+path, nil, "", "", "")
			if code != http.StatusNotFound {
				t.Fatalf("health listener exposes %s: %d %s", path, code, body)
			}
		}
	})
	t.Run("authentication", func(t *testing.T) {
		for _, operation := range []struct{ method, path string }{
			{"GET", "/"}, {"GET", "/config"}, {"GET", "/clusters"}, {"GET", "/missing"},
			{"PATCH", "/config"}, {"PUT", "/clusters/denied"}, {"DELETE", "/clusters/test"}, {"POST", "/config"},
		} {
			for _, auth := range []string{"", "Basic not-base64", "Bearer token", "Basic Og==", "Basic " + base64.StdEncoding.EncodeToString([]byte("wrong:wrong"))} {
				req, err := http.NewRequest(operation.method, s.api+operation.path, strings.NewReader("{}"))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Authorization", auth)
				req.Header.Set("Content-Type", "application/json")
				response, err := s.client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if response.StatusCode != http.StatusUnauthorized || !strings.Contains(response.Header.Get("WWW-Authenticate"), "Basic") {
					t.Fatalf("%s %s with %q: expected Basic challenge and 401, got %d", operation.method, operation.path, auth, response.StatusCode)
				}
			}
		}
		data, _ := s.apiRequest(t, "GET", "/clusters", "", 200)
		if strings.Contains(string(data), "denied") || !strings.Contains(string(data), `"test"`) {
			t.Fatalf("unauthorized request mutated clusters: %s", data)
		}
	})
	t.Run("routes_and_redaction", func(t *testing.T) {
		data, _ := s.apiRequest(t, "GET", "/", "", 200)
		var routes []struct{ Method, Path string }
		if err := json.Unmarshal(data, &routes); err != nil {
			t.Fatal(err)
		}
		for _, route := range []string{"GET /config", "PATCH /config", "GET /clusters", "PUT /clusters/{name}", "DELETE /clusters/{name}"} {
			found := false
			for _, entry := range routes {
				if entry.Method+" "+entry.Path == route {
					found = true
				}
			}
			if !found {
				t.Errorf("home response missing %s: %s", route, data)
			}
		}
		for _, path := range []string{"/config", "/clusters"} {
			data, headers := s.apiRequest(t, "GET", path, "", 200)
			if headers.Get("Cache-Control") != "no-store" || headers.Get("Content-Type") != "application/json" {
				t.Errorf("%s response headers: %v", path, headers)
			}
			if bytes.Contains(data, []byte("password")) || bytes.Contains(data, []byte(apiPassword)) || bytes.Contains(data, []byte(connectPassword)) {
				t.Errorf("%s exposes a password", path)
			}
		}
		for _, operation := range []struct {
			method, path string
			code         int
		}{
			{"GET", "/missing", 404}, {"POST", "/config", 405}, {"POST", "/clusters", 405}, {"DELETE", "/clusters/missing", 404},
		} {
			s.apiRequest(t, operation.method, operation.path, "{}", operation.code)
		}
	})
	t.Run("atomic_patch_validation", func(t *testing.T) {
		before, _ := s.apiRequest(t, "GET", "/config", "", 200)
		cases := []struct{ name, body string }{
			{"empty", ""}, {"array", "[]"}, {"null", "null"}, {"unknown", `{"unknown":1}`},
			{"nested_null", `{"pollingBehavior":null}`}, {"duplicate", `{"pollingBehavior":{"interval":"1s","interval":"2s"}}`},
			{"case_duplicate", `{"pollingBehavior":{"interval":"1s","Interval":"2s"}}`},
			{"zero_interval", `{"pollingBehavior":{"interval":"0s"}}`}, {"negative_interval", `{"pollingBehavior":{"interval":"-1s"}}`},
			{"numeric_duration", `{"pollingBehavior":{"interval":1000}}`}, {"zero_timeout", `{"communicationConfig":{"requestTimeout":"0s"}}`},
			{"zero_base", `{"pollingBehavior":{"backoff":{"baseDelay":"0s"}}}`}, {"max_below_base", `{"pollingBehavior":{"backoff":{"maxDelay":"500ms"}}}`},
			{"wrong_bool", `{"pollingBehavior":{"restartFailedTasks":"yes"}}`}, {"bad_level", `{"loggingConfig":{"level":"NOT-A-LEVEL"}}`},
			{"startup_auth", `{"apiConfig":{"authConfig":{"enabled":false}}}`},
			{"startup_username", `{"apiConfig":{"authConfig":{"username":"changed"}}}`},
			{"startup_password", `{"apiConfig":{"authConfig":{"password":"must-not-leak"}}}`},
			{"secret_in_error", `{"apiConfig":{"authConfig":{"password":"must-not-leak"},"unknown":1}}`},
			{"trailing_json", "{} {}"}, {"bad_utf8", "{\"loggingConfig\":{\"level\":\"\xff\"}}"},
			{"partial_invalid", `{"pollingBehavior":{"restartFailedTasks":true},"communicationConfig":{"requestTimeout":"0s"}}`},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				data, _ := s.apiRequest(t, "PATCH", "/config", tc.body, 400)
				if bytes.Contains(data, []byte("must-not-leak")) {
					t.Fatal("validation error exposes secret")
				}
				after, _ := s.apiRequest(t, "GET", "/config", "", 200)
				if !bytes.Equal(before, after) {
					t.Fatalf("invalid patch mutated config: %s", after)
				}
			})
		}
		s.apiRequest(t, "PATCH", "/config", "{}", 204)
		after, _ := s.apiRequest(t, "GET", "/config", "", 200)
		if !bytes.Equal(before, after) {
			t.Fatal("empty patch changed configuration")
		}
		code, data, _ := request(s.client, "PATCH", s.api+"/config", []byte(`{"PollingBehavior":{"Interval":"750ms"}}`), apiUsername, apiPassword, "application/json; charset=utf-8")
		if code != 204 {
			t.Fatalf("case-insensitive names/content-type parameters: %d %s", code, data)
		}
		after, _ = s.apiRequest(t, "GET", "/config", "", 200)
		var config map[string]any
		if err := json.Unmarshal(after, &config); err != nil {
			t.Fatal(err)
		}
		polling := config["pollingBehavior"].(map[string]any)
		if polling["interval"] != "750ms" || polling["restartFailedTasks"] != false {
			t.Fatalf("partial patch lost omitted fields/duration strings: %s", after)
		}
	})
	t.Run("cluster_validation", func(t *testing.T) {
		for index, body := range []string{
			"", "{}", "[]", "null", `{"host":"connect","port":"8443","unknown":true}`,
			`{"host":"connect","host":"connect","port":"8443"}`, `{"host":"connect","port":8443}`,
			`{"host":"https://connect","port":"8443"}`, `{"host":"connect","port":"0"}`, `{"host":"connect","port":"65536"}`,
			`{"host":"connect","port":"8443","https":null}`,
			`{"host":"connect","port":"8443","authConfig":{"enabled":true,"username":"u","password":"must-not-leak"}}`,
			`{"host":"connect","port":"8443","https":true,"authConfig":{"enabled":true,"username":"u"}}`,
			`{"host":"connect","port":"8443"} {}`, "{\"host\":\"\xff\",\"port\":\"8443\"}",
		} {
			t.Run(fmt.Sprint(index), func(t *testing.T) {
				data, _ := s.apiRequest(t, "PUT", "/clusters/rejected", body, 400)
				if bytes.Contains(data, []byte("must-not-leak")) {
					t.Fatal("validation error exposes secret")
				}
			})
		}
		data, _ := s.apiRequest(t, "GET", "/clusters", "", 200)
		if bytes.Contains(data, []byte("rejected")) {
			t.Fatal("invalid PUT created a cluster")
		}
		path := "/clusters/" + url.PathEscape("name with space")
		_, headers := s.apiRequest(t, "PUT", path, clusterJSON("connect", "8443", connectPassword), 201)
		t.Cleanup(func() { s.apiRequest(t, "DELETE", path, "", 204) })
		if headers.Get("Location") != path {
			t.Fatalf("incorrect created Location: %q", headers.Get("Location"))
		}
	})
	t.Run("body_limits_and_media_type", func(t *testing.T) {
		for _, operation := range []struct{ method, path string }{{"PATCH", "/config"}, {"PUT", "/clusters/rejected"}} {
			for _, contentType := range []string{"", "text/plain"} {
				code, _, _ := request(s.client, operation.method, s.api+operation.path, []byte("{}"), apiUsername, apiPassword, contentType)
				if code != 415 {
					t.Errorf("%s content-type %q: got %d", operation.method, contentType, code)
				}
			}
			s.apiRequest(t, operation.method, operation.path, strings.Repeat(" ", (64<<10)+1), 413)
		}
	})
	t.Run("slow_request_bodies", func(t *testing.T) {
		parsed, err := url.Parse(s.api)
		if err != nil {
			t.Fatal(err)
		}
		for _, operation := range []struct{ method, path string }{{"PATCH", "/config"}, {"PUT", "/clusters/slow"}} {
			t.Run(operation.method, func(t *testing.T) {
				connection, err := net.DialTimeout("tcp", parsed.Host, 5*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				defer connection.Close()
				if err := connection.SetDeadline(time.Now().Add(12 * time.Second)); err != nil {
					t.Fatal(err)
				}
				_, err = fmt.Fprintf(connection, "%s %s HTTP/1.1\r\nHost: %s\r\nAuthorization: Basic %s\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{", operation.method, operation.path, parsed.Host, base64.StdEncoding.EncodeToString([]byte(apiUsername+":"+apiPassword)))
				if err != nil {
					t.Fatal(err)
				}
				s.apiRequest(t, "GET", "/config", "", 200) // A stalled request must not monopolize the server.
				response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: operation.method})
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				if response.StatusCode != 408 {
					t.Fatalf("slow body got %d, want 408", response.StatusCode)
				}
			})
		}
	})
}
