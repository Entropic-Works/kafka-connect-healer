package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

func testHTTPAuthentication(t *testing.T, s *suite) {
	// Use the secondary Connect cluster so the suite's primary Healer cannot
	// restart these fixtures and mask a failure of authenticated HTTP requests.
	f := s.connector(t, s.secondary, 1)
	configuration := strings.Replace(applicationYAML, "restartFailedTasks: false", "restartFailedTasks: true", 1)
	app := s.startHealer(t, "http-auth", configuration,
		"test:\n  host: connect-secondary\n  port: \"8083\"\n  tls:\n    https: false\n  authConfig:\n    enabled: true\n    username: "+connectUsername+"\n",
		testcontainers.WithFiles(file("/config/cluster-secret.yaml", "test:\n  authConfig:\n    password: "+connectPassword+"\n")),
		testcontainers.WithCmd("--config=/config/application.yaml", "--connect-clusters=/config/clusters.yaml", "--connect-clusters-secret-config=/config/cluster-secret.yaml"))
	address := endpoint(t, app, "8080/tcp", "http")
	apiRequest := func(method, path, body string, expected int) []byte {
		t.Helper()
		code, data, _ := request(s.client, method, address+path, []byte(body), apiUsername, apiPassword, "application/json")
		if code != expected {
			t.Fatalf("%s %s: got %d, want %d: %s", method, path, code, expected, data)
		}
		return data
	}
	definition := func(https, auth bool, username, password string) string {
		port := "8083"
		if https {
			port = "8443"
		}
		data, err := json.Marshal(map[string]any{
			"host": "connect-secondary", "port": port, "tls": map[string]any{"https": https},
			"authConfig": map[string]any{"enabled": auth, "username": username, "password": password},
		})
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	warnings := func(want int) {
		t.Helper()
		output := logs(t, app)
		const message = "Connect Basic Auth is enabled over HTTP; credentials are sent without transport encryption"
		if got := countMatchingLogs(output, message); got != want {
			t.Fatalf("authenticated HTTP warnings = %d, want %d", got, want)
		}
		for _, line := range strings.Split(output, "\n") {
			if strings.Contains(line, message) && (!matchingLog(line, "level=WARN", "connect_cluster=test", "endpoint=http://connect-secondary:8083") || strings.Contains(line, connectUsername)) {
				t.Fatalf("warning lost cluster/endpoint context or exposed the username: %s", line)
			}
		}
		for _, secret := range []string{connectPassword, apiPassword, "wrong-http-password"} {
			if strings.Contains(output, secret) {
				t.Fatal("application logs exposed a password")
			}
		}
	}
	waitForPolls := func(endpoint string, previous int) {
		t.Helper()
		eventually(t, 10*time.Second, "successful polling at "+endpoint, func() bool {
			return countMatchingLogs(logs(t, app), `msg="poll cycle completed"`, "endpoint="+endpoint, "outcome=success") >= previous+2
		})
	}
	const httpEndpoint = "http://connect-secondary:8083"
	const httpsEndpoint = "https://connect-secondary:8443"
	waitForPolls(httpEndpoint, 0)
	warnings(1)
	before := apiRequest("GET", "/clusters", "", 200)
	if !bytes.Contains(before, []byte(`"enabled":true`)) || !bytes.Contains(before, []byte(`"https":false`)) || bytes.Contains(before, []byte(`"password"`)) {
		t.Fatalf("startup overlay did not retain authenticated HTTP or redact the password: %s", before)
	}

	// Identical and invalid PUTs must preserve the active worker and its settings.
	started := countMatchingLogs(logs(t, app), `msg="polling started"`)
	apiRequest("PUT", "/clusters/test", definition(false, true, connectUsername, connectPassword), 204)
	for _, credentials := range [][2]string{{"", connectPassword}, {" ", connectPassword}, {"team:user", connectPassword}, {connectUsername, ""}} {
		apiRequest("PUT", "/clusters/test", definition(false, true, credentials[0], credentials[1]), 400)
		if !bytes.Equal(apiRequest("GET", "/clusters", "", 200), before) {
			t.Fatal("invalid HTTP authentication update changed the stored definition")
		}
	}
	if countMatchingLogs(logs(t, app), `msg="polling started"`) != started {
		t.Fatal("identical or invalid PUT replaced the worker")
	}
	warnings(1)

	f.marker(t, "fail-task", true)
	f.wait(t, "RUNNING", "FAILED")
	eventually(t, 10*time.Second, "real task restart over authenticated HTTP", func() bool { return len(f.starts(t, "task")) >= 2 })
	f.marker(t, "fail-task", false)
	f.wait(t, "RUNNING", "RUNNING")
	eventually(t, 5*time.Second, "observed HTTP task recovery", func() bool {
		return matchingLog(logs(t, app), `msg="task recovered"`, "connector="+f.name, "endpoint="+httpEndpoint)
	})
	if !matchingLog(logs(t, app), `msg="task restart request accepted"`, "connector="+f.name, "endpoint="+httpEndpoint) {
		t.Fatal("task restart was not accepted over HTTP")
	}

	// A transport warning must never turn off authentication or bypass a 401.
	apiRequest("PUT", "/clusters/test", definition(false, true, connectUsername, "wrong-http-password"), 204)
	eventually(t, 10*time.Second, "wrong HTTP credentials rejected by Connect", func() bool {
		return matchingLog(logs(t, app), "endpoint="+httpEndpoint, "401 Unauthorized")
	})
	if !bytes.Contains(apiRequest("GET", "/clusters", "", 200), []byte(`"enabled":true`)) {
		t.Fatal("authentication was silently disabled after a 401")
	}
	f.marker(t, "fail-task", true)
	f.wait(t, "RUNNING", "FAILED")
	attempts := len(f.starts(t, "task"))
	remains(t, 1200*time.Millisecond, "wrong HTTP credentials restarted a task", func() bool { return len(f.starts(t, "task")) == attempts })
	warnings(2)
	offset := len(logs(t, app))
	apiRequest("PUT", "/clusters/test", definition(false, false, "", ""), 204)
	eventually(t, 10*time.Second, "unauthenticated HTTP rejected by Connect", func() bool {
		return matchingLog(logs(t, app)[offset:], "endpoint="+httpEndpoint, "401 Unauthorized")
	})
	warnings(2)

	f.marker(t, "fail-task", false)
	apiRequest("PUT", "/clusters/test", definition(true, true, connectUsername, connectPassword), 204)
	f.wait(t, "RUNNING", "RUNNING")
	waitForPolls(httpsEndpoint, 0)
	warnings(2)
	previous := countMatchingLogs(logs(t, app), `msg="poll cycle completed"`, "endpoint="+httpEndpoint, "outcome=success")
	apiRequest("PUT", "/clusters/test", definition(false, true, connectUsername, connectPassword), 204)
	waitForPolls(httpEndpoint, previous)
	warnings(3)

	connectorStarts := len(f.starts(t, "connector"))
	f.marker(t, "fail-connector", true)
	code, data, _ := f.worker.request(http.MethodPost, "/connectors/"+f.name+"/restart?includeTasks=true&onlyFailed=false", nil)
	if code != http.StatusAccepted {
		t.Fatalf("induce connector failure: %d %s", code, data)
	}
	eventually(t, 15*time.Second, "real connector restart over authenticated HTTP", func() bool {
		return len(f.starts(t, "connector")) >= connectorStarts+2 &&
			matchingLog(logs(t, app), `msg="connector restart request accepted"`, "connector="+f.name, "endpoint="+httpEndpoint)
	})
	f.marker(t, "fail-connector", false)
	f.wait(t, "RUNNING", "RUNNING")
	eventually(t, 5*time.Second, "observed HTTP connector recovery", func() bool {
		return matchingLog(logs(t, app), `msg="connector recovered"`, "connector="+f.name, "endpoint="+httpEndpoint)
	})
	warnings(3)
}
