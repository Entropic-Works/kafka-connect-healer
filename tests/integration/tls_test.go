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

func testTLSVerification(t *testing.T, s *suite) {
	for _, test := range []struct {
		name, host, failure string
		trusted             bool
	}{
		{name: "UntrustedCertificate", host: "connect-secondary", failure: "certificate signed by unknown authority"},
		{name: "HostnameMismatch", host: "connect-secondary-wrong-host", trusted: true, failure: "not connect-secondary-wrong-host"},
		{name: "TrustedCertificate", host: "connect-secondary", trusted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// The primary suite poller cannot remediate this secondary cluster.
			f := s.connector(t, s.secondary, 1)
			configuration := strings.Replace(applicationYAML, "restartFailedTasks: false", "restartFailedTasks: true", 1)
			clusters := "test:\n  host: " + test.host + "\n  port: \"8443\"\n  tls:\n    https: true\n  authConfig:\n    enabled: true\n    username: " + connectUsername + "\n    password: " + connectPassword + "\n"
			var options []testcontainers.ContainerCustomizer
			if test.failure != "" {
				options = append(options,
					testcontainers.WithFiles(file("/config/cluster-secret.yaml", "test:\n  tls:\n    insecureSkipVerify: true\n")),
					testcontainers.WithCmd("--config=/config/application.yaml", "--connect-clusters=/config/clusters.yaml", "--connect-clusters-secret-config=/config/cluster-secret.yaml"))
			}
			if !test.trusted {
				options = append(options,
					testcontainers.WithEnv(map[string]string{"SSL_CERT_FILE": "/config/untrusted-ca.crt", "SSL_CERT_DIR": "/config/no-cert-directory"}),
					testcontainers.WithFiles(file("/config/untrusted-ca.crt", "")))
			}
			app := s.startHealer(t, "tls-verification", configuration, clusters, options...)
			address := endpoint(t, app, "8080/tcp", "http")
			apiRequest := func(method, path, body string, expected int) []byte {
				t.Helper()
				code, data, _ := request(s.client, method, address+path, []byte(body), apiUsername, apiPassword, "application/json")
				if code != expected {
					t.Fatalf("%s %s: got %d, want %d: %s", method, path, code, expected, data)
				}
				return data
			}
			const warning = "Connect TLS certificate verification is disabled; server certificate chain and hostname are not verified"
			warnings := func(want int) {
				t.Helper()
				output := logs(t, app)
				if got := countMatchingLogs(output, warning); got != want {
					t.Fatalf("TLS verification warnings = %d, want %d", got, want)
				}
				for _, line := range strings.Split(output, "\n") {
					if strings.Contains(line, warning) && (!matchingLog(line, "level=WARN", "connect_cluster=test", "endpoint=https://"+test.host+":8443") || strings.Contains(line, connectUsername)) {
						t.Fatal("TLS warning lost cluster/endpoint context or exposed the username")
					}
				}
				if strings.Contains(output, connectPassword) || strings.Contains(output, apiPassword) {
					t.Fatal("TLS logs exposed credentials")
				}
			}
			waitForPolls := func(previous int) {
				t.Helper()
				eventually(t, 10*time.Second, "successful TLS polling", func() bool {
					return countMatchingLogs(logs(t, app), "connect_cluster=test", `msg="poll cycle completed"`, "outcome=success") >= previous+2
				})
			}
			waitForPolls(0)
			if test.failure == "" {
				warnings(0)
				return
			}
			warnings(1)
			data := apiRequest("GET", "/clusters", "", 200)
			var definitions map[string]map[string]json.RawMessage
			if err := json.Unmarshal(data, &definitions); err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(definitions["test"]["tls"], []byte(`"insecureSkipVerify":true`)) || bytes.Contains(data, []byte(`"password"`)) {
				t.Fatalf("TLS startup overlay did not preserve TLS settings or redact the password: %s", data)
			}

			// A second cluster must still reject the same certificate while the
			// first cluster polls successfully with its explicit bypass.
			strict := clusterJSON(test.host, "8443", connectPassword)
			bypass := clusterTLSJSON(test.host, "8443", connectPassword, true)
			apiRequest("PUT", "/clusters/verified", strict, 201)
			eventually(t, 10*time.Second, "strict cluster remains isolated", func() bool {
				return matchingLog(logs(t, app), "connect_cluster=verified", test.failure)
			})
			previous := countMatchingLogs(logs(t, app), "connect_cluster=test", `msg="poll cycle completed"`, "outcome=success")
			waitForPolls(previous)
			started := countMatchingLogs(logs(t, app), "connect_cluster=test", `msg="polling started"`)
			apiRequest("PUT", "/clusters/test", bypass, 204)
			invalid := strings.Replace(bypass, `"https":true`, `"https":false`, 1)
			apiRequest("PUT", "/clusters/test", invalid, 400)
			if countMatchingLogs(logs(t, app), "connect_cluster=test", `msg="polling started"`) != started {
				t.Fatal("identical or invalid TLS PUT replaced the worker")
			}
			warnings(1)

			apiRequest("PUT", "/clusters/test", strict, 204)
			offset := len(logs(t, app))
			eventually(t, 10*time.Second, "omitted bypass restores certificate verification", func() bool {
				return matchingLog(logs(t, app)[offset:], "connect_cluster=test", test.failure)
			})
			f.marker(t, "fail-task", true)
			f.wait(t, "RUNNING", "FAILED")
			taskStarts := len(f.starts(t, "task"))
			remains(t, 1200*time.Millisecond, "strict TLS failure still restarted a task", func() bool { return len(f.starts(t, "task")) == taskStarts })
			apiRequest("PUT", "/clusters/test", bypass, 204)
			eventually(t, 10*time.Second, "bypassed TLS permits a real task restart", func() bool { return len(f.starts(t, "task")) > taskStarts })
			f.marker(t, "fail-task", false)
			f.wait(t, "RUNNING", "RUNNING")
			eventually(t, 5*time.Second, "task recovery observed after TLS bypass", func() bool {
				return matchingLog(logs(t, app), "connect_cluster=test", "connector="+f.name, `msg="task recovered"`)
			})
			warnings(2)

			apiRequest("PUT", "/clusters/test", strict, 204)
			connectorStarts := len(f.starts(t, "connector"))
			f.marker(t, "fail-connector", true)
			code, data, _ := f.worker.request(http.MethodPost, "/connectors/"+f.name+"/restart?includeTasks=true&onlyFailed=false", nil)
			if code != http.StatusAccepted {
				t.Fatalf("induce connector failure: %d %s", code, data)
			}
			apiRequest("PUT", "/clusters/test", bypass, 204)
			eventually(t, 15*time.Second, "bypassed TLS permits a real connector restart", func() bool {
				return len(f.starts(t, "connector")) >= connectorStarts+2 &&
					matchingLog(logs(t, app), "connect_cluster=test", "connector="+f.name, `msg="connector restart request accepted"`)
			})
			f.marker(t, "fail-connector", false)
			f.wait(t, "RUNNING", "RUNNING")
			eventually(t, 5*time.Second, "connector recovery observed after TLS bypass", func() bool {
				return matchingLog(logs(t, app), "connect_cluster=test", "connector="+f.name, `msg="connector recovered"`)
			})
			if matchingLog(logs(t, app), "connect_cluster=verified", `msg="poll cycle completed"`, "outcome=success") {
				t.Fatal("strict cluster inherited another cluster's TLS bypass")
			}
			warnings(3)
		})
	}
}
