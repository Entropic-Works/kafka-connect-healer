package poll

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll/actions"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll/status"
)

func TestClusterManagerWarnsAboutInsecureConnectionsWhenApplied(t *testing.T) {
	for _, test := range []struct {
		name       string
		https      bool
		auth       bool
		skipVerify bool
		warnings   int
	}{
		{name: "authenticated HTTP", auth: true, warnings: 2},
		{name: "authenticated HTTPS", https: true, auth: true},
		{name: "unauthenticated HTTP"},
		{name: "unauthenticated HTTPS", https: true},
		{name: "unverified authenticated HTTPS", https: true, auth: true, skipVerify: true, warnings: 2},
		{name: "unverified unauthenticated HTTPS", https: true, skipVerify: true, warnings: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelWarn})))
			t.Cleanup(func() { slog.SetDefault(previous) })
			cluster := connectcluster.DefaultConfiguration()
			cluster.TLS = connectcluster.TLSConfiguration{HTTPS: test.https, InsecureSkipVerify: test.skipVerify}
			cluster.AuthConfig = connectcluster.AuthConfiguration{Enabled: test.auth, Username: "private-user", Password: "private-password"}
			configuration := config.DefaultConfiguration()
			configuration.PollingBehavior.Interval = config.Duration(time.Hour)
			manager, err := NewClusterManager(t.Context(), config.NewManager(configuration), map[string]connectcluster.Configuration{"production": cluster})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(manager.Close)
			if _, err := manager.PutCluster("production", cluster); err != nil {
				t.Fatal(err)
			}
			invalid := cluster
			invalid.Port = "0"
			if _, err := manager.PutCluster("production", invalid); err == nil {
				t.Fatal("invalid replacement was accepted")
			}
			other := connectcluster.DefaultConfiguration()
			other.Host = "staging.local"
			if _, err := manager.PutCluster("staging", other); err != nil {
				t.Fatal(err)
			}
			cluster.Host = "replacement.local"
			if _, err := manager.PutCluster("production", cluster); err != nil {
				t.Fatal(err)
			}
			manager.Close()
			if strings.Contains(output.String(), "private-user") || strings.Contains(output.String(), "private-password") {
				t.Fatal("warning exposed credentials")
			}
			lines := strings.FieldsFunc(output.String(), func(character rune) bool { return character == '\n' })
			if len(lines) != test.warnings {
				t.Fatalf("warning count = %d, want %d; logs: %s", len(lines), test.warnings, output.String())
			}
			for i, line := range lines {
				var event map[string]any
				if err := json.Unmarshal([]byte(line), &event); err != nil {
					t.Fatal(err)
				}
				host := "localhost"
				if i == 1 {
					host = cluster.Host
				}
				scheme := "http"
				message := "Connect Basic Auth is enabled over HTTP; credentials are sent without transport encryption"
				if test.skipVerify {
					scheme = "https"
					message = "Connect TLS certificate verification is disabled; server certificate chain and hostname are not verified"
				}
				if event["level"] != "WARN" || event["connect_cluster"] != "production" ||
					event["endpoint"] != scheme+"://"+net.JoinHostPort(host, cluster.Port) ||
					event["msg"] != message {
					t.Fatalf("unexpected warning: %v", event)
				}
			}
		})
	}
}

func TestRecoveryLogsFollowObservedFailures(t *testing.T) {
	snapshot := func(connectorState, taskState string) map[string]status.ConnectorStatus {
		return map[string]status.ConnectorStatus{"shared": {
			Name: "shared", Connector: status.WorkerStatus{State: connectorState},
			Tasks: []status.TaskStatus{{ID: 7, State: taskState}},
		}}
	}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil)).With("connect_cluster", "test", "endpoint", "https://connect:8443")
	failures := make(map[actions.RemediationAction]struct{})
	for _, step := range []struct {
		statuses map[string]status.ConnectorStatus
		failures int
		logs     int
	}{
		{snapshot("RUNNING", "RUNNING"), 0, 0},
		{snapshot("FAILED", "FAILED"), 2, 0},
		{snapshot("PAUSED", "UNASSIGNED"), 2, 0},
		{snapshot("running", "running"), 0, 2},
		{snapshot("RUNNING", "RUNNING"), 0, 0},
		{snapshot("RUNNING", "FAILED"), 1, 0},
		{snapshot("RUNNING", "RUNNING"), 0, 1},
		{snapshot("FAILED", "FAILED"), 2, 0},
		{map[string]status.ConnectorStatus{}, 0, 0},
		{snapshot("RUNNING", "RUNNING"), 0, 0},
	} {
		output.Reset()
		failures = logRecoveries(failures, step.statuses, logger)
		if len(failures) != step.failures {
			t.Fatalf("tracked failures = %v, want %d", failures, step.failures)
		}
		lines := strings.FieldsFunc(output.String(), func(character rune) bool { return character == '\n' })
		if len(lines) != step.logs {
			t.Fatalf("recovery logs = %s, want %d events", output.String(), step.logs)
		}
		for _, line := range lines {
			var event map[string]any
			if err := json.Unmarshal([]byte(line), &event); err != nil {
				t.Fatal(err)
			}
			if event["level"] != "INFO" || event["connect_cluster"] != "test" || event["endpoint"] != "https://connect:8443" || event["connector"] != "shared" {
				t.Fatalf("recovery lost target context: %v", event)
			}
			if event["msg"] == "task recovered" {
				if event["task_id"] != float64(7) || event["action"] != "restart_task" {
					t.Fatalf("task recovery = %v", event)
				}
			} else if event["msg"] != "connector recovered" || event["action"] != "restart_connector" || event["task_id"] != nil {
				t.Fatalf("connector recovery = %v", event)
			}
		}
	}
}

func TestPollLogsOneRestartOutcome(t *testing.T) {
	for _, test := range []struct {
		code           int
		level, message string
	}{
		{204, "INFO", "task restart request accepted"},
		{401, "ERROR", "remediation request failed"},
		{409, "WARN", "remediation request conflicted"},
		{500, "ERROR", "remediation request failed"},
	} {
		t.Run(http.StatusText(test.code), func(t *testing.T) {
			var output bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })
			gets := make(chan struct{}, 32)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.Method == http.MethodPost {
					w.WriteHeader(test.code)
					return
				}
				io.WriteString(w, `{"shared":{"status":{"name":"shared","connector":{"state":"RUNNING"},"tasks":[{"id":7,"state":"FAILED"}]}}}`)
				gets <- struct{}{}
			}))
			defer server.Close()
			address, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			configuration := config.DefaultConfiguration()
			configuration.PollingBehavior.Interval = config.Duration(5 * time.Millisecond)
			configuration.PollingBehavior.RestartFailedTasks = true
			configuration.PollingBehavior.Backoff.BaseDelay = config.Duration(time.Hour)
			configuration.PollingBehavior.Backoff.MaxDelay = config.Duration(time.Hour)
			ctx, cancel := context.WithCancel(t.Context())
			finished := make(chan struct{})
			go func() {
				NewConnectClusterPoller("test", connectcluster.Configuration{Host: address.Hostname(), Port: address.Port()}).Poll(ctx, config.NewManager(configuration))
				close(finished)
			}()
			t.Cleanup(func() { cancel(); <-finished })
			for range 2 {
				select {
				case <-gets:
				case <-time.After(5 * time.Second):
					t.Fatal("poll cycle did not complete")
				}
			}
			cancel()
			<-finished
			starts, outcomes := 0, 0
			for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
				var event map[string]any
				if err := json.Unmarshal([]byte(line), &event); err != nil {
					t.Fatal(err)
				}
				if event["msg"] == "requesting task restart" {
					starts++
					continue
				}
				if event["msg"] == "restart attempt recorded" {
					t.Fatal("duplicate bookkeeping log")
				}
				if event["msg"] != test.message {
					continue
				}
				outcomes++
				if event["level"] != test.level || event["connect_cluster"] != "test" || event["endpoint"] != server.URL || event["connector"] != "shared" || event["action"] != "restart_task" || event["task_id"] != float64(7) || event["status_code"] != float64(test.code) || event["attempt_count"] != float64(1) || event["attempted_at"] == nil || event["request_attempted"] != true || event["duration"].(float64) <= 0 {
					t.Fatalf("outcome = %v", event)
				}
			}
			if starts != 1 || outcomes != 1 {
				t.Fatalf("starts = %d, outcomes = %d; logs: %s", starts, outcomes, output.String())
			}
		})
	}
}
