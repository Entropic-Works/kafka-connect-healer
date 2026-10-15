package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func testStartup(t *testing.T, s *suite) {
	t.Run("DefaultsAndAPIOnly", func(t *testing.T) {
		app := s.startHealer(t, "api-only", "{}", "{}", testcontainers.WithCmd("--connect-clusters=/config/clusters.yaml"))
		address := endpoint(t, app, "8080/tcp", "http")
		eventually(t, 10*time.Second, "unauthenticated API ready", func() bool {
			code, _, _ := request(s.client, "GET", address+"/config", nil, "", "", "")
			return code == 200
		})
		code, data, _ := request(s.client, "GET", address+"/clusters", nil, "", "", "")
		if code != 200 || strings.TrimSpace(string(data)) != "{}" {
			t.Fatalf("explicit empty clusters should disable polling: %d %s", code, data)
		}
		code, data, _ = request(s.client, "GET", address+"/config", nil, "", "", "")
		if code != 200 || !strings.Contains(string(data), `"interval":"10s"`) || !strings.Contains(string(data), `"restartFailedTasks":true`) {
			t.Fatalf("defaults were not loaded: %d %s", code, data)
		}
		if strings.Contains(logs(t, app), "msg=\"polling started\"") {
			t.Fatal("API-only application started a poller")
		}
	})
	t.Run("OmittedFilesUseDefaultCluster", func(t *testing.T) {
		app := s.startHealer(t, "default-cluster", "{}", "{}", testcontainers.WithCmd())
		address := endpoint(t, app, "8080/tcp", "http")
		eventually(t, 10*time.Second, "default cluster visible", func() bool {
			code, data, _ := request(s.client, "GET", address+"/clusters", nil, "", "", "")
			return code == 200 && strings.Contains(string(data), `"default"`) && strings.Contains(string(data), `"host":"localhost"`)
		})
	})
	t.Run("SecretOverlays", func(t *testing.T) {
		base := strings.ReplaceAll(applicationYAML, "    username: healer-test\n    password: healer-test-only\n", "")
		clusterBase := strings.ReplaceAll(clustersYAML("8444"), "    password: connect-test-only\n", "")
		app := s.startHealer(t, "overlay", base, clusterBase,
			testcontainers.WithFiles(file("/config/secret.yaml", "apiConfig:\n  authConfig:\n    username: healer-test\n    password: healer-test-only\npollingBehavior:\n  backoff:\n    exponential: false\n"),
				file("/config/cluster-secret.yaml", "test:\n  authConfig:\n    password: connect-test-only\n")),
			testcontainers.WithCmd("--config=/config/application.yaml", "--secret-config=/config/secret.yaml", "--connect-clusters=/config/clusters.yaml", "--connect-clusters-secret-config=/config/cluster-secret.yaml"),
		)
		address := endpoint(t, app, "8080/tcp", "http")
		eventually(t, 10*time.Second, "overlay API ready", func() bool {
			code, _, _ := request(s.client, "GET", address+"/config", nil, apiUsername, apiPassword, "")
			return code == 200
		})
		code, data, _ := request(s.client, "GET", address+"/config", nil, apiUsername, apiPassword, "")
		if code != 200 || !strings.Contains(string(data), `"exponential":false`) || strings.Contains(string(data), apiPassword) {
			t.Fatalf("overlay did not preserve explicit false/redaction: %d %s", code, data)
		}
		f := s.connector(t, s.primary, 1)
		f.marker(t, "fail-connector", true)
		code, data, _ = s.primary.request("POST", "/connectors/"+f.name+"/restart?includeTasks=true&onlyFailed=false", nil)
		if code != 202 {
			t.Fatalf("induce connector failure: %d %s", code, data)
		}
		eventually(t, 10*time.Second, "merged Connect credentials used for remediation", func() bool {
			return matchingLog(logs(t, app), "connector="+f.name, "connector restart request accepted")
		})
		// Stop this additional poller before deleting its fixture.
		timeout := 5 * time.Second
		if err := app.Stop(t.Context(), &timeout); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("InvalidStartupExits", func(t *testing.T) {
		for _, tc := range []struct {
			name, config, clusters string
			args                   []string
		}{
			{"invalid_application", "pollingBehavior:\n  interval: 0s\n", "{}", []string{"--config=/config/application.yaml"}},
			{"unknown_yaml", "unknown: true\n", "{}", []string{"--config=/config/application.yaml"}},
			{"duplicate_yaml", "pollingBehavior:\n  interval: 1s\n  interval: 2s\n", "{}", []string{"--config=/config/application.yaml"}},
			{"missing_application", "{}", "{}", []string{"--config=/config/missing.yaml"}},
			{"missing_secret", "{}", "{}", []string{"--secret-config=/config/missing.yaml"}},
			{"invalid_cluster", "{}", "test:\n  host: https://connect\n", []string{"--connect-clusters=/config/clusters.yaml"}},
			{"missing_cluster_secret", "{}", "{}", []string{"--connect-clusters-secret-config=/config/missing.yaml"}},
			{"positional_argument", "{}", "{}", []string{"unexpected"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				app := runContainer(t, tc.name, image("HEALER_TEST_IMAGE", defaultHealerImage),
					testcontainers.WithFiles(file("/config/application.yaml", tc.config), file("/config/clusters.yaml", tc.clusters)),
					testcontainers.WithCmd(tc.args...), testcontainers.WithWaitStrategy(wait.ForExit().WithExitTimeout(15*time.Second)))
				state, err := app.State(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if state.ExitCode != 1 || state.Running {
					t.Fatalf("bad startup should exit 1, got %+v", state)
				}
				// Errors before logging.Configure use slog's default text format.
				if !strings.Contains(logs(t, app), "ERROR") || strings.Contains(logs(t, app), "starting API server") {
					t.Fatal("invalid configuration did not fail before startup")
				}
			})
		}
	})
	t.Run("APIFailureDoesNotStopPolling", func(t *testing.T) {
		// Sharing only this suite's Healer network namespace makes both HTTP ports
		// busy, without occupying host ports or touching another deployment.
		config := strings.Replace(applicationYAML, "restartFailedTasks: false", "restartFailedTasks: true", 1)
		app := runContainer(t, "api-bind-failure", image("HEALER_TEST_IMAGE", defaultHealerImage),
			testcontainers.WithHostConfigModifier(func(host *container.HostConfig) { host.NetworkMode = container.NetworkMode("container:" + s.healer.ID) }),
			testcontainers.WithFiles(file("/config/application.yaml", config), file("/config/clusters.yaml", clustersYAML("8444")), file("/config/ca.crt", string(s.ca))),
			testcontainers.WithEnv(map[string]string{"SSL_CERT_FILE": "/config/ca.crt"}),
			testcontainers.WithCmd("--config=/config/application.yaml", "--connect-clusters=/config/clusters.yaml"),
			testcontainers.WithWaitStrategy(wait.ForLog("API serving failed; polling will continue").WithStartupTimeout(20*time.Second)))
		f := s.connector(t, s.primary, 1)
		f.marker(t, "fail-task", true)
		eventually(t, 10*time.Second, "polling despite bind failure", func() bool { return len(f.starts(t, "task")) >= 3 })
		state, err := app.State(t.Context())
		if err != nil || !state.Running {
			t.Fatalf("API failure terminated application: %v %+v", err, state)
		}
	})
	t.Run("UntrustedConnectCertificate", func(t *testing.T) {
		app := s.startHealer(t, "untrusted-ca", applicationYAML, clustersYAML("8444"),
			testcontainers.WithEnv(map[string]string{"SSL_CERT_FILE": "/config/untrusted-ca.crt"}),
			testcontainers.WithFiles(file("/config/untrusted-ca.crt", "")))
		eventually(t, 10*time.Second, "untrusted certificate rejected", func() bool {
			return matchingLog(logs(t, app), "connect_cluster=test", "certificate signed by unknown authority")
		})
	})
	t.Run("SIGTERMAndRuntimeChangesAreEphemeral", func(t *testing.T) {
		s.resetAfter(t)
		s.patch(t, `{"pollingBehavior":{"restartFailedTasks":true},"communicationConfig":{"requestTimeout":"5s"}}`)
		s.apiRequest(t, "PUT", "/clusters/ephemeral", clusterJSON("connect-secondary", "8443", connectPassword), 201)
		s.setMode(t, "status-delay")
		started := time.Now()
		eventually(t, 8*time.Second, "in-flight slow outbound request", func() bool {
			for _, r := range s.requests(t) {
				if r.mode == "status-delay" && r.at.After(started) {
					return true
				}
			}
			return false
		})
		timeout := 10 * time.Second
		if err := s.healer.Stop(t.Context(), &timeout); err != nil {
			t.Fatal(err)
		}
		// Always restart before resetAfter and other suite cleanups use the API.
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			state, err := s.healer.State(ctx)
			if err == nil && !state.Running {
				if err := s.healer.Start(ctx); err != nil {
					t.Errorf("restore healer: %v", err)
				}
				address, err := s.healer.PortEndpoint(ctx, "8080/tcp", "http")
				if err != nil {
					t.Errorf("restore healer endpoint: %v", err)
				} else {
					s.api = address
				}
			}
		})
		state, err := s.healer.State(t.Context())
		if err != nil || state.Running || state.ExitCode != 0 {
			t.Fatalf("SIGTERM did not exit cleanly: %v %+v", err, state)
		}
		output := logs(t, s.healer)
		if !strings.Contains(output, "API graceful shutdown completed") || !strings.Contains(output, "Kafka Connect Healer stopped") {
			t.Fatal("missing graceful shutdown logs")
		}
		s.setMode(t, "pass")
		if err := s.healer.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		// Docker may allocate a different random host port on restart.
		s.api = endpoint(t, s.healer, "8080/tcp", "http")
		eventually(t, 15*time.Second, "Healer restarted with startup configuration", func() bool {
			code, data, _ := request(s.client, "GET", s.api+"/config", nil, apiUsername, apiPassword, "")
			return code == 200 && strings.Contains(string(data), `"restartFailedTasks":false`) && strings.Contains(string(data), `"requestTimeout":"2s"`)
		})
		data, _ := s.apiRequest(t, "GET", "/clusters", "", 200)
		var clusters map[string]json.RawMessage
		if err := json.Unmarshal(data, &clusters); err != nil {
			t.Fatal(err)
		}
		if len(clusters) != 1 || clusters["test"] == nil {
			t.Fatalf("runtime clusters persisted unexpectedly: %s", data)
		}
	})
}
