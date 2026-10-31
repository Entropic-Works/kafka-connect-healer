package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
	logsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

func TestExampleConfigurations(t *testing.T) {
	for _, test := range []struct {
		name         string
		application  string
		clusters     string
		secret       string
		clusterCount int
		authEnabled  bool
	}{
		{name: "local", application: "application.yaml", clusters: "connect-clusters.yaml", clusterCount: 1},
		{name: "API only", application: "application.yaml", clusters: "empty-clusters.yaml"},
		{name: "Kubernetes", application: "kubernetes/application.yaml", clusters: "kubernetes/connect-clusters.yaml", clusterCount: 1},
		{name: "authenticated smoke test", application: "application.yaml", clusters: "empty-clusters.yaml", secret: "../../tests/fixtures/docker/application-secret.yaml", authEnabled: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			application, err := config.LoadFiles(filepath.Join("../../examples", test.application), test.secret)
			if err != nil {
				t.Fatalf("load application example: %v", err)
			}
			if application.APIConfig.AuthConfig.Enabled != test.authEnabled {
				t.Fatalf("API auth enabled = %v, want %v", application.APIConfig.AuthConfig.Enabled, test.authEnabled)
			}
			clusters, err := connectcluster.LoadFiles(filepath.Join("../../examples", test.clusters), "")
			if err != nil {
				t.Fatalf("load cluster example: %v", err)
			}
			if len(clusters) != test.clusterCount {
				t.Fatalf("cluster count = %d, want %d", len(clusters), test.clusterCount)
			}
		})
	}
}

// Run the real main in a child process so flags, signals, and os.Exit stay isolated.
func TestMainProcess(t *testing.T) {
	if os.Getenv("KCH_TEST_MAIN_PROCESS") != "1" {
		return
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing child-process argument separator")
	}
	os.Args = append([]string{os.Args[0]}, os.Args[separator+1:]...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	main()
	os.Exit(0)
}

func mainProcess(ctx context.Context, arguments ...string) *exec.Cmd {
	arguments = append([]string{"-test.run=^TestMainProcess$", "--"}, arguments...)
	command := exec.CommandContext(ctx, os.Args[0], arguments...)
	command.Env = append(os.Environ(), "KCH_TEST_MAIN_PROCESS=1")
	return command
}

func TestMainStartupAndShutdown(t *testing.T) {
	emptyClusters := filepath.Join(t.TempDir(), "clusters.yaml")
	if err := os.WriteFile(emptyClusters, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name              string
		arguments         []string
		want              string
		healthUnavailable bool
		telemetry         bool
	}{
		{name: "defaults", want: "polling started"},
		{name: "explicit empty clusters", arguments: []string{"--connect-clusters=" + emptyClusters}, want: "no Connect clusters configured; polling disabled"},
		{name: "telemetry without pollers", arguments: []string{"--connect-clusters=" + emptyClusters}, want: "no Connect clusters configured; polling disabled", telemetry: true},
		{name: "health API bind failure", arguments: []string{"--connect-clusters=" + emptyClusters}, want: "health serving failed; polling will continue", healthUnavailable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			arguments := slices.Clone(test.arguments)
			var exportedLogs sync.Map
			if test.telemetry {
				application := filepath.Join(t.TempDir(), "application.yaml")
				if err := os.WriteFile(application, []byte("telemetryConfig:\n  enabled: true\n"), 0600); err != nil {
					t.Fatal(err)
				}
				arguments = append(arguments, "--config="+application)
				receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
					body, err := io.ReadAll(request.Body)
					if err != nil {
						t.Error(err)
						return
					}
					if request.URL.Path == "/v1/logs" {
						var payload logsv1.ExportLogsServiceRequest
						if err := proto.Unmarshal(body, &payload); err != nil {
							t.Error(err)
							return
						}
						for _, resource := range payload.ResourceLogs {
							for _, scope := range resource.ScopeLogs {
								for _, record := range scope.LogRecords {
									exportedLogs.Store(record.Body.GetStringValue(), true)
								}
							}
						}
					}
					w.Header().Set("Content-Type", "application/x-protobuf")
				}))
				t.Cleanup(receiver.Close)
				t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
				t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", receiver.URL+"/v1/metrics")
				t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", receiver.URL+"/v1/logs")
			}
			if test.healthUnavailable {
				listener, err := net.Listen("tcp", ":8081")
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := mainProcess(ctx, arguments...)
			output, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			command.Stderr = command.Stdout
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			t.Cleanup(func() {
				cancel()
				if !waited {
					command.Wait()
				}
			})
			ready := make(chan struct{}, 1)
			finishedOutput := make(chan struct{})
			var logs strings.Builder
			go func() {
				defer close(finishedOutput)
				scanner := bufio.NewScanner(output)
				for scanner.Scan() {
					line := scanner.Text()
					logs.WriteString(line + "\n")
					if strings.Contains(line, "starting API server") {
						ready <- struct{}{}
					}
				}
			}()
			select {
			case <-ready:
			case <-finishedOutput:
				t.Fatalf("application exited before API startup: %s", logs.String())
			case <-ctx.Done():
				t.Fatal("application did not start")
			}
			client := &http.Client{Transport: &http.Transport{}, Timeout: time.Second}
			t.Cleanup(client.CloseIdleConnections)
			if !test.healthUnavailable {
				waitForHTTPStatus(t, ctx, client, "http://127.0.0.1:8081/readyz", http.StatusOK)
				waitForHTTPStatus(t, ctx, client, "http://127.0.0.1:8081/livez", http.StatusOK)
			}
			waitForHTTPStatus(t, ctx, client, "http://127.0.0.1:8080/config", http.StatusOK)
			// Release probe connections before asserting graceful server shutdown.
			client.CloseIdleConnections()
			if err := command.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			<-finishedOutput
			err = command.Wait()
			waited = true
			if err != nil {
				t.Fatalf("application did not shut down cleanly: %v\n%s", err, logs.String())
			}
			if !strings.Contains(logs.String(), test.want) || !strings.Contains(logs.String(), "Kafka Connect Healer stopped") {
				t.Fatalf("missing expected startup or shutdown logs: %s", logs.String())
			}
			for _, header := range []string{" INFO  Application configuration initialized\n", " INFO  Connect clusters initialized\n"} {
				if strings.Count(logs.String(), header) != 1 {
					t.Errorf("expected one %q: %s", header, logs.String())
				}
			}
			if test.name == "explicit empty clusters" && strings.Contains(logs.String(), "polling started") {
				t.Fatalf("empty cluster file started a poller: %s", logs.String())
			}
			for _, message := range []string{"API graceful shutdown completed", "health graceful shutdown completed"} {
				if !strings.Contains(logs.String(), message) {
					t.Fatalf("missing %q: %s", message, logs.String())
				}
			}
			if test.telemetry {
				for _, message := range []string{test.want, "Application configuration initialized", "Connect clusters initialized", "starting API server", "starting health server", "API graceful shutdown completed", "health graceful shutdown completed", "Kafka Connect Healer stopped"} {
					if _, ok := exportedLogs.Load(message); !ok {
						t.Errorf("application log %q was not exported", message)
					}
				}
			}
		})
	}
}

func TestMainStopsOnAPIBindFailure(t *testing.T) {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	output, err := mainProcess(ctx).CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("application did not stop after API bind failure: %v\n%s", ctx.Err(), output)
	}
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 {
		t.Fatalf("API bind failure = %v, want exit code 1; logs: %s", err, output)
	}
	for _, message := range []string{
		"API serving failed; stopping application", "polling stopped",
		"API graceful shutdown completed", "health graceful shutdown completed",
		"Kafka Connect Healer stopped",
	} {
		if !strings.Contains(string(output), message) {
			t.Fatalf("missing %q: %s", message, output)
		}
	}
}

func waitForHTTPStatus(t *testing.T, ctx context.Context, client *http.Client, address string, status int) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err == nil {
			_, readError := io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if readError != nil {
				t.Fatalf("read %s response: %v", address, readError)
			}
			if response.StatusCode == status {
				return
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("%s did not return %d: %v", address, status, ctx.Err())
		}
	}
}

func TestWaitForHTTPStatusReusesConnection(t *testing.T) {
	var requests, connections atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, "ok\n")
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)
	client := &http.Client{Transport: &http.Transport{}, Timeout: time.Second}
	t.Cleanup(client.CloseIdleConnections)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	waitForHTTPStatus(t, ctx, client, server.URL+"/readyz", http.StatusOK)
	waitForHTTPStatus(t, ctx, client, server.URL+"/livez", http.StatusOK)
	if got := connections.Load(); got != 1 {
		t.Fatalf("probe requests opened %d connections, want one reused connection", got)
	}
}

func TestMainRejectsInvalidSuppliedFiles(t *testing.T) {
	invalid := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(invalid, []byte("unexpectedField: true"), 0600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	for _, arguments := range [][]string{
		{"--config=" + invalid}, {"--config=" + missing},
		{"--connect-clusters=" + invalid}, {"--connect-clusters=" + missing},
	} {
		t.Run(arguments[0], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			output, err := mainProcess(ctx, arguments...).CombinedOutput()
			if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 {
				t.Fatalf("startup error = %v, want exit code 1; logs: %s", err, output)
			}
			if !strings.Contains(string(output), "configuration loading failed") || strings.Contains(string(output), "starting API server") || strings.Contains(string(output), "polling started") {
				t.Fatalf("invalid supplied file did not fail before starting workers: %s", output)
			}
		})
	}
}
