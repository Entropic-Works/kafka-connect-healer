package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const collectorImage = "ghcr.io/open-telemetry/opentelemetry-collector-releases/opentelemetry-collector:0.163.1@sha256:f0e23036dce3db3f9cb80d907c75a0d034cd32e93511a1943482d46129b9ae9c"

const collectorYAML = `receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
extensions:
  health_check:
    endpoint: 0.0.0.0:13133
exporters:
  file/metrics:
    path: /telemetry/metrics.json
    flush_interval: 100ms
    append: true
  file/logs:
    path: /telemetry/logs.json
    flush_interval: 100ms
    append: true
service:
  extensions: [health_check]
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [file/metrics]
    logs:
      receivers: [otlp]
      exporters: [file/logs]
`

func (s *suite) startCollector(t *testing.T) *testcontainers.DockerContainer {
	t.Helper()
	metricsFile, logsFile := file("/telemetry/metrics.json", ""), file("/telemetry/logs.json", "")
	metricsFile.FileMode, logsFile.FileMode = 0o666, 0o666
	collector := runContainer(t, "collector", collectorImage,
		network.WithNetwork([]string{"collector"}, s.network),
		testcontainers.WithExposedPorts("13133/tcp"),
		testcontainers.WithFiles(file("/config/collector.yaml", collectorYAML), metricsFile, logsFile),
		testcontainers.WithCmd("--config=/config/collector.yaml"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/").WithPort("13133/tcp").WithStartupTimeout(30*time.Second)),
	)
	t.Cleanup(func() {
		for _, signal := range []string{"metrics", "logs"} {
			data := collectorOutput(t, collector, signal)
			name := strings.ReplaceAll(t.Name(), "/", "_") + "_" + collector.ID + "_" + signal + ".jsonl"
			if err := os.WriteFile(filepath.Join(artifactDirectory(), name), data, 0o600); err != nil {
				t.Errorf("save collector %s: %v", signal, err)
			}
		}
	})
	return collector
}

func telemetryEnvironment(service string) map[string]string {
	return map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318",
		"OTEL_SERVICE_NAME":           service,
		"OTEL_METRIC_EXPORT_INTERVAL": "200",
		"OTEL_BLRP_SCHEDULE_DELAY":    "200",
		"OTEL_EXPORTER_OTLP_TIMEOUT":  "1000",
	}
}

// These structures read the Collector's newline-delimited OTLP JSON output.
// int64 fields use JSON strings, as required by the OTLP JSON encoding.
type otlpValue struct {
	StringValue string
	IntValue    json.Number
}

type otlpAttribute struct {
	Key   string
	Value otlpValue
}

type otlpPoint struct {
	Attributes []otlpAttribute
	AsInt      json.Number
	Count      json.Number
}

type otlpMetric struct {
	Name      string
	Sum       struct{ DataPoints []otlpPoint }
	Histogram struct{ DataPoints []otlpPoint }
}

type otlpLog struct {
	Body       otlpValue
	Attributes []otlpAttribute
}

type otlpResource struct{ Attributes []otlpAttribute }

type otlpBatch struct {
	ResourceMetrics []struct {
		Resource     otlpResource
		ScopeMetrics []struct{ Metrics []otlpMetric }
	}
	ResourceLogs []struct {
		Resource  otlpResource
		ScopeLogs []struct{ LogRecords []otlpLog }
	}
}

func attributesMatch(attributes []otlpAttribute, expected map[string]string) bool {
	for key, want := range expected {
		found := false
		for _, attribute := range attributes {
			value := attribute.Value.StringValue
			if attribute.Value.IntValue != "" {
				value = attribute.Value.IntValue.String()
			}
			if attribute.Key == key && value == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (s *suite) telemetry(t *testing.T, signal string) []otlpBatch {
	t.Helper()
	data := collectorOutput(t, s.collector, signal)
	lines := bytes.Split(data, []byte("\n"))
	var batches []otlpBatch
	// A file can be copied while a new batch is being written. Only complete
	// newline-terminated records are inspected; the next read includes the rest.
	for _, line := range lines[:len(lines)-1] {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var batch otlpBatch
		if err := json.Unmarshal(line, &batch); err != nil {
			t.Fatalf("decode collector %s: %v", signal, err)
		}
		batches = append(batches, batch)
	}
	return batches
}

func collectorOutput(t *testing.T, collector *testcontainers.DockerContainer, signal string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	reader, err := collector.CopyFileFromContainer(ctx, "/telemetry/"+signal+".json")
	if errdefs.IsNotFound(err) {
		// The file exporter opens its file lazily after the first received batch.
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func (s *suite) metricValue(t *testing.T, name string, attributes map[string]string) int64 {
	t.Helper()
	var maximum int64
	for _, batch := range s.telemetry(t, "metrics") {
		for _, resource := range batch.ResourceMetrics {
			if !attributesMatch(resource.Resource.Attributes, map[string]string{"service.name": "kafka-connect-healer-integration"}) {
				t.Fatal("exported metric lost its configured service.name")
			}
			for _, scope := range resource.ScopeMetrics {
				for _, metric := range scope.Metrics {
					if metric.Name != name {
						continue
					}
					for _, point := range append(metric.Sum.DataPoints, metric.Histogram.DataPoints...) {
						if !attributesMatch(point.Attributes, attributes) {
							continue
						}
						value := point.AsInt
						if value == "" {
							value = point.Count
						}
						count, err := value.Int64()
						if err != nil {
							t.Fatal(err)
						}
						if count > maximum {
							maximum = count
						}
					}
				}
			}
		}
	}
	return maximum
}

func (s *suite) exportedLog(t *testing.T, service, message string, attributes map[string]string) bool {
	t.Helper()
	for _, batch := range s.telemetry(t, "logs") {
		for _, resource := range batch.ResourceLogs {
			if !attributesMatch(resource.Resource.Attributes, map[string]string{"service.name": service}) {
				continue
			}
			for _, scope := range resource.ScopeLogs {
				for _, record := range scope.LogRecords {
					if record.Body.StringValue == message && attributesMatch(record.Attributes, attributes) {
						return true
					}
				}
			}
		}
	}
	return false
}

func testTelemetry(t *testing.T, s *suite) {
	s.resetAfter(t)
	for _, message := range []string{"Application configuration initialized", "Connect clusters initialized"} {
		eventually(t, 10*time.Second, "structured startup summary exported", func() bool { return s.exportedLog(t, "kafka-connect-healer-integration", message, nil) })
	}
	configuration, _ := s.apiRequest(t, "GET", "/config", "", 200)
	var settings struct{ TelemetryConfig struct{ Enabled bool } }
	if err := json.Unmarshal(configuration, &settings); err != nil || !settings.TelemetryConfig.Enabled {
		t.Fatalf("integration application did not enable telemetry: %s (%v)", configuration, err)
	}
	s.patch(t, `{"pollingBehavior":{"restartFailedTasks":true}}`)
	s.apiRequest(t, "PUT", "/clusters/telemetry-secondary", clusterJSON("connect-secondary", "8443", connectPassword), 201)
	t.Cleanup(func() { s.apiRequest(t, "DELETE", "/clusters/telemetry-secondary", "", 204) })
	primary := s.connector(t, s.primary, 2)
	secondary := s.namedConnector(t, s.secondary, primary.name, 2)
	primary.marker(t, "fail-task-1", true)
	secondary.marker(t, "fail-task-0", true)
	for _, target := range []struct{ cluster, task string }{{"test", "1"}, {"telemetry-secondary", "0"}} {
		attributes := map[string]string{"connect_cluster": target.cluster, "connector": primary.name, "task_id": target.task, "action": "restart_task", "outcome": "accepted"}
		eventually(t, 20*time.Second, "accepted task metric for "+target.cluster, func() bool {
			return s.metricValue(t, "healer.restart.requests", attributes) > 0
		})
		for _, name := range []string{"healer.poll.cycles", "healer.poll.duration"} {
			eventually(t, 10*time.Second, name+" for "+target.cluster, func() bool {
				return s.metricValue(t, name, map[string]string{"connect_cluster": target.cluster, "outcome": "success"}) > 0
			})
		}
	}
	s.setMode(t, "restart-500")
	eventually(t, 10*time.Second, "failed restart request metric", func() bool {
		return s.metricValue(t, "healer.restart.requests", map[string]string{"connect_cluster": "test", "connector": primary.name, "task_id": "1", "action": "restart_task", "outcome": "error"}) > 0
	})
	s.setMode(t, "status-500")
	eventually(t, 10*time.Second, "failed cycle metric and exported failure log", func() bool {
		return s.metricValue(t, "healer.poll.cycles", map[string]string{"connect_cluster": "test", "outcome": "error"}) > 0 &&
			s.exportedLog(t, "kafka-connect-healer-integration", "poll cycle failed while retrieving connector statuses", map[string]string{"connect_cluster": "test"})
	})
	s.setMode(t, "pass")
	primary.marker(t, "fail-task-1", false)
	secondary.marker(t, "fail-task-0", false)
	primary.wait(t, "RUNNING", "RUNNING")
	secondary.wait(t, "RUNNING", "RUNNING")

	primary.marker(t, "fail-connector", true)
	code, data, _ := s.primary.request("POST", "/connectors/"+primary.name+"/restart?includeTasks=true&onlyFailed=false", nil)
	if code != 202 {
		t.Fatalf("induce connector failure: %d %s", code, data)
	}
	eventually(t, 20*time.Second, "accepted connector restart metric", func() bool {
		return s.metricValue(t, "healer.restart.requests", map[string]string{"connect_cluster": "test", "connector": primary.name, "action": "restart_connector", "outcome": "accepted"}) > 0
	})
	primary.marker(t, "fail-connector", false)
	primary.wait(t, "RUNNING", "RUNNING")
	for _, event := range []struct {
		message    string
		attributes map[string]string
	}{
		{"starting API server", map[string]string{"component": "api"}},
		{"starting health server", map[string]string{"component": "health"}},
		{"task restart request accepted", map[string]string{"connect_cluster": "test", "connector": primary.name, "task_id": "1"}},
	} {
		eventually(t, 10*time.Second, "exported "+event.message, func() bool {
			return s.exportedLog(t, "kafka-connect-healer-integration", event.message, event.attributes)
		})
	}

	t.Run("CollectorOutage", func(t *testing.T) {
		beforeCycles := s.metricValue(t, "healer.poll.cycles", map[string]string{"connect_cluster": "test", "outcome": "success"})
		timeout := 5 * time.Second
		if err := s.collector.Stop(t.Context(), &timeout); err != nil {
			t.Fatal(err)
		}
		restarted := false
		t.Cleanup(func() {
			if !restarted {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := s.collector.Start(ctx); err != nil {
					t.Errorf("restore collector: %v", err)
				}
			}
		})
		before := len(s.requests(t))
		eventually(t, 10*time.Second, "polling and local diagnostics during collector outage", func() bool {
			return len(s.requests(t)) >= before+4 && strings.Contains(logs(t, s.healer), "telemetry error")
		})
		for _, path := range []string{"/livez", "/readyz"} {
			code, _, _ := request(s.client, "GET", endpoint(t, s.healer, "8081/tcp", "http")+path, nil, "", "", "")
			if code != http.StatusOK {
				t.Fatalf("%s failed during telemetry outage: %d", path, code)
			}
		}
		s.apiRequest(t, "GET", "/config", "", 200)
		if err := s.collector.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		restarted = true
		s.apiRequest(t, "PUT", "/clusters/telemetry-restored", clusterJSON("connect-secondary", "8443", connectPassword), 201)
		s.apiRequest(t, "DELETE", "/clusters/telemetry-restored", "", 204)
		eventually(t, 20*time.Second, "metrics and logs resume after collector restart", func() bool {
			return s.metricValue(t, "healer.poll.cycles", map[string]string{"connect_cluster": "test", "outcome": "success"}) > beforeCycles &&
				s.exportedLog(t, "kafka-connect-healer-integration", "Connect cluster configured", map[string]string{"connect_cluster": "telemetry-restored"})
		})
	})

	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("Shutdown/enabled=%t", enabled), func(t *testing.T) {
			service := fmt.Sprintf("telemetry-shutdown-%t", enabled)
			configuration := applicationYAML + fmt.Sprintf("telemetryConfig:\n  enabled: %t\n", enabled)
			environment := telemetryEnvironment(service)
			// Long intervals ensure shutdown, rather than periodic export, flushes logs.
			environment["OTEL_BLRP_SCHEDULE_DELAY"] = "60000"
			app := s.startHealer(t, service, configuration, "{}", testcontainers.WithEnv(environment))
			timeout := 10 * time.Second
			if err := app.Stop(t.Context(), &timeout); err != nil {
				t.Fatal(err)
			}
			state, err := app.State(t.Context())
			if err != nil || state.ExitCode != 0 {
				t.Fatalf("shutdown did not exit cleanly: %v %+v", err, state)
			}
			if enabled {
				eventually(t, 5*time.Second, "shutdown flush reaches collector", func() bool {
					return s.exportedLog(t, service, "Kafka Connect Healer stopped", nil)
				})
			} else {
				for _, batch := range s.telemetry(t, "logs") {
					for _, resource := range batch.ResourceLogs {
						if attributesMatch(resource.Resource.Attributes, map[string]string{"service.name": service}) {
							t.Fatal("disabled telemetry exported logs")
						}
					}
				}
			}
		})
	}
}
