package telemetry_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/logging"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll/actions"
	"github.com/Entropic-Works/kafka-connect-healer/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	logsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
)

func TestOTLPExportAndLogFiltering(t *testing.T) {
	var mutex sync.Mutex
	metricsSeen := make(map[string]bool)
	var messages []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer test-token" || request.Header.Get("Content-Type") != "application/x-protobuf" {
			t.Error("missing configured authentication or protobuf content type")
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			return
		}
		mutex.Lock()
		defer mutex.Unlock()
		switch request.URL.Path {
		case "/v1/metrics":
			var payload metricsv1.ExportMetricsServiceRequest
			if err := proto.Unmarshal(body, &payload); err != nil {
				t.Error(err)
			}
			for _, resource := range payload.ResourceMetrics {
				for _, scope := range resource.ScopeMetrics {
					for _, metric := range scope.Metrics {
						metricsSeen[metric.Name] = true
					}
				}
			}
		case "/v1/logs":
			var payload logsv1.ExportLogsServiceRequest
			if err := proto.Unmarshal(body, &payload); err != nil {
				t.Error(err)
			}
			for _, resource := range payload.ResourceLogs {
				for _, scope := range resource.ScopeLogs {
					for _, record := range scope.LogRecords {
						messages = append(messages, record.Body.GetStringValue())
						foundCluster := false
						for _, attribute := range record.Attributes {
							if attribute.Key == "connect_cluster" && attribute.Value.GetStringValue() == "production" {
								foundCluster = true
							}
						}
						if (record.Body.GetStringValue() == "accepted" || record.Body.GetStringValue() == "enabled debug") && !foundCluster {
							t.Error("polling log lost its cluster attribute")
						}
					}
				}
			}
		default:
			t.Errorf("unexpected export path %s", request.URL.Path)
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(server.Close)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=Bearer%20test-token")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", server.URL+"/v1/metrics")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", server.URL+"/v1/logs")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "Authorization=Bearer%20test-token")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_HEADERS", "Authorization=Bearer%20test-token")
	previousMeter, previousLogs := otel.GetMeterProvider(), global.GetLoggerProvider()
	previousLogger, previousErrors := slog.Default(), otel.GetErrorHandler()
	t.Cleanup(func() {
		otel.SetMeterProvider(previousMeter)
		global.SetLoggerProvider(previousLogs)
		slog.SetDefault(previousLogger)
		otel.SetErrorHandler(previousErrors)
	})
	var console bytes.Buffer
	level := new(slog.LevelVar)
	base := slog.New(slog.NewTextHandler(&console, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(base)
	shutdown, err := telemetry.Start(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	metrics, err := telemetry.NewPollMetrics("production")
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordCycle(t.Context(), time.Now(), "success")
	metrics.RecordRestart(t.Context(), actions.RemediationAction{ConnectorName: "orders", Kind: actions.RestartConnector}, "accepted")
	slog.SetDefault(logging.WithTelemetry(base))
	slog.Info("application started")
	slog.With("component", "api").Info("API started")
	slog.With("component", "health").Info("health started")
	log.Print("standard log")
	logger := slog.With("connect_cluster", "production")
	logger.Debug("filtered debug")
	logger.Info("accepted")
	level.Set(slog.LevelDebug)
	logger.WithGroup("request").With("connector", "orders").Debug("enabled debug")
	base.Info("console only")
	otel.Handle(errors.New("export failed diagnostic"))
	slog.Info("application stopped")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := otel.GetMeterProvider().(*sdkmetric.MeterProvider).ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := global.GetLoggerProvider().(*sdklog.LoggerProvider).ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	mutex.Lock()
	defer mutex.Unlock()
	for _, name := range []string{"healer.poll.cycles", "healer.poll.duration", "healer.restart.requests"} {
		if !metricsSeen[name] {
			t.Errorf("metric %s was not exported", name)
		}
	}
	wantMessages := []string{"application started", "API started", "health started", "standard log", "accepted", "enabled debug", "application stopped"}
	if strings.Join(messages, "|") != strings.Join(wantMessages, "|") {
		t.Fatalf("exported log messages = %v", messages)
	}
	if bytes.Contains(console.Bytes(), []byte("filtered debug")) || !bytes.Contains(console.Bytes(), []byte("console only")) || !bytes.Contains(console.Bytes(), []byte("enabled debug")) || !bytes.Contains(console.Bytes(), []byte("export failed diagnostic")) {
		t.Fatalf("console logging did not retain its level and output: %s", console.String())
	}
}

func TestMetricsSeparateClustersConnectorsAndTasks(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		provider.Shutdown(context.Background())
	})
	for _, cluster := range []string{"east", "west"} {
		metrics, err := telemetry.NewPollMetrics(cluster)
		if err != nil {
			t.Fatal(err)
		}
		metrics.RecordCycle(t.Context(), time.Now(), "success")
		for _, connector := range []string{"orders", "payments"} {
			metrics.RecordRestart(t.Context(), actions.RemediationAction{ConnectorName: connector, Kind: actions.RestartConnector}, "accepted")
			for _, taskID := range []int{0, 7} {
				metrics.RecordRestart(t.Context(), actions.RemediationAction{ConnectorName: connector, Kind: actions.RestartTask, TaskID: taskID}, "accepted")
			}
		}
	}
	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]int)
	for _, scope := range data.ScopeMetrics {
		for _, metric := range scope.Metrics {
			switch metric.Name {
			case "healer.poll.cycles":
				points := metric.Data.(metricdata.Sum[int64]).DataPoints
				seen[metric.Name] = len(points)
				for _, point := range points {
					if point.Value != 1 || point.Attributes.Len() != 2 {
						t.Fatalf("cycle metrics should only separate clusters and outcomes: %v", point)
					}
				}
			case "healer.poll.duration":
				seen[metric.Name] = len(metric.Data.(metricdata.Histogram[float64]).DataPoints)
			case "healer.restart.requests":
				points := metric.Data.(metricdata.Sum[int64]).DataPoints
				seen[metric.Name] = len(points)
				for _, point := range points {
					if point.Value != 1 {
						t.Fatalf("restart targets were aggregated together: %v", point)
					}
					action, _ := point.Attributes.Value("action")
					taskID, hasTaskID := point.Attributes.Value("task_id")
					if hasTaskID != (action.AsString() == "restart_task") || (hasTaskID && taskID.AsInt64() != 0 && taskID.AsInt64() != 7) {
						t.Fatalf("unexpected task ID attribute: %v", point)
					}
				}
			default:
				t.Errorf("unexpected metric %s", metric.Name)
			}
		}
	}
	if seen["healer.poll.cycles"] != 2 || seen["healer.poll.duration"] != 2 || seen["healer.restart.requests"] != 12 {
		t.Fatalf("metric series counts = %v", seen)
	}
}

func TestInvalidResourceConfigurationFailsStartup(t *testing.T) {
	previousErrors := otel.GetErrorHandler()
	t.Cleanup(func() { otel.SetErrorHandler(previousErrors) })
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "missing-equals-sign")
	shutdown, err := telemetry.Start(t.Context())
	if err == nil || shutdown != nil || !strings.Contains(err.Error(), "create telemetry resource") {
		t.Fatalf("Start() = (%v, %v), want resource initialization error", shutdown != nil, err)
	}
}

func TestInvalidExporterEnvironmentUsesSDKFallback(t *testing.T) {
	previousMeter, previousLogs := otel.GetMeterProvider(), global.GetLoggerProvider()
	previousLogger, previousErrors := slog.Default(), otel.GetErrorHandler()
	t.Cleanup(func() {
		otel.SetMeterProvider(previousMeter)
		global.SetLoggerProvider(previousLogs)
		slog.SetDefault(previousLogger)
		otel.SetErrorHandler(previousErrors)
	})
	var console bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&console, nil)))
	var exports atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		exports.Add(1)
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(server.Close)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "://invalid")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "://invalid")
	shutdown, err := telemetry.Start(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(console.String(), "invalid") {
		t.Fatalf("invalid environment value produced no diagnostic: %s", console.String())
	}
	if exports.Load() == 0 {
		t.Fatal("invalid signal endpoint did not fall back to the shared endpoint")
	}
}
