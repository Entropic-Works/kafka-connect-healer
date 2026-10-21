package poll

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
	"github.com/Entropic-Works/kafka-connect-healer/internal/logging"
	"github.com/Entropic-Works/kafka-connect-healer/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log/global"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestPollingTelemetry(t *testing.T) {
	for _, test := range []struct {
		name, state, requestOutcome, cycleOutcome, cancelMethod string
		statusCode, restartCode, cycles                         int
		disabled, timeout                                       bool
	}{
		{name: "healthy", state: "RUNNING", cycleOutcome: "success"},
		{name: "accepted task", state: "task_failed", restartCode: 204, requestOutcome: "accepted", cycleOutcome: "success"},
		{name: "accepted connector", state: "FAILED", restartCode: 202, requestOutcome: "accepted", cycleOutcome: "success"},
		{name: "restart error", state: "FAILED", restartCode: 500, requestOutcome: "error", cycleOutcome: "error"},
		{name: "restart conflict", state: "FAILED", restartCode: 409, requestOutcome: "error", cycleOutcome: "error"},
		{name: "status error", statusCode: 503, cycleOutcome: "error"},
		{name: "invalid snapshot", state: "invalid", cycleOutcome: "error"},
		{name: "status timeout", timeout: true, cycleOutcome: "error"},
		{name: "status canceled", cancelMethod: http.MethodGet, cycleOutcome: "canceled"},
		{name: "restart canceled", state: "FAILED", cancelMethod: http.MethodPost, requestOutcome: "canceled", cycleOutcome: "canceled"},
		{name: "backoff skips requests", state: "FAILED", restartCode: 204, cycles: 2, requestOutcome: "accepted", cycleOutcome: "success"},
		{name: "disabled", state: "FAILED", restartCode: 204, disabled: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			previousProvider, previousLogger := otel.GetMeterProvider(), slog.Default()
			otel.SetMeterProvider(provider)
			t.Cleanup(func() {
				otel.SetMeterProvider(previousProvider)
				slog.SetDefault(previousLogger)
				if err := provider.Shutdown(context.Background()); err != nil {
					t.Error(err)
				}
			})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cycles := test.cycles
			if cycles == 0 {
				cycles = 1
			}
			completed := 0
			slog.SetDefault(slog.New(&cycleLogHandler{
				Handler: slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}),
				after: func(record slog.Record) {
					if record.Message == "poll cycle completed" || record.Message == "poll cycle failed while retrieving connector statuses" {
						completed++
						if completed == cycles {
							cancel()
						}
					}
				},
			}))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.Method == test.cancelMethod {
					cancel()
					<-request.Context().Done()
					return
				}
				if request.Method == http.MethodPost {
					w.WriteHeader(test.restartCode)
					return
				}
				if test.timeout {
					<-request.Context().Done()
					return
				}
				if test.statusCode != 0 {
					w.WriteHeader(test.statusCode)
					return
				}
				switch test.state {
				case "task_failed":
					io.WriteString(w, `{"orders":{"status":{"name":"orders","connector":{"state":"RUNNING"},"tasks":[{"id":7,"state":"FAILED"}]}}}`)
				case "FAILED":
					io.WriteString(w, `{"orders":{"status":{"name":"orders","connector":{"state":"FAILED"}}}}`)
				case "RUNNING":
					io.WriteString(w, `{"orders":{"status":{"name":"orders","connector":{"state":"RUNNING"}}}}`)
				default:
					io.WriteString(w, `null`)
				}
			}))
			defer server.Close()
			address, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			configuration := config.DefaultConfiguration()
			configuration.TelemetryConfig.Enabled = !test.disabled
			configuration.PollingBehavior.Interval = config.Duration(50 * time.Millisecond)
			if test.timeout {
				configuration.CommunicationConfig.RequestTimeout = config.Duration(10 * time.Millisecond)
			}
			poller := NewConnectClusterPoller("production", connectcluster.Configuration{Host: address.Hostname(), Port: address.Port()})
			poller.Poll(ctx, config.NewManager(configuration))
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatal("polling did not finish the expected cycles")
			}
			data := collectPollingMetrics(t, reader)
			if test.disabled {
				if len(data) != 0 {
					t.Fatalf("disabled telemetry recorded metrics: %v", data)
				}
				return
			}
			cycleData := data["healer.poll.cycles"].Data.(metricdata.Sum[int64]).DataPoints
			if len(cycleData) != 1 || cycleData[0].Value != int64(cycles) {
				t.Fatalf("cycle counts = %v, want %d", cycleData, cycles)
			}
			assertMetricAttribute(t, cycleData[0].Attributes, "connect_cluster", "production")
			assertMetricAttribute(t, cycleData[0].Attributes, "outcome", test.cycleOutcome)
			durations := data["healer.poll.duration"].Data.(metricdata.Histogram[float64]).DataPoints
			if len(durations) != 1 || durations[0].Count != uint64(cycles) || durations[0].Sum <= 0 {
				t.Fatalf("cycle durations = %v", durations)
			}
			restarts, hasRestarts := data["healer.restart.requests"]
			if test.requestOutcome == "" {
				if hasRestarts {
					t.Fatalf("unexpected restart metrics: %v", restarts)
				}
				return
			}
			requests := restarts.Data.(metricdata.Sum[int64]).DataPoints
			if len(requests) != 1 || requests[0].Value != 1 {
				t.Fatalf("restart counts = %v, want one actual request", requests)
			}
			assertMetricAttribute(t, requests[0].Attributes, "connect_cluster", "production")
			assertMetricAttribute(t, requests[0].Attributes, "connector", "orders")
			assertMetricAttribute(t, requests[0].Attributes, "outcome", test.requestOutcome)
			action := "restart_connector"
			if test.state == "task_failed" {
				action = "restart_task"
			}
			assertMetricAttribute(t, requests[0].Attributes, "action", action)
			taskID, hasTaskID := requests[0].Attributes.Value("task_id")
			if test.state == "task_failed" {
				if !hasTaskID || taskID.AsInt64() != 7 {
					t.Fatalf("task restart task_id = %v, want 7", taskID)
				}
			} else if hasTaskID {
				t.Fatal("connector restarts should not include a task ID")
			}
		})
	}
}

func TestPollingContinuesWhenOTLPExportsFail(t *testing.T) {
	var exports, polls atomic.Int64
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		exports.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(receiver.Close)
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", receiver.URL+"/v1/metrics")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", receiver.URL+"/v1/logs")
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "1")
	t.Setenv("OTEL_BLRP_SCHEDULE_DELAY", "1")
	previousMeter, previousLogs, previousLogger := otel.GetMeterProvider(), global.GetLoggerProvider(), slog.Default()
	previousErrors := otel.GetErrorHandler()
	t.Cleanup(func() {
		otel.SetMeterProvider(previousMeter)
		otel.SetErrorHandler(previousErrors)
		global.SetLoggerProvider(previousLogs)
		slog.SetDefault(previousLogger)
	})
	shutdown, err := telemetry.Start(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		// Export errors are expected; cleanup must still respect the deadline.
		shutdown(ctx)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	completed := 0
	slog.SetDefault(logging.WithTelemetry(slog.New(&cycleLogHandler{
		Handler: slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}),
		after: func(record slog.Record) {
			if record.Message == "poll cycle completed" {
				completed++
				if completed >= 2 && exports.Load() > 0 {
					cancel()
				}
			}
		},
	})))
	connect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		polls.Add(1)
		io.WriteString(w, `{}`)
	}))
	defer connect.Close()
	address, err := url.Parse(connect.URL)
	if err != nil {
		t.Fatal(err)
	}
	configuration := config.DefaultConfiguration()
	configuration.TelemetryConfig.Enabled = true
	configuration.PollingBehavior.Interval = config.Duration(10 * time.Millisecond)
	NewConnectClusterPoller("production", connectcluster.Configuration{Host: address.Hostname(), Port: address.Port()}).Poll(ctx, config.NewManager(configuration))
	if ctx.Err() == context.DeadlineExceeded || polls.Load() < 2 || exports.Load() == 0 {
		t.Fatalf("polling did not continue during export failure: polls=%d exports=%d", polls.Load(), exports.Load())
	}
}

func collectPollingMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	metrics := make(map[string]metricdata.Metrics)
	for _, scope := range data.ScopeMetrics {
		for _, value := range scope.Metrics {
			metrics[value.Name] = value
		}
	}
	return metrics
}

func assertMetricAttribute(t *testing.T, attributes attribute.Set, key, want string) {
	t.Helper()
	value, ok := attributes.Value(attribute.Key(key))
	if !ok || value.AsString() != want {
		t.Fatalf("attribute %s = %v, want %q", key, value, want)
	}
}

type cycleLogHandler struct {
	slog.Handler
	after func(slog.Record)
}

func (handler *cycleLogHandler) Handle(ctx context.Context, record slog.Record) error {
	err := handler.Handler.Handle(ctx, record)
	handler.after(record)
	return err
}

func (handler *cycleLogHandler) WithAttrs(attributes []slog.Attr) slog.Handler {
	return &cycleLogHandler{Handler: handler.Handler.WithAttrs(attributes), after: handler.after}
}
