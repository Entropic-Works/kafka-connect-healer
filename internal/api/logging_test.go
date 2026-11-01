package api

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
	"github.com/Entropic-Works/kafka-connect-healer/internal/logging"
)

type logBuffer struct {
	sync.Mutex
	bytes.Buffer
}

func (buffer *logBuffer) Write(data []byte) (int, error) {
	buffer.Lock()
	defer buffer.Unlock()
	return buffer.Buffer.Write(data)
}
func (buffer *logBuffer) String() string {
	buffer.Lock()
	defer buffer.Unlock()
	return buffer.Buffer.String()
}

func TestManagersLogOnlyTheirOwnConfiguration(t *testing.T) {
	var output logBuffer
	previous := slog.Default()
	// Use the same dynamic console level as production, then capture its handler.
	logging.Configure()
	base := slog.Default().Handler()
	// A delegating handler preserves production filtering with structured capture.
	slog.SetDefault(slog.New(&captureHandler{Handler: slog.NewJSONHandler(&output, nil), filter: base}))
	t.Cleanup(func() { slog.SetDefault(previous); logging.SetLevel(slog.LevelInfo) })
	configuration := config.DefaultConfiguration()
	configuration.PollingBehavior.Interval = config.Duration(time.Hour)
	configuration.APIConfig.AuthConfig = config.AuthConfiguration{Enabled: true, Username: "private-api-user", Password: "private-api-password"}
	server := newTestServer(t, config.NewManager(configuration))
	summaries := func() []map[string]any {
		t.Helper()
		var summaries []map[string]any
		for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
			var event map[string]any
			if err := json.Unmarshal([]byte(line), &event); err != nil {
				t.Fatal(err)
			}
			if event["Application"] != nil || event["Connect clusters"] != nil {
				summaries = append(summaries, event)
			}
		}
		return summaries
	}
	initial := summaries()
	if len(initial) != 2 || initial[0]["Application"] == nil || initial[1]["Connect clusters"] == nil {
		t.Fatalf("startup summaries: %v", initial)
	}
	cluster := `{"host":"localhost","port":"8083","authConfig":{"enabled":true,"username":"private-cluster-user","password":"private-cluster-password"}}`
	for _, test := range []struct {
		method, path, body string
		status             int
		section            string
		count              int
	}{
		{"PATCH", "/config", `{"communicationConfig":{"requestTimeout":"3s"}}`, 204, "Application", 0},
		{"PATCH", "/config", `{"communicationConfig":{"requestTimeout":"3s"}}`, 204, "", 0},
		{"PATCH", "/config", `{"communicationConfig":{"requestTimeout":"0s"}}`, 400, "", 0},
		{"GET", "/config", "", 200, "", 0},
		{"PUT", "/clusters/zeta", cluster, 201, "Connect clusters", 1},
		{"PUT", "/clusters/zeta", cluster, 204, "", 0},
		{"PUT", "/clusters/Count", strings.Replace(cluster, "8083", "8084", 1), 201, "Connect clusters", 2},
		{"PUT", "/clusters/zeta", strings.Replace(cluster, "8083", "8085", 1), 204, "Connect clusters", 2},
		{"PUT", "/clusters/rejected", `{}`, 400, "", 0},
		{"DELETE", "/clusters/zeta", "", 204, "Connect clusters", 1},
		{"DELETE", "/clusters/zeta", "", 404, "", 0},
		{"PATCH", "/config", `{"loggingConfig":{"level":"WARN"}}`, 204, "", 0},
		{"PUT", "/clusters/quiet", cluster, 201, "", 0},
		{"PATCH", "/config", `{"loggingConfig":{"level":"ERROR"}}`, 204, "", 0},
		{"PATCH", "/config", `{"loggingConfig":{"level":"INFO"}}`, 204, "Application", 0},
		{"DELETE", "/clusters/Count", "", 204, "Connect clusters", 1},
		{"DELETE", "/clusters/quiet", "", 204, "Connect clusters", 0},
	} {
		t.Run(test.method+" "+test.path+" "+test.body, func(t *testing.T) {
			before := len(summaries())
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
			request.SetBasicAuth("private-api-user", "private-api-password")
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			server.Handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status %d: %s", response.Code, response.Body.String())
			}
			events := summaries()
			want := before
			if test.section != "" {
				want++
			}
			if len(events) != want {
				t.Fatalf("summaries %d, want %d: %s", len(events), want, output.String())
			}
			if test.section == "" {
				return
			}
			latest := events[len(events)-1]
			other := "Application"
			if test.section == "Application" {
				other = "Connect clusters"
			}
			if latest[test.section] == nil || latest[other] != nil {
				t.Fatalf("wrong manager state logged: %v", latest)
			}
			fields := latest[test.section].(map[string]any)
			if test.section == "Connect clusters" {
				definitions, _ := fields["Definitions"].(map[string]any)
				if fields["Count"] != float64(test.count) || len(definitions) != test.count {
					t.Fatalf("cluster snapshot: %v", fields)
				}
			} else if fields["Request timeout"] != "3s" || fields["API authentication"] != true {
				t.Fatalf("application snapshot: %v", fields)
			}
		})
	}
	for _, secret := range []string{"private-api-user", "private-api-password", "private-cluster-user", "private-cluster-password"} {
		if strings.Contains(output.String(), secret) {
			t.Fatalf("logs exposed %s", secret)
		}
	}
}

type captureHandler struct {
	slog.Handler
	filter slog.Handler
}

func (handler *captureHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return handler.filter.Enabled(ctx, level)
}
func (handler *captureHandler) WithAttrs(attributes []slog.Attr) slog.Handler {
	return &captureHandler{Handler: handler.Handler.WithAttrs(attributes), filter: handler.filter}
}
func (handler *captureHandler) WithGroup(name string) slog.Handler {
	return &captureHandler{Handler: handler.Handler.WithGroup(name), filter: handler.filter}
}
