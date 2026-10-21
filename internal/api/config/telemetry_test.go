package config

import (
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
)

func TestTelemetryConfigResponses(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, test := range []struct {
			name, body, wantError string
			changesInterval       bool
		}{
			{"toggle", fmt.Sprintf(`{"telemetryConfig":{"enabled":%t}}`, !enabled), "telemetryConfig: telemetry is startup-only; update the YAML configuration and restart", false},
			{"toggle with valid update", fmt.Sprintf(`{"telemetryConfig":{"enabled":%t},"pollingBehavior":{"interval":"30s"}}`, !enabled), "telemetryConfig: telemetry is startup-only; update the YAML configuration and restart", false},
			{"same value", fmt.Sprintf(`{"telemetryConfig":{"enabled":%t}}`, enabled), "", false},
			{"same value with valid update", fmt.Sprintf(`{"telemetryConfig":{"enabled":%t},"pollingBehavior":{"interval":"30s"}}`, enabled), "", true},
			{"omitted", `{"pollingBehavior":{"interval":"30s"}}`, "", true},
			{"empty object", `{"telemetryConfig":{}}`, "", false},
			{"null object", `{"telemetryConfig":null}`, "/telemetryConfig: null values are not allowed in configuration patches", false},
			{"null enabled", `{"telemetryConfig":{"enabled":null}}`, "/telemetryConfig/enabled: null values are not allowed in configuration patches", false},
			{"wrong type", `{"telemetryConfig":{"enabled":"sensitive-value"}}`, "/telemetryConfig/enabled: invalid value", false},
			{"exporter setting", `{"telemetryConfig":{"endpoint":"sensitive-value"}}`, "/telemetryConfig/endpoint: unknown field", false},
		} {
			t.Run(fmt.Sprintf("enabled=%t/%s", enabled, test.name), func(t *testing.T) {
				before := config.DefaultConfiguration()
				before.TelemetryConfig.Enabled = enabled
				manager := config.NewManager(before)
				_, changes := manager.ConfigurationSnapshot()
				handler := NewConfigHandler(manager)
				response := httptest.NewRecorder()
				handler.PatchConfig(response, newPatchRequest(test.body))
				if test.wantError != "" {
					if response.Code != http.StatusBadRequest || response.Body.String() != test.wantError+"\n" {
						t.Fatalf("PATCH = %d %q, want 400 %q", response.Code, response.Body.String(), test.wantError+"\n")
					}
					if strings.Contains(response.Body.String(), "sensitive-value") {
						t.Fatal("error response exposed a submitted value")
					}
				} else if response.Code != http.StatusNoContent || response.Body.Len() != 0 {
					t.Fatalf("PATCH = %d %q, want 204 with no body", response.Code, response.Body.String())
				}
				want := before
				if test.changesInterval {
					want.PollingBehavior.Interval = config.Duration(30 * time.Second)
				}
				if manager.GetConfiguration() != want {
					t.Fatal("patch changed unexpected configuration fields")
				}
				notified := false
				select {
				case <-changes:
					notified = true
				default:
				}
				if notified != test.changesInterval {
					t.Fatalf("worker notification = %t, want %t", notified, test.changesInterval)
				}
				get := httptest.NewRecorder()
				handler.GetConfig(get, httptest.NewRequest(http.MethodGet, "/config", nil))
				var actual config.ApplicationConfiguration
				if err := json.Unmarshal(get.Body.Bytes(), &actual); err != nil {
					t.Fatal(err)
				}
				if get.Code != http.StatusOK || actual != want {
					t.Fatalf("GET returned %d with configuration inconsistent with the accepted changes", get.Code)
				}
			})
		}
	}
}
