package config

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
)

func newPatchRequest(body string) *http.Request {
	request := httptest.NewRequest(http.MethodPatch, "/config", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	return request
}

func TestGetConfigReturnsLatestConfigurationWithoutPassword(t *testing.T) {
	configuration := config.DefaultConfiguration()
	configuration.APIConfig.AuthConfig = config.AuthConfiguration{
		Enabled: true, Username: "api-user", Password: "api-secret",
	}
	manager := config.NewManager(configuration)
	handler := NewConfigHandler(manager)
	if err := manager.UpdateConfiguration(func(configuration *config.ApplicationConfiguration) error {
		configuration.PollingBehavior.Interval = config.Duration(250 * time.Millisecond)
		return nil
	}); err != nil {
		t.Fatalf("UpdateConfiguration() error = %v", err)
	}
	response := httptest.NewRecorder()
	handler.GetConfig(response, httptest.NewRequest(http.MethodGet, "/config", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if got := response.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	var body struct {
		PollingBehavior struct {
			Interval string `json:"interval"`
		} `json:"pollingBehavior"`
		APIConfig struct {
			AuthConfig map[string]jsontext.Value `json:"authConfig"`
		} `json:"apiConfig"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode config response: %v", err)
	}
	if body.PollingBehavior.Interval != "250ms" {
		t.Fatalf("interval = %q, want 250ms", body.PollingBehavior.Interval)
	}
	if _, present := body.APIConfig.AuthConfig["password"]; present {
		t.Fatal("config response includes the API password field")
	}
	if got := manager.GetConfiguration().APIConfig.AuthConfig.Password; got != "api-secret" {
		t.Fatal("GET config modified the stored API password")
	}
	var fields map[string]jsontext.Value
	if err := json.Unmarshal(response.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 5 {
		t.Fatal("GET config includes fields outside application configuration")
	}
	for _, field := range []string{"pollingBehavior", "communicationConfig", "apiConfig", "loggingConfig", "telemetryConfig"} {
		if _, present := fields[field]; !present {
			t.Fatalf("GET config omitted application field %q", field)
		}
	}
}

func TestPatchConfigAppliesPartialUpdateAndNotifiesWorkers(t *testing.T) {
	before := config.DefaultConfiguration()
	before.APIConfig.AuthConfig = config.AuthConfiguration{
		Enabled: true, Username: "user", Password: "secret",
	}
	manager := config.NewManager(before)
	_, changes := manager.ConfigurationSnapshot()
	handler := NewConfigHandler(manager)
	response := httptest.NewRecorder()
	request := newPatchRequest(`{
		"pollingBehavior": {"interval": "30s", "restartFailedTasks": false},
		"loggingConfig": {"level": "DEBUG"}
	}`)
	handler.PatchConfig(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	want := before
	want.PollingBehavior.Interval = config.Duration(30 * time.Second)
	want.PollingBehavior.RestartFailedTasks = false
	if err := want.LoggingConfig.Level.UnmarshalText([]byte("DEBUG")); err != nil {
		t.Fatal(err)
	}
	if got := manager.GetConfiguration(); got != want {
		t.Fatal("patch did not change exactly the supplied fields")
	}
	select {
	case <-changes:
	default:
		t.Fatal("accepted patch did not notify workers")
	}
	getResponse := httptest.NewRecorder()
	handler.GetConfig(getResponse, httptest.NewRequest(http.MethodGet, "/config", nil))
	if strings.Contains(getResponse.Body.String(), "secret") || strings.Contains(getResponse.Body.String(), `"password"`) {
		t.Fatal("GET after PATCH exposed the preserved password")
	}
}

func TestPatchConfigRejectsInvalidUpdatesAtomically(t *testing.T) {
	for _, body := range []string{
		``, `null`, `[]`, `"string"`, `{`, `{} {}`,
		`{"unknown":true}`,
		`{"pollingBehavior":{"interval":"30s"},"telemetryConfig":{"enabled":true}}`,
		`{"connectConfigs":{"production":{"host":"changed"}}}`,
		`{"connectClusterAPIConfigurations":{"production":{"host":"changed"}}}`,
		`{"pollingBehavior":{"interval":"30s"},"connectClusterAPIs":{}}`,
		`{"connectConfig":{"host":"http://localhost"}}`,
		`{"pollingBehavior":{"interval":"1s","unknown":true}}`,
		`{"pollingBehavior":{"interval":1000}}`,
		`{"pollingBehavior":{"interval":"0s"},"connectConfig":{"host":"changed"}}`,
		`{"pollingBehavior":{"backoff":{"maxDelay":"1s"}}}`,
		`{"connectConfig":{"authConfig":{"enabled":true}}}`,
		`{"loggingConfig":{"level":"sensitive-test-value"}}`,
		`{"pollingBehavior":null}`,
		`{"pollingBehavior":{"restartFailedTasks":null}}`,
		`{"pollingBehavior":{"backoff":{"enabled":null}}}`,
		`{"connectConfig":{"authConfig":{"password":null}}}`,
		`{"pollingBehavior":{"interval":"1s"},"loggingConfig":null}`,
		`{"pollingBehavior":{},"pollingBehavior":{"interval":"30s"}}`,
		`{"pollingBehavior":{"interval":"1s","interval":"30s"}}`,
		`{"pollingBehavior":{"backoff":{"enabled":true,"enabled":false}}}`,
		`{"pollingBehavior":{"interval":"1s","Interval":"30s"}}`,
		`{"connectConfig":{"host":"first","h\u006fst":"second"}}`,
		`{"connectConfig":{"authConfig":{"password":"sensitive-test-value","password":"second"}}}`,
		`{"polling_behavior":{"interval":"30s"}}`,
		`{"pollingBehavior":{"restart-failed-tasks":false}}`,
		`{"connectConfig":{"host":"\ud800"}}`,
		"{\"connectConfig\":{\"host\":\"\xff\"}}",
	} {
		t.Run(body, func(t *testing.T) {
			before := config.DefaultConfiguration()
			manager := config.NewManager(before)
			_, changes := manager.ConfigurationSnapshot()
			response := httptest.NewRecorder()
			NewConfigHandler(manager).PatchConfig(response, newPatchRequest(body))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", response.Code)
			}
			if manager.GetConfiguration() != before {
				t.Fatal("rejected patch changed configuration")
			}
			select {
			case <-changes:
				t.Fatal("rejected patch notified workers")
			default:
			}
			if strings.Contains(response.Body.String(), "sensitive-test-value") {
				t.Fatal("error response exposed a supplied value")
			}
		})
	}
}

func TestPatchConfigAcceptsCaseInsensitiveFieldNames(t *testing.T) {
	manager := config.NewManager(config.DefaultConfiguration())
	response := httptest.NewRecorder()
	NewConfigHandler(manager).PatchConfig(response, newPatchRequest(`{"PollingBehavior":{"Interval":"30s"}}`))
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if manager.GetConfiguration().PollingBehavior.Interval != config.Duration(30*time.Second) {
		t.Fatal("case-insensitive field names did not update the configuration")
	}
}

func TestPatchConfigAllowsSameKeyInSeparateObjects(t *testing.T) {
	manager := config.NewManager(config.DefaultConfiguration())
	response := httptest.NewRecorder()
	request := newPatchRequest(`{
		"pollingBehavior":{"restartFailedTasks":false,"backoff":{"enabled":false}},
		"apiConfig":{"authConfig":{"enabled":false}}
	}`)
	NewConfigHandler(manager).PatchConfig(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	want := config.DefaultConfiguration()
	want.PollingBehavior.RestartFailedTasks = false
	want.PollingBehavior.Backoff.Enabled = false
	if manager.GetConfiguration() != want {
		t.Fatal("patch did not apply valid fields in separate objects")
	}
	if response.Body.Len() != 0 {
		t.Fatal("204 response has a body")
	}
}

func TestPatchConfigRejectsOversizedBody(t *testing.T) {
	manager := config.NewManager(config.DefaultConfiguration())
	response := httptest.NewRecorder()
	NewConfigHandler(manager).PatchConfig(response, newPatchRequest(strings.Repeat(" ", (64<<10)+1)))
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", response.Code)
	}
	if manager.GetConfiguration() != config.DefaultConfiguration() {
		t.Fatal("oversized patch changed configuration")
	}
}

func TestPatchConfigContentType(t *testing.T) {
	for _, test := range []struct {
		contentType string
		status      int
	}{
		{"application/json", http.StatusNoContent},
		{"application/json; charset=utf-8", http.StatusNoContent},
		{"Application/JSON", http.StatusNoContent},
		{"", http.StatusUnsupportedMediaType},
		{"text/plain", http.StatusUnsupportedMediaType},
		{"application/yaml", http.StatusUnsupportedMediaType},
		{"application/merge-patch+json", http.StatusUnsupportedMediaType},
		{"application/json; invalid", http.StatusUnsupportedMediaType},
	} {
		t.Run(test.contentType, func(t *testing.T) {
			before := config.DefaultConfiguration()
			manager := config.NewManager(before)
			_, changes := manager.ConfigurationSnapshot()
			request := newPatchRequest(`{"pollingBehavior":{"interval":"30s"}}`)
			request.Header.Set("Content-Type", test.contentType)
			response := httptest.NewRecorder()
			NewConfigHandler(manager).PatchConfig(response, request)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
			if test.status == http.StatusNoContent {
				if manager.GetConfiguration().PollingBehavior.Interval != config.Duration(30*time.Second) {
					t.Fatal("valid content type did not allow the update")
				}
				return
			}
			if manager.GetConfiguration() != before {
				t.Fatal("unsupported content type changed configuration")
			}
			if got := response.Header().Get("Accept-Patch"); got != "application/json" {
				t.Fatalf("Accept-Patch = %q, want application/json", got)
			}
			select {
			case <-changes:
				t.Fatal("unsupported content type notified workers")
			default:
			}
		})
	}
}

func TestPatchConfigErrorsIdentifyFieldsWithoutValues(t *testing.T) {
	for _, test := range []struct {
		body string
		want string
	}{
		{`{"pollingBehavior":{"interval":"sensitive-value"}}`, "/pollingBehavior/interval: invalid value"},
		{`{"PollingBehavior":{"Interval":123}}`, "/PollingBehavior/Interval: invalid value"},
		{`{"loggingConfig":{"level":"sensitive-value"}}`, "/loggingConfig/level: invalid value"},
		{`{"pollingBehavior":{"restartFailedTasks":"sensitive-value"}}`, "/pollingBehavior/restartFailedTasks: invalid value"},
		{`{"connectConfig":{"authConfig":{"password":["sensitive-value"]}}}`, "/connectConfig: unknown field"},
		{`{"pollingBehavior":"sensitive-value"}`, "/pollingBehavior: invalid value"},
		{`{"pollingBehavior":{"interval":"0s"}}`, "pollingBehavior.interval:"},
		{`{"connectConfig":{"port":"sensitive-value"}}`, "/connectConfig: unknown field"},
		{`{"pollingBehavior":{"interval":"30s","typo":true}}`, "/pollingBehavior/typo: unknown field"},
		{`{"connectConfig":{"https":true,"authConfig":{"enabled":true,"username":"user"}}}`, "/connectConfig: unknown field"},
		{`{"pollingBehavior":{"restartFailedTasks":true,"restartFailedTaſks":false}}`, "duplicate keys"},
	} {
		t.Run(test.want, func(t *testing.T) {
			before := config.DefaultConfiguration()
			manager := config.NewManager(before)
			_, changes := manager.ConfigurationSnapshot()
			response := httptest.NewRecorder()
			NewConfigHandler(manager).PatchConfig(response, newPatchRequest(test.body))
			if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), test.want) {
				t.Fatalf("status = %d, body = %s, want 400 containing %q", response.Code, response.Body.String(), test.want)
			}
			if strings.Contains(response.Body.String(), "sensitive-value") {
				t.Fatal("error exposed an input value")
			}
			if manager.GetConfiguration() != before {
				t.Fatal("invalid patch changed configuration")
			}
			select {
			case <-changes:
				t.Fatal("invalid patch notified workers")
			default:
			}
		})
	}
}
