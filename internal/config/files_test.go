package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func writeConfigFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadFilesDefaultsAndPartialOverlay(t *testing.T) {
	base := writeConfigFile(t, `
apiConfig:
  authConfig:
    enabled: true
    username: base-user
pollingBehavior:
  interval: 1m30s
  restartFailedTasks: false
  backoff:
    baseDelay: 30s
loggingConfig:
  level: DEBUG
telemetryConfig:
  enabled: true
`)
	overlay := writeConfigFile(t, `
apiConfig:
  authConfig:
    username: secret-user
    password: "test-password"
pollingBehavior:
  interval: 500ms
  backoff:
    enabled: false
    exponential: false
`)
	got, err := LoadFiles(base, overlay)
	if err != nil {
		t.Fatalf("LoadFiles() error = %v", err)
	}
	want := DefaultConfiguration()
	want.APIConfig.AuthConfig = AuthConfiguration{Enabled: true, Username: "secret-user", Password: "test-password"}
	want.PollingBehavior.Interval = Duration(500 * time.Millisecond)
	want.PollingBehavior.RestartFailedTasks = false
	want.PollingBehavior.Backoff.BaseDelay = Duration(30 * time.Second)
	want.PollingBehavior.Backoff.Enabled = false
	want.PollingBehavior.Backoff.Exponential = false
	want.LoggingConfig.Level = slog.LevelDebug
	want.TelemetryConfig.Enabled = true
	if got != want {
		t.Fatal("merged configuration did not preserve defaults, base values, and explicit overlay values")
	}
}

func TestLoadFilesWithoutOverlay(t *testing.T) {
	for _, path := range []string{"", writeConfigFile(t, "{}"), writeConfigFile(t, "pollingBehavior:\n  backoff: {}\n")} {
		got, err := LoadFiles(path, "")
		if err != nil {
			t.Fatalf("LoadFiles() error = %v", err)
		}
		if got != DefaultConfiguration() {
			t.Fatal("omitted fields did not retain defaults")
		}
	}
}

func TestLoadFilesOverlayWithoutBase(t *testing.T) {
	overlay := writeConfigFile(t, "pollingBehavior:\n  interval: 30s\nloggingConfig:\n  level: DEBUG")
	got, err := LoadFiles("", overlay)
	if err != nil {
		t.Fatal(err)
	}
	want := DefaultConfiguration()
	want.PollingBehavior.Interval = Duration(30 * time.Second)
	want.LoggingConfig.Level = slog.LevelDebug
	if got != want {
		t.Fatal("overlay without a base file did not preserve defaults and apply supplied values")
	}
}

func TestLoadFilesAPIAuthenticationSecretOverlay(t *testing.T) {
	base := writeConfigFile(t, `
apiConfig:
  authConfig:
    enabled: true
    username: base-user
`)
	overlay := writeConfigFile(t, `
apiConfig:
  authConfig:
    username: api-user
    password: api-secret
`)
	got, err := LoadFiles(base, overlay)
	if err != nil {
		t.Fatalf("LoadFiles() error = %v", err)
	}
	want := DefaultConfiguration()
	want.APIConfig.AuthConfig = AuthConfiguration{Enabled: true, Username: "api-user", Password: "api-secret"}
	if got != want {
		t.Fatal("API authentication overlay changed unrelated configuration")
	}
	if _, err := LoadFiles(base, ""); err == nil || !strings.Contains(err.Error(), "apiConfig.authConfig.password:") {
		t.Fatalf("missing API credentials accepted or error lacked field: %v", err)
	}
}

func TestLoadFilesOverlayCanExplicitlyClearValues(t *testing.T) {
	base := writeConfigFile(t, `
apiConfig:
  authConfig:
    enabled: true
    username: base-user
    password: base-password
`)
	overlay := writeConfigFile(t, `
apiConfig:
  authConfig:
    enabled: false
    username: ""
    password: ""
`)
	got, err := LoadFiles(base, overlay)
	if err != nil {
		t.Fatal(err)
	}
	if got.APIConfig.AuthConfig != (AuthConfiguration{}) {
		t.Fatal("explicit false and empty strings did not clear base values")
	}
}

func TestLoadFilesRejectsInvalidYAMLInEitherFile(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{"empty file", ""},
		{"comments only", "# no settings"},
		{"scalar root", "hello"},
		{"sequence root", "[]"},
		{"malformed syntax", "apiConfig: ["},
		{"multiple documents", "{}\n---\n{}"},
		{"trailing empty document", "{}\n---"},
		{"unknown top level", "pollingBehavour: {}"},
		{"unknown nested field", "pollingBehavior:\n  backoff:\n    maxDely: 1m"},
		{"duplicate top level", "apiConfig: {}\napiConfig: {}"},
		{"duplicate nested field", "apiConfig:\n  authConfig:\n    username: first\n    username: second"},
		{"null root", "null"},
		{"null section", "apiConfig: null"},
		{"null scalar", "pollingBehavior:\n  interval: null"},
		{"empty scalar", "pollingBehavior:\n  restartFailedTasks:"},
		{"numeric duration", "pollingBehavior:\n  interval: 1000"},
		{"unitless duration", "pollingBehavior:\n  interval: \"1000\""},
		{"overflow duration", "pollingBehavior:\n  interval: 9223372036854775808ns"},
		{"invalid duration", "communicationConfig:\n  requestTimeout: someday"},
		{"invalid boolean", "pollingBehavior:\n  restartFailedTasks: perhaps"},
		{"invalid log level", "loggingConfig:\n  level: VERBOSE"},
		{"numeric log level", "loggingConfig:\n  level: 4"},
		{"alias", "apiConfig:\n  authConfig:\n    username: &user admin\n    password: *user"},
		{"merge key", "apiConfig:\n  <<: {authConfig: {}}"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := writeConfigFile(t, test.data)
			valid := writeConfigFile(t, "{}")
			for _, paths := range [][2]string{{invalid, ""}, {valid, invalid}, {"", invalid}} {
				got, err := LoadFiles(paths[0], paths[1])
				if err == nil {
					t.Fatal("LoadFiles() accepted invalid YAML")
				}
				if got != (ApplicationConfiguration{}) {
					t.Fatal("LoadFiles() returned partial configuration on failure")
				}
			}
		})
	}
}

func TestLoadFilesValidatesMergedConfiguration(t *testing.T) {
	for _, test := range []struct {
		data string
		want string
	}{
		{"pollingBehavior:\n  interval: 0s", "polling interval"},
		{"pollingBehavior:\n  interval: -1s", "polling interval"},
		{"pollingBehavior:\n  backoff:\n    baseDelay: 0s", "backoff base delay"},
		{"pollingBehavior:\n  backoff:\n    maxDelay: 1s", "backoff maximum delay"},
		{"communicationConfig:\n  requestTimeout: 0s", "HTTP request timeout"},
		{"apiConfig:\n  authConfig:\n    enabled: true", "apiConfig.authConfig.username:"},
		{"apiConfig:\n  authConfig:\n    enabled: true\n    username: admin", "apiConfig.authConfig.password:"},
	} {
		got, err := LoadFiles(writeConfigFile(t, "{}"), writeConfigFile(t, test.data))
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("LoadFiles() error = %v, want %q", err, test.want)
		}
		if got != (ApplicationConfiguration{}) {
			t.Fatal("LoadFiles() returned invalid configuration")
		}
	}
}

func TestLoadFilesRequiresEverySpecifiedFile(t *testing.T) {
	valid := writeConfigFile(t, "{}")
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	for _, paths := range [][2]string{{"", missing}, {missing, ""}, {valid, missing}, {t.TempDir(), ""}} {
		if _, err := LoadFiles(paths[0], paths[1]); err == nil {
			t.Fatal("LoadFiles() accepted a missing or unreadable file")
		}
	}
}

func TestLoadFilesErrorsDoNotExposeCredentials(t *testing.T) {
	const sensitive = "credential-that-must-not-be-logged"
	for _, data := range []string{
		"apiConfig:\n  authConfig:\n    password: [" + sensitive + "]",
		"loggingConfig:\n  level: " + sensitive,
		"pollingBehavior:\n  interval: " + sensitive,
		"apiConfig:\n  authConfig:\n    password: first\n    password: " + sensitive,
	} {
		invalid := writeConfigFile(t, data)
		for _, paths := range [][2]string{{invalid, ""}, {writeConfigFile(t, "{}"), invalid}} {
			_, err := LoadFiles(paths[0], paths[1])
			if err == nil || strings.Contains(err.Error(), sensitive) {
				t.Fatalf("expected a safe configuration error, got %v", err)
			}
		}
	}
}

func TestConfigurationYAMLRoundTrip(t *testing.T) {
	want := DefaultConfiguration()
	want.PollingBehavior.RestartFailedTasks = false
	want.LoggingConfig.Level = slog.LevelWarn
	data, err := yaml.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := LoadFiles(writeConfigFile(t, string(data)), "")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatal("YAML round trip changed configuration")
	}
}
