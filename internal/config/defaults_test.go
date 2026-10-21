package config

import "testing"

func TestDefaultConfigurationMatchesComponentDefaults(t *testing.T) {
	got := DefaultConfiguration()
	if got.TelemetryConfig.Enabled {
		t.Fatal("telemetry should be disabled by default")
	}

	if got.CommunicationConfig != DefaultCommunicationConfiguration() {
		t.Fatalf("CommunicationConfig = %#v, want %#v", got.CommunicationConfig, DefaultCommunicationConfiguration())
	}
	if got.PollingBehavior != DefaultPollingBehavior() {
		t.Fatalf("PollingBehavior = %#v, want %#v", got.PollingBehavior, DefaultPollingBehavior())
	}
	if got.LoggingConfig != DefaultLoggingConfiguration() {
		t.Fatalf("LoggingConfig = %#v, want %#v", got.LoggingConfig, DefaultLoggingConfiguration())
	}
	if got.APIConfig != DefaultAPIConfiguration() || got.APIConfig.AuthConfig.Enabled {
		t.Fatal("API authentication should match its defaults and be disabled")
	}
}
