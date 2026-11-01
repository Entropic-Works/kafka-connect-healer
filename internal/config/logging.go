package config

import "log/slog"

// LogValue is an explicit allowlist of operational settings. Credentials are
// omitted, including when this value is logged through a structured handler.
func (configuration ApplicationConfiguration) LogValue() slog.Value {
	polling := configuration.PollingBehavior
	return slog.GroupValue(
		slog.String("Polling interval", polling.Interval.String()),
		slog.Bool("Restart failed tasks", polling.RestartFailedTasks),
		slog.String("Request timeout", configuration.CommunicationConfig.RequestTimeout.String()),
		slog.Bool("API authentication", configuration.APIConfig.AuthConfig.Enabled),
		slog.String("Log level", configuration.LoggingConfig.Level.String()),
		slog.Bool("Telemetry", configuration.TelemetryConfig.Enabled),
		slog.Group("Restart backoff",
			slog.Bool("Enabled", polling.Backoff.Enabled),
			slog.String("Base delay", polling.Backoff.BaseDelay.String()),
			slog.String("Maximum delay", polling.Backoff.MaxDelay.String()),
			slog.Bool("Exponential", polling.Backoff.Exponential),
		),
	)
}
