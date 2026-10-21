package logging

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
)

// WithTelemetry exports this logger's records while retaining its console output
// and dynamic log level.
func WithTelemetry(logger *slog.Logger) *slog.Logger {
	console := logger.Handler()
	return slog.New(&telemetryHandler{
		Handler: slog.NewMultiHandler(console, otelslog.NewHandler("github.com/Entropic-Works/kafka-connect-healer")),
		console: console,
	})
}

type telemetryHandler struct {
	slog.Handler
	console slog.Handler
}

func (handler *telemetryHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return handler.console.Enabled(ctx, level)
}

func (handler *telemetryHandler) WithAttrs(attributes []slog.Attr) slog.Handler {
	return &telemetryHandler{Handler: handler.Handler.WithAttrs(attributes), console: handler.console}
}

func (handler *telemetryHandler) WithGroup(name string) slog.Handler {
	return &telemetryHandler{Handler: handler.Handler.WithGroup(name), console: handler.console}
}
