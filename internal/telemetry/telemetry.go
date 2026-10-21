// Package telemetry configures OTLP/HTTP export for polling metrics and application logs.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// Start installs shared providers before application log export and polling start.
// The returned function must run after polling stops, with a fresh context and a
// shutdown deadline.
func Start(ctx context.Context) (func(context.Context) error, error) {
	// Keep SDK diagnostics on the console logger captured before log export is
	// enabled. Exporting its own failures would create a logging feedback loop.
	console := slog.Default()
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		console.Error("telemetry error", "error", err)
	}))
	serviceResource, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", "kafka-connect-healer")),
		resource.WithFromEnv(), resource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, fmt.Errorf("create telemetry resource: %w", err)
	}
	metricExporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create metrics exporter: %w", err)
	}
	logExporter, err := otlploghttp.New(ctx)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create logs exporter: %w", err), metricExporter.Shutdown(ctx))
	}
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(serviceResource),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
	)
	loggerProvider := sdklog.NewLoggerProvider(
		sdklog.WithResource(serviceResource),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
	)
	otel.SetMeterProvider(meterProvider)
	global.SetLoggerProvider(loggerProvider)
	return func(ctx context.Context) error {
		return errors.Join(loggerProvider.Shutdown(ctx), meterProvider.Shutdown(ctx))
	}, nil
}
