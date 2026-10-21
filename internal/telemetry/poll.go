package telemetry

import (
	"context"
	"errors"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/poll/actions"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// PollMetrics belongs to one poller. A nil receiver disables instrumentation.
type PollMetrics struct {
	cluster  attribute.KeyValue
	cycles   metric.Int64Counter
	duration metric.Float64Histogram
	restarts metric.Int64Counter
}

func NewPollMetrics(cluster string) (*PollMetrics, error) {
	meter := otel.Meter("github.com/Entropic-Works/kafka-connect-healer/internal/poll")
	cycles, cyclesErr := meter.Int64Counter("healer.poll.cycles", metric.WithDescription("Polling cycles by outcome"))
	duration, durationErr := meter.Float64Histogram("healer.poll.duration", metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
		metric.WithDescription("Time spent retrieving statuses and attempting remediation"))
	restarts, restartsErr := meter.Int64Counter("healer.restart.requests",
		metric.WithDescription("Restart requests attempted, not confirmed recoveries"))
	if err := errors.Join(cyclesErr, durationErr, restartsErr); err != nil {
		return nil, err
	}
	return &PollMetrics{cluster: attribute.String("connect_cluster", cluster), cycles: cycles, duration: duration, restarts: restarts}, nil
}

func (metrics *PollMetrics) RecordCycle(ctx context.Context, started time.Time, outcome string) {
	if metrics == nil {
		return
	}
	attributes := metric.WithAttributes(metrics.cluster, attribute.String("outcome", outcome))
	metrics.cycles.Add(ctx, 1, attributes)
	metrics.duration.Record(ctx, time.Since(started).Seconds(), attributes)
}

func (metrics *PollMetrics) RecordRestart(ctx context.Context, action actions.RemediationAction, outcome string) {
	if metrics == nil {
		return
	}
	attributes := []attribute.KeyValue{metrics.cluster, attribute.String("connector", action.ConnectorName),
		attribute.String("action", string(action.Kind)), attribute.String("outcome", outcome)}
	if action.Kind == actions.RestartTask {
		attributes = append(attributes, attribute.Int("task_id", action.TaskID))
	}
	metrics.restarts.Add(ctx, 1, metric.WithAttributes(attributes...))
}
