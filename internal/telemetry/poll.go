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
	cluster     attribute.KeyValue
	cycles      metric.Int64Counter
	duration    metric.Float64Histogram
	restarts    metric.Int64Counter
	nextAttempt metric.Float64Gauge
}

func NewPollMetrics(cluster string) (*PollMetrics, error) {
	meter := otel.Meter("github.com/Entropic-Works/kafka-connect-healer/internal/poll")
	cycles, cyclesErr := meter.Int64Counter("healer.poll.cycles", metric.WithDescription("Polling cycles by outcome"))
	duration, durationErr := meter.Float64Histogram("healer.poll.duration", metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
		metric.WithDescription("Time spent retrieving statuses and attempting remediation"))
	restarts, restartsErr := meter.Int64Counter("healer.restart.requests",
		metric.WithDescription("Restart requests attempted, not confirmed recoveries"))
	nextAttempt, nextAttemptErr := meter.Float64Gauge("healer.restart.next_attempt_time", metric.WithUnit("s"),
		metric.WithDescription("Unix timestamp when restart backoff permits the next attempt, or zero without a deadline"))
	if err := errors.Join(cyclesErr, durationErr, restartsErr, nextAttemptErr); err != nil {
		return nil, err
	}
	return &PollMetrics{cluster: attribute.String("connect_cluster", cluster), cycles: cycles, duration: duration, restarts: restarts, nextAttempt: nextAttempt}, nil
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
	attributes := append(metrics.actionAttributes(action), attribute.String("outcome", outcome))
	metrics.restarts.Add(ctx, 1, metric.WithAttributes(attributes...))
}

func (metrics *PollMetrics) RecordNextAttemptTime(ctx context.Context, action actions.RemediationAction, nextAttempt time.Time) {
	if metrics == nil {
		return
	}
	var timestamp float64
	if !nextAttempt.IsZero() {
		timestamp = float64(nextAttempt.UnixMilli()) / 1000
	}
	metrics.nextAttempt.Record(ctx, timestamp, metric.WithAttributes(metrics.actionAttributes(action)...))
}

func (metrics *PollMetrics) actionAttributes(action actions.RemediationAction) []attribute.KeyValue {
	attributes := []attribute.KeyValue{metrics.cluster, attribute.String("connector", action.ConnectorName), attribute.String("action", string(action.Kind))}
	if action.Kind == actions.RestartTask {
		attributes = append(attributes, attribute.Int("task_id", action.TaskID))
	}
	return attributes
}
