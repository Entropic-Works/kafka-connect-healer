package poll

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll/actions"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll/backoff"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll/requests"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll/status"
	"github.com/Entropic-Works/kafka-connect-healer/internal/telemetry"
)

// ConnectClusterPoller owns the client and backoff state for one cluster.
// Its mutable state is used only by its polling goroutine.
type ConnectClusterPoller struct {
	name                 string
	connectClient        requests.ConnectAPI
	backoffFilter        backoff.Filter
	clusterConfiguration connectcluster.Configuration
}

func NewConnectClusterPoller(name string, configuration connectcluster.Configuration) *ConnectClusterPoller {
	return &ConnectClusterPoller{name: name, clusterConfiguration: configuration}
}

func (poller *ConnectClusterPoller) Poll(ctx context.Context, configurationManager *config.Manager) {
	configuration, configurationUpdateChannel := configurationManager.ConfigurationSnapshot()
	poller.connectClient = requests.NewConnectAPI(poller.clusterConfiguration, configuration.CommunicationConfig.RequestTimeout.Duration())
	poller.backoffFilter = backoff.Filter{
		BackoffConfig:   configuration.PollingBehavior.Backoff,
		BackoffStatuses: map[string]backoff.Status{},
	}
	logger := slog.Default()
	var metrics *telemetry.PollMetrics
	if configuration.TelemetryConfig.Enabled {
		var err error
		metrics, err = telemetry.NewPollMetrics(poller.name)
		if err != nil {
			logger.Error("polling metrics initialization failed; polling will continue", "connect_cluster", poller.name, "error", err)
		}
	}
	logger = logger.With("connect_cluster", poller.name, "endpoint", poller.connectClient.BaseURL)
	logPollingSettings(logger, "polling started", configuration)
	defer logger.Info("polling stopped")

	ticker := time.NewTicker(configuration.PollingBehavior.Interval.Duration())
	defer ticker.Stop()
	failedTargets := make(map[actions.RemediationAction]struct{})

	for {
		select {
		case <-ctx.Done():
			return
		case <-configurationUpdateChannel:
			var newConfiguration config.ApplicationConfiguration
			newConfiguration, configurationUpdateChannel = configurationManager.ConfigurationSnapshot()
			if newConfiguration.CommunicationConfig != configuration.CommunicationConfig {
				// Requests run synchronously in this goroutine; update only between cycles.
				poller.connectClient.HTTPClient.Timeout = newConfiguration.CommunicationConfig.RequestTimeout.Duration()
			}
			if newConfiguration.PollingBehavior.Backoff != configuration.PollingBehavior.Backoff {
				poller.backoffFilter.BackoffConfig = newConfiguration.PollingBehavior.Backoff
			}
			if newConfiguration.PollingBehavior.Interval != configuration.PollingBehavior.Interval {
				ticker.Reset(newConfiguration.PollingBehavior.Interval.Duration())
			}

			if newConfiguration.PollingBehavior != configuration.PollingBehavior ||
				newConfiguration.CommunicationConfig != configuration.CommunicationConfig {
				logPollingSettings(logger, "polling settings updated", newConfiguration)
			}
			configuration = newConfiguration

		case <-ticker.C:
			cycleStarted := time.Now()
			connectorStatuses, err := status.FindConnectorStatuses(ctx, poller.connectClient)
			if err != nil {
				if ctx.Err() != nil {
					metrics.RecordCycle(ctx, cycleStarted, "canceled")
					return
				}
				metrics.RecordCycle(ctx, cycleStarted, "error")
				logger.Error("poll cycle failed while retrieving connector statuses", "error", err, "duration", time.Since(cycleStarted))
				continue
			}

			cycleOutcome := "success"
			logger.Debug("connector statuses retrieved", "count", len(connectorStatuses))
			failedTargets = logRecoveries(failedTargets, connectorStatuses, logger)
			pruneBackoffsForMissingStatuses(&poller.backoffFilter, connectorStatuses)
			resetBackoffsForHealthyStatuses(&poller.backoffFilter, connectorStatuses)
			connectorRemediationActions := actions.GenerateActionsFromStatuses(connectorStatuses, configuration.PollingBehavior.RestartFailedTasks, logger)
			logger.Debug("remediation actions determined", "action_count", len(connectorRemediationActions))
			if poller.backoffFilter.BackoffConfig.Enabled {
				connectorRemediationActions = FilterByBackoffs(poller.backoffFilter, connectorRemediationActions, logger)
			}
			restartAttempts := 0
			for _, remediationAction := range connectorRemediationActions {
				if ctx.Err() != nil {
					metrics.RecordCycle(ctx, cycleStarted, "canceled")
					return
				}
				actionStarted := time.Now()
				result, err := actions.TakeAction(ctx, remediationAction, poller.connectClient, logger)
				requestDuration := time.Since(actionStarted)
				requestOutcome := "accepted"
				if err != nil {
					requestOutcome, cycleOutcome = "error", "error"
					if ctx.Err() != nil {
						requestOutcome, cycleOutcome = "canceled", "canceled"
					}
				}
				outcomeLogger := remediationAction.Logger(logger).With("request_attempted", result.RequestAttempted,
					"status_code", result.StatusCode, "duration", requestDuration)
				if result.RequestAttempted {
					metrics.RecordRestart(ctx, remediationAction, requestOutcome)
					restartAttempts++
					var backoffStatus backoff.Status
					switch remediationAction.Kind {
					case actions.RestartConnector:
						backoffStatus = poller.backoffFilter.UpdateBackoffStatus(result.AttemptedAt, remediationAction.ConnectorName, nil)
					case actions.RestartTask:
						backoffStatus = poller.backoffFilter.UpdateBackoffStatus(result.AttemptedAt, remediationAction.ConnectorName, &remediationAction.TaskID)
					default:
						remediationAction.Logger(logger).Error("unsupported remediation action")
					}
					outcomeLogger = outcomeLogger.With("attempt_count", backoffStatus.Attempts, "attempted_at", result.AttemptedAt)
				}
				switch {
				case ctx.Err() != nil && err != nil:
					outcomeLogger.Debug("restart request canceled", "error", err)
				case err != nil && result.StatusCode == http.StatusConflict:
					outcomeLogger.Warn("remediation request conflicted", "error", err)
				case err != nil:
					outcomeLogger.Error("remediation request failed", "error", err)
				case remediationAction.Kind == actions.RestartTask:
					outcomeLogger.Info("task restart request accepted")
				default:
					outcomeLogger.Info("connector restart request accepted")
				}
			}
			metrics.RecordCycle(ctx, cycleStarted, cycleOutcome)
			logger.Debug("poll cycle completed", "outcome", cycleOutcome, "connector_count", len(connectorStatuses), "restart_attempts", restartAttempts, "duration", time.Since(cycleStarted))
		}
	}
}

// Track observed failures independently of restart attempts. Missing targets are
// forgotten; fetch errors never call this function or change recovery history.
func logRecoveries(previousFailures map[actions.RemediationAction]struct{}, statuses map[string]status.ConnectorStatus, logger *slog.Logger) map[actions.RemediationAction]struct{} {
	nextFailures := make(map[actions.RemediationAction]struct{}, len(previousFailures))
	observe := func(action actions.RemediationAction, state string) {
		_, wasFailed := previousFailures[action]
		if strings.EqualFold(state, "RUNNING") {
			if wasFailed {
				message := "connector recovered"
				if action.Kind == actions.RestartTask {
					message = "task recovered"
				}
				action.Logger(logger).Info(message)
			}
		} else if wasFailed || strings.EqualFold(state, "FAILED") {
			nextFailures[action] = struct{}{}
		}
	}
	for _, connector := range statuses {
		observe(actions.RemediationAction{ConnectorName: connector.Name, Kind: actions.RestartConnector}, connector.Connector.State)
		for _, task := range connector.Tasks {
			observe(actions.RemediationAction{ConnectorName: connector.Name, Kind: actions.RestartTask, TaskID: task.ID}, task.State)
		}
	}
	return nextFailures
}

func logPollingSettings(logger *slog.Logger, message string, configuration config.ApplicationConfiguration) {
	logger.Info(message,
		"poll_interval", configuration.PollingBehavior.Interval.String(),
		"request_timeout", configuration.CommunicationConfig.RequestTimeout.String(),
		"restart_failed_tasks", configuration.PollingBehavior.RestartFailedTasks,
		"backoff_enabled", configuration.PollingBehavior.Backoff.Enabled,
		"backoff_base_delay", configuration.PollingBehavior.Backoff.BaseDelay.String(),
		"backoff_max_delay", configuration.PollingBehavior.Backoff.MaxDelay.String(),
		"backoff_exponential", configuration.PollingBehavior.Backoff.Exponential,
	)
}

// Call only after successfully retrieving a complete status snapshot.
func pruneBackoffsForMissingStatuses(backoffFilter *backoff.Filter, connectorStatuses map[string]status.ConnectorStatus) {
	if len(backoffFilter.BackoffStatuses) == 0 {
		return
	}
	missingStatuses := maps.Clone(backoffFilter.BackoffStatuses)
	for _, connectorStatus := range connectorStatuses {
		delete(missingStatuses, connectorStatus.Name)
		for _, taskStatus := range connectorStatus.Tasks {
			delete(missingStatuses, fmt.Sprintf(backoff.TaskKeyFormat, connectorStatus.Name, taskStatus.ID))
		}
	}
	for key := range missingStatuses {
		delete(backoffFilter.BackoffStatuses, key)
	}
}

func resetBackoffsForHealthyStatuses(backoffFilter *backoff.Filter, connectorStatuses map[string]status.ConnectorStatus) {
	for _, connectorStatus := range connectorStatuses {
		if strings.EqualFold(connectorStatus.Connector.State, "RUNNING") {
			backoffFilter.ResetBackoffStatus(connectorStatus.Name, nil)
		}

		for _, taskStatus := range connectorStatus.Tasks {
			if strings.EqualFold(taskStatus.State, "RUNNING") {
				backoffFilter.ResetBackoffStatus(connectorStatus.Name, &taskStatus.ID)
			}
		}
	}
}

func FilterByBackoffs(backoffFilter backoff.Filter, originalActions []actions.RemediationAction, logger *slog.Logger) []actions.RemediationAction {
	var filteredActions []actions.RemediationAction
	for _, action := range originalActions {
		switch action.Kind {
		case actions.RestartConnector:
			if !backoffFilter.IsInBackoffWindow(action.ConnectorName, nil) {
				filteredActions = append(filteredActions, action)
			} else {
				action.Logger(logger).Debug("connector restart skipped because it is in the backoff window")
			}

		case actions.RestartTask:
			if !backoffFilter.IsInBackoffWindow(action.ConnectorName, &action.TaskID) {
				filteredActions = append(filteredActions, action)
			} else {
				action.Logger(logger).Debug("task restart skipped because it is in the backoff window")
			}
		default:
			action.Logger(logger).Error("cannot filter unsupported remediation action")
		}
	}
	return filteredActions
}
