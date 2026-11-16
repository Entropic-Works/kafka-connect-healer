package backoff

import (
	"fmt"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
)

const TaskKeyFormat string = "ConnectorName-%s_TaskID-%d"

type Status struct {
	LastAttemptTime time.Time
	Attempts        int
}

type Filter struct {
	BackoffConfig   config.BackoffConfiguration
	BackoffStatuses map[string]Status
}

func (backoffFilter *Filter) UpdateBackoffStatus(attemptTime time.Time, connectorName string, taskID *int) Status {
	key := getKey(connectorName, taskID)
	status := backoffFilter.BackoffStatuses[key]
	status.LastAttemptTime = attemptTime
	status.Attempts++
	backoffFilter.BackoffStatuses[key] = status
	return status
}

func (backoffFilter *Filter) ResetBackoffStatus(connectorName string, taskID *int) {
	key := getKey(connectorName, taskID)
	delete(backoffFilter.BackoffStatuses, key)
}

func (backoffFilter *Filter) IsInBackoffWindow(connectorName string, taskID *int) bool {
	return time.Now().Before(backoffFilter.NextAttemptTime(connectorName, taskID))
}

func (backoffFilter *Filter) NextAttemptTime(connectorName string, taskID *int) time.Time {
	backoffStatus := backoffFilter.getBackoffStatus(connectorName, taskID)
	if backoffStatus.LastAttemptTime.IsZero() {
		return time.Time{}
	}
	return determineNextActionTime(
		backoffStatus.LastAttemptTime,
		backoffFilter.BackoffConfig.BaseDelay.Duration(),
		backoffFilter.BackoffConfig.MaxDelay.Duration(),
		backoffStatus.Attempts,
		backoffFilter.BackoffConfig.Exponential,
	)
}

func (backoffFilter *Filter) getBackoffStatus(
	connectorName string, taskID *int,
) Status {
	return backoffFilter.BackoffStatuses[getKey(connectorName, taskID)]
}

func determineNextActionTime(
	lastAttemptTime time.Time,
	baseDelay time.Duration,
	maxDelay time.Duration,
	attempts int,
	exponential bool,
) time.Time {
	delay := baseDelay

	if maxDelay > 0 && delay > maxDelay {
		delay = maxDelay
	}

	if exponential {
		for attempt := 1; attempt < attempts && delay < maxDelay; attempt++ {
			if delay > maxDelay/2 {
				delay = maxDelay
				break
			}
			delay *= 2
		}
	}

	return lastAttemptTime.Add(delay)
}

func getKey(connectorName string, taskID *int) string {
	if taskID == nil {
		return connectorName
	} else {
		return fmt.Sprintf(TaskKeyFormat, connectorName, *taskID)
	}
}
