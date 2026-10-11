package integration

import (
	"context"
	"strings"
	"testing"
	"time"
)

func testRequestFailures(t *testing.T, s *suite) {
	for _, fault := range []struct{ mode, level, code, message string }{
		{"restart-401", "ERROR", "401", "remediation request failed"},
		{"restart-409", "WARN", "409", "remediation request conflicted"},
		{"restart-500", "ERROR", "500", "remediation request failed"},
		{"restart-redirect", "ERROR", "302", "remediation request failed"},
		{"restart-disconnect", "ERROR", "0", "remediation request failed"},
		{"restart-delay", "ERROR", "0", "remediation request failed"},
	} {
		t.Run(fault.mode, func(t *testing.T) {
			s.resetAfter(t)
			s.patch(t, `{"pollingBehavior":{"restartFailedTasks":true}}`)
			f := s.connector(t, s.primary, 1)
			s.setMode(t, fault.mode)
			f.marker(t, "fail-task", true)
			f.wait(t, "RUNNING", "FAILED")
			eventually(t, 20*time.Second, "four outbound attempts despite failures", func() bool { return len(s.attempts(t, f, fault.mode)) >= 4 })
			attempts := s.attempts(t, f, fault.mode)
			checkBackoff(t, attempts, []time.Duration{time.Second, 2 * time.Second, 2 * time.Second})
			if len(f.starts(t, "task")) != 1 {
				t.Fatal("fault gateway allowed a real restart")
			}
			eventually(t, 5*time.Second, "correlated fourth failure outcome", func() bool {
				return matchingLog(logs(t, s.healer), "level="+fault.level, "msg=\""+fault.message+"\"", "connect_cluster=test", "connector="+f.name, "task_id=0", "attempt_count=4", "status_code="+fault.code, "request_attempted=true", "attempted_at=", "duration=", "action=restart_task")
			})
			for _, request := range s.requests(t) {
				if request.path == "/redirect-target" {
					t.Fatal("Healer followed a redirect")
				}
			}
			f.marker(t, "fail-task", false)
			s.setMode(t, "pass")
			f.wait(t, "RUNNING", "RUNNING")
		})
	}
}

func testInvalidSnapshots(t *testing.T, s *suite) {
	for _, mode := range []string{"status-malformed", "status-null", "status-missing", "status-500", "status-redirect"} {
		t.Run(mode, func(t *testing.T) {
			s.resetAfter(t)
			s.patch(t, `{"pollingBehavior":{"restartFailedTasks":true}}`)
			f := s.connector(t, s.primary, 1)
			// Record a first rejected attempt, then prevent valid snapshots. A
			// fetch/decode error must not reset its backoff history.
			s.setMode(t, "restart-500")
			f.marker(t, "fail-task", true)
			eventually(t, 5*time.Second, "first attempt recorded", func() bool {
				return matchingLog(logs(t, s.healer), "connector="+f.name, "attempt_count=1", "status_code=500")
			})
			s.setMode(t, mode)
			settle(t, 700*time.Millisecond)
			before := len(s.attempts(t, f, ""))
			remains(t, 2200*time.Millisecond, "invalid status snapshot generated remediation", func() bool { return len(s.attempts(t, f, "")) == before && len(f.starts(t, "task")) == 1 })
			// Keep the task failing so we can inspect the next recorded count.
			s.setMode(t, "pass")
			eventually(t, 8*time.Second, "history survives fetch failure", func() bool {
				return matchingLog(logs(t, s.healer), "connector="+f.name, "attempt_count=2", "status_code=204")
			})
			f.marker(t, "fail-task", false)
			f.wait(t, "RUNNING", "RUNNING")
			for _, request := range s.requests(t) {
				if request.path == "/redirect-target" {
					t.Fatal("status redirect was followed")
				}
			}
		})
	}
	t.Run("ValidEmptySnapshotPrunesHistory", func(t *testing.T) {
		s.resetAfter(t)
		s.patch(t, `{"pollingBehavior":{"restartFailedTasks":true}}`)
		f := s.connector(t, s.primary, 1)
		s.setMode(t, "restart-500")
		f.marker(t, "fail-task", true)
		eventually(t, 8*time.Second, "two rejected attempts", func() bool {
			return matchingLog(logs(t, s.healer), "connector="+f.name, "attempt_count=2", "status_code=500")
		})
		s.setMode(t, "status-empty")
		settle(t, 1200*time.Millisecond)
		before := len(s.attempts(t, f, ""))
		remains(t, time.Second, "empty snapshot generated action", func() bool { return len(s.attempts(t, f, "")) == before })
		s.setMode(t, "pass")
		eventually(t, 5*time.Second, "removed target history pruned", func() bool {
			return matchingLog(logs(t, s.healer), "connector="+f.name, "attempt_count=1", "status_code=204")
		})
	})
}

func testIsolation(t *testing.T, s *suite) {
	s.resetAfter(t)
	s.patch(t, `{"pollingBehavior":{"restartFailedTasks":true}}`)
	primary := s.connector(t, s.primary, 1)
	// Same connector name in independent clusters must have independent identity.
	secondary := s.namedConnector(t, s.secondary, primary.name, 1)
	s.apiRequest(t, "PUT", "/clusters/secondary", clusterJSON("connect-secondary", "8443", connectPassword), 201)
	t.Cleanup(func() { s.apiRequest(t, "DELETE", "/clusters/secondary", "", 204) })
	s.patch(t, `{"communicationConfig":{"requestTimeout":"400ms"}}`)
	s.setMode(t, "status-delay")
	eventually(t, 8*time.Second, "outbound timeout logged", func() bool { return matchingLog(logs(t, s.healer), "connect_cluster=test", "Client.Timeout") })
	s.apiRequest(t, "GET", "/config", "", 200)
	s.patch(t, `{"communicationConfig":{"requestTimeout":"5s"}}`)
	secondary.marker(t, "fail-task", true)
	eventually(t, 15*time.Second, "secondary progresses while primary blocked", func() bool { return len(secondary.starts(t, "task")) >= 4 })
	if len(primary.starts(t, "task")) != 1 || !primary.state("RUNNING", "RUNNING") {
		t.Fatal("secondary failure affected primary")
	}
	if !matchingLog(logs(t, s.healer), "connect_cluster=secondary", "connector="+primary.name, "task_id=0", "attempt_count=3", "action=restart_task") {
		t.Fatal("multi-cluster outcome lacks target identity")
	}
	secondary.marker(t, "fail-task", false)
	secondary.wait(t, "RUNNING", "RUNNING")
	s.setMode(t, "pass")
	s.apiRequest(t, "PUT", "/clusters/test", clusterJSON("wrong-host", "8443", connectPassword), 204)
	eventually(t, 8*time.Second, "TLS hostname failure", func() bool {
		return matchingLog(logs(t, s.healer), "connect_cluster=test", "certificate", "wrong-host")
	})
	s.apiRequest(t, "PUT", "/clusters/test", clusterJSON("connect", "8444", connectPassword), 204)
	primary.marker(t, "fail-task", true)
	eventually(t, 10*time.Second, "primary recovers after TLS config corrected", func() bool { return len(primary.starts(t, "task")) >= 2 })
	primary.marker(t, "fail-task", false)
	primary.wait(t, "RUNNING", "RUNNING")

	// Deleting a worker cancels its current outbound request; it must not wait
	// for the configured five-second request timeout to finish naturally.
	s.setMode(t, "status-delay")
	started := time.Now()
	eventually(t, 8*time.Second, "primary request blocked before deletion", func() bool {
		for _, r := range s.requests(t) {
			if r.mode == "status-delay" && r.at.After(started) {
				return true
			}
		}
		return false
	})
	started = time.Now()
	s.apiRequest(t, "DELETE", "/clusters/test", "", 204)
	if elapsed := time.Since(started); elapsed >= 4*time.Second {
		t.Errorf("DELETE failed to cancel an in-flight request promptly: %v", elapsed)
	}
	s.setMode(t, "pass")
	s.apiRequest(t, "PUT", "/clusters/test", clusterJSON("connect", "8444", connectPassword), 201)
}

func testBrokerOutage(t *testing.T, s *suite) {
	s.resetAfter(t)
	f := s.connector(t, s.primary, 1)
	timeout := 10 * time.Second
	if err := s.broker.Stop(t.Context(), &timeout); err != nil {
		t.Fatal(err)
	}
	restarted := false
	t.Cleanup(func() {
		if !restarted {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := s.broker.Start(ctx); err != nil {
				t.Errorf("restore broker: %v", err)
			}
		}
	})
	s.apiRequest(t, "GET", "/config", "", 200)
	s.apiRequest(t, "GET", "/clusters", "", 200)
	s.patch(t, `{"pollingBehavior":{"restartFailedTasks":true}}`)
	// The consumer starts at the latest offset, so the check requires fresh
	// records after restarting the same broker (its data layer is retained).
	if err := s.broker.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted = true
	f.wait(t, "RUNNING", "RUNNING")
	output := execOK(t, s.broker, "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "broker:9092", "--topic", "healer-e2e-records", "--max-messages", "1", "--timeout-ms", "20000")
	if !strings.Contains(output, "record-") {
		t.Fatalf("no fresh record after broker restored: %s", output)
	}
}

func matchingLog(logs string, fields ...string) bool {
	return countMatchingLogs(logs, fields...) > 0
}

func countMatchingLogs(logs string, fields ...string) int {
	count := 0
	for _, line := range strings.Split(logs, "\n") {
		matches := true
		for _, field := range fields {
			if !strings.Contains(line, field) {
				matches = false
				break
			}
		}
		if matches {
			count++
		}
	}
	return count
}
