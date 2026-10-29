package integration

import (
	"strings"
	"testing"
	"time"
)

func TestIntegration(t *testing.T) {
	s := newSuite(t)
	t.Run("API", func(t *testing.T) { testAPI(t, s) })
	t.Run("HTTPAuthentication", func(t *testing.T) { testHTTPAuthentication(t, s) })
	t.Run("KafkaAndTLS", func(t *testing.T) {
		s.resetAfter(t)
		code, _, _ := request(s.primary.client, "GET", s.primary.endpoint+"/connectors", nil, "", "", "")
		if code != 401 {
			t.Fatalf("Connect Basic auth is not active: %d", code)
		}
		code, data, _ := s.primary.request("GET", "/connector-plugins", nil)
		if code != 200 || !strings.Contains(string(data), "e2e.HealerTestConnector") {
			t.Fatalf("fixture plugin missing: %d %s", code, data)
		}
		f := s.connector(t, s.primary, 1)
		output := execOK(t, s.broker, "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "broker:9092", "--topic", "healer-e2e-records", "--from-beginning", "--max-messages", "1", "--timeout-ms", "15000")
		if !strings.Contains(output, "record-") || len(f.starts(t, "task")) != 1 {
			t.Fatalf("no real source record/task start: %s", output)
		}
	})
	t.Run("TaskPolicyBackoffAndRecovery", func(t *testing.T) {
		s.resetAfter(t)
		f := s.connector(t, s.primary, 1)
		if matchingLog(logs(t, s.healer), "msg=\"task recovered\"", "connector="+f.name) {
			t.Fatal("initially healthy task logged a recovery")
		}
		f.marker(t, "fail-task", true)
		f.wait(t, "RUNNING", "FAILED")
		remains(t, 1500*time.Millisecond, "disabled task policy issued a restart", func() bool { return len(s.attempts(t, f, "")) == 0 && len(f.starts(t, "task")) == 1 })
		s.patch(t, `{"pollingBehavior":{"restartFailedTasks":true}}`)
		eventually(t, 20*time.Second, "four real task restart attempts", func() bool { return len(f.starts(t, "task")) >= 5 })
		attempts := s.attempts(t, f, "")
		checkBackoff(t, attempts, []time.Duration{time.Second, 2 * time.Second, 2 * time.Second})
		for _, attempt := range attempts {
			if attempt.path != "/connectors/"+f.name+"/tasks/0/restart" {
				t.Errorf("wrong task restart path: %s", attempt.path)
			}
		}
		outcomes := logs(t, s.healer)
		if !matchingLog(outcomes, "connector="+f.name, "attempt_count=4", "task restart request accepted") {
			t.Fatalf("missing accepted-attempt logging: %s", outcomes)
		}
		for _, message := range []string{"remediation action determined", "requesting task restart", "task restart skipped because it is in the backoff window"} {
			if !matchingLog(outcomes, "msg=\""+message+"\"", "connect_cluster=test", "connector="+f.name, "task_id=0", "action=restart_task") {
				t.Errorf("missing correlated log for %s", message)
			}
		}
		f.marker(t, "fail-task", false)
		f.wait(t, "RUNNING", "RUNNING")
		eventually(t, 5*time.Second, "one observed recovery", func() bool {
			return matchingLog(logs(t, s.healer), "msg=\"task recovered\"", "connect_cluster=test", "connector="+f.name, "task_id=0")
		})
		remains(t, 1200*time.Millisecond, "healthy task was restarted", func() bool { return f.state("RUNNING", "RUNNING") })
		before := len(s.attempts(t, f, ""))
		logOffset := len(logs(t, s.healer))
		if countMatchingLogs(logs(t, s.healer), "msg=\"task recovered\"", "connector="+f.name) != 1 {
			t.Fatal("recovery logged more than once per episode")
		}
		f.marker(t, "fail-task", true)
		eventually(t, 10*time.Second, "two new attempts after healthy reset", func() bool { return len(s.attempts(t, f, "")) >= before+2 })
		checkBackoff(t, s.attempts(t, f, "")[before:], []time.Duration{time.Second})
		// Inspect the reset count instead of relying on a tight CI timing bound.
		if !matchingLog(logs(t, s.healer)[logOffset:], "connector="+f.name, "attempt_count=1", "task restart request accepted") {
			t.Fatal("healthy reset retained the old attempt count")
		}
	})
	t.Run("ConnectorFailureIgnoresTaskPolicy", func(t *testing.T) {
		s.resetAfter(t)
		f := s.connector(t, s.primary, 1)
		f.marker(t, "fail-connector", true)
		code, body, _ := s.primary.request("POST", "/connectors/"+f.name+"/restart?includeTasks=true&onlyFailed=false", nil)
		if code != 202 {
			t.Fatalf("induce connector failure: %d %s", code, body)
		}
		eventually(t, time.Minute, "real FAILED connector and healer attempts", func() bool { return len(f.starts(t, "connector")) >= 4 })
		attempts := s.attempts(t, f, "")
		if len(attempts) < 2 {
			t.Fatal("no connector remediation while task restarts disabled")
		}
		for _, attempt := range attempts {
			if attempt.path != "/connectors/"+f.name+"/restart?includeTasks=true&onlyFailed=true" {
				t.Errorf("incorrect connector restart: %s", attempt.path)
			}
		}
		f.marker(t, "fail-connector", false)
		f.wait(t, "RUNNING", "RUNNING")
		eventually(t, 5*time.Second, "connector recovery logged", func() bool { return strings.Contains(logs(t, s.healer), "msg=\"connector recovered\"") })
	})
	t.Run("PausedAndStoppedConnectors", func(t *testing.T) {
		s.resetAfter(t)
		s.patch(t, `{"pollingBehavior":{"restartFailedTasks":true}}`)
		f := s.connector(t, s.primary, 1)
		for _, operation := range []struct{ route, state string }{{"pause", "PAUSED"}, {"stop", "STOPPED"}} {
			code, data, _ := s.primary.request("PUT", "/connectors/"+f.name+"/"+operation.route, nil)
			if code != 202 && code != 204 {
				t.Fatalf("%s: %d %s", operation.route, code, data)
			}
			eventually(t, 20*time.Second, operation.state, func() bool {
				code, body, _ := s.primary.request("GET", "/connectors/"+f.name+"/status", nil)
				return code == 200 && strings.Contains(string(body), `"state":"`+operation.state+`"`)
			})
			remains(t, 1500*time.Millisecond, operation.state+" connector was restarted", func() bool { return len(s.attempts(t, f, "")) == 0 })
		}
	})
	t.Run("OnlyFailedTaskIsRestarted", func(t *testing.T) {
		s.resetAfter(t)
		s.patch(t, `{"pollingBehavior":{"restartFailedTasks":true}}`)
		f := s.connector(t, s.primary, 3)
		f.marker(t, "fail-task-1", true)
		eventually(t, 15*time.Second, "failed task 1 restarted", func() bool { return len(f.starts(t, "task-1")) >= 3 })
		if len(f.starts(t, "task-0")) != 1 || len(f.starts(t, "task-2")) != 1 {
			t.Fatal("healthy sibling tasks were restarted")
		}
		for _, attempt := range s.attempts(t, f, "") {
			if attempt.path != "/connectors/"+f.name+"/tasks/1/restart" {
				t.Fatalf("wrong task restarted: %s", attempt.path)
			}
		}
		f.marker(t, "fail-task-1", false)
		f.wait(t, "RUNNING", "RUNNING")
	})
	t.Run("FixedAndDisabledBackoff", func(t *testing.T) {
		s.resetAfter(t)
		s.patch(t, `{"pollingBehavior":{"restartFailedTasks":true,"backoff":{"exponential":false}}}`)
		f := s.connector(t, s.primary, 1)
		f.marker(t, "fail-task", true)
		eventually(t, 15*time.Second, "four fixed-delay attempts", func() bool { return len(s.attempts(t, f, "")) >= 4 })
		checkBackoff(t, s.attempts(t, f, ""), []time.Duration{time.Second, time.Second, time.Second})
		s.patch(t, `{"pollingBehavior":{"backoff":{"enabled":false}}}`)
		before := len(s.attempts(t, f, ""))
		eventually(t, 6*time.Second, "disabled-backoff attempts", func() bool { return len(s.attempts(t, f, "")) >= before+4 })
		attempts := s.attempts(t, f, "")[before:]
		if attempts[3].at.Sub(attempts[0].at) >= 2700*time.Millisecond {
			t.Fatalf("disabled backoff still throttles attempts: %v", attempts[3].at.Sub(attempts[0].at))
		}
		s.patch(t, `{"pollingBehavior":{"restartFailedTasks":false}}`)
		// Wait for the in-flight poll to finish before asserting the stable policy.
		settle(t, time.Second)
		before = len(s.attempts(t, f, ""))
		remains(t, 1500*time.Millisecond, "runtime policy off still restarts tasks", func() bool { return len(s.attempts(t, f, "")) == before })
	})
	t.Run("RuntimeIntervalAndLogLevel", func(t *testing.T) {
		s.resetAfter(t)
		s.patch(t, `{"pollingBehavior":{"interval":"3s"},"loggingConfig":{"level":"WARN"}}`)
		settle(t, time.Second)
		debug := strings.Count(logs(t, s.healer), "level=DEBUG")
		requestCount := len(s.requests(t))
		remains(t, time.Second, "WARN level continued DEBUG logs", func() bool { return strings.Count(logs(t, s.healer), "level=DEBUG") == debug })
		if len(s.requests(t))-requestCount > 1 {
			t.Fatal("poll interval update was not applied")
		}
		s.patch(t, `{"pollingBehavior":{"interval":"200ms"},"loggingConfig":{"level":"DEBUG"}}`)
		requestCount = len(s.requests(t))
		eventually(t, 5*time.Second, "faster polling and restored DEBUG", func() bool {
			return len(s.requests(t))-requestCount >= 4 && strings.Count(logs(t, s.healer), "level=DEBUG") > debug
		})
	})
	t.Run("ClusterLifecycle", func(t *testing.T) { testClusterLifecycle(t, s) })
	t.Run("RequestFailures", func(t *testing.T) { testRequestFailures(t, s) })
	t.Run("InvalidSnapshots", func(t *testing.T) { testInvalidSnapshots(t, s) })
	t.Run("TimeoutTLSAndClusterIsolation", func(t *testing.T) { testIsolation(t, s) })
	t.Run("BrokerOutage", func(t *testing.T) { testBrokerOutage(t, s) })
	t.Run("Telemetry", func(t *testing.T) { testTelemetry(t, s) })
	t.Run("StartupAndShutdown", func(t *testing.T) { testStartup(t, s) })
}

func checkBackoff(t *testing.T, attempts []proxyRequest, delays []time.Duration) {
	t.Helper()
	if len(attempts) < len(delays)+1 {
		t.Fatalf("got %d attempts, need %d", len(attempts), len(delays)+1)
	}
	for index, delay := range delays {
		gap := attempts[index+1].at.Sub(attempts[index].at)
		// Request timestamps are milliseconds; allow rounding, not early retries.
		if gap < delay-50*time.Millisecond {
			t.Errorf("attempt %d backoff %v, minimum %v", index+2, gap, delay)
		}
	}
}

func settle(t *testing.T, duration time.Duration) {
	t.Helper()
	// Only deliberate observation windows use timers; readiness uses eventually.
	select {
	case <-time.After(duration):
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
}

func testClusterLifecycle(t *testing.T, s *suite) {
	s.resetAfter(t)
	s.patch(t, `{"pollingBehavior":{"restartFailedTasks":true}}`)
	f := s.connector(t, s.primary, 1)
	s.apiRequest(t, "DELETE", "/clusters/test", "", 204)
	// Ensure restoration uses the correct creation status if the test fails here.
	t.Cleanup(func() {
		code, body, _ := request(s.client, "PUT", s.api+"/clusters/test", []byte(clusterJSON("connect", "8444", connectPassword)), apiUsername, apiPassword, "application/json")
		if code != 201 && code != 204 {
			t.Errorf("restore primary cluster: %d %s", code, body)
		}
	})
	f.marker(t, "fail-task", true)
	f.wait(t, "RUNNING", "FAILED")
	remains(t, 1500*time.Millisecond, "deleted cluster still polls/restarts", func() bool { return len(s.attempts(t, f, "")) == 0 })
	s.apiRequest(t, "PUT", "/clusters/test", clusterJSON("connect", "8444", connectPassword), 201)
	eventually(t, 10*time.Second, "new poller starts", func() bool { return len(s.attempts(t, f, "")) >= 3 })
	started := strings.Count(logs(t, s.healer), "msg=\"polling started\"")
	s.apiRequest(t, "PUT", "/clusters/test", clusterJSON("connect", "8444", connectPassword), 204)
	settle(t, 700*time.Millisecond)
	if strings.Count(logs(t, s.healer), "msg=\"polling started\"") != started {
		t.Fatal("identical PUT restarted the poller")
	}
	s.apiRequest(t, "PUT", "/clusters/test", clusterJSON("connect", "8444", "wrong-password"), 204)
	before := len(f.starts(t, "task"))
	eventually(t, 5*time.Second, "wrong outbound credentials reported", func() bool { return strings.Contains(logs(t, s.healer), "401 Unauthorized") })
	remains(t, 1200*time.Millisecond, "wrong credentials restarted a real task", func() bool { return len(f.starts(t, "task")) == before })
	s.apiRequest(t, "PUT", "/clusters/test", clusterJSON("connect", "8444", connectPassword), 204)
	f.marker(t, "fail-task", false)
	f.wait(t, "RUNNING", "RUNNING")
	if strings.Count(logs(t, s.healer), "msg=\"polling started\"") < started+2 {
		t.Fatal("changed PUT did not replace workers")
	}
}
