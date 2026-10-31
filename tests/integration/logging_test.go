package integration

import (
	"strings"
	"testing"
)

func testConfigurationLogging(t *testing.T, s *suite) {
	s.resetAfter(t)
	t.Cleanup(func() {
		code, body, _ := request(s.client, "DELETE", s.api+"/clusters/logging-test", nil, apiUsername, apiPassword, "")
		if code != 204 && code != 404 {
			t.Errorf("cleanup logging cluster: %d %s", code, body)
		}
	})
	applicationEvent := " INFO  Application configuration updated\n"
	clusterEvent := " INFO  Connect cluster configured | "
	removedEvent := " INFO  Connect cluster removed | "
	check := func(method, path, body string, status int, event string, fields ...string) {
		t.Helper()
		before := logs(t, s.healer)
		s.apiRequest(t, method, path, body, status)
		after := logs(t, s.healer)
		for _, header := range []string{applicationEvent, clusterEvent, removedEvent} {
			want := strings.Count(before, header)
			if header == event {
				want++
			}
			if got := strings.Count(after, header); got != want {
				t.Fatalf("%s %s: %q events = %d, want %d\n%s", method, path, header, got, want, after)
			}
		}
		if event == "" {
			return
		}
		snapshot := after[strings.LastIndex(after, event):]
		// Restrict assertions to this event's indented continuation lines.
		lines := strings.Split(snapshot, "\n")
		end := 1
		for end < len(lines) && strings.HasPrefix(lines[end], "    ") {
			end++
		}
		snapshot = strings.Join(lines[:end], "\n")
		for _, field := range fields {
			if !strings.Contains(snapshot, field) {
				t.Errorf("%s %s: missing %q in %s", method, path, field, snapshot)
			}
		}
		other := "Application:"
		if event == applicationEvent {
			other = "Connect clusters:"
		}
		if strings.Contains(snapshot, other) {
			t.Fatalf("manager logged unrelated state: %s", snapshot)
		}
		for _, secret := range []string{apiUsername, apiPassword, connectUsername, connectPassword} {
			if strings.Contains(after, secret) {
				t.Fatal("logs exposed credentials")
			}
		}
	}
	check("PATCH", "/config", `{"communicationConfig":{"requestTimeout":"3s"}}`, 204, applicationEvent, "Request timeout: 3s", "Telemetry: enabled", "API authentication: enabled")
	check("PATCH", "/config", `{"communicationConfig":{"requestTimeout":"3s"}}`, 204, "")
	check("PATCH", "/config", `{"communicationConfig":{"requestTimeout":"0s"}}`, 400, "")
	cluster := clusterJSON("connect-secondary", "8443", connectPassword)
	check("PUT", "/clusters/logging-test", cluster, 201, clusterEvent, "Count: 2", "logging-test:", "test:", "Endpoint: https://connect-secondary:8443", "Endpoint: https://connect:8444", "TLS verification: enabled (system trust store)")
	check("PUT", "/clusters/logging-test", cluster, 204, "")
	check("PUT", "/clusters/logging-test", `{}`, 400, "")
	check("PUT", "/clusters/logging-test", clusterJSON("connect-secondary", "8444", connectPassword), 204, clusterEvent, "Count: 2", "Endpoint: https://connect-secondary:8444")
	check("DELETE", "/clusters/logging-test", "", 204, removedEvent, "Count: 1", "test:")
	check("DELETE", "/clusters/logging-test", "", 404, "")
	check("PATCH", "/config", `{"loggingConfig":{"level":"WARN"}}`, 204, "")
	check("PUT", "/clusters/logging-test", cluster, 201, "")
	check("PATCH", "/config", `{"loggingConfig":{"level":"ERROR"}}`, 204, "")
	check("PATCH", "/config", `{"loggingConfig":{"level":"INFO"}}`, 204, applicationEvent, "Log level: INFO", "Request timeout: 3s")
	check("DELETE", "/clusters/logging-test", "", 204, removedEvent, "Count: 1")
}
