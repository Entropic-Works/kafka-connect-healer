package poll

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll/actions"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll/backoff"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll/status"
)

func TestPollStopsBeforeNextTick(t *testing.T) {
	configuration := config.DefaultConfiguration()
	configuration.PollingBehavior.Interval = config.Duration(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan struct{})
	go func() {
		NewConnectClusterPoller("default", connectcluster.DefaultConfiguration()).Poll(ctx, config.NewManager(configuration))
		close(finished)
	}()

	cancel()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("Poll did not stop without waiting for the next tick")
	}
}

func TestPollCancellationInterruptsStatusRequest(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(previousLogger)
	requestStarted := make(chan struct{})
	requestCanceled := make(chan struct{})
	handlerCtx, stopHandler := context.WithCancel(context.Background())
	var startedOnce, canceledOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.RequestURI() != "/connectors?expand=status" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.RequestURI())
			w.WriteHeader(http.StatusNotFound)
			return
		}
		startedOnce.Do(func() { close(requestStarted) })
		select {
		case <-r.Context().Done():
			canceledOnce.Do(func() { close(requestCanceled) })
		case <-handlerCtx.Done():
		}
	}))
	defer server.Close()
	defer stopHandler()
	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	configuration := config.DefaultConfiguration()
	clusterConfiguration := connectcluster.Configuration{Host: serverURL.Hostname(), Port: serverURL.Port()}
	configuration.PollingBehavior.Interval = config.Duration(10 * time.Millisecond)
	// A long timeout proves cancellation ends the request, rather than timeout.
	configuration.CommunicationConfig.RequestTimeout = config.Duration(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan struct{})
	go func() {
		NewConnectClusterPoller("test", clusterConfiguration).Poll(ctx, config.NewManager(configuration))
		close(finished)
	}()
	select {
	case <-requestStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Poll did not start a status request")
	}

	cancel()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("Poll did not stop after cancellation during a status request")
	}
	select {
	case <-requestCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("status request did not observe cancellation")
	}
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("normal cancellation logged an error: %s", logs.String())
	}
}

func TestFilterByBackoffs(t *testing.T) {
	recentAttempt := time.Now().Add(-30 * time.Minute)
	backoffConfig := config.BackoffConfiguration{BaseDelay: config.Duration(time.Hour)}

	tests := []struct {
		name     string
		statuses map[string]backoff.Status
		actions  []actions.RemediationAction
		expected []actions.RemediationAction
	}{
		{
			name: "actions without a prior attempt are retained",
			actions: []actions.RemediationAction{
				{ConnectorName: "source-connector", Kind: actions.RestartConnector},
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 1},
			},
			expected: []actions.RemediationAction{
				{ConnectorName: "source-connector", Kind: actions.RestartConnector},
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 1},
			},
		},
		{
			name: "connector restart in the window is omitted",
			statuses: map[string]backoff.Status{
				"source-connector": {
					LastAttemptTime: recentAttempt,
				},
			},
			actions: []actions.RemediationAction{
				{ConnectorName: "source-connector", Kind: actions.RestartConnector},
			},
			expected: nil,
		},
		{
			name: "task restart in the window is omitted",
			statuses: map[string]backoff.Status{
				fmt.Sprintf(backoff.TaskKeyFormat, "sink-connector", 1): {
					LastAttemptTime: recentAttempt,
				},
			},
			actions: []actions.RemediationAction{
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 1},
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 2},
			},
			expected: []actions.RemediationAction{
				{ConnectorName: "sink-connector", Kind: actions.RestartTask, TaskID: 2},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filter := backoff.Filter{
				BackoffConfig:   backoffConfig,
				BackoffStatuses: test.statuses,
			}

			if got := FilterByBackoffs(filter, test.actions, slog.Default()); !slices.Equal(got, test.expected) {
				t.Fatalf("FilterByBackoffs() = %#v, want %#v", got, test.expected)
			}
		})
	}
}

func TestPruneBackoffsForMissingStatuses(t *testing.T) {
	connectorAttempt := backoff.Status{LastAttemptTime: time.Now(), Attempts: 3}
	taskAttempt := backoff.Status{LastAttemptTime: time.Now().Add(-time.Minute), Attempts: 2}
	taskKey := fmt.Sprintf(backoff.TaskKeyFormat, "connector-a", 0)
	tests := []struct {
		name     string
		statuses map[string]status.ConnectorStatus
		expected map[string]backoff.Status
	}{
		{
			name: "present connector and task retain their history",
			statuses: map[string]status.ConnectorStatus{
				"connector-a": {Name: "connector-a", Tasks: []status.TaskStatus{{ID: 0}}},
			},
			expected: map[string]backoff.Status{"connector-a": connectorAttempt, taskKey: taskAttempt},
		},
		{
			name: "deleted connector clears connector and task history",
			statuses: map[string]status.ConnectorStatus{
				"connector-b": {Name: "connector-b", Tasks: []status.TaskStatus{{ID: 0}}},
			},
			expected: map[string]backoff.Status{},
		},
		{
			name: "deleted task clears only its own history",
			statuses: map[string]status.ConnectorStatus{
				"connector-a": {Name: "connector-a", Tasks: []status.TaskStatus{{ID: 1}}},
			},
			expected: map[string]backoff.Status{"connector-a": connectorAttempt},
		},
		{
			name:     "empty successful snapshot clears all history",
			statuses: map[string]status.ConnectorStatus{},
			expected: map[string]backoff.Status{},
		},
		{
			name: "paused statuses are still present",
			statuses: map[string]status.ConnectorStatus{
				"connector-a": {
					Name: "connector-a", Connector: status.WorkerStatus{State: "PAUSED"},
					Tasks: []status.TaskStatus{{ID: 0, State: "PAUSED"}},
				},
			},
			expected: map[string]backoff.Status{"connector-a": connectorAttempt, taskKey: taskAttempt},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filter := backoff.Filter{
				BackoffStatuses: map[string]backoff.Status{"connector-a": connectorAttempt, taskKey: taskAttempt},
			}
			pruneBackoffsForMissingStatuses(&filter, test.statuses)
			if !maps.Equal(filter.BackoffStatuses, test.expected) {
				t.Fatalf("backoff statuses = %#v, want %#v", filter.BackoffStatuses, test.expected)
			}
		})
	}

	t.Run("no stored history remains unallocated", func(t *testing.T) {
		filter := backoff.Filter{}
		pruneBackoffsForMissingStatuses(&filter, nil)
		if filter.BackoffStatuses != nil {
			t.Fatalf("backoff statuses = %#v, want nil", filter.BackoffStatuses)
		}
	})
}

func TestResetBackoffsForHealthyStatuses(t *testing.T) {
	filter := backoff.Filter{
		BackoffStatuses: map[string]backoff.Status{
			"healthy-connector": {Attempts: 1},
			fmt.Sprintf(backoff.TaskKeyFormat, "healthy-connector", 1): {Attempts: 1},
			fmt.Sprintf(backoff.TaskKeyFormat, "healthy-connector", 2): {Attempts: 1},
			"failed-connector": {Attempts: 1},
			fmt.Sprintf(backoff.TaskKeyFormat, "failed-connector", 1): {Attempts: 1},
		},
	}
	connectorStatuses := map[string]status.ConnectorStatus{
		"healthy-connector": {
			Name:      "healthy-connector",
			Connector: status.WorkerStatus{State: "RUNNING"},
			Tasks: []status.TaskStatus{
				{ID: 1, State: "RUNNING"},
				{ID: 2, State: "FAILED"},
			},
		},
		"failed-connector": {
			Name:      "failed-connector",
			Connector: status.WorkerStatus{State: "FAILED"},
			Tasks:     []status.TaskStatus{{ID: 1, State: "FAILED"}},
		},
	}

	resetBackoffsForHealthyStatuses(&filter, connectorStatuses)

	expected := map[string]backoff.Status{
		fmt.Sprintf(backoff.TaskKeyFormat, "healthy-connector", 2): {Attempts: 1},
		"failed-connector": {Attempts: 1},
		fmt.Sprintf(backoff.TaskKeyFormat, "failed-connector", 1): {Attempts: 1},
	}
	if !maps.Equal(filter.BackoffStatuses, expected) {
		t.Fatalf("backoff statuses = %#v, want %#v", filter.BackoffStatuses, expected)
	}
}

func TestPollRemediationAndBackoffLifecycle(t *testing.T) {
	const failedStatuses = `{
		"connector-a": {"status": {"name": "connector-a", "connector": {"state": "FAILED"}, "tasks": [], "type": "source"}},
		"connector-b": {"status": {"name": "connector-b", "connector": {"state": "RUNNING"}, "tasks": [{"id": 2, "state": "FAILED"}], "type": "sink"}}
	}`
	const healthyStatuses = `{
		"connector-a": {"status": {"name": "connector-a", "connector": {"state": "RUNNING"}, "tasks": [], "type": "source"}},
		"connector-b": {"status": {"name": "connector-b", "connector": {"state": "RUNNING"}, "tasks": [{"id": 2, "state": "RUNNING"}], "type": "sink"}}
	}`

	type statusResponse struct {
		code int
		body string
	}
	requests := make(chan string, 16)
	responses := make(chan statusResponse)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RequestURI() == "/connectors?expand=status":
			requests <- "GET"
			select {
			case response := <-responses:
				w.WriteHeader(response.code)
				_, _ = w.Write([]byte(response.body))
			case <-r.Context().Done():
			}
		case r.Method == http.MethodPost:
			requests <- r.URL.RequestURI()
			if r.URL.Path == "/connectors/connector-a/restart" {
				w.WriteHeader(http.StatusInternalServerError)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Error("Poll did not stop after cancellation")
		}
		server.Close()
	})

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}

	clusterConfiguration := connectcluster.Configuration{
		Host: serverURL.Hostname(), Port: serverURL.Port(), TLS: connectcluster.TLSConfiguration{HTTPS: serverURL.Scheme == "https"},
	}
	pollConfig := config.ApplicationConfiguration{
		CommunicationConfig: config.CommunicationConfiguration{RequestTimeout: config.Duration(time.Second)},
		PollingBehavior: config.PollingBehavior{
			Interval:           config.Duration(10 * time.Millisecond),
			RestartFailedTasks: true,
			Backoff: config.BackoffConfiguration{
				Enabled:   true,
				BaseDelay: config.Duration(time.Hour),
				MaxDelay:  config.Duration(time.Hour),
			},
		},
	}
	go func() {
		NewConnectClusterPoller("test", clusterConfiguration).Poll(ctx, config.NewManager(pollConfig))
		close(finished)
	}()

	readRequest := func() string {
		t.Helper()
		select {
		case request := <-requests:
			return request
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for a Connect API request")
			return ""
		}
	}
	if request := readRequest(); request != "GET" {
		t.Fatalf("first request = %q, want GET", request)
	}

	checkCycle := func(name string, response statusResponse, expected []string) {
		t.Helper()
		select {
		case responses <- response:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: timed out sending status response", name)
		}

		var got []string
		for {
			request := readRequest()
			if request == "GET" {
				break // The next status request means this poll cycle has finished.
			}
			got = append(got, request)
		}
		slices.Sort(got)
		slices.Sort(expected)
		if !slices.Equal(got, expected) {
			t.Fatalf("%s: restart requests = %v, want %v", name, got, expected)
		}
	}

	restarts := []string{
		"/connectors/connector-a/restart?includeTasks=true&onlyFailed=true",
		"/connectors/connector-b/tasks/2/restart",
	}
	checkCycle("initial failure", statusResponse{http.StatusOK, failedStatuses}, restarts)
	checkCycle("failed statuses remain in backoff", statusResponse{http.StatusOK, failedStatuses}, nil)
	checkCycle("healthy statuses clear backoff", statusResponse{http.StatusOK, healthyStatuses}, nil)
	checkCycle("new failures are restarted", statusResponse{http.StatusOK, failedStatuses}, restarts)
	checkCycle("status retrieval error does not reset backoff", statusResponse{http.StatusInternalServerError, ""}, nil)
	checkCycle("failures after retrieval error remain in backoff", statusResponse{http.StatusOK, failedStatuses}, nil)
	checkCycle("malformed status response does not reset backoff", statusResponse{http.StatusOK, "{"}, nil)
	checkCycle("failures after decoding error remain in backoff", statusResponse{http.StatusOK, failedStatuses}, nil)
	checkCycle("null status response does not reset backoff", statusResponse{http.StatusOK, "null"}, nil)
	checkCycle("failures after null response remain in backoff", statusResponse{http.StatusOK, failedStatuses}, nil)
	checkCycle("missing status does not reset backoff", statusResponse{http.StatusOK, `{"connector":{}}`}, nil)
	checkCycle("failures after missing status remain in backoff", statusResponse{http.StatusOK, failedStatuses}, nil)
	checkCycle("deleted task clears task backoff", statusResponse{http.StatusOK, `{
		"connector-a": {"status": {"name": "connector-a", "connector": {"state": "FAILED"}, "tasks": []}},
		"connector-b": {"status": {"name": "connector-b", "connector": {"state": "RUNNING"}, "tasks": []}}
	}`}, nil)
	checkCycle("reappearing task is restarted", statusResponse{http.StatusOK, failedStatuses}, []string{restarts[1]})
	checkCycle("deleted connector clears connector backoff", statusResponse{http.StatusOK, `{
		"connector-b": {"status": {"name": "connector-b", "connector": {"state": "RUNNING"}, "tasks": [{"id": 2, "state": "FAILED"}]}}
	}`}, nil)
	checkCycle("reappearing connector is restarted", statusResponse{http.StatusOK, failedStatuses}, []string{restarts[0]})
	checkCycle("empty successful response clears all backoff", statusResponse{http.StatusOK, "{}"}, nil)
	checkCycle("failures after empty snapshot are restarted", statusResponse{http.StatusOK, failedStatuses}, restarts)
}

func TestClusterPollersKeepIndependentBackoffsAndApplyApplicationUpdates(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	type requestEvent struct {
		cluster string
		method  string
	}
	events := make(chan requestEvent, 64)
	configuration := config.DefaultConfiguration()
	configuration.PollingBehavior.Interval = config.Duration(100 * time.Millisecond)
	configuration.PollingBehavior.Backoff.BaseDelay = config.Duration(time.Hour)
	configuration.PollingBehavior.Backoff.MaxDelay = config.Duration(time.Hour)
	manager := config.NewManager(configuration)
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	pollers := make(map[string]*ConnectClusterPoller)
	endpoints := make(map[string]string)
	for _, name := range []string{"production", "staging"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			events <- requestEvent{cluster: name, method: r.Method}
			if r.Method == http.MethodGet {
				io.WriteString(w, `{"shared-connector":{"status":{"name":"shared-connector","connector":{"state":"FAILED"}}}}`)
			} else if name == "staging" {
				w.WriteHeader(http.StatusInternalServerError)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		}))
		t.Cleanup(server.Close)
		serverURL, err := url.Parse(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		poller := NewConnectClusterPoller(name, connectcluster.Configuration{
			Host: serverURL.Hostname(), Port: serverURL.Port(),
		})
		pollers[name] = poller
		endpoints[name] = server.URL
		workers.Go(func() { poller.Poll(ctx, manager) })
	}
	finished := make(chan struct{})
	go func() { workers.Wait(); close(finished) }()
	stop := func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Fatal("cluster pollers did not stop after cancellation")
		}
	}
	t.Cleanup(stop)
	readEvent := func() requestEvent {
		t.Helper()
		select {
		case event := <-events:
			return event
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for a cluster request")
			return requestEvent{}
		}
	}
	posts := make(map[string]int)
	for posts["production"] == 0 || posts["staging"] == 0 {
		event := readEvent()
		if event.method == http.MethodPost {
			posts[event.cluster]++
		}
	}
	clients := make(map[string]*http.Client)
	for name, poller := range pollers {
		clients[name] = poller.connectClient.HTTPClient
	}
	if err := manager.UpdateConfiguration(func(application *config.ApplicationConfiguration) error {
		application.PollingBehavior.Interval = config.Duration(5 * time.Millisecond)
		application.PollingBehavior.Backoff.Exponential = false
		application.CommunicationConfig.RequestTimeout = config.Duration(250 * time.Millisecond)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	gets := make(map[string]int)
	for gets["production"] < 3 || gets["staging"] < 3 {
		event := readEvent()
		if event.method == http.MethodPost {
			posts[event.cluster]++
		} else {
			gets[event.cluster]++
		}
	}
	stop()
	for name, poller := range pollers {
		if poller.connectClient.HTTPClient != clients[name] {
			t.Fatalf("cluster %q replaced its HTTP client during an application update", name)
		}
		if posts[name] != 1 {
			t.Fatalf("cluster %q made %d restarts, want one independent attempt", name, posts[name])
		}
		if poller.connectClient.HTTPClient.Timeout != 250*time.Millisecond || poller.backoffFilter.BackoffConfig.Exponential {
			t.Fatalf("cluster %q did not apply shared application settings", name)
		}
		if len(poller.backoffFilter.BackoffStatuses) != 1 || poller.backoffFilter.BackoffStatuses["shared-connector"].Attempts != 1 {
			t.Fatalf("cluster %q lost its independent backoff history", name)
		}
	}

	// Read only after both pollers stop, so concurrent logging has finished.
	messages := map[string]map[string]bool{"production": {}, "staging": {}}
	decoder := jsontext.NewDecoder(bytes.NewReader(logs.Bytes()))
	for {
		value, err := decoder.ReadValue()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		var record struct {
			Message     string    `json:"msg"`
			Cluster     string    `json:"connect_cluster"`
			Endpoint    string    `json:"endpoint"`
			Action      string    `json:"action"`
			Connector   string    `json:"connector"`
			TaskID      *int      `json:"task_id"`
			Attempts    int       `json:"attempt_count"`
			AttemptedAt time.Time `json:"attempted_at"`
			StatusCode  int       `json:"status_code"`
			Duration    int64     `json:"duration"`
		}
		if err := json.Unmarshal(value, &record); err != nil {
			t.Fatal(err)
		}
		clusterMessages, exists := messages[record.Cluster]
		if !exists || record.Endpoint != endpoints[record.Cluster] {
			t.Fatalf("log lost its cluster context: %s", value)
		}
		if record.Action != "" && (record.Action != string(actions.RestartConnector) || record.Connector != "shared-connector" || record.TaskID != nil) {
			t.Fatalf("log lost its action identity: %s", value)
		}
		if record.Message == "connector restart request accepted" || record.Message == "remediation request failed" {
			wantStatus := http.StatusNoContent
			if record.Cluster == "staging" {
				wantStatus = http.StatusInternalServerError
			}
			if record.Attempts != 1 || record.AttemptedAt.IsZero() || record.StatusCode != wantStatus || record.Action == "" || record.Duration <= 0 {
				t.Fatalf("attempt log lost its outcome: %s", value)
			}
		}
		clusterMessages[record.Message] = true
	}
	for name, clusterMessages := range messages {
		for _, message := range []string{
			"polling started", "polling settings updated", "polling stopped",
			"remediation action determined", "remediation actions determined", "requesting connector restart",
			"connector restart skipped because it is in the backoff window", "poll cycle completed",
		} {
			if !clusterMessages[message] {
				t.Errorf("cluster %q missing log message %q", name, message)
			}
		}
	}
	if !messages["production"]["connector restart request accepted"] || !messages["staging"]["remediation request failed"] {
		t.Fatal("restart success or failure logs lost their cluster identity")
	}
}

func TestBlockedClusterDoesNotStopOtherPollers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blocked := make(chan struct{})
	var started sync.Once
	blockedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started.Do(func() { close(blocked) })
		<-r.Context().Done()
	}))
	t.Cleanup(blockedServer.Close)
	requests := make(chan struct{}, 16)
	healthyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requests <- struct{}{}:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, "{}")
	}))
	t.Cleanup(healthyServer.Close)
	configuration := config.DefaultConfiguration()
	configuration.PollingBehavior.Interval = config.Duration(10 * time.Millisecond)
	configuration.CommunicationConfig.RequestTimeout = config.Duration(time.Hour)
	manager := config.NewManager(configuration)
	var workers sync.WaitGroup
	for name, server := range map[string]*httptest.Server{"blocked": blockedServer, "healthy": healthyServer} {
		serverURL, err := url.Parse(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		poller := NewConnectClusterPoller(name, connectcluster.Configuration{
			Host: serverURL.Hostname(), Port: serverURL.Port(),
		})
		workers.Go(func() { poller.Poll(ctx, manager) })
	}
	finished := make(chan struct{})
	go func() { workers.Wait(); close(finished) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("pollers did not stop after cancellation")
		}
	})
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked cluster did not start its request")
	}
	for range 3 {
		select {
		case <-requests:
		case <-time.After(time.Second):
			t.Fatal("healthy cluster stopped polling while the other cluster was blocked")
		}
	}
}

func TestPollTimeoutUpdateAppliesToRequestsWithoutReplacingClient(t *testing.T) {
	ready := make(chan struct{})
	cancellations := make(chan time.Duration, 16)
	var first atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !first.Swap(true) {
			io.WriteString(w, "{}")
			close(ready)
			return
		}
		started := time.Now()
		<-r.Context().Done()
		select {
		case cancellations <- time.Since(started):
		default:
		}
	}))
	t.Cleanup(server.Close)
	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	configuration := config.DefaultConfiguration()
	configuration.PollingBehavior.Interval = config.Duration(10 * time.Millisecond)
	configuration.CommunicationConfig.RequestTimeout = config.Duration(time.Second)
	manager := config.NewManager(configuration)
	poller := NewConnectClusterPoller("test", connectcluster.Configuration{
		Host: serverURL.Hostname(), Port: serverURL.Port(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { poller.Poll(ctx, manager); close(finished) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("poller did not stop after cancellation")
		}
	})
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("poller did not retrieve its initial statuses")
	}
	client := poller.connectClient.HTTPClient
	if err := manager.UpdateConfiguration(func(application *config.ApplicationConfiguration) error {
		application.CommunicationConfig.RequestTimeout = config.Duration(30 * time.Millisecond)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case duration := <-cancellations:
			// A cycle already in progress can still use the old one-second timeout.
			if duration < 500*time.Millisecond {
				cancel()
				<-finished
				if poller.connectClient.HTTPClient != client || client.Timeout != 30*time.Millisecond {
					t.Fatal("timeout update replaced the client or did not apply the new timeout")
				}
				return
			}
		case <-deadline.C:
			t.Fatal("no request used the updated timeout")
		}
	}
}
