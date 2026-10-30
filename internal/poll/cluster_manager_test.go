package poll

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
)

func newLifecycleTestManager(t *testing.T) *ClusterManager {
	t.Helper()
	configuration := config.DefaultConfiguration()
	configuration.PollingBehavior.Interval = config.Duration(time.Hour)
	manager, err := NewClusterManager(t.Context(), config.NewManager(configuration), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.Close)
	return manager
}

func waitForWorker(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cluster worker")
	}
}

func testWorker(manager *ClusterManager, name string) *clusterWorker {
	manager.clustersMutex.RLock()
	defer manager.clustersMutex.RUnlock()
	return manager.clusterWorkers[name]
}

func TestClusterManagerCreateNoOpAndInvalidReplacement(t *testing.T) {
	manager := newLifecycleTestManager(t)
	original := connectcluster.DefaultConfiguration()
	created, err := manager.PutCluster("production", original)
	if err != nil || !created {
		t.Fatalf("create = %t, %v", created, err)
	}
	worker := testWorker(manager, "production")
	created, err = manager.PutCluster("production", original)
	if err != nil || created || testWorker(manager, "production") != worker {
		t.Fatal("identical PUT replaced a worker")
	}
	invalid := original
	invalid.Port = "0"
	if _, err := manager.PutCluster("production", invalid); err == nil {
		t.Fatal("invalid replacement was accepted")
	}
	if _, err := manager.PutCluster(" ", original); err == nil {
		t.Fatal("blank cluster name was accepted")
	}
	if testWorker(manager, "production") != worker || manager.GetClusters()["production"] != original {
		t.Fatal("invalid PUT changed the current worker")
	}
	select {
	case <-worker.done:
		t.Fatal("no-op or invalid PUT stopped the current worker")
	default:
	}
}

func TestClusterManagerPollsAndRestartsOverAuthenticatedHTTP(t *testing.T) {
	requests := make(chan string, 32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok || username != "connect-user" || password != "connect-password" {
			t.Error("HTTP request did not include the configured Basic Auth credentials")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		requests <- r.Method + " " + r.URL.RequestURI()
		if r.Method == http.MethodGet {
			io.WriteString(w, `{"source":{"status":{"name":"source","connector":{"state":"FAILED"}}}}`)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	t.Cleanup(server.Close)
	address, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	configuration := config.DefaultConfiguration()
	configuration.PollingBehavior.Interval = config.Duration(10 * time.Millisecond)
	manager, err := NewClusterManager(t.Context(), config.NewManager(configuration), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.Close)
	cluster := connectcluster.Configuration{
		Host: address.Hostname(), Port: address.Port(),
		AuthConfig: connectcluster.AuthConfiguration{Enabled: true, Username: "connect-user", Password: "connect-password"},
	}
	if _, err := manager.PutCluster("production", cluster); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"GET /connectors?expand=status", "POST /connectors/source/restart?includeTasks=true&onlyFailed=true", "GET /connectors?expand=status"} {
		select {
		case got := <-requests:
			if got != want {
				t.Fatalf("request = %q, want %q", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s", want)
		}
	}
	if manager.GetClusters()["production"] != cluster {
		t.Fatal("HTTP cluster authentication changed during polling")
	}
}

func TestClusterManagerGetClustersReturnsIndependentCopy(t *testing.T) {
	manager := newLifecycleTestManager(t)
	original := connectcluster.DefaultConfiguration()
	original.AuthConfig.Password = "secret"
	if _, err := manager.PutCluster("production", original); err != nil {
		t.Fatal(err)
	}
	snapshot := manager.GetClusters()
	changed := snapshot["production"]
	changed.AuthConfig.Password = ""
	snapshot["production"] = changed
	delete(snapshot, "production")
	snapshot["other"] = changed
	current := manager.GetClusters()
	if len(current) != 1 || current["production"] != original {
		t.Fatal("changing a snapshot modified the manager")
	}
}

// Keep worker completion under test control to verify lifecycle ordering without
// sleeps or relying on how quickly an HTTP request reacts to cancellation.
func installStoppingWorker(t *testing.T, manager *ClusterManager) (*clusterWorker, <-chan struct{}, func()) {
	t.Helper()
	stopRequested := make(chan struct{})
	done := make(chan struct{})
	var stopOnce, finishOnce sync.Once
	worker := &clusterWorker{
		configuration: connectcluster.DefaultConfiguration(),
		cancel:        func() { stopOnce.Do(func() { close(stopRequested) }) },
		done:          done,
	}
	manager.clustersMutex.Lock()
	manager.clusterWorkers["production"] = worker
	manager.clustersMutex.Unlock()
	finish := func() { finishOnce.Do(func() { close(done) }) }
	t.Cleanup(finish)
	return worker, stopRequested, finish
}

func TestPutClusterWaitsForPreviousWorkerAndLeavesOtherClustersAlone(t *testing.T) {
	manager := newLifecycleTestManager(t)
	otherConfiguration := connectcluster.DefaultConfiguration()
	otherConfiguration.Host = "staging.local"
	if _, err := manager.PutCluster("staging", otherConfiguration); err != nil {
		t.Fatal(err)
	}
	otherWorker := testWorker(manager, "staging")
	previousWorker, stopRequested, finishPreviousWorker := installStoppingWorker(t, manager)
	replacement := connectcluster.DefaultConfiguration()
	replacement.Host = "replacement.local"
	type putResult struct {
		created bool
		err     error
	}
	result := make(chan putResult, 1)
	go func() {
		created, err := manager.PutCluster("production", replacement)
		result <- putResult{created: created, err: err}
	}()
	waitForWorker(t, stopRequested)
	if testWorker(manager, "production") != previousWorker {
		t.Fatal("replacement started before the previous worker exited")
	}
	select {
	case <-result:
		t.Fatal("PUT returned before the previous worker exited")
	default:
	}
	finishPreviousWorker()
	select {
	case completed := <-result:
		if completed.err != nil || completed.created {
			t.Fatalf("replace = %t, %v", completed.created, completed.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("PUT did not finish after the previous worker exited")
	}
	if testWorker(manager, "production") == previousWorker || manager.GetClusters()["production"] != replacement ||
		testWorker(manager, "staging") != otherWorker {
		t.Fatal("replacement did not replace exactly the requested cluster")
	}
}

func TestDeleteClusterWaitsForWorkerExit(t *testing.T) {
	manager := newLifecycleTestManager(t)
	_, stopRequested, finishWorker := installStoppingWorker(t, manager)
	result := make(chan error, 1)
	go func() { result <- manager.DeleteCluster("production") }()
	waitForWorker(t, stopRequested)
	if len(manager.GetClusters()) != 1 {
		t.Fatal("DELETE removed the definition before the worker exited")
	}
	select {
	case <-result:
		t.Fatal("DELETE returned before the worker exited")
	default:
	}
	finishWorker()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DELETE did not finish after the worker exited")
	}
	if len(manager.GetClusters()) != 0 || !errors.Is(manager.DeleteCluster("production"), ErrClusterNotFound) {
		t.Fatal("DELETE did not remove the cluster")
	}
}

func TestDeleteClusterCancelsInFlightStatusRequest(t *testing.T) {
	requestStarted := make(chan struct{})
	requestCanceled := make(chan struct{})
	var startedOnce, canceledOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedOnce.Do(func() { close(requestStarted) })
		<-r.Context().Done()
		canceledOnce.Do(func() { close(requestCanceled) })
	}))
	t.Cleanup(server.Close)
	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	configuration := config.DefaultConfiguration()
	configuration.PollingBehavior.Interval = config.Duration(5 * time.Millisecond)
	configuration.CommunicationConfig.RequestTimeout = config.Duration(time.Hour)
	manager, err := NewClusterManager(t.Context(), config.NewManager(configuration), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.Close)
	if _, err := manager.PutCluster("production", connectcluster.Configuration{
		Host: serverURL.Hostname(), Port: serverURL.Port(),
	}); err != nil {
		t.Fatal(err)
	}
	waitForWorker(t, requestStarted)
	worker := testWorker(manager, "production")
	if err := manager.DeleteCluster("production"); err != nil {
		t.Fatal(err)
	}
	waitForWorker(t, requestCanceled)
	waitForWorker(t, worker.done)
}

func TestClusterManagerCloseStopsWorkersAndRejectsChanges(t *testing.T) {
	manager := newLifecycleTestManager(t)
	for _, name := range []string{"production", "staging"} {
		configuration := connectcluster.DefaultConfiguration()
		configuration.Host = name + ".local"
		if _, err := manager.PutCluster(name, configuration); err != nil {
			t.Fatal(err)
		}
	}
	workers := []*clusterWorker{testWorker(manager, "production"), testWorker(manager, "staging")}
	manager.Close()
	manager.Close()
	for _, worker := range workers {
		waitForWorker(t, worker.done)
	}
	if len(manager.GetClusters()) != 0 {
		t.Fatal("closed manager retained cluster definitions")
	}
	if _, err := manager.PutCluster("production", connectcluster.DefaultConfiguration()); !errors.Is(err, ErrClusterManagerClosed) {
		t.Fatalf("PUT after Close = %v", err)
	}
	if err := manager.DeleteCluster("production"); !errors.Is(err, ErrClusterManagerClosed) {
		t.Fatalf("DELETE after Close = %v", err)
	}
}

func TestClusterManagerNotReadyWhileCloseWaitsForWorkers(t *testing.T) {
	manager := newLifecycleTestManager(t)
	_, stopRequested, finishWorker := installStoppingWorker(t, manager)
	if !manager.Ready() {
		t.Fatal("running manager is not ready")
	}
	closed := make(chan struct{})
	go func() {
		manager.Close()
		close(closed)
	}()
	waitForWorker(t, stopRequested)
	ready := make(chan bool, 1)
	go func() { ready <- manager.Ready() }()
	select {
	case got := <-ready:
		if got {
			t.Fatal("manager is ready while closing")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("readiness blocked waiting for worker shutdown")
	}
	finishWorker()
	waitForWorker(t, closed)
}

func TestNewClusterManagerRejectsInvalidInitialDefinitionsAndCanceledContext(t *testing.T) {
	configurationManager := config.NewManager(config.DefaultConfiguration())
	for _, definitions := range []map[string]connectcluster.Configuration{
		{" ": connectcluster.DefaultConfiguration()},
		{"valid": connectcluster.DefaultConfiguration(), "invalid": {Host: "localhost", Port: "0"}},
	} {
		manager, err := NewClusterManager(t.Context(), configurationManager, definitions)
		if err == nil || manager != nil {
			if manager != nil {
				manager.Close()
			}
			t.Fatal("invalid initial definitions started a manager")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if manager, err := NewClusterManager(ctx, configurationManager, nil); manager != nil || !errors.Is(err, ErrClusterManagerClosed) {
		t.Fatalf("canceled constructor = %v, %v", manager, err)
	}
}

func TestClusterManagerConcurrentLifecycleOperations(t *testing.T) {
	manager := newLifecycleTestManager(t)
	start := make(chan struct{})
	operationErrors := make(chan error, 30)
	var operations sync.WaitGroup
	for operation := range 3 {
		operations.Go(func() {
			<-start
			for iteration := range 10 {
				name := fmt.Sprintf("cluster-%d", iteration%3)
				switch operation {
				case 0:
					configuration := connectcluster.DefaultConfiguration()
					configuration.Host = name + ".local"
					_, err := manager.PutCluster(name, configuration)
					operationErrors <- err
				case 1:
					operationErrors <- manager.DeleteCluster(name)
				case 2:
					snapshot := manager.GetClusters()
					delete(snapshot, name)
				}
			}
		})
	}
	close(start)
	operations.Wait()
	close(operationErrors)
	for err := range operationErrors {
		if err != nil && !errors.Is(err, ErrClusterNotFound) {
			t.Fatal(err)
		}
	}
	manager.Close()
}
