package poll

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"

	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
)

var (
	ErrClusterNotFound      = errors.New("Connect cluster not found")
	ErrClusterManagerClosed = errors.New("Connect cluster manager is shutting down")
)

type clusterWorker struct {
	configuration connectcluster.Configuration
	cancel        context.CancelFunc
	done          chan struct{}
}

// ClusterManager owns cluster definitions and their polling goroutines.
type ClusterManager struct {
	lifecycleMutex       sync.Mutex // Serialize lifecycle changes, including waiting for stops.
	clustersMutex        sync.RWMutex
	applicationContext   context.Context // Application lifetime, never an API request context.
	configurationManager *config.Manager
	clusterWorkers       map[string]*clusterWorker
	closed               bool // Writes hold both mutexes; reads hold either one.
}

func NewClusterManager(applicationContext context.Context, configurationManager *config.Manager, clusters map[string]connectcluster.Configuration) (*ClusterManager, error) {
	if applicationContext.Err() != nil {
		return nil, ErrClusterManagerClosed
	}
	// Validate every initial definition before starting any goroutine.
	for name, configuration := range clusters {
		if err := validateCluster(name, configuration); err != nil {
			return nil, fmt.Errorf("cluster %q: %w", name, err)
		}
	}
	manager := &ClusterManager{
		applicationContext: applicationContext, configurationManager: configurationManager,
		clusterWorkers: make(map[string]*clusterWorker, len(clusters)),
	}
	for name, configuration := range clusters {
		manager.clusterWorkers[name] = manager.startCluster(name, configuration)
	}
	return manager, nil
}

func (manager *ClusterManager) GetClusters() map[string]connectcluster.Configuration {
	manager.clustersMutex.RLock()
	defer manager.clustersMutex.RUnlock()
	clusters := make(map[string]connectcluster.Configuration, len(manager.clusterWorkers))
	for name, worker := range manager.clusterWorkers {
		clusters[name] = worker.configuration
	}
	return clusters
}

// Ready reports whether the manager can accept work. Connect availability and
// successful polling are not required, including when no clusters are configured.
func (manager *ClusterManager) Ready() bool {
	manager.clustersMutex.RLock()
	defer manager.clustersMutex.RUnlock()
	return !manager.closed && manager.applicationContext.Err() == nil
}

// PutCluster replaces one complete definition. Identical definitions are a no-op.
func (manager *ClusterManager) PutCluster(name string, configuration connectcluster.Configuration) (bool, error) {
	if err := validateCluster(name, configuration); err != nil {
		return false, err
	}
	manager.lifecycleMutex.Lock()
	defer manager.lifecycleMutex.Unlock()
	if manager.closed || manager.applicationContext.Err() != nil {
		return false, ErrClusterManagerClosed
	}
	manager.clustersMutex.RLock()
	previous, exists := manager.clusterWorkers[name]
	manager.clustersMutex.RUnlock()
	if exists {
		if previous.configuration == configuration {
			return false, nil
		}
		previous.cancel()
		<-previous.done
	}
	if manager.applicationContext.Err() != nil {
		return false, ErrClusterManagerClosed
	}
	manager.clustersMutex.Lock()
	manager.clusterWorkers[name] = manager.startCluster(name, configuration)
	manager.clustersMutex.Unlock()
	connectcluster.WarnDuplicateEndpoints(manager.GetClusters())
	slog.Info("Connect cluster configured", "connect_cluster", name, "created", !exists)
	return !exists, nil
}

func (manager *ClusterManager) DeleteCluster(name string) error {
	manager.lifecycleMutex.Lock()
	defer manager.lifecycleMutex.Unlock()
	if manager.closed || manager.applicationContext.Err() != nil {
		return ErrClusterManagerClosed
	}
	manager.clustersMutex.RLock()
	worker, exists := manager.clusterWorkers[name]
	manager.clustersMutex.RUnlock()
	if !exists {
		return ErrClusterNotFound
	}
	worker.cancel()
	<-worker.done
	manager.clustersMutex.Lock()
	delete(manager.clusterWorkers, name)
	manager.clustersMutex.Unlock()
	slog.Info("Connect cluster removed", "connect_cluster", name)
	return nil
}

// Close rejects new changes, cancels all pollers, and waits for them to exit.
func (manager *ClusterManager) Close() {
	manager.lifecycleMutex.Lock()
	defer manager.lifecycleMutex.Unlock()
	if manager.closed {
		return
	}
	manager.clustersMutex.Lock()
	manager.closed = true
	clusters := manager.clusterWorkers
	manager.clusterWorkers = make(map[string]*clusterWorker)
	manager.clustersMutex.Unlock()
	for _, worker := range clusters {
		worker.cancel()
	}
	for _, worker := range clusters {
		<-worker.done
	}
}

func (manager *ClusterManager) startCluster(name string, configuration connectcluster.Configuration) *clusterWorker {
	if configuration.AuthConfig.Enabled && !configuration.HTTPS {
		slog.Warn("Connect Basic Auth is enabled over HTTP; credentials are sent without transport encryption",
			"connect_cluster", name, "endpoint", "http://"+net.JoinHostPort(configuration.Host, configuration.Port))
	}
	ctx, cancel := context.WithCancel(manager.applicationContext)
	worker := &clusterWorker{configuration: configuration, cancel: cancel, done: make(chan struct{})}
	poller := NewConnectClusterPoller(name, configuration)
	go func() {
		defer close(worker.done)
		defer cancel()
		poller.Poll(ctx, manager.configurationManager)
	}()
	return worker
}

func validateCluster(name string, configuration connectcluster.Configuration) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("cluster name must not be empty")
	}
	return connectcluster.ValidateConfiguration(configuration)
}
