package config

import (
	"fmt"
	"sync"
)

type Manager struct {
	configurationMutex         sync.RWMutex
	configuration              ApplicationConfiguration
	configurationUpdateChannel chan struct{}
}

// NewManager requires a configuration that has already passed ValidateConfiguration.
func NewManager(configuration ApplicationConfiguration) *Manager {
	return &Manager{
		configuration:              configuration,
		configurationUpdateChannel: make(chan struct{}),
	}
}

func (m *Manager) GetConfiguration() ApplicationConfiguration {
	m.configurationMutex.RLock()
	defer m.configurationMutex.RUnlock()

	return m.configuration
}

func (m *Manager) ConfigurationSnapshot() (ApplicationConfiguration, <-chan struct{}) {
	m.configurationMutex.RLock()
	defer m.configurationMutex.RUnlock()

	return m.configuration, m.configurationUpdateChannel
}

// UpdateConfiguration runs changeConfig while holding the configuration lock.
// The callback must not call other methods on this manager.
func (m *Manager) UpdateConfiguration(changeConfig func(*ApplicationConfiguration) error) error {
	m.configurationMutex.Lock()
	defer m.configurationMutex.Unlock()

	nextConfiguration := m.configuration
	if err := changeConfig(&nextConfiguration); err != nil {
		return err
	}
	if nextConfiguration.APIConfig != m.configuration.APIConfig {
		return fmt.Errorf("apiConfig: API authentication is startup-only; update the YAML configuration and restart")
	}
	if nextConfiguration.TelemetryConfig != m.configuration.TelemetryConfig {
		return fmt.Errorf("telemetryConfig: telemetry is startup-only; update the YAML configuration and restart")
	}
	if err := ValidateConfiguration(nextConfiguration); err != nil {
		return err
	}
	if nextConfiguration == m.configuration {
		return nil
	}
	m.configuration = nextConfiguration
	close(m.configurationUpdateChannel)
	m.configurationUpdateChannel = make(chan struct{})
	return nil
}
