package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/api"
	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
	"github.com/Entropic-Works/kafka-connect-healer/internal/health"
	"github.com/Entropic-Works/kafka-connect-healer/internal/logging"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll"
)

func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("config", "", "path to optional application YAML configuration; omitted uses defaults")
	secretPath := flag.String("secret-config", "", "path to an optional Secret YAML overlay")
	clustersPath := flag.String("connect-clusters", "", "path to optional named Connect cluster configurations; omitted uses localhost:8083")
	clustersSecretPath := flag.String("connect-clusters-secret-config", "", "path to an optional Connect cluster Secret YAML overlay")
	flag.Parse()
	if flag.NArg() != 0 {
		slog.Error("unexpected positional arguments; use configuration file flags")
		return 1
	}
	startupConfiguration, err := config.LoadFiles(*configPath, *secretPath)
	if err != nil {
		slog.Error("configuration loading failed", "error", err)
		return 1
	}
	loggingLevel := new(slog.LevelVar)
	loggingLevel.Set(startupConfiguration.LoggingConfig.Level)
	logging.Configure(loggingLevel)

	clusters, err := connectcluster.LoadFiles(*clustersPath, *clustersSecretPath)
	if err != nil {
		slog.Error("Connect cluster configuration loading failed", "error", err)
		return 1
	}
	clusterSource := *clustersPath
	if clusterSource == "" {
		clusterSource = "defaults"
	}
	if len(clusters) == 0 {
		slog.Info("no Connect clusters configured; polling disabled", "cluster_config_source", clusterSource)
	} else {
		slog.Info("Connect clusters configured", "cluster_count", len(clusters), "cluster_config_source", clusterSource)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	configurationManager := config.NewManager(startupConfiguration)
	configuration := configurationManager.GetConfiguration()
	clusterManager, err := poll.NewClusterManager(ctx, configurationManager, clusters)
	if err != nil {
		slog.Error("Connect cluster manager startup failed", "error", err)
		return 1
	}

	apiComponents := api.Components{
		ConfigurationManager: configurationManager,
		ClusterManager:       clusterManager,
	}
	server := api.NewServer(apiComponents)
	apiLogger := slog.With("component", "api", "address", server.Addr)
	var apiReady atomic.Bool
	healthServer := health.NewServer(func() bool {
		return apiReady.Load() && clusterManager.Ready()
	})
	healthLogger := slog.With("component", "health", "address", healthServer.Addr)
	apiFailed := make(chan struct{})
	var workers sync.WaitGroup

	workers.Go(func() {
		apiLogger.Info("starting API server", "authentication_enabled", configuration.APIConfig.AuthConfig.Enabled)
		listener, err := net.Listen("tcp", server.Addr)
		if err == nil {
			apiReady.Store(true)
			err = server.Serve(listener)
			apiReady.Store(false)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			apiLogger.Error("API serving failed; stopping application", "error", err)
			close(apiFailed)
		} else if errors.Is(err, http.ErrServerClosed) {
			apiLogger.Debug("API listener stopped")
		}
	})

	workers.Go(func() {
		healthLogger.Info("starting health server")
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			healthLogger.Error("health serving failed; polling will continue", "error", err)
		} else if errors.Is(err, http.ErrServerClosed) {
			healthLogger.Debug("health listener stopped")
		}
	})

	workers.Go(func() {
		configuration, configurationUpdateChannel := configurationManager.ConfigurationSnapshot()
		loggingLevel.Set(configuration.LoggingConfig.Level)

		for {
			select {
			case <-ctx.Done():
				return
			case <-configurationUpdateChannel:
				configuration, configurationUpdateChannel = configurationManager.ConfigurationSnapshot()
				loggingLevel.Set(configuration.LoggingConfig.Level)
			}
		}
	})

	exitCode := 0
	select {
	case <-ctx.Done():
	case <-apiFailed:
		exitCode = 1
		stop()
	}
	slog.Info("Kafka Connect Healer stopping")
	clusterManager.Close()

	const shutdownTimeout = 5 * time.Second
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	workers.Go(func() { shutdownServer(shutdownCtx, server, apiLogger, "API", shutdownTimeout) })
	workers.Go(func() { shutdownServer(shutdownCtx, healthServer, healthLogger, "health", shutdownTimeout) })

	workers.Wait()
	slog.Info("Kafka Connect Healer stopped")
	return exitCode
}

func shutdownServer(shutdownCtx context.Context, server *http.Server, logger *slog.Logger, name string, timeout time.Duration) {
	shutdownStarted := time.Now()
	logger.Info(name+" graceful shutdown started", "timeout", timeout)
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Warn(name+" graceful shutdown failed; closing connections", "duration", time.Since(shutdownStarted), "error", err)
		if err := server.Close(); err != nil {
			logger.Error(name+" forced shutdown failed", "duration", time.Since(shutdownStarted), "error", err)
		} else {
			logger.Info(name+" connections forcibly closed", "duration", time.Since(shutdownStarted))
		}
	} else {
		logger.Info(name+" graceful shutdown completed", "duration", time.Since(shutdownStarted))
	}
}
