package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/api/clusters"
	configAPI "github.com/Entropic-Works/kafka-connect-healer/internal/api/config"
	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll"
)

type Components struct {
	ConfigurationManager *config.Manager
	ClusterManager       *poll.ClusterManager
}

func NewServer(components Components) *http.Server {
	configHandler := configAPI.NewConfigHandler(components.ConfigurationManager)
	clustersHandler := clusters.NewClustersHandler(components.ClusterManager)
	configuration := components.ConfigurationManager.GetConfiguration()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", homeHandler)
	mux.HandleFunc("GET /config", configHandler.GetConfig)
	mux.HandleFunc("PATCH /config", configHandler.PatchConfig)
	mux.HandleFunc("GET /clusters", clustersHandler.GetClusters)
	mux.HandleFunc("PUT /clusters/{name}", clustersHandler.PutCluster)
	mux.HandleFunc("PUT /clusters/{name}/start", clustersHandler.StartCluster)
	mux.HandleFunc("PUT /clusters/{name}/stop", clustersHandler.StopCluster)
	mux.HandleFunc("DELETE /clusters/{name}", clustersHandler.DeleteCluster)
	return &http.Server{
		Addr:              ":8080",
		Handler:           basicAuth(mux, configuration.APIConfig.AuthConfig),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       time.Minute,
		ErrorLog:          slog.NewLogLogger(slog.Default().Handler(), slog.LevelError),
	}
}
