package config

import (
	"bytes"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"

	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
)

type ConfigHandler struct {
	manager *config.Manager
}

// PatchConfig applies supplied JSON fields to the current in-memory configuration.
// UpdateConfiguration validates and commits the change atomically, then notifies
// the polling workers. The manager applies logging changes synchronously.
// Startup files are never written.
func (handler *ConfigHandler) PatchConfig(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		w.Header().Set("Accept-Patch", "application/json")
		http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			http.Error(w, "configuration patch exceeds 64 KiB", http.StatusRequestEntityTooLarge)
		} else if timeoutError, ok := errors.AsType[net.Error](err); ok && timeoutError.Timeout() {
			http.Error(w, "configuration patch read timed out", http.StatusRequestTimeout)
		} else {
			http.Error(w, "could not read configuration patch", http.StatusBadRequest)
		}
		return
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		http.Error(w, "configuration patch must be one JSON object", http.StatusBadRequest)
		return
	}
	if err := validatePatchJSON(data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = handler.manager.UpdateConfiguration(func(configuration *config.ApplicationConfiguration) error {
		return decodeConfigPatch(data, configuration)
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func NewConfigHandler(manager *config.Manager) *ConfigHandler {
	return &ConfigHandler{manager: manager}
}

func (handler *ConfigHandler) GetConfig(
	w http.ResponseWriter,
	r *http.Request,
) {
	configuration := handler.manager.GetConfiguration()
	configuration.APIConfig.AuthConfig.Password = ""

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")

	if err := json.MarshalWrite(w, configuration); err != nil {
		slog.Error("failed to write config API response", "method", r.Method, "path", r.URL.Path, "error", err)
		return
	}
}
