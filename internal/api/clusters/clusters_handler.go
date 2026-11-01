package clusters

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/url"

	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll"
)

type ClustersHandler struct {
	manager *poll.ClusterManager
}

func NewClustersHandler(manager *poll.ClusterManager) *ClustersHandler {
	return &ClustersHandler{manager: manager}
}

func (handler *ClustersHandler) GetClusters(w http.ResponseWriter, r *http.Request) {
	clusters := handler.manager.GetClusters()
	for name, configuration := range clusters {
		configuration.AuthConfig.Password = ""
		clusters[name] = configuration
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.MarshalWrite(w, clusters); err != nil {
		slog.Error("failed to write clusters API response", "method", r.Method, "path", r.URL.Path, "error", err)
	}
}

func (handler *ClustersHandler) PutCluster(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			http.Error(w, "cluster configuration exceeds 64 KiB", http.StatusRequestEntityTooLarge)
		} else if timeout, ok := errors.AsType[net.Error](err); ok && timeout.Timeout() {
			http.Error(w, "cluster configuration read timed out", http.StatusRequestTimeout)
		} else {
			http.Error(w, "could not read cluster configuration", http.StatusBadRequest)
		}
		return
	}
	var configuration connectcluster.Configuration
	if !validClusterJSON(data) || json.Unmarshal(data, &configuration, json.RejectUnknownMembers(true)) != nil {
		// Decoder errors can contain passwords; do not return the original text.
		http.Error(w, "cluster configuration must be one valid JSON object with known fields and no null values", http.StatusBadRequest)
		return
	}
	name := r.PathValue("name")
	created, err := handler.manager.PutCluster(name, configuration)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, poll.ErrClusterManagerClosed) {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, err.Error(), status)
		return
	}
	if created {
		w.Header().Set("Location", "/clusters/"+url.PathEscape(name))
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (handler *ClustersHandler) DeleteCluster(w http.ResponseWriter, r *http.Request) {
	writeClusterLifecycleResponse(w, handler.manager.DeleteCluster(r.PathValue("name")))
}

func (handler *ClustersHandler) StartCluster(w http.ResponseWriter, r *http.Request) {
	writeClusterLifecycleResponse(w, handler.manager.StartCluster(r.PathValue("name")))
}

func (handler *ClustersHandler) StopCluster(w http.ResponseWriter, r *http.Request) {
	writeClusterLifecycleResponse(w, handler.manager.StopCluster(r.PathValue("name")))
}

func writeClusterLifecycleResponse(w http.ResponseWriter, err error) {
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, poll.ErrClusterNotFound) {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func validClusterJSON(data []byte) bool {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return false
	}
	decoder := jsontext.NewDecoder(bytes.NewReader(data))
	for {
		token, err := decoder.ReadToken()
		if err == io.EOF {
			return true
		}
		if err != nil || token.Kind() == jsontext.KindNull {
			return false
		}
	}
}
