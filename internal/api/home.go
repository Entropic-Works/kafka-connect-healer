package api

import (
	"encoding/json/v2"
	"log/slog"
	"net/http"
)

type routeInfo struct {
	Method        string `json:"method"`
	Path          string `json:"path"`
	Functionality string `json:"functionality"`
}

func homeHandler(w http.ResponseWriter, r *http.Request) {
	routes := []routeInfo{
		{
			Method:        "GET",
			Path:          "/",
			Functionality: "api methods, routes, and functionalities",
		},
		{
			Method:        "GET",
			Path:          "/config",
			Functionality: "retrieves current configuration",
		},
		{
			Method:        "PATCH",
			Path:          "/config",
			Functionality: "updates runtime configuration",
		},
		{
			Method:        "GET",
			Path:          "/clusters",
			Functionality: "retrieves configured Connect clusters without passwords",
		},
		{
			Method:        "PUT",
			Path:          "/clusters/{name}",
			Functionality: "creates or replaces a Connect cluster and its poller",
		},
		{
			Method:        "PUT",
			Path:          "/clusters/{name}/start",
			Functionality: "starts polling a configured Connect cluster",
		},
		{
			Method:        "PUT",
			Path:          "/clusters/{name}/stop",
			Functionality: "stops polling a Connect cluster and keeps its definition",
		},
		{
			Method:        "DELETE",
			Path:          "/clusters/{name}",
			Functionality: "stops and removes a Connect cluster",
		},
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.MarshalWrite(w, routes); err != nil {
		slog.Error("failed to write home API response", "method", r.Method, "path", r.URL.Path, "error", err)
		return
	}
}
