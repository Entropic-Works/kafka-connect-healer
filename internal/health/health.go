package health

import (
	"io"
	"log/slog"
	"net/http"
	"time"
)

// NewServer serves only unauthenticated probes on a separate listener.
// ready reports whether all required application components are operational.
func NewServer(ready func() bool) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", livenessHandler)
	mux.HandleFunc("GET /readyz", readinessHandler(ready))
	// Keep probe request deadlines below the application's shutdown grace period.
	return &http.Server{
		Addr:              ":8081",
		Handler:           mux,
		ReadHeaderTimeout: time.Second,
		ReadTimeout:       2 * time.Second,
		WriteTimeout:      2 * time.Second,
		IdleTimeout:       time.Minute,
		ErrorLog:          slog.NewLogLogger(slog.Default().Handler(), slog.LevelError),
	}
}

func livenessHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	io.WriteString(w, "ok\n")
}

func readinessHandler(ready func() bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !ready() {
			w.Header().Set("Cache-Control", "no-store")
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		livenessHandler(w, r)
	}
}
