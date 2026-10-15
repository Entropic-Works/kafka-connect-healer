package health

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestServerReadinessTransitions(t *testing.T) {
	var ready atomic.Bool
	server := NewServer(ready.Load)
	for _, available := range []bool{false, true, false} {
		ready.Store(available)
		for _, path := range []string{"/livez", "/readyz"} {
			response := httptest.NewRecorder()
			server.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			want := http.StatusOK
			if path == "/readyz" && !available {
				want = http.StatusServiceUnavailable
			}
			if response.Code != want {
				t.Fatalf("%s with ready=%t: got %d, want %d", path, available, response.Code, want)
			}
		}
	}
}
