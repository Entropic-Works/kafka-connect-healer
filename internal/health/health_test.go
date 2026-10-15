package health

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
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

func TestServerShutdownWithIncompleteRequest(t *testing.T) {
	for _, test := range []struct {
		name           string
		request        string
		waitForHandler bool
	}{
		{name: "no headers"},
		{name: "partial headers", request: "GET /livez HTTP/1.1\r\nHost:"},
		{name: "partial body", request: "GET /readyz HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\n\r\nx", waitForHandler: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			handled := make(chan struct{})
			server := NewServer(func() bool {
				close(handled)
				return true
			})
			accepted := make(chan struct{})
			server.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					close(accepted)
				}
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			served := make(chan error, 1)
			go func() { served <- server.Serve(listener) }()
			t.Cleanup(func() {
				server.Close()
				if err := <-served; !errors.Is(err, http.ErrServerClosed) {
					t.Errorf("Serve() = %v, want http.ErrServerClosed", err)
				}
			})
			connection, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { connection.Close() })
			if err := connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(connection, test.request); err != nil {
				t.Fatal(err)
			}
			select {
			case <-accepted:
			case <-time.After(5 * time.Second):
				t.Fatal("server did not accept the connection")
			}
			if test.waitForHandler {
				select {
				case <-handled:
				case <-time.After(5 * time.Second):
					t.Fatal("server did not handle the request headers")
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := server.Shutdown(ctx); err != nil {
				t.Fatalf("graceful shutdown with an incomplete request: %v", err)
			}
		})
	}
}
