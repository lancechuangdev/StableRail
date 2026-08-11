// Package observability provides dependency-free structured HTTP telemetry.
package observability

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type Metrics struct{ requests, errors, durationNanos atomic.Uint64 }

func (m *Metrics) Middleware(next http.Handler, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		trace := r.Header.Get("X-Request-ID")
		if trace == "" {
			trace = strconv.FormatInt(start.UnixNano(), 36)
		}
		w.Header().Set("X-Request-ID", trace)
		capture := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(capture, r)
		elapsed := time.Since(start)
		m.requests.Add(1)
		m.durationNanos.Add(uint64(elapsed))
		if capture.status >= 500 {
			m.errors.Add(1)
		}
		logger.InfoContext(r.Context(), "HTTP request", "request_id", trace, "method", r.Method, "path", r.URL.Path, "status", capture.status, "duration_ms", float64(elapsed.Microseconds())/1000)
	})
}

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		requests := m.requests.Load()
		fmt.Fprintf(w, "# TYPE stablerail_http_requests_total counter\nstablerail_http_requests_total %d\n# TYPE stablerail_http_errors_total counter\nstablerail_http_errors_total %d\n# TYPE stablerail_http_request_duration_seconds_total counter\nstablerail_http_request_duration_seconds_total %g\n", requests, m.errors.Load(), float64(m.durationNanos.Load())/float64(time.Second))
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(status int) {
	if w.wrote {
		return
	}
	w.wrote = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(body []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}
