package observability

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

type MetricsSource interface {
	Snapshot(context.Context) (MetricsSnapshot, error)
}

func RegisterMetricsRoutes(mux *http.ServeMux, source MetricsSource) {
	mux.HandleFunc("GET /api/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		snapshot, err := source.Snapshot(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "metrics query failed", "error", err)
			http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(snapshot)
	})
}

type requestIDKey struct{}

var safeRequestID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// RequestLogger emits one structured event for every HTTP request
func RequestLogger(next http.Handler, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if !safeRequestID.MatchString(requestID) {
			requestID = uuid.NewString()
		}

		ctx := context.WithValue(r.Context(), requestIDKey{}, requestID)
		r = r.WithContext(ctx)
		w.Header().Set("X-Request-ID", requestID)

		correlationID := correlationFromResourcePath(r.URL.Path)
		if correlationID != "" {
			w.Header().Set("X-Correlation-ID", correlationID)
		}

		pattern := "unmatched"
		if mux, ok := next.(*http.ServeMux); ok {
			_, matched := mux.Handler(r)
			if matched != "" {
				pattern = matched
			}
		}

		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)

		if headerID := recorder.Header().Get("X-Correlation-ID"); headerID != "" {
			correlationID = headerID
		}
		if correlationID == "" {
			correlationID = requestID
		}

		logger.InfoContext(ctx, "http_request",
			"request_id", requestID,
			"correlation_id", correlationID,
			"method", r.Method,
			"route", pattern,
			"status", recorder.status,
			"duration_ms", float64(time.Since(start).Microseconds())/1000,
			"response_bytes", recorder.bytes,
		)
	})
}

func correlationFromResourcePath(path string) string {
	for _, prefix := range []string{"/api/v1/notifications/", "/api/v1/batches/"} {
		if !strings.HasPrefix(path, prefix) {
			continue
		}
		id := strings.TrimPrefix(path, prefix)
		if _, err := uuid.Parse(id); err == nil {
			return id
		}
	}
	return ""
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.status = status
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(data []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(data)
	r.bytes += n
	return n, err
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}
