package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeMetricsSource struct {
	snapshot MetricsSnapshot
	err      error
}

func (f fakeMetricsSource) Snapshot(context.Context) (MetricsSnapshot, error) {
	return f.snapshot, f.err
}

func TestMetricsEndpoint(t *testing.T) {
	mux := http.NewServeMux()
	RegisterMetricsRoutes(mux, fakeMetricsSource{snapshot: MetricsSnapshot{
		WindowSeconds: 300,
		GeneratedAt:   time.Now().UTC(),
		Queue: QueueMetrics{
			Depth: 5,
			Ready: 2,
			ByChannel: map[string]ChannelQueueMetrics{
				"sms": {Pending: 2, Ready: 1},
			},
		},
	}})

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
	if recorder.Header().Get("Content-Type") != "application/json" ||
		recorder.Header().Get("Cache-Control") != "no-store" {
		t.Error("metrics response headers are incorrect")
	}
	var result MetricsSnapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Queue.Depth != 5 || result.Queue.Ready != 2 || result.WindowSeconds != 300 {
		t.Errorf("unexpected metrics snapshot: %+v", result)
	}
}

func TestMetricsUnavailable(t *testing.T) {
	mux := http.NewServeMux()
	RegisterMetricsRoutes(mux, fakeMetricsSource{err: errors.New("database down")})

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/metrics", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", recorder.Code)
	}
	if strings.Contains(recorder.Body.String(), "database down") {
		t.Error("internal database details must not leak into the response")
	}
}

func TestRequestLoggerCorrelationAndPrivacy(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/notifications/{id}", func(w http.ResponseWriter, r *http.Request) {
		if RequestID(r.Context()) != "request-42" {
			t.Error("request ID was not propagated through context")
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("ok"))
	})

	id := uuid.NewString()
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/notifications/"+id+"?recipient=private@example.com",
		nil,
	)
	req.Header.Set("X-Request-ID", "request-42")

	recorder := httptest.NewRecorder()
	RequestLogger(mux, logger).ServeHTTP(recorder, req)
	if recorder.Code != http.StatusAccepted || recorder.Header().Get("X-Request-ID") != "request-42" {
		t.Errorf("unexpected response: %d headers=%v", recorder.Code, recorder.Header())
	}
	if recorder.Header().Get("X-Correlation-ID") != id {
		t.Errorf("unexpected correlation ID: %q", recorder.Header().Get("X-Correlation-ID"))
	}

	var event map[string]any
	if err := json.NewDecoder(&logs).Decode(&event); err != nil {
		t.Fatal(err)
	}
	if event["request_id"] != "request-42" || event["correlation_id"] != id ||
		event["route"] != "GET /api/v1/notifications/{id}" ||
		event["status"] != float64(http.StatusAccepted) {
		t.Errorf("unexpected log event: %v", event)
	}
	if strings.Contains(logs.String(), "private@example.com") {
		t.Error("query parameters must not appear in request logs")
	}
}

func TestRequestLoggerInvalidRequestID(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", "not valid id")
	recorder := httptest.NewRecorder()
	RequestLogger(mux, logger).ServeHTTP(recorder, req)

	value := recorder.Header().Get("X-Request-ID")
	if value == "not valid id" {
		t.Fatal("invalid caller-provided request ID was accepted")
	}
	if _, err := uuid.Parse(value); err != nil {
		t.Fatalf("expected generated UUID, got %q: %v", value, err)
	}
}
