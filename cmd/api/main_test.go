// The tests in this file verify:
// healthz returns HTTP 200
// readyz returns HTTP 200 when the database is available
// readyz returns HTTP 503 when the database is unavailable
package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Fake database dependency for HTTP tests.
type mockPinger struct {
	err error
}

func (m mockPinger) Ping(ctx context.Context) error {
	return m.err
}

func TestHealthz(t *testing.T) {
	handler := newHandler(mockPinger{})

	request := httptest.NewRequest(
		http.MethodGet, "/healthz", nil,
	)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Errorf(
			"expected status 200, got %d",
			response.Code,
		)
	}

	if strings.TrimSpace(response.Body.String()) != "OK" {
		t.Errorf("unexpected response: %s", response.Body.String())
	}
}

func TestReadyz(t *testing.T) {
	tests := []struct {
		name           string
		databaseError  error
		expectedStatus int
	}{
		{
			name:           "database available",
			databaseError:  nil,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "database unavailable",
			databaseError:  errors.New("connection lost"),
			expectedStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newHandler(mockPinger{
				err: tt.databaseError,
			})

			request := httptest.NewRequest(
				http.MethodGet, "/readyz", nil,
			)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != tt.expectedStatus {
				t.Errorf(
					"expected status %d, got %d",
					tt.expectedStatus,
					response.Code,
				)
			}
		})
	}
}
