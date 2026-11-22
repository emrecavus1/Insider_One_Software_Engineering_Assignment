package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

type fakeListRepository struct {
	called int
	filter notification.ListFilter
	err    error
}

func (f *fakeListRepository) List(
	ctx context.Context,
	filter notification.ListFilter,
) (notification.ListResult, error) {
	f.called++
	f.filter = filter

	if f.err != nil {
		return notification.ListResult{}, f.err
	}

	return notification.ListResult{
		Notifications: []notification.Notification{
			{
				ID:     "11111111-1111-4111-8111-111111111111",
				Status: notification.StatusPending,
			},
		},
		Total: 3,
		Page:  filter.Page,
		Limit: filter.Limit,
	}, nil
}

func TestListNotifications(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected int
	}{
		{"default pagination", "", http.StatusOK},
		{
			"combined filters",
			"?status=pending&channel=sms&" +
				"created_from=2026-11-20T00:00:00Z&" +
				"created_to=2026-11-23T00:00:00Z&" +
				"page=2&limit=5",
			http.StatusOK,
		},
		{"invalid status", "?status=unknown", http.StatusBadRequest},
		{"invalid channel", "?channel=whatsapp", http.StatusBadRequest},
		{"invalid start date", "?created_from=invalid", http.StatusBadRequest},
		{
			"reversed date range",
			"?created_from=2026-11-23T00:00:00Z&" +
				"created_to=2026-11-20T00:00:00Z",
			http.StatusBadRequest,
		},
		{"invalid page", "?page=0", http.StatusBadRequest},
		{"non-numeric page", "?page=abc", http.StatusBadRequest},
		{"invalid limit", "?limit=101", http.StatusBadRequest},
		{"negative limit", "?limit=-1", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeListRepository{}

			mux := http.NewServeMux()
			RegisterListRoutes(mux, repo)

			req := httptest.NewRequest(
				http.MethodGet,
				"/api/v1/notifications"+tt.query,
				nil,
			)

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tt.expected {
				t.Fatalf(
					"expected %d, got %d: %s",
					tt.expected,
					rec.Code,
					rec.Body.String(),
				)
			}

			if tt.expected != http.StatusOK {
				if repo.called != 0 {
					t.Error("invalid filters reached repository")
				}
				return
			}

			if repo.called != 1 {
				t.Fatal("expected one repository call")
			}

			var result notification.ListResult
			if err := json.Unmarshal(
				rec.Body.Bytes(), &result,
			); err != nil {
				t.Fatalf("decode response: %v", err)
			}

			if result.Total != 3 || len(result.Notifications) != 1 {
				t.Errorf("unexpected list result: %+v", result)
			}

			if tt.name == "default pagination" {
				if repo.filter.Page != 1 || repo.filter.Limit != 20 {
					t.Errorf("incorrect default pagination: %+v", repo.filter)
				}
			}

			if tt.name == "combined filters" {
				if repo.filter.Page != 2 || repo.filter.Limit != 5 {
					t.Error("incorrect pagination")
				}
				if repo.filter.Status != notification.StatusPending ||
					repo.filter.Channel != notification.ChannelSMS {
					t.Error("incorrect status or channel filter")
				}
				if repo.filter.CreatedFrom == nil ||
					repo.filter.CreatedTo == nil {
					t.Error("date filters were not parsed")
				}
			}
		})
	}
}

func TestListRepositoryFailure(t *testing.T) {
	repo := &fakeListRepository{
		err: errors.New("database unavailable"),
	}

	mux := http.NewServeMux()
	RegisterListRoutes(mux, repo)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/notifications",
		nil,
	)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}
