// This file avoids duplicating header validation in the two HTTP handlers
package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

var ErrInvalidIdempotencyKey = errors.New(
	"idempotency key must be 1 to 128 printable ASCII characters",
)

// Reading an optional Idempotency-Key header
// Missing is allowed; malformed or repeated headers are not
func readIdempotencyKey(
	r *http.Request,
) (key string, present bool, err error) {
	values := r.Header.Values("Idempotency-Key")

	if len(values) == 0 {
		return "", false, nil
	}

	if len(values) != 1 {
		return "", true, ErrInvalidIdempotencyKey
	}

	key = values[0]

	if len(key) == 0 || len(key) > 128 {
		return "", true, ErrInvalidIdempotencyKey
	}

	for i := 0; i < len(key); i++ {
		if key[i] < 33 || key[i] > 126 {
			return "", true, ErrInvalidIdempotencyKey
		}
	}

	return key, true, nil
}

func createSingleWithIdempotency(
	ctx context.Context,
	repo NotificationRepository,
	request notification.CreateRequest,
	key string,
	hasKey bool,
) (notification.Notification, bool, error) {
	if !hasKey {
		result, err := repo.Create(ctx, request)
		return result, false, err
	}

	creator, ok := repo.(SingleIdempotencyCreator)
	if !ok {
		return notification.Notification{}, false,
			errors.New("repository does not support single idempotency")
	}

	return creator.CreateIdempotent(ctx, request, key)
}

func createBatchWithIdempotency(
	ctx context.Context,
	repo BatchRepository,
	requests []notification.CreateRequest,
	key string,
	hasKey bool,
) (notification.BatchCreateResult, bool, error) {
	if !hasKey {
		result, err := repo.CreateBatch(ctx, requests)
		return result, false, err
	}

	creator, ok := repo.(BatchIdempotencyCreator)
	if !ok {
		return notification.BatchCreateResult{}, false,
			errors.New("repository does not support batch idempotency")
	}

	return creator.CreateBatchIdempotent(ctx, requests, key)
}

type SingleIdempotencyCreator interface {
	CreateIdempotent(
		context.Context,
		notification.CreateRequest,
		string,
	) (notification.Notification, bool, error)
}

type BatchIdempotencyCreator interface {
	CreateBatchIdempotent(
		context.Context,
		[]notification.CreateRequest,
		string,
	) (notification.BatchCreateResult, bool, error)
}
