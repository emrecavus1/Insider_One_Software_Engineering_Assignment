package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

var ErrIdempotencyConflict = errors.New(
	"idempotency key already used for a different request",
)

// findIdempotentSingle checks whether an idempotency key already refers to an existing notification

// In this case, found=true means this request has already been processed.
func (r *NotificationRepository) findIdempotentSingle(
	ctx context.Context,
	key string,
	hash string,
) (notification.Notification, bool, error) {

	var storedHash string
	var notificationID string

	err := r.db.QueryRow(ctx, `
		SELECT
			request_hash,
			COALESCE(notification_id::text, '')
		FROM idempotency_keys
		WHERE key = $1
	`, key).Scan(&storedHash, &notificationID)

	if errors.Is(err, pgx.ErrNoRows) {
		return notification.Notification{}, false, nil
	}

	if err != nil {
		return notification.Notification{}, false,
			fmt.Errorf("lookup idempotency key: %w", err)
	}

	// The same key cannot represent a different request or a different resource type
	if storedHash != hash || notificationID == "" {
		return notification.Notification{}, false,
			ErrIdempotencyConflict
	}

	existing, err := r.GetByID(ctx, notificationID)
	if err != nil {
		return notification.Notification{}, false,
			fmt.Errorf("retrieve idempotent notification: %w", err)
	}

	return existing, true, nil
}

// CreateIdempotent creates at most one notification for a given key and normalized request payload

// In this case, replayed=true means the notification already existed
func (r *NotificationRepository) CreateIdempotent(
	ctx context.Context,
	request notification.CreateRequest,
	key string,
) (notification.Notification, bool, error) {

	if strings.TrimSpace(key) == "" || len(key) > 128 {
		return notification.Notification{}, false,
			fmt.Errorf("idempotency key must contain 1 to 128 characters")
	}

	normalized, err := request.NormalizeAndValidate()
	if err != nil {
		return notification.Notification{}, false, err
	}

	hash, err := notification.FingerprintSingle(normalized)
	if err != nil {
		return notification.Notification{}, false, err
	}

	// Fast path: avoiding inserting anything if this key has already been committed
	existing, found, err := r.findIdempotentSingle(
		ctx, key, hash,
	)
	if err != nil || found {
		return existing, found, err
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return notification.Notification{}, false,
			fmt.Errorf("begin idempotency transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var result notification.Notification
	var status string

	// This INSERT must use the same columns and defaults as our existing Create repository method
	err = tx.QueryRow(ctx, `
		INSERT INTO notifications (
			recipient,
			channel,
			content,
			priority
		)
		VALUES ($1, $2, $3, $4)
		RETURNING id::text, status, created_at
	`,
		normalized.Recipient,
		string(normalized.Channel),
		normalized.Content,
		string(normalized.Priority),
	).Scan(
		&result.ID,
		&status,
		&result.CreatedAt,
	)

	if err != nil {
		return notification.Notification{}, false,
			fmt.Errorf("insert notification: %w", err)
	}

	result.Recipient = normalized.Recipient
	result.Channel = normalized.Channel
	result.Content = normalized.Content
	result.Priority = normalized.Priority
	result.Status = notification.Status(status)

	// The database's unique key constraint decides requests arriving at the same time
	var insertedKey string

	err = tx.QueryRow(ctx, `
		INSERT INTO idempotency_keys (
			key,
			request_hash,
			notification_id
		)
		VALUES ($1, $2, $3::uuid)
		ON CONFLICT (key) DO NOTHING
		RETURNING key
	`, key, hash, result.ID).Scan(&insertedKey)

	if errors.Is(err, pgx.ErrNoRows) {
		// If another transaction committed this key first.
		// Roll back our temporary notification before retrieving the winning request's result
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return notification.Notification{}, false,
				fmt.Errorf("rollback duplicate request: %w", rollbackErr)
		}

		existing, found, lookupErr := r.findIdempotentSingle(
			ctx, key, hash,
		)
		if lookupErr != nil {
			return notification.Notification{}, false, lookupErr
		}

		if !found {
			return notification.Notification{}, false,
				fmt.Errorf("idempotency record missing after conflict")
		}

		return existing, true, nil
	}

	if err != nil {
		return notification.Notification{}, false,
			fmt.Errorf("insert idempotency key: %w", err)
	}

	// Both records become visible together
	if err := tx.Commit(ctx); err != nil {
		return notification.Notification{}, false,
			fmt.Errorf("commit idempotent notification: %w", err)
	}

	return result, false, nil
}
