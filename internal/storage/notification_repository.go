// This file is to encapsulate database operations
package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

var ErrNotificationNotFound = errors.New("notification not found")

type NotificationRepository struct {
	db *pgxpool.Pool
}

func NewNotificationRepository(db *pgxpool.Pool) *NotificationRepository {
	return &NotificationRepository{db: db}
}

// Create validates and persists a notification.
func (r *NotificationRepository) Create(
	ctx context.Context,
	request notification.CreateRequest,
) (notification.Notification, error) {

	validated, err := request.NormalizeAndValidate()
	if err != nil {
		return notification.Notification{}, fmt.Errorf(
			"validate notification: %w", err,
		)
	}

	query := `
		INSERT INTO notifications (
			recipient, channel, content, priority
		)
		VALUES ($1, $2, $3, $4)
		RETURNING id::text, status, created_at
	`

	var result notification.Notification
	var status string

	err = r.db.QueryRow(
		ctx,
		query,
		validated.Recipient,
		string(validated.Channel),
		validated.Content,
		string(validated.Priority),
	).Scan(
		&result.ID,
		&status,
		&result.CreatedAt,
	)

	if err != nil {
		return notification.Notification{}, fmt.Errorf(
			"insert notification: %w", err,
		)
	}

	result.Recipient = validated.Recipient
	result.Channel = validated.Channel
	result.Content = validated.Content
	result.Priority = validated.Priority
	result.Status = notification.Status(status)

	return result, nil
}

// GetByID retrieves one notification.
func (r *NotificationRepository) GetByID(
	ctx context.Context,
	id string,
) (notification.Notification, error) {

	query := `
		SELECT
			id::text,
			COALESCE(batch_id::text, ''),
			recipient,
			channel,
			content,
			priority,
			status,
			created_at
		FROM notifications
		WHERE id = $1::uuid
	`

	var result notification.Notification
	var batchID string
	var channel, priority, status string

	err := r.db.QueryRow(ctx, query, id).Scan(
		&result.ID,
		&batchID,
		&result.Recipient,
		&channel,
		&result.Content,
		&priority,
		&status,
		&result.CreatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return notification.Notification{}, ErrNotificationNotFound
	}

	if err != nil {
		return notification.Notification{}, fmt.Errorf(
			"retrieve notification: %w", err,
		)
	}

	if batchID != "" {
		result.BatchID = &batchID
	}

	result.Channel = notification.Channel(channel)
	result.Priority = notification.Priority(priority)
	result.Status = notification.Status(status)

	return result, nil
}
