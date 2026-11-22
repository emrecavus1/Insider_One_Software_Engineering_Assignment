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

var ErrNotificationNotCancellable = errors.New(
	"notification cannot be cancelled in its current state",
)

type NotificationRepository struct {
	db *pgxpool.Pool
}

func NewNotificationRepository(db *pgxpool.Pool) *NotificationRepository {
	return &NotificationRepository{db: db}
}

// Create validates and persists a notification
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

// GetByID retrieves one notification
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

// We need to handle batch requests as well

func (r *NotificationRepository) CreateBatch(
	ctx context.Context,
	requests []notification.CreateRequest,
) (notification.BatchCreateResult, error) {

	// enforcing the batch size requirement
	if len(requests) == 0 || len(requests) > 1000 {
		return notification.BatchCreateResult{},
			fmt.Errorf("batch must contain between 1 and 1000 notifications")
	}

	// validating everything before starting the transaction
	validated := make([]notification.CreateRequest, len(requests))

	for i, request := range requests {
		result, err := request.NormalizeAndValidate()
		if err != nil {
			return notification.BatchCreateResult{},
				fmt.Errorf("notification %d: %w", i+1, err)
		}
		validated[i] = result
	}

	// starting a PostgreSQL transaction
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return notification.BatchCreateResult{},
			fmt.Errorf("begin batch transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// creating the batch record
	var batchID string

	err = tx.QueryRow(
		ctx,
		"INSERT INTO batches DEFAULT VALUES RETURNING id::text",
	).Scan(&batchID)

	if err != nil {
		return notification.BatchCreateResult{},
			fmt.Errorf("create batch: %w", err)
	}

	notificationIDs := make([]string, 0, len(validated))

	// inserting every notification into the same transaction
	for _, request := range validated {
		var notificationID string

		err = tx.QueryRow(
			ctx,
			`INSERT INTO notifications
				(batch_id, recipient, channel, content, priority)
			 VALUES ($1::uuid, $2, $3, $4, $5)
			 RETURNING id::text`,
			batchID,
			request.Recipient,
			string(request.Channel),
			request.Content,
			string(request.Priority),
		).Scan(&notificationID)

		if err != nil {
			return notification.BatchCreateResult{},
				fmt.Errorf("insert batch notification: %w", err)
		}

		notificationIDs = append(notificationIDs, notificationID)
	}

	// committing all changes together
	if err := tx.Commit(ctx); err != nil {
		return notification.BatchCreateResult{},
			fmt.Errorf("commit batch transaction: %w", err)
	}

	return notification.BatchCreateResult{
		BatchID:         batchID,
		Count:           len(notificationIDs),
		NotificationIDs: notificationIDs,
	}, nil
}

func (r *NotificationRepository) Cancel(
	ctx context.Context,
	id string,
) (notification.Notification, error) {

	// Cancel only notifications that are not currently being processed or already finalized
	query := `
		UPDATE notifications
		SET
			status = 'cancelled',
			cancelled_at = NOW(),
			updated_at = NOW(),
			worker_id = NULL,
			lease_expires_at = NULL
		WHERE id = $1::uuid
			AND status IN ('pending', 'retrying')
		RETURNING
			id::text,
			COALESCE(batch_id::text, ''),
			recipient,
			channel,
			content,
			priority,
			status,
			created_at
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

	if err == nil {
		if batchID != "" {
			result.BatchID = &batchID
		}

		result.Channel = notification.Channel(channel)
		result.Priority = notification.Priority(priority)
		result.Status = notification.Status(status)

		return result, nil
	}

	// No updated row: either the notification does not exist or its status does not allow cancellation
	if !errors.Is(err, pgx.ErrNoRows) {
		return notification.Notification{},
			fmt.Errorf("cancel notification: %w", err)
	}

	_, err = r.GetByID(ctx, id)

	if errors.Is(err, ErrNotificationNotFound) {
		return notification.Notification{}, ErrNotificationNotFound
	}

	if err != nil {
		return notification.Notification{},
			fmt.Errorf("check notification status: %w", err)
	}

	return notification.Notification{}, ErrNotificationNotCancellable
}
