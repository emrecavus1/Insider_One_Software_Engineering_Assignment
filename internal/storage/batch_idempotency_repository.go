package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

// findIdempotentBatch looks for a previously committed batch request using the supplied idempotency key
func (r *NotificationRepository) findIdempotentBatch(
	ctx context.Context,
	key string,
	hash string,
) (notification.BatchCreateResult, bool, error) {

	var storedHash string
	var batchID string
	var storedResult []byte

	err := r.db.QueryRow(ctx, `
		SELECT
			request_hash,
			COALESCE(batch_id::text, ''),
			batch_result
		FROM idempotency_keys
		WHERE key = $1
	`, key).Scan(
		&storedHash,
		&batchID,
		&storedResult,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return notification.BatchCreateResult{}, false, nil
	}

	if err != nil {
		return notification.BatchCreateResult{}, false,
			fmt.Errorf("lookup batch idempotency key: %w", err)
	}

	// The key must belong to the same batch request
	// Keys associated with single notifications are rejected

	if storedHash != hash || batchID == "" {
		return notification.BatchCreateResult{}, false,
			ErrIdempotencyConflict
	}

	if len(storedResult) == 0 {
		return notification.BatchCreateResult{}, false,
			fmt.Errorf("batch idempotency result is missing")
	}

	var result notification.BatchCreateResult

	if err := json.Unmarshal(storedResult, &result); err != nil {
		return notification.BatchCreateResult{}, false,
			fmt.Errorf("decode stored batch result: %w", err)
	}

	if result.BatchID != batchID ||
		result.Count != len(result.NotificationIDs) {
		return notification.BatchCreateResult{}, false,
			fmt.Errorf("stored batch result is inconsistent")
	}

	return result, true, nil
}

// CreateBatchIdempotent creates a batch atomically with its idempotency record

// In this case, replayed=true indicates that the original batch has been returned without creating another batch

func (r *NotificationRepository) CreateBatchIdempotent(
	ctx context.Context,
	requests []notification.CreateRequest,
	key string,
) (notification.BatchCreateResult, bool, error) {

	if strings.TrimSpace(key) == "" || len(key) > 128 {
		return notification.BatchCreateResult{}, false,
			fmt.Errorf("idempotency key must contain 1 to 128 characters")
	}

	// Validate and normalize every request before opening the transaction, using our existing domain rules
	if len(requests) < 1 || len(requests) > 1000 {
		return notification.BatchCreateResult{}, false,
			fmt.Errorf("batch must contain between 1 and 1000 notifications")
	}

	normalized := make([]notification.CreateRequest, len(requests))

	for i, request := range requests {
		value, err := request.NormalizeAndValidate()
		if err != nil {
			return notification.BatchCreateResult{}, false,
				fmt.Errorf("notification %d: %w", i+1, err)
		}
		normalized[i] = value
	}

	hash, err := notification.FingerprintBatch(normalized)
	if err != nil {
		return notification.BatchCreateResult{}, false, err
	}

	// Fast path for previously committed requests
	existing, found, err := r.findIdempotentBatch(
		ctx, key, hash,
	)
	if err != nil || found {
		return existing, found, err
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return notification.BatchCreateResult{}, false,
			fmt.Errorf("begin batch idempotency transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	batchID := uuid.NewString()

	// Creating the batch inside the same transaction
	_, err = tx.Exec(ctx, `
		INSERT INTO batches (id)
		VALUES ($1::uuid)
	`, batchID)

	if err != nil {
		return notification.BatchCreateResult{}, false,
			fmt.Errorf("insert batch: %w", err)
	}

	result := notification.BatchCreateResult{
		BatchID:         batchID,
		Count:           len(normalized),
		NotificationIDs: make([]string, 0, len(normalized)),
	}

	// Inserting notifications in request order
	// Their returned IDs are appended in that same order
	for i, request := range normalized {
		var notificationID string

		err := tx.QueryRow(ctx, `
			INSERT INTO notifications (
				batch_id,
				recipient,
				channel,
				content,
				priority
			)
			VALUES ($1::uuid, $2, $3, $4, $5)
			RETURNING id::text
		`,
			batchID,
			request.Recipient,
			string(request.Channel),
			request.Content,
			string(request.Priority),
		).Scan(&notificationID)

		if err != nil {
			return notification.BatchCreateResult{}, false,
				fmt.Errorf("insert batch notification %d: %w", i+1, err)
		}

		result.NotificationIDs = append(
			result.NotificationIDs,
			notificationID,
		)
	}

	// Saving the exact original response to preserve notification ID ordering across replays
	responseJSON, err := json.Marshal(result)
	if err != nil {
		return notification.BatchCreateResult{}, false,
			fmt.Errorf("encode batch result: %w", err)
	}

	var insertedKey string

	err = tx.QueryRow(ctx, `
		INSERT INTO idempotency_keys (
			key,
			request_hash,
			batch_id,
			batch_result
		)
		VALUES ($1, $2, $3::uuid, $4::jsonb)
		ON CONFLICT (key) DO NOTHING
		RETURNING key
	`,
		key,
		hash,
		batchID,
		string(responseJSON),
	).Scan(&insertedKey)

	if errors.Is(err, pgx.ErrNoRows) {
		// This represents another transaction won the race
		// Removing our temporary batch and notifications
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return notification.BatchCreateResult{}, false,
				fmt.Errorf("rollback duplicate batch: %w", rollbackErr)
		}

		// Fetching the winning request's committed result.
		existing, found, lookupErr := r.findIdempotentBatch(
			ctx, key, hash,
		)

		if lookupErr != nil {
			return notification.BatchCreateResult{}, false, lookupErr
		}

		if !found {
			return notification.BatchCreateResult{}, false,
				fmt.Errorf("idempotency record missing after conflict")
		}

		return existing, true, nil
	}

	if err != nil {
		return notification.BatchCreateResult{}, false,
			fmt.Errorf("insert batch idempotency key: %w", err)
	}

	// Batch, notifications, key and response become committed together
	if err := tx.Commit(ctx); err != nil {
		return notification.BatchCreateResult{}, false,
			fmt.Errorf("commit idempotent batch: %w", err)
	}

	return result, false, nil
}
