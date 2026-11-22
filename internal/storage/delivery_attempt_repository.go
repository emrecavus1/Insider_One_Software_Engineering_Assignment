package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

var ErrAttemptAlreadyStarted = errors.New(
	"a delivery attempt is already in progress",
)

var ErrAttemptNotActive = errors.New(
	"delivery attempt is not active",
)

var ErrAttemptsExhausted = errors.New(
	"notification has exhausted its delivery attempts",
)

func validateClaimIdentity(job notification.ClaimedNotification) error {
	if _, err := uuid.Parse(job.ID); err != nil {
		return fmt.Errorf("invalid notification ID: %w", err)
	}

	if _, err := uuid.Parse(job.ClaimToken); err != nil {
		return fmt.Errorf("invalid claim token: %w", err)
	}

	return nil
}

// BeginDeliveryAttempt records a new attempt under an active claim
func (r *NotificationRepository) BeginDeliveryAttempt(
	ctx context.Context,
	job notification.ClaimedNotification,
) (int, error) {
	if err := validateClaimIdentity(job); err != nil {
		return 0, err
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin attempt transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var attemptCount, maxAttempts int

	err = tx.QueryRow(ctx, `
		SELECT attempt_count, max_attempts
		FROM notifications
		WHERE id = $1::uuid
		  AND claim_token = $2::uuid
		  AND status = 'processing'
		  AND lease_expires_at > clock_timestamp()
		FOR UPDATE
	`, job.ID, job.ClaimToken).Scan(
		&attemptCount,
		&maxAttempts,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrClaimNotOwned
	}

	if err != nil {
		return 0, fmt.Errorf("lock notification claim: %w", err)
	}

	if attemptCount >= maxAttempts {
		return 0, ErrAttemptsExhausted
	}

	var alreadyStarted bool

	err = tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM delivery_attempts
			WHERE notification_id = $1::uuid
			  AND outcome = 'started'
		)
	`, job.ID).Scan(&alreadyStarted)

	if err != nil {
		return 0, fmt.Errorf("inspect active attempt: %w", err)
	}

	if alreadyStarted {
		return 0, ErrAttemptAlreadyStarted
	}

	attemptNumber := attemptCount + 1

	_, err = tx.Exec(ctx, `
		UPDATE notifications
		SET attempt_count = $2,
		    updated_at = clock_timestamp()
		WHERE id = $1::uuid
	`, job.ID, attemptNumber)

	if err != nil {
		return 0, fmt.Errorf("increment attempt count: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO delivery_attempts (
			notification_id,
			attempt_number,
			outcome
		)
		VALUES ($1::uuid, $2, 'started')
	`, job.ID, attemptNumber)

	if err != nil {
		return 0, fmt.Errorf("insert delivery attempt: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit delivery attempt: %w", err)
	}

	return attemptNumber, nil
}

// FinishDeliveryAttempt records the outcome and releases claim ownership
func (r *NotificationRepository) FinishDeliveryAttempt(
	ctx context.Context,
	job notification.ClaimedNotification,
	attemptNumber int,
	result notification.DeliveryResult,
	retryDelay time.Duration,
) (notification.Status, error) {
	if err := validateClaimIdentity(job); err != nil {
		return "", err
	}

	if attemptNumber < 1 {
		return "", fmt.Errorf("attempt number must be positive")
	}

	if retryDelay < 0 || retryDelay > 24*time.Hour {
		return "", fmt.Errorf("invalid retry delay")
	}

	if result.HTTPStatus != 0 &&
		(result.HTTPStatus < 100 || result.HTTPStatus > 599) {
		return "", fmt.Errorf("invalid provider HTTP status")
	}

	switch result.Outcome {
	case notification.OutcomeAccepted,
		notification.OutcomeRetryableFailure,
		notification.OutcomePermanentFailure,
		notification.OutcomeUnknown:
	default:
		return "", fmt.Errorf("invalid delivery outcome: %s", result.Outcome)
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin completion transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var currentAttempt, maxAttempts int

	err = tx.QueryRow(ctx, `
		SELECT attempt_count, max_attempts
		FROM notifications
		WHERE id = $1::uuid
		  AND claim_token = $2::uuid
		  AND status = 'processing'
		  AND lease_expires_at > clock_timestamp()
		FOR UPDATE
	`, job.ID, job.ClaimToken).Scan(
		&currentAttempt,
		&maxAttempts,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrClaimNotOwned
	}

	if err != nil {
		return "", fmt.Errorf("lock delivery claim: %w", err)
	}

	if currentAttempt != attemptNumber {
		return "", ErrAttemptNotActive
	}

	var outcome notification.Status

	switch result.Outcome {
	case notification.OutcomeAccepted:
		outcome = notification.StatusAccepted

	case notification.OutcomePermanentFailure:
		outcome = notification.StatusFailed

	case notification.OutcomeRetryableFailure,
		notification.OutcomeUnknown:
		if currentAttempt >= maxAttempts {
			outcome = notification.StatusFailed
		} else {
			outcome = notification.StatusRetrying
		}
	}

	var httpStatus any
	if result.HTTPStatus != 0 {
		httpStatus = result.HTTPStatus
	}

	errorMessage := strings.TrimSpace(result.ErrorMessage)
	if errorMessage == "" && outcome != notification.StatusAccepted {
		errorMessage = string(result.Outcome)
	}

	command, err := tx.Exec(ctx, `
		UPDATE delivery_attempts
		SET
			outcome = $3,
			http_status = $4,
			provider_message_id = NULLIF($5, ''),
			error_message = NULLIF($6, ''),
			finished_at = clock_timestamp()
		WHERE notification_id = $1::uuid
		  AND attempt_number = $2
		  AND outcome = 'started'
	`,
		job.ID,
		attemptNumber,
		string(result.Outcome),
		httpStatus,
		result.ProviderMessageID,
		errorMessage,
	)

	if err != nil {
		return "", fmt.Errorf("complete delivery attempt: %w", err)
	}

	if command.RowsAffected() != 1 {
		return "", ErrAttemptNotActive
	}

	command, err = tx.Exec(ctx, `
		UPDATE notifications
		SET
			status = $3,
			worker_id = NULL,
			claim_token = NULL,
			lease_expires_at = NULL,
			next_attempt_at = CASE
				WHEN $3 = 'retrying'
					THEN clock_timestamp() +
						($4::bigint * INTERVAL '1 microsecond')
				ELSE next_attempt_at
			END,
			accepted_at = CASE
				WHEN $3 = 'accepted'
					THEN clock_timestamp()
				ELSE accepted_at
			END,
			provider_message_id = CASE
				WHEN $3 = 'accepted'
					THEN NULLIF($5, '')
				ELSE provider_message_id
			END,
			last_error = CASE
				WHEN $3 = 'accepted'
					THEN NULL
				ELSE $6
			END,
			updated_at = clock_timestamp()
		WHERE id = $1::uuid
		  AND claim_token = $2::uuid
		  AND status = 'processing'
	`,
		job.ID,
		job.ClaimToken,
		string(outcome),
		retryDelay.Microseconds(),
		result.ProviderMessageID,
		errorMessage,
	)

	if err != nil {
		return "", fmt.Errorf("update notification outcome: %w", err)
	}

	if command.RowsAffected() != 1 {
		return "", ErrClaimNotOwned
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit delivery outcome: %w", err)
	}

	return outcome, nil
}
