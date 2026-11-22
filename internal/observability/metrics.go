package observability

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	metricsWindow = 5 * time.Minute
	cacheTTL      = 2 * time.Second
)

type ChannelQueueMetrics struct {
	Pending    int64 `json:"pending"`
	Retrying   int64 `json:"retrying"`
	Processing int64 `json:"processing"`
	Ready      int64 `json:"ready"`
	Scheduled  int64 `json:"scheduled"`
	Blocked    int64 `json:"blocked"`
}

type QueueMetrics struct {
	Depth      int64                          `json:"depth"`
	Pending    int64                          `json:"pending"`
	Retrying   int64                          `json:"retrying"`
	Processing int64                          `json:"processing"`
	Ready      int64                          `json:"ready"`
	Scheduled  int64                          `json:"scheduled"`
	Blocked    int64                          `json:"blocked"`
	ByChannel  map[string]ChannelQueueMetrics `json:"by_channel"`
}

type LatencyMetrics struct {
	AverageMs float64 `json:"average_ms"`
	P50Ms     float64 `json:"p50_ms"`
	P95Ms     float64 `json:"p95_ms"`
}

type DeliveryMetrics struct {
	CompletedAttempts int64          `json:"completed_attempts"`
	Accepted          int64          `json:"accepted"`
	RetryableFailures int64          `json:"retryable_failures"`
	PermanentFailures int64          `json:"permanent_failures"`
	Unknown           int64          `json:"unknown"`
	SuccessRate       float64        `json:"success_rate"`
	FailureRate       float64        `json:"failure_rate"`
	UnknownRate       float64        `json:"unknown_rate"`
	AttemptLatencyMs  LatencyMetrics `json:"attempt_latency_ms"`
	EndToEndLatencyMs LatencyMetrics `json:"end_to_end_latency_ms"`
}

type MetricsSnapshot struct {
	GeneratedAt   time.Time       `json:"generated_at"`
	WindowSeconds int             `json:"window_seconds"`
	Queue         QueueMetrics    `json:"queue"`
	Delivery      DeliveryMetrics `json:"delivery"`
}

type MetricsRepository struct {
	db       *pgxpool.Pool
	mu       sync.Mutex
	cached   MetricsSnapshot
	cachedAt time.Time
}

func NewMetricsRepository(db *pgxpool.Pool) *MetricsRepository {
	return &MetricsRepository{db: db}
}

// Snapshot reports queue state and delivery outcomes across all worker instances
func (r *MetricsRepository) Snapshot(ctx context.Context) (MetricsSnapshot, error) {
	if r == nil || r.db == nil {
		return MetricsSnapshot{}, fmt.Errorf("metrics database is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return MetricsSnapshot{}, err
	}

	if !r.cachedAt.IsZero() && time.Since(r.cachedAt) < cacheTTL {
		return r.cached, nil
	}

	result := MetricsSnapshot{
		GeneratedAt:   time.Now().UTC(),
		WindowSeconds: int(metricsWindow / time.Second),
		Queue: QueueMetrics{ByChannel: map[string]ChannelQueueMetrics{
			"sms": {}, "email": {}, "push": {},
		}},
	}

	const queueSQL = `
		SELECT channel, status, COUNT(*)::bigint,
			COUNT(*) FILTER (
				WHERE status IN ('pending', 'retrying')
				  AND attempt_count < max_attempts
				  AND next_attempt_at <= NOW()
			)::bigint,
			COUNT(*) FILTER (
				WHERE status IN ('pending', 'retrying')
				  AND attempt_count < max_attempts
				  AND next_attempt_at > NOW()
			)::bigint,
			COUNT(*) FILTER (
				WHERE status IN ('pending', 'retrying')
				  AND attempt_count >= max_attempts
			)::bigint
		FROM notifications
		WHERE status IN ('pending', 'retrying', 'processing')
		GROUP BY channel, status
	`

	rows, err := r.db.Query(ctx, queueSQL)
	if err != nil {
		return MetricsSnapshot{}, fmt.Errorf("query queue metrics: %w", err)
	}

	for rows.Next() {
		var channel, status string
		var count, ready, scheduled, blocked int64
		if err := rows.Scan(&channel, &status, &count, &ready, &scheduled, &blocked); err != nil {
			rows.Close()
			return MetricsSnapshot{}, fmt.Errorf("scan queue metrics: %w", err)
		}

		byChannel := result.Queue.ByChannel[channel]
		switch status {
		case "pending":
			byChannel.Pending += count
			result.Queue.Pending += count
		case "retrying":
			byChannel.Retrying += count
			result.Queue.Retrying += count
		case "processing":
			byChannel.Processing += count
			result.Queue.Processing += count
		}
		byChannel.Ready += ready
		byChannel.Scheduled += scheduled
		byChannel.Blocked += blocked
		result.Queue.Ready += ready
		result.Queue.Scheduled += scheduled
		result.Queue.Blocked += blocked
		result.Queue.ByChannel[channel] = byChannel
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return MetricsSnapshot{}, fmt.Errorf("iterate queue metrics: %w", err)
	}
	rows.Close()

	result.Queue.Depth = result.Queue.Pending + result.Queue.Retrying + result.Queue.Processing

	const attemptsSQL = `
		SELECT
			COUNT(*) FILTER (WHERE outcome = 'accepted')::bigint,
			COUNT(*) FILTER (WHERE outcome = 'retryable_failure')::bigint,
			COUNT(*) FILTER (WHERE outcome = 'permanent_failure')::bigint,
			COUNT(*) FILTER (WHERE outcome = 'unknown')::bigint,
			COALESCE(AVG(
				(EXTRACT(EPOCH FROM finished_at - started_at) * 1000)::double precision
			) FILTER (WHERE outcome = 'accepted'), 0)::double precision,
			COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (
				ORDER BY (EXTRACT(EPOCH FROM finished_at - started_at) * 1000)::double precision
			) FILTER (WHERE outcome = 'accepted'), 0)::double precision,
			COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (
				ORDER BY (EXTRACT(EPOCH FROM finished_at - started_at) * 1000)::double precision
			) FILTER (WHERE outcome = 'accepted'), 0)::double precision
		FROM delivery_attempts
		WHERE finished_at >= NOW() - INTERVAL '5 minutes'
		  AND finished_at <= NOW()
	`

	delivery := &result.Delivery
	err = r.db.QueryRow(ctx, attemptsSQL).Scan(
		&delivery.Accepted,
		&delivery.RetryableFailures,
		&delivery.PermanentFailures,
		&delivery.Unknown,
		&delivery.AttemptLatencyMs.AverageMs,
		&delivery.AttemptLatencyMs.P50Ms,
		&delivery.AttemptLatencyMs.P95Ms,
	)
	if err != nil {
		return MetricsSnapshot{}, fmt.Errorf("query delivery metrics: %w", err)
	}

	delivery.CompletedAttempts = delivery.Accepted + delivery.RetryableFailures +
		delivery.PermanentFailures + delivery.Unknown
	if delivery.CompletedAttempts > 0 {
		denominator := float64(delivery.CompletedAttempts)
		delivery.SuccessRate = float64(delivery.Accepted) / denominator
		delivery.FailureRate = float64(delivery.RetryableFailures+delivery.PermanentFailures) / denominator
		delivery.UnknownRate = float64(delivery.Unknown) / denominator
	}

	const endToEndSQL = `
		SELECT
			COALESCE(AVG(
				(EXTRACT(EPOCH FROM accepted_at - created_at) * 1000)::double precision
			), 0)::double precision,
			COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (
				ORDER BY (EXTRACT(EPOCH FROM accepted_at - created_at) * 1000)::double precision
			), 0)::double precision,
			COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (
				ORDER BY (EXTRACT(EPOCH FROM accepted_at - created_at) * 1000)::double precision
			), 0)::double precision
		FROM notifications
		WHERE status = 'accepted'
		  AND accepted_at >= NOW() - INTERVAL '5 minutes'
		  AND accepted_at <= NOW()
	`

	err = r.db.QueryRow(ctx, endToEndSQL).Scan(
		&delivery.EndToEndLatencyMs.AverageMs,
		&delivery.EndToEndLatencyMs.P50Ms,
		&delivery.EndToEndLatencyMs.P95Ms,
	)
	if err != nil {
		return MetricsSnapshot{}, fmt.Errorf("query end-to-end metrics: %w", err)
	}

	r.cached = result
	r.cachedAt = time.Now()
	return result, nil
}
