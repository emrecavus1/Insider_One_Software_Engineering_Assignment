package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/config"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/provider"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/ratelimit"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/worker"
)

func main() {
	if err := run(); err != nil {
		log.Printf("Worker error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	db, err := storage.OpenPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	defer db.Close()
	logger.Info("worker connected to PostgreSQL")

	enabled := false
	if raw := strings.TrimSpace(os.Getenv("DELIVERY_ENABLED")); raw != "" {
		enabled, err = strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("invalid DELIVERY_ENABLED: %w", err)
		}
	}
	if !enabled {
		return runStandby(ctx, db, logger)
	}

	endpoint := strings.TrimSpace(os.Getenv("WEBHOOK_URL"))
	if err := provider.ValidateWebhookSiteURL(endpoint); err != nil {
		return fmt.Errorf("validate WEBHOOK_URL: %w", err)
	}
	sender, err := provider.NewHTTPSender(endpoint, 8*time.Second)
	if err != nil {
		return fmt.Errorf("configure HTTP sender: %w", err)
	}

	redisHost := strings.TrimSpace(os.Getenv("REDIS_HOST"))
	if redisHost == "" {
		redisHost = "127.0.0.1"
	}
	redisPort := strings.TrimSpace(os.Getenv("REDIS_PORT"))
	if redisPort == "" {
		redisPort = "16379"
	}
	redisClient := redis.NewClient(&redis.Options{Addr: net.JoinHostPort(redisHost, redisPort)})
	defer redisClient.Close()

	pingCtx, cancelPing := context.WithTimeout(ctx, 3*time.Second)
	err = redisClient.Ping(pingCtx).Err()
	cancelPing()
	if err != nil {
		return fmt.Errorf("connect to Redis: %w", err)
	}

	limiter, err := ratelimit.NewRedisLimiter(redisClient, "insider-one:rate:v1", 100)
	if err != nil {
		return fmt.Errorf("configure rate limiter: %w", err)
	}

	repo := storage.NewNotificationRepository(db)
	processor, err := worker.NewRateLimitedDeliveryProcessor(repo, sender, limiter, 2*time.Second)
	if err != nil {
		return fmt.Errorf("configure delivery processor: %w", err)
	}
	if err := processor.SetRetryPolicy(worker.RetryPolicy{
		BaseDelay:      2 * time.Second,
		MaxDelay:       24 * time.Hour,
		JitterFraction: 0.2,
	}); err != nil {
		return fmt.Errorf("configure retry policy: %w", err)
	}

	runner, err := worker.NewRunner(repo, processor, worker.Options{
		Concurrency:       4,
		PollInterval:      250 * time.Millisecond,
		LeaseDuration:     30 * time.Second,
		ProcessingTimeout: 20 * time.Second,
		HeartbeatInterval: 5 * time.Second,
		WorkerIDPrefix:    "delivery-" + uuid.NewString(),
	}, logger)
	if err != nil {
		return fmt.Errorf("configure worker runner: %w", err)
	}

	recovery, err := worker.NewRecoveryLoop(repo, worker.RecoveryOptions{
		Interval:  5 * time.Second,
		BatchSize: 100,
	}, logger)
	if err != nil {
		return fmt.Errorf("configure lease recovery: %w", err)
	}
	service, err := worker.NewService(runner, recovery)
	if err != nil {
		return fmt.Errorf("configure worker service: %w", err)
	}

	logger.Info("live delivery enabled", "concurrency", 4, "rate_per_channel", 100)
	service.Run(ctx)
	logger.Info("worker stopped successfully")
	return nil
}

func runStandby(ctx context.Context, db *pgxpool.Pool, logger *slog.Logger) error {
	logger.Warn("worker running in standby mode", "reason", "delivery disabled")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("worker shutdown signal received")
			logger.Info("worker stopped successfully")
			return nil
		case <-ticker.C:
			queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			var readyCount int64
			err := db.QueryRow(queryCtx, `
                SELECT COUNT(*)
                FROM notifications
                WHERE status IN ('pending', 'retrying')
                  AND next_attempt_at <= NOW()
                  AND attempt_count < max_attempts
            `).Scan(&readyCount)
			cancel()
			if err != nil {
				if ctx.Err() == nil {
					logger.Error("failed to inspect notification queue", "error", err)
				}
				continue
			}
			logger.Info("notification queue inspected", "ready_notifications", readyCount)
		}
	}
}
