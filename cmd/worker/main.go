package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/config"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

func main() {
	if err := run(); err != nil {
		log.Printf("Worker error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	logger := slog.New(slog.NewJSONHandler(
		os.Stdout,
		nil,
	))

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

	logger.Warn(
		"worker running in standby mode",
		"reason", "delivery processor not configured",
	)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("worker shutdown signal received")
			logger.Info("worker stopped successfully")
			return nil

		case <-ticker.C:
			queryCtx, cancel := context.WithTimeout(
				ctx,
				3*time.Second,
			)

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
				if ctx.Err() != nil {
					continue
				}

				logger.Error(
					"failed to inspect notification queue",
					"error", err,
				)
				continue
			}

			logger.Info(
				"notification queue inspected",
				"ready_notifications", readyCount,
			)
		}
	}
}
