// This file is to test our connection pool
// Failure case: The supplied database connection string is invalid
// Success case: Our application successfully connects to the running PostgreSQL database

package storage_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/config"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

func TestOpenPostgresInvalidURL(t *testing.T) {
	pool, err := storage.OpenPostgres(
		context.Background(),
		"postgres://%zz",
	)

	if pool != nil {
		pool.Close()
	}

	if err == nil {
		t.Fatal("expected an error for an invalid database URL")
	}
}

func TestOpenPostgresIntegration(t *testing.T) {
	if os.Getenv("RUN_DB_TESTS") != "1" {
		t.Skip("database integration tests are not enabled")
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load configuration: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := storage.OpenPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("failed to connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("PostgreSQL ping failed: %v", err)
	}
}
