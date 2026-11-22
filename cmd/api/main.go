// This file will run our pipeline
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/config"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/httpapi"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

func main() {
	if err := run(); err != nil {
		log.Printf("Application error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	// listening for termination signals
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// loading application configuration
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	// connecting to PostgreSQL
	db, err := storage.OpenPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	log.Println("PostgreSQL connection established")

	// initializing our HTTP router
	mux := newHandler(db)

	// registering notification endpoints
	notificationRepo := storage.NewNotificationRepository(db)
	httpapi.RegisterNotificationRoutes(mux, notificationRepo)
	httpapi.RegisterBatchRoutes(mux, notificationRepo)
	httpapi.RegisterBatchStatusRoutes(mux, notificationRepo)
	httpapi.RegisterCancellationRoutes(mux, notificationRepo)

	server := &http.Server{
		Addr:              cfg.HTTPAddress,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// running the HTTP server in a separate goroutine
	serverErrors := make(chan error, 1)

	go func() {
		serverErrors <- server.ListenAndServe()
	}()

	log.Printf("API server listening on %s", cfg.HTTPAddress)

	// Wait for a termination signal or server failure
	select {
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("HTTP server failed: %w", err)
		}
		return nil

	case <-ctx.Done():
		log.Println("Shutdown signal received")
	}

	// stop listening for signals and begin graceful shutdown, where remaining transcations gracefully finish
	stop()

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	log.Println("Waiting for active requests to finish...")

	if err := server.Shutdown(shutdownCtx); err != nil {
		// force closing connections if the timeout expires
		_ = server.Close()
		return fmt.Errorf("graceful shutdown failed: %w", err)
	}

	// ensuring the HTTP server goroutine has exited
	if err := <-serverErrors; err != nil &&
		!errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("HTTP server failed: %w", err)
	}

	log.Println("HTTP server stopped successfully")
	return nil
}

type databasePinger interface {
	Ping(context.Context) error
}

// Instead of requiring an actual PostgreSQL connection in every HTTP test,
// we can provide a fake object that implements the same Ping() method

func newHandler(db databasePinger) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "OK")
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(
			r.Context(),
			2*time.Second,
		)
		defer cancel()

		if err := db.Ping(ctx); err != nil {
			http.Error(
				w,
				"Database unavailable",
				http.StatusServiceUnavailable,
			)
			return
		}

		fmt.Fprintln(w, "Ready")
	})

	return mux
}
