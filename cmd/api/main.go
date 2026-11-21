// This file will run our pipeline
package main
import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/config"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

func main() {
	ctx := context.Background()

	// loading application configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	// connecting to PostgreSQL.
	db, err := storage.OpenPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Println("PostgreSQL connection established")

	mux := http.NewServeMux()

	// confirming that the API server is running.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "OK")
	})

	// verifying that the database is reachable.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		pingCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := db.Ping(pingCtx); err != nil {
			http.Error(w, "Database unavailable", http.StatusServiceUnavailable)
			return
		}

		fmt.Fprintln(w, "Ready")
	})

	server := &http.Server{
		Addr:              cfg.HTTPAddress,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("API server listening on %s", cfg.HTTPAddress)

	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
