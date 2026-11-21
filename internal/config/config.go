// This file is responsible for loading environment variables to enable configuration
package config

import (
	"errors"
	"net"
	"net/url"
	"os"
)

type Config struct {
	HTTPAddress string
	DatabaseURL string
}

// Reading the PostgreSQL credentials from environment variables rather than hardcoding them
func Load() (Config, error) {
	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		return Config{}, errors.New("POSTGRES_PASSWORD is required")
	}

	dbURL := &url.URL{
		Scheme: "postgres",
		User: url.UserPassword(
			getenv("POSTGRES_USER", "notification_user"),
			password,
		),
		Host: net.JoinHostPort(
			getenv("POSTGRES_HOST", "127.0.0.1"),
			getenv("POSTGRES_PORT", "15432"),
		),
		Path: "/" + getenv("POSTGRES_DB", "notifications"),
	}

	query := dbURL.Query()
	query.Set("sslmode", "disable")
	dbURL.RawQuery = query.Encode()

	return Config{
		HTTPAddress: getenv("HTTP_ADDR", ":8080"),
		DatabaseURL: dbURL.String(),
	}, nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
