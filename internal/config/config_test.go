// We want to ensure that our Go application:
// rejects missing database passwords
// uses default values when optional environment variables aren't supplied
// correctly applies custom environment variables
package config

import (
	"net/url"
	"testing"
)

func TestLoadRequiresPassword(t *testing.T) {
	t.Setenv("POSTGRES_PASSWORD", "")

	_, err := Load()
	if err == nil {
		t.Fatal("expected an error when database password is missing")
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Setenv("POSTGRES_PASSWORD", "test-password")

	for _, key := range []string{
		"POSTGRES_USER",
		"POSTGRES_HOST",
		"POSTGRES_PORT",
		"POSTGRES_DB",
		"HTTP_ADDR",
	} {
		t.Setenv(key, "")
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("failed to load configuration: %v", err)
	}

	if cfg.HTTPAddress != ":8080" {
		t.Errorf("expected HTTP address :8080, got %s", cfg.HTTPAddress)
	}

	dbURL, err := url.Parse(cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("invalid database URL: %v", err)
	}

	if dbURL.Host != "127.0.0.1:15432" {
		t.Errorf("unexpected database host: %s", dbURL.Host)
	}

	if dbURL.User.Username() != "notification_user" {
		t.Errorf("unexpected database user: %s", dbURL.User.Username())
	}

	if dbURL.Path != "/notifications" {
		t.Errorf("unexpected database name: %s", dbURL.Path)
	}

	if dbURL.Query().Get("sslmode") != "disable" {
		t.Error("expected SSL mode to be disabled for local development")
	}
}

func TestLoadCustomConfiguration(t *testing.T) {
	t.Setenv("POSTGRES_USER", "custom_user")
	t.Setenv("POSTGRES_PASSWORD", "custom@password")
	t.Setenv("POSTGRES_HOST", "localhost")
	t.Setenv("POSTGRES_PORT", "5433")
	t.Setenv("POSTGRES_DB", "custom_database")
	t.Setenv("HTTP_ADDR", ":9090")

	// Setenv temporarily sets an environment variable for a test and restores its original value afterward.
	// This allows us to test different configurations without modifying your actual .env file
	cfg, err := Load()
	if err != nil {
		t.Fatalf("failed to load configuration: %v", err)
	}

	dbURL, err := url.Parse(cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("invalid database URL: %v", err)
	}

	if cfg.HTTPAddress != ":9090" {
		t.Errorf("expected HTTP address :9090, got %s", cfg.HTTPAddress)
	}

	if dbURL.Host != "localhost:5433" {
		t.Errorf("unexpected database host: %s", dbURL.Host)
	}

	if dbURL.Path != "/custom_database" {
		t.Errorf("unexpected database name: %s", dbURL.Path)
	}

	password, ok := dbURL.User.Password()
	if !ok || password != "custom@password" {
		t.Error("database password was not encoded correctly")
	}
}
