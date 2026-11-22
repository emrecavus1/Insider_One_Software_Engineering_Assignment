# This is a file to automate our testing mechanism

# To load local development configuration
include .env

DB_URL = postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@127.0.0.1:$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable

MIGRATE = migrate -path migrations -database "$(DB_URL)"

.PHONY: test test-db test-integration run-worker

# To start the Go API with local environment variables
run-api:
	@set -a; . ./.env; set +a; \
	go run ./cmd/api

# To run Go unit tests
test:
	go test ./...

# To run PostgreSQL integration tests
# Automatically loading environment variables from .env
test-db:
	@set -a; . ./.env; set +a; \
	RUN_DB_TESTS=1 go test -v ./internal/storage


# To run database schema tests
test-integration: test-db
	docker compose exec -T postgres \
		psql -v ON_ERROR_STOP=1 \
		-U $(POSTGRES_USER) \
		-d $(POSTGRES_DB) \
		< tests/schema_smoke.sql

# To start the standalone worker process.
run-worker:
	@set -a; . ./.env; set +a; \
	go run ./cmd/worker

# applying pending database migrations
migrate-up:
	@$(MIGRATE) up

# To roll back the most recent migration
migrate-down:
	@test "$(CONFIRM_DOWN)" = "yes" || \
		(echo "Rollback requires CONFIRM_DOWN=yes"; exit 1)
	@$(MIGRATE) down 1

# displaying current database migration version
migrate-version:
	@$(MIGRATE) version

