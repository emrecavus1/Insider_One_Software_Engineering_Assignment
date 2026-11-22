include .env

DB_URL = postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@127.0.0.1:$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable
MIGRATE = migrate -path migrations -database "$(DB_URL)"

.PHONY: run-api run-worker test test-db test-redis test-ratelimit-integration test-observability test-provider test-integration test-all migrate-up migrate-down migrate-version

run-api:
	@set -a; . ./.env; set +a; \
	go run ./cmd/api

run-worker:
	@set -a; . ./.env; set +a; \
	go run ./cmd/worker

test:
	go test ./...

test-db:
	@set -a; . ./.env; set +a; \
	RUN_DB_TESTS=1 go test -v ./internal/storage

test-redis:
	@set -a; . ./.env; set +a; \
	RUN_REDIS_TESTS=1 go test -v ./internal/ratelimit

test-ratelimit-integration:
	@set -a; . ./.env; set +a; \
	RUN_DB_TESTS=1 RUN_REDIS_TESTS=1 \
	go test -v -count=1 \
	-run '^TestRateLimitedWorkerPipelineIntegration$$' \
	./internal/storage

test-observability:
	@set -a; . ./.env; set +a; \
	RUN_DB_TESTS=1 go test -v -count=1 ./internal/observability

test-provider:
	@set -a; . ./.env; set +a; \
	RUN_DB_TESTS=1 RUN_REDIS_TESTS=1 \
	go test -v -count=1 \
	-run '^TestProviderHTTP(Pipeline|RetryPipeline)Integration$$' \
	./internal/storage

test-integration: test-db test-redis test-ratelimit-integration test-observability test-provider
	docker compose exec -T postgres \
		psql -v ON_ERROR_STOP=1 \
		-U $(POSTGRES_USER) \
		-d $(POSTGRES_DB) \
		< tests/schema_smoke.sql

# Run every Go and infrastructure test with one command
test-all:
	docker compose up -d --wait postgres redis
	docker compose run --rm migrate
	@set -a; . ./.env; set +a; \
	RUN_DB_TESTS=1 RUN_REDIS_TESTS=1 go test -race -count=1 ./...
	go vet ./...
	docker compose exec -T postgres \
		psql -v ON_ERROR_STOP=1 \
		-U $(POSTGRES_USER) \
		-d $(POSTGRES_DB) \
		< tests/schema_smoke.sql

migrate-up:
	@$(MIGRATE) up

migrate-down:
	@test "$(CONFIRM_DOWN)" = "yes" || \
		(echo "Rollback requires CONFIRM_DOWN=yes"; exit 1)
	@$(MIGRATE) down 1

migrate-version:
	@$(MIGRATE) version
