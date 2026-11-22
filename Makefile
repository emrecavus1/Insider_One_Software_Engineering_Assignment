# This is a file to automate our testing mechanism

# To load local development configuration
include .env

DB_URL = postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@127.0.0.1:$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable

MIGRATE = migrate -path migrations -database "$(DB_URL)"

.PHONY: test test-integration migrate-up migrate-down migrate-version

# To run Go unit tests
test:
	go test ./...

# To run database schema tests
test-integration:
	docker compose exec -T postgres \
		psql -v ON_ERROR_STOP=1 \
		-U notification_user \
		-d notifications \
		< tests/schema_smoke.sql

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

