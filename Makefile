# This is a file to automate our testing mechanism
.PHONY: test test-integration

# To run Go tests
test:
	go test ./...

# To run PostgreSQL schema integrity tests
test-integration:
	docker compose exec -T postgres \
		psql -v ON_ERROR_STOP=1 \
		-U notification_user \
		-d notifications \
		< tests/schema_smoke.sql
