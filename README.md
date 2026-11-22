# Event-Driven Notification System

A Go service for queuing and sending SMS, email and push notifications through a simulated provider. It uses PostgreSQL for the queue, Redis for rate limiting, and separate API and worker processes

## Running with Docker

Requires Docker with the Compose plugin

```bash
cp -n .env.example .env
docker compose up --build -d
```

This starts PostgreSQL, Redis, database migrations, the API and the worker. The API is available at `http://localhost:8080`. A new `.env` keeps delivery disabled. If you already have `.env`, check `DELIVERY_ENABLED` before starting the worker

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
docker compose ps
```

The database volume is kept when containers stop. `docker compose down -v` deletes the database volume

### Enabling webhook delivery

1. Create a URL at [webhook.site](https://webhook.site/) and configure it to return HTTP `202` with JSON like `{"messageId":"demo-1","status":"accepted","timestamp":"2026-11-22T19:00:00Z"}`
2. Set `WEBHOOK_URL=https://webhook.site/<your-uuid>` and `DELIVERY_ENABLED=true` in `.env`
3. Run `docker compose up -d --build` to apply the configuration

Only enable delivery for test data. Starting the active worker also processes notifications that are already pending. Delivery is simulated: `accepted` means webhook.site acknowledged the request, not that a phone or inbox received a message

Logs and shutdown:

```bash
docker compose logs -f api worker
docker compose down
```

## API

Full specification: [api/openapi.yaml](api/openapi.yaml)

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `POST` | `/api/v1/notifications` | Create one notification |
| `POST` | `/api/v1/notifications/batch` | Create 1–1000 notifications |
| `GET` | `/api/v1/notifications/{id}` | Get notification status |
| `GET` | `/api/v1/batches/{id}` | Get batch status counts |
| `POST` | `/api/v1/notifications/{id}/cancel` | Cancel a pending or retrying notification |
| `GET` | `/api/v1/notifications` | List with filters and pagination |
| `GET` | `/api/v1/metrics` | Queue and delivery metrics |
| `GET` | `/healthz` | Liveness |
| `GET` | `/readyz` | Database readiness |

Create a notification:

```bash
curl -i http://localhost:8080/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-001' \
  -d '{"recipient":"+905551234567","channel":"sms","content":"Test message","priority":"high"}'
```

Create a batch:

```bash
curl -i http://localhost:8080/api/v1/notifications/batch \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: batch-001' \
  -d '{"notifications":[{"recipient":"person@example.com","channel":"email","content":"Hello"},{"recipient":"push-device-1","channel":"push","content":"New update"}]}'
```

Use the returned UUIDs to check or cancel a notification:

```bash
curl http://localhost:8080/api/v1/notifications/NOTIFICATION_UUID
curl -X POST http://localhost:8080/api/v1/notifications/NOTIFICATION_UUID/cancel
curl http://localhost:8080/api/v1/batches/BATCH_UUID
curl 'http://localhost:8080/api/v1/notifications?status=pending&channel=sms&page=1&limit=20'
curl http://localhost:8080/api/v1/metrics
```

List filters are `status`, `channel`, `created_from`, `created_to`, `page` and `limit`. Dates use RFC 3339; the end date is exclusive. Pagination defaults to page 1 and 20 results

The optional `Idempotency-Key` header prevents duplicate creation for the same request. A replay returns HTTP `200`; reusing the key with different content returns HTTP `409`

## Processing

- Priorities are `high`, `normal` and `low`, with `normal` as the default
- SMS content is limited to 160 characters, email to 10,000 and push to 256
- Workers claim notifications atomically from PostgreSQL and use leases to recover abandoned work
- Redis limits each channel to 100 admitted sends per rolling second across worker instances
- Failed attempts use exponential backoff with jitter and provider `Retry-After` where available
- Delivery attempts are tracked in PostgreSQL; the default maximum is four attempts
- The metrics endpoint reports current queue counts and delivery outcomes and latencies over the last five minutes

See [docs/architecture.md](docs/architecture.md) for the flow and delivery semantics

## Tests

Requires Go `1.27.4`, Docker and the Compose plugin

```bash
make test-all
```

The command starts PostgreSQL and Redis, applies migrations, runs the Go tests with the race detector, runs `go vet`, and executes the schema checks. Database and Redis tests use isolated test namespaces or schemas

For a shorter run without infrastructure tests:

```bash
make test
```

The repository also contains versioned migrations in `migrations/` and a GitHub Actions workflow that runs `make test-all` on pushes and pull requests

## Notes

- The API and metrics endpoint have no authentication and are bound to localhost in Docker Compose
- The public API does not support user-specified delivery times, templates or WebSocket status updates
- Distributed tracing is not included; log correlation uses request and notification IDs
- Provider HTTP acceptance is not end-user delivery confirmation
