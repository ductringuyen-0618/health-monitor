# health-monitor

Monitors registered HTTP endpoints, stores UP/DOWN/PENDING state in Postgres, and POSTs a webhook when a target fails two consecutive checks. One Go binary: `MODE=api` serves the REST API, `MODE=worker` runs the poller. Design: `docs/superpowers/specs/2026-09-13-health-monitor-design.md`.

## API

| Method | Path | Body | Responses |
|---|---|---|---|
| POST | /targets | `{"url": "...", "webhook_url": "..."}` | 201, 400, 409 |
| GET | /targets | | 200 |
| GET | /targets/{id} | | 200, 404 |
| DELETE | /targets/{id} | | 204, 404 |
| GET | /healthz | | 200, 503 |

Webhook payload on DOWN:

```json
{"event":"target.down","target_id":"…","url":"…","status":"DOWN","consecutive_failures":2,"last_status_code":503,"last_error":null,"occurred_at":"…"}
```

## Run locally

    ./scripts/db-up.sh         # Postgres on localhost:5433 (Docker)
    ./scripts/run-api.sh       # :8080
    ./scripts/run-worker.sh
    go run ./cmd/demo-sink     # :9090 — /ok, /bad, /hook
    ./scripts/demo.sh          # registers targets, waits for DOWN, prints the webhook

## Test

    ./scripts/test.sh          # gofmt, go vet, go test (store tests need the compose DB)

## Configuration

Copy `.env.template` to `.env` and export the values you need (`set -a; source .env; set +a`) before running any of the scripts above.

| Name | Default | Notes |
|---|---|---|
| `DATABASE_URL` | required | |
| `MODE` | `api` | `api` or `worker` |
| `PORT` | `8080` | api only |
| `POLL_INTERVAL` | `15s` | clamped to 10s–30s |
| `CHECK_TIMEOUT` | `5s` | per check |
| `MAX_CONCURRENT_CHECKS` | `20` | per worker |
| `DEFAULT_WEBHOOK_URL` | empty | fallback when a target has none |

## How the worker scales

Workers claim due rows with `FOR UPDATE SKIP LOCKED` and push `next_check_at` forward as a lease, so any number of worker instances split the work without double-checking a target. Raise `instance_count` for `poller` in `deployments/app-platform.yaml`.

Capacity: `effective_interval ≈ due_targets × avg_check_latency ÷ (workers × MAX_CONCURRENT_CHECKS)`. Under overload the interval stretches evenly across targets; the first hard ceiling is Postgres write throughput on check results.

## Deploy

    doctl apps create --spec deployments/app-platform.yaml
    doctl apps list

## Known limitations

- **No authentication.** Anyone who can reach the API can register a target, so the worker can be pointed at private addresses (RFC1918, link-local, localhost) and the resulting status code is readable through `GET /targets`. Deploy it behind a trusted edge, or add an allowlist, before exposing it to untrusted callers. Out of scope for this build by design.
- **A dead webhook receiver slows checking.** Webhook delivery runs inside the check goroutine and holds its concurrency slot through all retries, so an unreachable receiver can occupy a slot for up to 27 seconds. With `MAX_CONCURRENT_CHECKS` slots all held this way, checking stalls until the retries finish. Raising `MAX_CONCURRENT_CHECKS` or adding more workers mitigates it; moving delivery to its own queue is the real fix.
