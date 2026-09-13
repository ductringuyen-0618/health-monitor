# Health Monitor: design

Date: 2026-09-13
Status: approved in brainstorming, awaiting spec review

## Goal

A service that monitors user-registered HTTP endpoints, stores their state in Postgres, and POSTs a webhook when an endpoint returns a non-200 result on two consecutive checks. Deployed to DigitalOcean App Platform as an API service plus a worker, both built from one Go binary.

Source brief: `challenge.md`. Time budget: 3 hours.

## Scope

In scope: exactly the brief, plus the decisions below. Out of scope: recovery webhooks, alert history, authentication, per-target intervals, sharding.

Decisions taken during brainstorming:

- Timeouts, DNS failures, connection resets, and TLS errors count as failed checks.
- Only an HTTP 200 counts as a successful check.
- DOWN recovers to UP after one successful check. No webhook on recovery.
- The webhook fires once, on the transition into DOWN. It does not repeat while the target stays DOWN.
- Webhook URL is per target, with an optional global fallback `DEFAULT_WEBHOOK_URL`.
- A target with no webhook URL and no fallback is still accepted. A DOWN transition is logged and nothing is dispatched.
- Webhook delivery retries 3 times with backoff, then logs and gives up.
- Multiple worker instances are supported from day one via a Postgres claim query. No worker pinning.

## Components

One Go module, one binary, mode chosen by `MODE=api|worker`.

```
cmd/monitor/main.go             parse config, open DB, run migrations, start api or worker
internal/config                 env parsing with defaults and clamping
internal/store                  pgx access, embedded migrations, versioned runner
internal/api                    chi router, handlers, validation, JSON
internal/poller                 drain loop, checker, transition detection
internal/alert                  webhook dispatcher with retry
migrations/0001_init.sql
deployments/app-platform.yaml
Dockerfile
docker-compose.yml
Makefile
scripts/demo.sh
```

Both modes run migrations at startup. Migrations are idempotent and versioned, so start order between api and worker does not matter.

## Data model

```sql
CREATE TABLE targets (
  id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  url                  text NOT NULL UNIQUE,
  webhook_url          text,
  status               text NOT NULL DEFAULT 'PENDING'
                       CHECK (status IN ('PENDING','UP','DOWN')),
  consecutive_failures int  NOT NULL DEFAULT 0,
  last_checked_at      timestamptz,
  last_status_code     int,
  last_error           text,
  next_check_at        timestamptz NOT NULL DEFAULT now(),
  created_at           timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX targets_due ON targets (next_check_at);
```

`last_status_code` is null when the check produced no HTTP response. `webhook_url` null means use the global fallback.

## Store operations

- `Create(url, webhookURL)` inserts and returns the row. Duplicate `url` returns `ErrConflict`.
- `List()` returns all rows ordered by `created_at`.
- `Get(id)` returns one row or `ErrNotFound`.
- `Delete(id)` returns `ErrNotFound` when no row was deleted.
- `ClaimDue(limit, interval)` runs:

```sql
UPDATE targets
SET next_check_at = now() + $2
WHERE id IN (
  SELECT id FROM targets
  WHERE next_check_at <= now()
  ORDER BY next_check_at
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
RETURNING *;
```

  This is a single short transaction. The row lock is held only until commit. The pushed-forward `next_check_at` is the lease: if a worker dies after commit, the row becomes due again after `interval` and any live worker claims it. No reaper is needed. Connections set `idle_in_transaction_session_timeout` to 5s so a half-open client cannot hold a lock.

- `RecordCheck(id, ok, statusCode, errText)` is one UPDATE that:
  - on success sets `consecutive_failures = 0`, `status = 'UP'`;
  - on failure sets `consecutive_failures = consecutive_failures + 1` and `status = 'DOWN'` only when the new counter is at least 2, otherwise leaves `status` unchanged;
  - always sets `last_checked_at`, `last_status_code`, `last_error`;
  - returns the previous status and the new status, so the caller detects a transition without a second query. Zero rows matched means the target was deleted mid-check; the call is a no-op.

The transition rule lives in that statement so concurrent workers cannot act on a stale counter.

## API

chi router. JSON in and out. No authentication.

| Method | Path | Body | Responses |
|---|---|---|---|
| POST | /targets | `{"url": "...", "webhook_url": "..."}` | 201 target, 400 invalid, 409 duplicate |
| GET | /targets | | 200 `[target]` |
| GET | /targets/{id} | | 200 target, 404 |
| DELETE | /targets/{id} | | 204, 404 |
| GET | /healthz | | 200 `{"status":"ok","db":"up"}`, 503 when the DB ping fails |

Validation: `url` required, must parse with scheme `http` or `https` and a non-empty host. `webhook_url` optional, same rule when present. Any other body shape or unknown JSON is 400.

Target JSON:

```json
{
  "id": "uuid",
  "url": "https://example.com",
  "webhook_url": "https://hooks.example.com/x",
  "status": "UP",
  "consecutive_failures": 0,
  "last_checked_at": "2026-09-13T18:00:00Z",
  "last_status_code": 200,
  "last_error": null,
  "next_check_at": "2026-09-13T18:00:15Z",
  "created_at": "2026-09-13T17:59:00Z"
}
```

Errors: `{"error": "message"}` with the matching status. Middleware: request id, structured request log, recover. Graceful shutdown on SIGTERM with a 10s drain.

## Poller

Drain loop, not a fixed tick:

1. Compute `free` = free slots in the semaphore of size `MAX_CONCURRENT_CHECKS`.
2. If `free > 0`, call `ClaimDue(free, POLL_INTERVAL)`.
3. Start one goroutine per claimed row, each holding a semaphore slot.
4. If nothing was claimed, sleep until the earliest `next_check_at` or 1s, whichever is sooner, then repeat.
5. Log the count of due rows still unclaimed on each iteration so stretch under load is visible.

Claiming only what can start immediately guarantees a claimed row finishes within `CHECK_TIMEOUT`, well inside its lease, so no row is ever re-claimed while in flight.

Checker: one shared `http.Client` with `CHECK_TIMEOUT`, follows up to 5 redirects, reads and discards at most 64 KB of body. Success means final status exactly 200. Everything else is a failure with `last_error` set and `last_status_code` set when a response existed. Each goroutine defers a recover so a panic in one check is logged and the rest continue.

Transition: after `RecordCheck`, if previous status was not DOWN and new status is DOWN, call the dispatcher in the same goroutine. Recovery is silent.

Capacity: `effective_interval ≈ due_targets × avg_check_latency ÷ (workers × MAX_CONCURRENT_CHECKS)`. Under overload the interval stretches; ordering by `next_check_at` keeps it fair and no target is checked twice per lease. The first hard ceiling is Postgres write throughput on `RecordCheck`; sharding is the next step and is out of scope.

## Alert dispatcher

POST to the target's `webhook_url`, else `DEFAULT_WEBHOOK_URL`. If both are empty, log and return.

```json
{
  "event": "target.down",
  "target_id": "uuid",
  "url": "https://example.com",
  "status": "DOWN",
  "consecutive_failures": 2,
  "last_status_code": 503,
  "last_error": null,
  "occurred_at": "2026-09-13T18:00:15Z"
}
```

Any 2xx is delivered. Otherwise retry after 1s, 2s, 4s. After the third retry, log target id and last error and give up. Per-attempt timeout 5s. The state change is already committed before dispatch, so a failing webhook never affects status.

## Configuration

| Name | Default | Notes |
|---|---|---|
| `DATABASE_URL` | required | injected by App Platform |
| `MODE` | `api` | `api` or `worker` |
| `PORT` | `8080` | api only |
| `POLL_INTERVAL` | `15s` | clamped to 10s to 30s |
| `CHECK_TIMEOUT` | `5s` | per HTTP check |
| `MAX_CONCURRENT_CHECKS` | `20` | semaphore size and claim limit |
| `DEFAULT_WEBHOOK_URL` | empty | fallback when a target has none |

## Deployment

- Dockerfile: two-stage, `golang:1.27` builder to `gcr.io/distroless/static`, static binary, non-root, one image for both modes.
- `deployments/app-platform.yaml`: service `api` with HTTP health check on `/healthz`; worker `poller` with `MODE=worker`; database `db` (dev Postgres). Both components from the same GitHub repo and Dockerfile, `instance_count: 1`. Scaling the worker is an `instance_count` change only.
- `docker-compose.yml`: Postgres on host port 5433 to avoid the existing stack on 5432.
- `Makefile`: `run-api`, `run-worker`, `test`, `lint` (`go vet`, `gofmt -l`).
- `scripts/demo.sh`: takes a base URL, registers a healthy target, a target that returns 500, and a webhook receiver URL, then polls GET until the failing target reports DOWN. Used both locally and against the live deployment during review.

## Error handling

- Database unreachable: API returns 503 on `/healthz` and on any DB-backed route. Worker logs and retries on its next loop iteration with backoff from 1s to a 30s cap. Neither process exits.
- Bad target: every failure class is a recorded failed check and never escapes the goroutine.
- Webhook failure: retries then a log line. Status is already committed.
- Deleted mid-check: `RecordCheck` matches zero rows and is a no-op.
- Panic in a check: recovered, logged, other checks unaffected.

## Testing

Tests are written before the code they cover.

- `store`: integration tests against real Postgres via testcontainers. Create and duplicate, get, delete and not-found, claim exclusivity with two concurrent claimers over the same due rows, and the 0 to 1 to 2 failure sequence producing PENDING, PENDING, DOWN then UP after one success.
- `poller`: unit tests with `httptest.Server` for 200, 500, timeout, and a panicking check; a fake store asserting the dispatcher is called exactly once on the transition and not on later failing checks.
- `alert`: unit tests with `httptest.Server` for first-try delivery, 500 then success, and give up after the third retry.
- `api`: handler tests with a fake store for every response code in the API table, including URL validation cases.
- End to end: a script against docker-compose that registers a target backed by a local flapping server and asserts the webhook receiver gets exactly one `target.down` after the second failure.

## Evidence for review

- Live `/healthz` returning `db: up`.
- `scripts/demo.sh <live-url>` output showing the transition and the received webhook body.
- Worker log lines showing claim counts with the worker scaled to 2 instances and no duplicate checks of the same target within one interval.
