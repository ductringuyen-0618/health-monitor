# Health Monitor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Go service that monitors registered HTTP endpoints, persists UP/DOWN/PENDING state in Postgres, and POSTs a webhook when a target fails two consecutive checks, deployed live on DigitalOcean App Platform as an API service plus a worker.

**Architecture:** One binary, `MODE=api` serves a chi REST API, `MODE=worker` runs a drain loop that claims due targets from Postgres with `FOR UPDATE SKIP LOCKED` and checks them concurrently under a semaphore. The two-strikes transition is computed inside a single UPDATE so multiple workers cannot double-fire. The webhook dispatcher retries with backoff after the state change is already committed.

**Tech Stack:** Go 1.27, `github.com/go-chi/chi/v5`, `github.com/jackc/pgx/v5` (pgxpool), `github.com/google/uuid`, `log/slog`, Postgres 16, Docker, DigitalOcean App Platform via `doctl`, GitHub via `gh`.

**Spec:** `docs/superpowers/specs/2026-09-13-health-monitor-design.md`

## Global Constraints

- Go module path `github.com/ductringuyen-0618/health-monitor`, `go 1.27` in go.mod.
- Status values are exactly `PENDING`, `UP`, `DOWN`.
- A check succeeds only on HTTP status exactly 200. Timeouts, DNS, connection, and TLS errors are failures.
- `status` becomes `DOWN` only when `consecutive_failures` reaches 2. One success resets to `UP`. No webhook on recovery.
- The webhook fires once, on the transition into DOWN. Retries: 3, backoff 1s, 2s, 4s, per-attempt timeout 5s.
- `POLL_INTERVAL` default `15s`, clamped to `[10s, 30s]`. `CHECK_TIMEOUT` default `5s`. `MAX_CONCURRENT_CHECKS` default `20`. `PORT` default `8080`. `MODE` default `api`. `DATABASE_URL` required.
- Every JSON error body is `{"error": "message"}`.
- Lint gate before every commit: `gofmt -l .` prints nothing and `go vet ./...` passes.
- Store integration tests run against a real Postgres reached through `TEST_DATABASE_URL` and skip when it is unset. Local value: `postgres://postgres:postgres@localhost:5433/monitor?sslmode=disable`.
- This machine has no `make` and no `jq`. Use the scripts in `scripts/` and Python for JSON in shell.
- Docker Desktop must be running before Task 2's tests. Start it from the Start menu and wait until `docker ps` succeeds.
- Commit messages end with the two attribution lines given in the session reminder.

---

## File Structure

| Path | Responsibility |
|---|---|
| `go.mod`, `go.sum` | module and dependencies |
| `.gitignore`, `.gitattributes` | ignore binaries, force LF |
| `cmd/monitor/main.go` | config, DB, migrations, pick api or worker, signal handling |
| `cmd/demo-sink/main.go` | tiny local server: `/ok` 200, `/bad` 500, `/hook` records webhooks |
| `internal/config/config.go` | env parsing, defaults, clamping |
| `internal/config/config_test.go` | config tests |
| `internal/store/store.go` | `Target`, `CheckResult`, `Transition`, errors, `Store`, `New`, `Ping`, `Close` |
| `internal/store/migrate.go` | versioned runner over embedded SQL |
| `internal/store/targets.go` | `Create`, `List`, `Get`, `Delete` |
| `internal/store/claim.go` | `ClaimDue`, `CountDue`, `RecordCheck` |
| `internal/store/store_test.go` | test helper: connect, migrate, truncate |
| `internal/store/targets_test.go`, `internal/store/claim_test.go` | integration tests |
| `migrations/embed.go`, `migrations/0001_init.sql` | schema |
| `internal/api/router.go` | `TargetStore` interface, `NewRouter`, middleware |
| `internal/api/handlers.go` | handlers, validation, JSON helpers |
| `internal/api/api_test.go` | handler tests with a fake store |
| `internal/poller/checker.go` | `HTTPChecker` |
| `internal/poller/checker_test.go` | checker tests |
| `internal/poller/poller.go` | `Store`, `Checker`, `Dispatcher` interfaces, drain loop, `handle` |
| `internal/poller/poller_test.go` | loop tests with fakes |
| `internal/alert/alert.go` | `Alert`, payload, `Dispatcher` |
| `internal/alert/alert_test.go` | dispatcher tests |
| `Dockerfile` | two-stage build |
| `docker-compose.yml` | Postgres on 5433 |
| `deployments/app-platform.yaml` | App Platform spec |
| `scripts/db-up.sh`, `scripts/test.sh`, `scripts/run-api.sh`, `scripts/run-worker.sh`, `scripts/demo.sh` | local workflow |
| `README.md` | run, test, deploy, capacity formula |
| `docs/evidence/` | deploy and demo output |

---

### Task 1: Module scaffold and config

**Files:**
- Create: `go.mod`, `.gitignore`, `.gitattributes`, `internal/config/config.go`, `internal/config/config_test.go`, `docker-compose.yml`, `scripts/db-up.sh`, `scripts/test.sh`

**Interfaces:**
- Produces: `config.Load(getenv func(string) string) (config.Config, error)` with fields `DatabaseURL string, Mode string, Port string, PollInterval time.Duration, CheckTimeout time.Duration, MaxConcurrentChecks int, DefaultWebhookURL string`.

- [ ] **Step 1: Initialise the module and repo hygiene files**

```bash
cd /d/Portfolio/health-monitor
go mod init github.com/ductringuyen-0618/health-monitor
printf '/monitor\n/monitor.exe\n/bin/\n/tmp/\n.env\n' > .gitignore
printf '* text=auto eol=lf\n*.sh text eol=lf\n' > .gitattributes
git branch -M main
```

- [ ] **Step 2: Write the failing config test**

`internal/config/config_test.go`:

```go
package config

import (
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{"DATABASE_URL": "postgres://x"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Mode != "api" || cfg.Port != "8080" {
		t.Errorf("mode/port defaults wrong: %+v", cfg)
	}
	if cfg.PollInterval != 15*time.Second || cfg.CheckTimeout != 5*time.Second {
		t.Errorf("duration defaults wrong: %+v", cfg)
	}
	if cfg.MaxConcurrentChecks != 20 || cfg.DefaultWebhookURL != "" {
		t.Errorf("other defaults wrong: %+v", cfg)
	}
}

func TestLoadRequiresDatabaseURL(t *testing.T) {
	if _, err := Load(env(map[string]string{})); err == nil {
		t.Fatal("expected error for missing DATABASE_URL")
	}
}

func TestLoadRejectsBadMode(t *testing.T) {
	if _, err := Load(env(map[string]string{"DATABASE_URL": "x", "MODE": "cron"})); err == nil {
		t.Fatal("expected error for bad MODE")
	}
}

func TestLoadClampsPollInterval(t *testing.T) {
	cases := map[string]time.Duration{"2s": 10 * time.Second, "20s": 20 * time.Second, "5m": 30 * time.Second}
	for in, want := range cases {
		cfg, err := Load(env(map[string]string{"DATABASE_URL": "x", "POLL_INTERVAL": in}))
		if err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if cfg.PollInterval != want {
			t.Errorf("%s: got %v want %v", in, cfg.PollInterval, want)
		}
	}
}

func TestLoadParsesOverrides(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"DATABASE_URL": "x", "MODE": "worker", "PORT": "9000",
		"CHECK_TIMEOUT": "2s", "MAX_CONCURRENT_CHECKS": "50", "DEFAULT_WEBHOOK_URL": "https://h",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != "worker" || cfg.Port != "9000" || cfg.CheckTimeout != 2*time.Second ||
		cfg.MaxConcurrentChecks != 50 || cfg.DefaultWebhookURL != "https://h" {
		t.Errorf("overrides not applied: %+v", cfg)
	}
}

func TestLoadRejectsBadNumbers(t *testing.T) {
	for _, m := range []map[string]string{
		{"DATABASE_URL": "x", "MAX_CONCURRENT_CHECKS": "0"},
		{"DATABASE_URL": "x", "MAX_CONCURRENT_CHECKS": "abc"},
		{"DATABASE_URL": "x", "CHECK_TIMEOUT": "soon"},
	} {
		if _, err := Load(env(m)); err == nil {
			t.Errorf("expected error for %v", m)
		}
	}
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `go test ./internal/config/ -v`
Expected: build failure, `undefined: Load`.

- [ ] **Step 4: Implement config**

`internal/config/config.go`:

```go
// Package config reads process configuration from the environment.
package config

import (
	"fmt"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL         string
	Mode                string
	Port                string
	PollInterval        time.Duration
	CheckTimeout        time.Duration
	MaxConcurrentChecks int
	DefaultWebhookURL   string
}

const (
	minPollInterval = 10 * time.Second
	maxPollInterval = 30 * time.Second
)

// Load builds a Config from getenv (normally os.Getenv). Missing values take
// defaults; POLL_INTERVAL is clamped to [10s, 30s].
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		DatabaseURL:         getenv("DATABASE_URL"),
		Mode:                orDefault(getenv("MODE"), "api"),
		Port:                orDefault(getenv("PORT"), "8080"),
		PollInterval:        15 * time.Second,
		CheckTimeout:        5 * time.Second,
		MaxConcurrentChecks: 20,
		DefaultWebhookURL:   getenv("DEFAULT_WEBHOOK_URL"),
	}
	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.Mode != "api" && cfg.Mode != "worker" {
		return cfg, fmt.Errorf("MODE must be api or worker, got %q", cfg.Mode)
	}
	var err error
	if cfg.PollInterval, err = duration(getenv("POLL_INTERVAL"), cfg.PollInterval); err != nil {
		return cfg, fmt.Errorf("POLL_INTERVAL: %w", err)
	}
	cfg.PollInterval = min(max(cfg.PollInterval, minPollInterval), maxPollInterval)
	if cfg.CheckTimeout, err = duration(getenv("CHECK_TIMEOUT"), cfg.CheckTimeout); err != nil {
		return cfg, fmt.Errorf("CHECK_TIMEOUT: %w", err)
	}
	if v := getenv("MAX_CONCURRENT_CHECKS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return cfg, fmt.Errorf("MAX_CONCURRENT_CHECKS must be a positive integer, got %q", v)
		}
		cfg.MaxConcurrentChecks = n
	}
	return cfg, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func duration(v string, def time.Duration) (time.Duration, error) {
	if v == "" {
		return def, nil
	}
	return time.ParseDuration(v)
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `go test ./internal/config/ -v`
Expected: all six tests PASS.

- [ ] **Step 6: Add compose and scripts**

`docker-compose.yml`:

```yaml
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: monitor
    ports:
      - "5433:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 2s
      timeout: 2s
      retries: 15
```

`scripts/db-up.sh`:

```bash
#!/usr/bin/env bash
# Start the local Postgres and wait until it accepts connections.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose up -d db
for _ in $(seq 1 30); do
  if docker compose exec -T db pg_isready -U postgres >/dev/null 2>&1; then
    echo "postgres ready on localhost:5433"
    exit 0
  fi
  sleep 1
done
echo "postgres did not become ready" >&2
exit 1
```

`scripts/test.sh`:

```bash
#!/usr/bin/env bash
# Lint and run every test. Store tests need TEST_DATABASE_URL; run scripts/db-up.sh first.
set -euo pipefail
cd "$(dirname "$0")/.."
export TEST_DATABASE_URL="${TEST_DATABASE_URL:-postgres://postgres:postgres@localhost:5433/monitor?sslmode=disable}"
unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then
  echo "gofmt needed on: $unformatted" >&2
  exit 1
fi
go vet ./...
go test ./... "$@"
```

Then: `chmod +x scripts/*.sh`

- [ ] **Step 7: Lint and commit**

```bash
gofmt -l . && go vet ./... && go test ./...
git add -A
git commit -m "feat: scaffold module, config loading, and local scripts"
```

---

### Task 2: Store types, migrations, and target CRUD

**Files:**
- Create: `migrations/embed.go`, `migrations/0001_init.sql`, `internal/store/store.go`, `internal/store/migrate.go`, `internal/store/targets.go`, `internal/store/store_test.go`, `internal/store/targets_test.go`

**Interfaces:**
- Produces:
  - `store.Target{ID string; URL string; WebhookURL *string; Status string; ConsecutiveFailures int; LastCheckedAt *time.Time; LastStatusCode *int; LastError *string; NextCheckAt time.Time; CreatedAt time.Time}` with JSON tags matching the spec.
  - `store.CheckResult{OK bool; StatusCode *int; Err *string}`, `store.Transition{From, To string; ConsecutiveFailures int}`.
  - `store.ErrNotFound`, `store.ErrConflict`.
  - `store.New(ctx, databaseURL string) (*Store, error)`, `(*Store).Migrate(ctx) error`, `(*Store).Ping(ctx) error`, `(*Store).Close()`.
  - `(*Store).Create(ctx, url string, webhookURL *string) (Target, error)`, `List(ctx) ([]Target, error)`, `Get(ctx, id string) (Target, error)`, `Delete(ctx, id string) error`.

- [ ] **Step 1: Start Docker Desktop and Postgres**

Start Docker Desktop from the Start menu. Then:

```bash
docker ps
./scripts/db-up.sh
```

Expected: `postgres ready on localhost:5433`.

- [ ] **Step 2: Add dependencies and the migration**

```bash
go get github.com/jackc/pgx/v5@latest github.com/google/uuid@latest
```

`migrations/0001_init.sql`:

```sql
CREATE TABLE IF NOT EXISTS targets (
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
CREATE INDEX IF NOT EXISTS targets_due ON targets (next_check_at);
```

`migrations/embed.go`:

```go
// Package migrations embeds the SQL schema files applied by store.Migrate.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
```

- [ ] **Step 3: Write the test helper and failing CRUD tests**

`internal/store/store_test.go`:

```go
package store

import (
	"context"
	"os"
	"testing"
)

// testStore connects to TEST_DATABASE_URL, migrates, and truncates targets.
// Tests skip when the variable is unset so unit-only runs stay green.
func testStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := s.pool.Exec(ctx, "TRUNCATE targets"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return s, ctx
}

func ptr[T any](v T) *T { return &v }
```

`internal/store/targets_test.go`:

```go
package store

import (
	"errors"
	"testing"
)

func TestMigrateIsIdempotent(t *testing.T) {
	s, ctx := testStore(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

func TestCreateAndGet(t *testing.T) {
	s, ctx := testStore(t)
	created, err := s.Create(ctx, "https://example.com", ptr("https://hooks.example.com"))
	if err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.Status != "PENDING" || created.ConsecutiveFailures != 0 {
		t.Errorf("unexpected row: %+v", created)
	}
	if created.WebhookURL == nil || *created.WebhookURL != "https://hooks.example.com" {
		t.Errorf("webhook not stored: %+v", created)
	}
	got, err := s.Get(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.URL != "https://example.com" {
		t.Errorf("get returned %+v", got)
	}
}

func TestCreateDuplicateIsConflict(t *testing.T) {
	s, ctx := testStore(t)
	if _, err := s.Create(ctx, "https://dup.example.com", nil); err != nil {
		t.Fatal(err)
	}
	_, err := s.Create(ctx, "https://dup.example.com", nil)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
}

func TestListOrdersByCreation(t *testing.T) {
	s, ctx := testStore(t)
	for _, u := range []string{"https://a.example.com", "https://b.example.com"} {
		if _, err := s.Create(ctx, u, nil); err != nil {
			t.Fatal(err)
		}
	}
	list, err := s.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].URL != "https://a.example.com" {
		t.Errorf("list = %+v", list)
	}
}

func TestGetAndDeleteNotFound(t *testing.T) {
	s, ctx := testStore(t)
	if _, err := s.Get(ctx, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, ErrNotFound) {
		t.Errorf("get unknown uuid: %v", err)
	}
	if _, err := s.Get(ctx, "not-a-uuid"); !errors.Is(err, ErrNotFound) {
		t.Errorf("get bad uuid: %v", err)
	}
	if err := s.Delete(ctx, "not-a-uuid"); !errors.Is(err, ErrNotFound) {
		t.Errorf("delete bad uuid: %v", err)
	}
}

func TestDelete(t *testing.T) {
	s, ctx := testStore(t)
	created, err := s.Create(ctx, "https://del.example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, created.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("second delete: %v", err)
	}
}
```

- [ ] **Step 4: Run to verify failure**

Run: `TEST_DATABASE_URL='postgres://postgres:postgres@localhost:5433/monitor?sslmode=disable' go test ./internal/store/ -v`
Expected: build failure, `undefined: New`.

- [ ] **Step 5: Implement store.go**

`internal/store/store.go`:

```go
// Package store is the Postgres persistence layer for monitored targets.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound = errors.New("target not found")
	ErrConflict = errors.New("target url already registered")
)

type Target struct {
	ID                  string     `json:"id"`
	URL                 string     `json:"url"`
	WebhookURL          *string    `json:"webhook_url"`
	Status              string     `json:"status"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	LastCheckedAt       *time.Time `json:"last_checked_at"`
	LastStatusCode      *int       `json:"last_status_code"`
	LastError           *string    `json:"last_error"`
	NextCheckAt         time.Time  `json:"next_check_at"`
	CreatedAt           time.Time  `json:"created_at"`
}

// CheckResult is the outcome of one HTTP check. StatusCode is nil when no
// response was received. Err is nil on success.
type CheckResult struct {
	OK         bool
	StatusCode *int
	Err        *string
}

// Transition reports the status before and after RecordCheck.
type Transition struct {
	From                string
	To                  string
	ConsecutiveFailures int
}

type Store struct {
	pool *pgxpool.Pool
}

// New opens a connection pool. It does not ping; the first query does.
// idle_in_transaction_session_timeout keeps a half-open client from holding
// a claim lock.
func New(ctx context.Context, databaseURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) Close() { s.pool.Close() }

const targetColumns = `id::text, url, webhook_url, status, consecutive_failures,
	last_checked_at, last_status_code, last_error, next_check_at, created_at`

type scanner interface {
	Scan(dest ...any) error
}

func scanTarget(row scanner) (Target, error) {
	var t Target
	err := row.Scan(&t.ID, &t.URL, &t.WebhookURL, &t.Status, &t.ConsecutiveFailures,
		&t.LastCheckedAt, &t.LastStatusCode, &t.LastError, &t.NextCheckAt, &t.CreatedAt)
	return t, err
}
```

- [ ] **Step 6: Implement migrate.go**

`internal/store/migrate.go`:

```go
package store

import (
	"context"
	"fmt"
	"sort"

	"github.com/ductringuyen-0618/health-monitor/migrations"
)

// Migrate applies every embedded *.sql file in name order that is not yet
// recorded in schema_migrations. Safe to run from several processes: the
// insert into schema_migrations serialises on the primary key.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		if err := s.applyOne(ctx, name); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	return nil
}

func (s *Store) applyOne(ctx context.Context, name string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = $1 FOR UPDATE)`, name).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	sql, err := migrations.FS.ReadFile(name)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, string(sql)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1)`, name); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
```

- [ ] **Step 7: Implement targets.go**

`internal/store/targets.go`:

```go
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const uniqueViolation = "23505"

func (s *Store) Create(ctx context.Context, url string, webhookURL *string) (Target, error) {
	row := s.pool.QueryRow(ctx,
		`INSERT INTO targets (url, webhook_url) VALUES ($1, $2) RETURNING `+targetColumns, url, webhookURL)
	t, err := scanTarget(row)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return Target{}, ErrConflict
	}
	if err != nil {
		return Target{}, fmt.Errorf("insert target: %w", err)
	}
	return t, nil
}

func (s *Store) List(ctx context.Context) ([]Target, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+targetColumns+` FROM targets ORDER BY created_at, id`)
	if err != nil {
		return nil, fmt.Errorf("list targets: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Target, error) { return scanTarget(row) })
}

func (s *Store) Get(ctx context.Context, id string) (Target, error) {
	if _, err := uuid.Parse(id); err != nil {
		return Target{}, ErrNotFound
	}
	t, err := scanTarget(s.pool.QueryRow(ctx, `SELECT `+targetColumns+` FROM targets WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Target{}, ErrNotFound
	}
	if err != nil {
		return Target{}, fmt.Errorf("get target: %w", err)
	}
	return t, nil
}

func (s *Store) Delete(ctx context.Context, id string) error {
	if _, err := uuid.Parse(id); err != nil {
		return ErrNotFound
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM targets WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete target: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `go mod tidy && ./scripts/test.sh -v -run 'TestMigrate|TestCreate|TestList|TestGet|TestDelete'`
Expected: all store tests PASS. Also run `go test ./internal/store/` with the variable unset to confirm they SKIP.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat: postgres store with migrations and target CRUD"
```

---

### Task 3: Claim, backlog count, and check recording

**Files:**
- Create: `internal/store/claim.go`, `internal/store/claim_test.go`

**Interfaces:**
- Consumes: `Store`, `Target`, `CheckResult`, `Transition`, `ErrNotFound` from Task 2.
- Produces: `(*Store).ClaimDue(ctx, limit int, interval time.Duration) ([]Target, error)`, `(*Store).CountDue(ctx) (int, error)`, `(*Store).RecordCheck(ctx, id string, r CheckResult) (Transition, error)` returning `ErrNotFound` when the row is gone.

- [ ] **Step 1: Write the failing tests**

`internal/store/claim_test.go`:

```go
package store

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func mustCreate(t *testing.T, s *Store, url string) Target {
	t.Helper()
	ctx := t.Context()
	tgt, err := s.Create(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	return tgt
}

func TestClaimDuePushesNextCheckAndRespectsLimit(t *testing.T) {
	s, ctx := testStore(t)
	for _, u := range []string{"https://1.example.com", "https://2.example.com", "https://3.example.com"} {
		mustCreate(t, s, u)
	}
	first, err := s.ClaimDue(ctx, 2, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("claimed %d, want 2", len(first))
	}
	if !first[0].NextCheckAt.After(time.Now().Add(10 * time.Second)) {
		t.Errorf("next_check_at not pushed forward: %v", first[0].NextCheckAt)
	}
	second, err := s.ClaimDue(ctx, 10, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 {
		t.Fatalf("second claim got %d, want 1", len(second))
	}
	third, err := s.ClaimDue(ctx, 10, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 0 {
		t.Fatalf("third claim got %d, want 0", len(third))
	}
}

func TestClaimDueIsExclusiveAcrossConcurrentClaimers(t *testing.T) {
	s, ctx := testStore(t)
	const n = 20
	for i := 0; i < n; i++ {
		mustCreate(t, s, "https://c"+string(rune('a'+i))+".example.com")
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				got, err := s.ClaimDue(ctx, 3, 15*time.Second)
				if err != nil {
					t.Error(err)
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, tg := range got {
					seen[tg.ID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("claimed %d distinct targets, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Errorf("target %s claimed %d times", id, c)
		}
	}
}

func TestCountDue(t *testing.T) {
	s, ctx := testStore(t)
	mustCreate(t, s, "https://due1.example.com")
	mustCreate(t, s, "https://due2.example.com")
	n, err := s.CountDue(ctx)
	if err != nil || n != 2 {
		t.Fatalf("count = %d, err = %v", n, err)
	}
	if _, err := s.ClaimDue(ctx, 1, 15*time.Second); err != nil {
		t.Fatal(err)
	}
	n, err = s.CountDue(ctx)
	if err != nil || n != 1 {
		t.Fatalf("after claim count = %d, err = %v", n, err)
	}
}

func TestRecordCheckTwoStrikesThenRecover(t *testing.T) {
	s, ctx := testStore(t)
	tgt := mustCreate(t, s, "https://flap.example.com")
	fail := CheckResult{StatusCode: ptr(503), Err: ptr("unexpected status 503")}
	ok := CheckResult{OK: true, StatusCode: ptr(200)}

	tr, err := s.RecordCheck(ctx, tgt.ID, fail)
	if err != nil {
		t.Fatal(err)
	}
	if tr.From != "PENDING" || tr.To != "PENDING" || tr.ConsecutiveFailures != 1 {
		t.Errorf("first failure: %+v", tr)
	}
	tr, err = s.RecordCheck(ctx, tgt.ID, fail)
	if err != nil {
		t.Fatal(err)
	}
	if tr.From != "PENDING" || tr.To != "DOWN" || tr.ConsecutiveFailures != 2 {
		t.Errorf("second failure: %+v", tr)
	}
	tr, err = s.RecordCheck(ctx, tgt.ID, fail)
	if err != nil {
		t.Fatal(err)
	}
	if tr.From != "DOWN" || tr.To != "DOWN" || tr.ConsecutiveFailures != 3 {
		t.Errorf("third failure: %+v", tr)
	}
	tr, err = s.RecordCheck(ctx, tgt.ID, ok)
	if err != nil {
		t.Fatal(err)
	}
	if tr.From != "DOWN" || tr.To != "UP" || tr.ConsecutiveFailures != 0 {
		t.Errorf("recovery: %+v", tr)
	}
	got, err := s.Get(ctx, tgt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "UP" || got.LastStatusCode == nil || *got.LastStatusCode != 200 ||
		got.LastError != nil || got.LastCheckedAt == nil {
		t.Errorf("row after recovery: %+v", got)
	}
}

func TestRecordCheckUpWithOneFailureStaysUp(t *testing.T) {
	s, ctx := testStore(t)
	tgt := mustCreate(t, s, "https://blip.example.com")
	if _, err := s.RecordCheck(ctx, tgt.ID, CheckResult{OK: true, StatusCode: ptr(200)}); err != nil {
		t.Fatal(err)
	}
	tr, err := s.RecordCheck(ctx, tgt.ID, CheckResult{Err: ptr("timeout")})
	if err != nil {
		t.Fatal(err)
	}
	if tr.From != "UP" || tr.To != "UP" || tr.ConsecutiveFailures != 1 {
		t.Errorf("blip: %+v", tr)
	}
	got, _ := s.Get(ctx, tgt.ID)
	if got.LastStatusCode != nil || got.LastError == nil || *got.LastError != "timeout" {
		t.Errorf("row after timeout: %+v", got)
	}
}

func TestRecordCheckDeletedTarget(t *testing.T) {
	s, ctx := testStore(t)
	tgt := mustCreate(t, s, "https://gone.example.com")
	if err := s.Delete(ctx, tgt.ID); err != nil {
		t.Fatal(err)
	}
	_, err := s.RecordCheck(ctx, tgt.ID, CheckResult{OK: true, StatusCode: ptr(200)})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `./scripts/test.sh -run 'TestClaim|TestCount|TestRecord'`
Expected: build failure, `undefined: ClaimDue` (and the others).

- [ ] **Step 3: Implement claim.go**

`internal/store/claim.go`:

```go
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ClaimDue atomically takes up to limit due targets and pushes their
// next_check_at forward by interval. The push is the lease: a claimed row is
// invisible to other workers until the interval elapses. SKIP LOCKED lets
// concurrent workers claim disjoint rows without waiting on each other.
func (s *Store) ClaimDue(ctx context.Context, limit int, interval time.Duration) ([]Target, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE targets
		SET next_check_at = now() + make_interval(secs => $2)
		WHERE id IN (
			SELECT id FROM targets
			WHERE next_check_at <= now()
			ORDER BY next_check_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING `+targetColumns, limit, interval.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim due targets: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Target, error) { return scanTarget(row) })
}

// CountDue reports how many targets are due right now, capped at 10000 so the
// count stays cheap on large tables.
func (s *Store) CountDue(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM (SELECT 1 FROM targets WHERE next_check_at <= now() LIMIT 10000) d`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count due targets: %w", err)
	}
	return n, nil
}

// RecordCheck applies one check result in a single statement. On success the
// counter resets and status becomes UP. On failure the counter increments and
// status becomes DOWN once the counter reaches 2. The previous status is read
// in the same statement so callers can detect the DOWN transition.
func (s *Store) RecordCheck(ctx context.Context, id string, r CheckResult) (Transition, error) {
	if _, err := uuid.Parse(id); err != nil {
		return Transition{}, ErrNotFound
	}
	var tr Transition
	err := s.pool.QueryRow(ctx, `
		WITH prev AS (SELECT status AS prev_status FROM targets WHERE id = $1)
		UPDATE targets t SET
			consecutive_failures = CASE WHEN $2 THEN 0 ELSE t.consecutive_failures + 1 END,
			status = CASE
				WHEN $2 THEN 'UP'
				WHEN t.consecutive_failures + 1 >= 2 THEN 'DOWN'
				ELSE t.status END,
			last_checked_at  = now(),
			last_status_code = $3,
			last_error       = $4
		FROM prev
		WHERE t.id = $1
		RETURNING prev.prev_status, t.status, t.consecutive_failures`,
		id, r.OK, r.StatusCode, r.Err).Scan(&tr.From, &tr.To, &tr.ConsecutiveFailures)
	if errors.Is(err, pgx.ErrNoRows) {
		return Transition{}, ErrNotFound
	}
	if err != nil {
		return Transition{}, fmt.Errorf("record check: %w", err)
	}
	return tr, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/test.sh -v -run 'TestClaim|TestCount|TestRecord'`
Expected: all PASS. If Postgres complains about `$3` type, change `last_status_code = $3` to `last_status_code = $3::int` and `last_error = $4::text`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: claim due targets with skip locked and record check transitions"
```

---

### Task 4: REST API

**Files:**
- Create: `internal/api/router.go`, `internal/api/handlers.go`, `internal/api/api_test.go`

**Interfaces:**
- Consumes: `store.Target`, `store.ErrNotFound`, `store.ErrConflict`.
- Produces: `api.TargetStore` interface (`Create(ctx, url string, webhookURL *string) (store.Target, error)`, `List(ctx) ([]store.Target, error)`, `Get(ctx, id string) (store.Target, error)`, `Delete(ctx, id string) error`, `Ping(ctx) error`) and `api.NewRouter(s TargetStore, log *slog.Logger) http.Handler`.

- [ ] **Step 1: Add chi**

```bash
go get github.com/go-chi/chi/v5@latest
```

- [ ] **Step 2: Write the failing handler tests**

`internal/api/api_test.go`:

```go
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ductringuyen-0618/health-monitor/internal/store"
)

type fakeStore struct {
	targets map[string]store.Target
	pingErr error
	nextID  int
}

func newFake() *fakeStore { return &fakeStore{targets: map[string]store.Target{}} }

func (f *fakeStore) Create(_ context.Context, url string, webhook *string) (store.Target, error) {
	for _, t := range f.targets {
		if t.URL == url {
			return store.Target{}, store.ErrConflict
		}
	}
	f.nextID++
	t := store.Target{ID: "id-" + strings.Repeat("0", f.nextID), URL: url, WebhookURL: webhook,
		Status: "PENDING", NextCheckAt: time.Now(), CreatedAt: time.Now()}
	f.targets[t.ID] = t
	return t, nil
}

func (f *fakeStore) List(context.Context) ([]store.Target, error) {
	out := []store.Target{}
	for _, t := range f.targets {
		out = append(out, t)
	}
	return out, nil
}

func (f *fakeStore) Get(_ context.Context, id string) (store.Target, error) {
	t, ok := f.targets[id]
	if !ok {
		return store.Target{}, store.ErrNotFound
	}
	return t, nil
}

func (f *fakeStore) Delete(_ context.Context, id string) error {
	if _, ok := f.targets[id]; !ok {
		return store.ErrNotFound
	}
	delete(f.targets, id)
	return nil
}

func (f *fakeStore) Ping(context.Context) error { return f.pingErr }

func do(t *testing.T, h http.Handler, method, path, body string) (*http.Response, string) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	res := rec.Result()
	b, _ := io.ReadAll(res.Body)
	return res, string(b)
}

func newHandler(f *fakeStore) http.Handler {
	return NewRouter(f, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestHealthz(t *testing.T) {
	f := newFake()
	res, body := do(t, newHandler(f), "GET", "/healthz", "")
	if res.StatusCode != 200 || !strings.Contains(body, `"db":"up"`) {
		t.Errorf("healthy: %d %s", res.StatusCode, body)
	}
	f.pingErr = errors.New("down")
	res, body = do(t, newHandler(f), "GET", "/healthz", "")
	if res.StatusCode != 503 || !strings.Contains(body, `"db":"down"`) {
		t.Errorf("unhealthy: %d %s", res.StatusCode, body)
	}
}

func TestCreateTarget(t *testing.T) {
	h := newHandler(newFake())
	res, body := do(t, h, "POST", "/targets", `{"url":"https://example.com","webhook_url":"https://hooks.example.com"}`)
	if res.StatusCode != 201 {
		t.Fatalf("status %d body %s", res.StatusCode, body)
	}
	var got store.Target
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	if got.URL != "https://example.com" || got.Status != "PENDING" || got.WebhookURL == nil {
		t.Errorf("body %s", body)
	}
	if !strings.Contains(body, `"last_error":null`) {
		t.Errorf("null fields should be present: %s", body)
	}
}

func TestCreateTargetValidation(t *testing.T) {
	h := newHandler(newFake())
	cases := []string{
		`{}`,
		`{"url":""}`,
		`{"url":"example.com"}`,
		`{"url":"ftp://example.com"}`,
		`{"url":"https://"}`,
		`{"url":"https://example.com","webhook_url":"nope"}`,
		`not json`,
		`[]`,
	}
	for _, c := range cases {
		res, body := do(t, h, "POST", "/targets", c)
		if res.StatusCode != 400 || !strings.Contains(body, `"error"`) {
			t.Errorf("%s: status %d body %s", c, res.StatusCode, body)
		}
	}
}

func TestCreateTargetIgnoresUnknownFields(t *testing.T) {
	res, body := do(t, newHandler(newFake()), "POST", "/targets", `{"url":"https://example.com","extra":1}`)
	if res.StatusCode != 201 {
		t.Errorf("status %d body %s", res.StatusCode, body)
	}
}

func TestCreateTargetDuplicate(t *testing.T) {
	h := newHandler(newFake())
	do(t, h, "POST", "/targets", `{"url":"https://example.com"}`)
	res, body := do(t, h, "POST", "/targets", `{"url":"https://example.com"}`)
	if res.StatusCode != 409 || !strings.Contains(body, `"error"`) {
		t.Errorf("status %d body %s", res.StatusCode, body)
	}
}

func TestListTargetsEmptyIsArray(t *testing.T) {
	res, body := do(t, newHandler(newFake()), "GET", "/targets", "")
	if res.StatusCode != 200 || strings.TrimSpace(body) != "[]" {
		t.Errorf("status %d body %q", res.StatusCode, body)
	}
}

func TestGetAndDeleteTarget(t *testing.T) {
	f := newFake()
	h := newHandler(f)
	_, body := do(t, h, "POST", "/targets", `{"url":"https://example.com"}`)
	var created store.Target
	_ = json.Unmarshal([]byte(body), &created)

	res, _ := do(t, h, "GET", "/targets/"+created.ID, "")
	if res.StatusCode != 200 {
		t.Errorf("get: %d", res.StatusCode)
	}
	res, body = do(t, h, "GET", "/targets/missing", "")
	if res.StatusCode != 404 || !strings.Contains(body, `"error"`) {
		t.Errorf("get missing: %d %s", res.StatusCode, body)
	}
	res, _ = do(t, h, "DELETE", "/targets/"+created.ID, "")
	if res.StatusCode != 204 {
		t.Errorf("delete: %d", res.StatusCode)
	}
	res, _ = do(t, h, "DELETE", "/targets/"+created.ID, "")
	if res.StatusCode != 404 {
		t.Errorf("delete again: %d", res.StatusCode)
	}
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./internal/api/`
Expected: build failure, `undefined: NewRouter`.

- [ ] **Step 4: Implement router.go**

`internal/api/router.go`:

```go
// Package api serves the target management REST API.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/ductringuyen-0618/health-monitor/internal/store"
)

// TargetStore is what the API needs from persistence.
type TargetStore interface {
	Create(ctx context.Context, url string, webhookURL *string) (store.Target, error)
	List(ctx context.Context) ([]store.Target, error)
	Get(ctx context.Context, id string) (store.Target, error)
	Delete(ctx context.Context, id string) error
	Ping(ctx context.Context) error
}

func NewRouter(s TargetStore, log *slog.Logger) http.Handler {
	h := &handlers{store: s, log: log}
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(requestLogger(log))
	r.Use(middleware.Recoverer)
	r.Get("/healthz", h.healthz)
	r.Route("/targets", func(r chi.Router) {
		r.Post("/", h.create)
		r.Get("/", h.list)
		r.Get("/{id}", h.get)
		r.Delete("/{id}", h.delete)
	})
	return r
}

func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			log.Info("request",
				"id", middleware.GetReqID(r.Context()),
				"method", r.Method, "path", r.URL.Path,
				"status", ww.Status(), "ms", time.Since(start).Milliseconds())
		})
	}
}
```

- [ ] **Step 5: Implement handlers.go**

`internal/api/handlers.go`:

```go
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"

	"github.com/ductringuyen-0618/health-monitor/internal/store"
)

type handlers struct {
	store TargetStore
	log   *slog.Logger
}

type createRequest struct {
	URL        string  `json:"url"`
	WebhookURL *string `json:"webhook_url"`
}

func (h *handlers) healthz(w http.ResponseWriter, r *http.Request) {
	if err := h.store.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "degraded", "db": "down"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "db": "up"})
}

func (h *handlers) create(w http.ResponseWriter, r *http.Request) {
	var in createRequest
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "body must be a JSON object")
		return
	}
	if err := validateURL(in.URL); err != nil {
		writeError(w, http.StatusBadRequest, "url: "+err.Error())
		return
	}
	if in.WebhookURL != nil {
		if err := validateURL(*in.WebhookURL); err != nil {
			writeError(w, http.StatusBadRequest, "webhook_url: "+err.Error())
			return
		}
	}
	t, err := h.store.Create(r.Context(), in.URL, in.WebhookURL)
	if errors.Is(err, store.ErrConflict) {
		writeError(w, http.StatusConflict, "url already registered")
		return
	}
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

func (h *handlers) list(w http.ResponseWriter, r *http.Request) {
	targets, err := h.store.List(r.Context())
	if err != nil {
		h.fail(w, err)
		return
	}
	if targets == nil {
		targets = []store.Target{}
	}
	writeJSON(w, http.StatusOK, targets)
}

func (h *handlers) get(w http.ResponseWriter, r *http.Request) {
	t, err := h.store.Get(r.Context(), chi.URLParam(r, "id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "target not found")
		return
	}
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (h *handlers) delete(w http.ResponseWriter, r *http.Request) {
	err := h.store.Delete(r.Context(), chi.URLParam(r, "id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "target not found")
		return
	}
	if err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// fail logs the underlying error and answers 503: every non-domain error the
// store returns is a database problem.
func (h *handlers) fail(w http.ResponseWriter, err error) {
	h.log.Error("store error", "err", err)
	writeError(w, http.StatusServiceUnavailable, "database unavailable")
}

func validateURL(raw string) error {
	if raw == "" {
		return fmt.Errorf("is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("is not a valid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https")
	}
	if u.Host == "" {
		return fmt.Errorf("must include a host")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/api/ -v`
Expected: all PASS. Note: `[]` decodes into a struct as an error, and `{}` fails on the empty url, so both return 400.

- [ ] **Step 7: Commit**

```bash
gofmt -l . && go vet ./...
git add -A
git commit -m "feat: target management REST API"
```

---

### Task 5: Binary in api mode

**Files:**
- Create: `cmd/monitor/main.go`, `scripts/run-api.sh`

**Interfaces:**
- Consumes: `config.Load`, `store.New`, `(*store.Store).Migrate`, `api.NewRouter`.
- Produces: a runnable binary. Task 8 adds the `worker` branch to `runWorker`.

- [ ] **Step 1: Write main.go**

`cmd/monitor/main.go`:

```go
// Command monitor runs either the REST API (MODE=api) or the polling worker
// (MODE=worker) against the same Postgres database.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ductringuyen-0618/health-monitor/internal/api"
	"github.com/ductringuyen-0618/health-monitor/internal/config"
	"github.com/ductringuyen-0618/health-monitor/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := migrateWithRetry(ctx, st, log); err != nil {
		return err
	}

	log.Info("starting", "mode", cfg.Mode)
	switch cfg.Mode {
	case "api":
		return runAPI(ctx, cfg, st, log)
	case "worker":
		return runWorker(ctx, cfg, st, log)
	}
	return fmt.Errorf("unknown mode %q", cfg.Mode)
}

// migrateWithRetry gives a freshly provisioned database a minute to come up
// before the process gives up and lets the platform restart it.
func migrateWithRetry(ctx context.Context, st *store.Store, log *slog.Logger) error {
	var err error
	for attempt := 1; attempt <= 12; attempt++ {
		if err = st.Migrate(ctx); err == nil {
			return nil
		}
		log.Warn("migrate failed, retrying", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
	return fmt.Errorf("migrate: %w", err)
}

func runAPI(ctx context.Context, cfg config.Config, st *store.Store, log *slog.Logger) error {
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           api.NewRouter(st, log),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Info("api listening", "port", cfg.Port)
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	log.Info("api stopped")
	return nil
}

func runWorker(ctx context.Context, cfg config.Config, st *store.Store, log *slog.Logger) error {
	return fmt.Errorf("worker mode not implemented yet")
}
```

- [ ] **Step 2: Add the run script**

`scripts/run-api.sh`:

```bash
#!/usr/bin/env bash
# Run the API against the local compose Postgres.
set -euo pipefail
cd "$(dirname "$0")/.."
export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5433/monitor?sslmode=disable}"
export MODE=api
exec go run ./cmd/monitor
```

`chmod +x scripts/run-api.sh`

- [ ] **Step 3: Smoke test by hand**

In one terminal: `./scripts/run-api.sh`. In another:

```bash
curl -s localhost:8080/healthz
curl -s -X POST localhost:8080/targets -H 'Content-Type: application/json' -d '{"url":"https://example.com"}'
curl -s localhost:8080/targets
```

Expected: `{"status":"ok","db":"up"}`, a 201 body with `"status":"PENDING"`, then a one-element array. Stop the server with Ctrl+C and confirm the log shows `api stopped`.

- [ ] **Step 4: Commit**

```bash
gofmt -l . && go vet ./... && go test ./...
git add -A
git commit -m "feat: monitor binary with api mode and graceful shutdown"
```

---

### Task 6: HTTP checker

**Files:**
- Create: `internal/poller/checker.go`, `internal/poller/checker_test.go`

**Interfaces:**
- Consumes: `store.CheckResult`.
- Produces: `poller.NewHTTPChecker(timeout time.Duration) *HTTPChecker` and `(*HTTPChecker).Check(ctx, url string) store.CheckResult`.

- [ ] **Step 1: Write the failing tests**

`internal/poller/checker_test.go`:

```go
package poller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCheckOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(strings.Repeat("x", 200_000))) // larger than the 64 KB cap
	}))
	defer srv.Close()
	res := NewHTTPChecker(2 * time.Second).Check(context.Background(), srv.URL)
	if !res.OK || res.StatusCode == nil || *res.StatusCode != 200 || res.Err != nil {
		t.Errorf("got %+v", res)
	}
}

func TestCheckNon200IsFailure(t *testing.T) {
	for _, code := range []int{201, 301, 404, 500} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if code == 301 {
				w.Header().Set("Location", "/elsewhere")
			}
			w.WriteHeader(code)
		}))
		res := NewHTTPChecker(2 * time.Second).Check(context.Background(), srv.URL)
		srv.Close()
		if res.OK || res.StatusCode == nil || res.Err == nil {
			t.Errorf("%d: got %+v", code, res)
		}
	}
}

func TestCheckFollowsRedirectToOK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/end", 302) })
	mux.HandleFunc("/end", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	res := NewHTTPChecker(2 * time.Second).Check(context.Background(), srv.URL+"/start")
	if !res.OK {
		t.Errorf("got %+v", res)
	}
}

func TestCheckTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
	}))
	defer srv.Close()
	res := NewHTTPChecker(100 * time.Millisecond).Check(context.Background(), srv.URL)
	if res.OK || res.StatusCode != nil || res.Err == nil {
		t.Errorf("got %+v", res)
	}
}

func TestCheckConnectionRefused(t *testing.T) {
	res := NewHTTPChecker(time.Second).Check(context.Background(), "http://127.0.0.1:1")
	if res.OK || res.StatusCode != nil || res.Err == nil {
		t.Errorf("got %+v", res)
	}
}

func TestCheckBadURL(t *testing.T) {
	res := NewHTTPChecker(time.Second).Check(context.Background(), "::not a url")
	if res.OK || res.Err == nil {
		t.Errorf("got %+v", res)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/poller/`
Expected: build failure, `undefined: NewHTTPChecker`.

- [ ] **Step 3: Implement checker.go**

`internal/poller/checker.go`:

```go
// Package poller claims due targets, checks them, and records the outcome.
package poller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ductringuyen-0618/health-monitor/internal/store"
)

const maxBodyBytes = 64 << 10

type HTTPChecker struct {
	client *http.Client
}

// NewHTTPChecker builds a checker whose timeout covers connect, headers, and
// body read, and which follows at most five redirects.
func NewHTTPChecker(timeout time.Duration) *HTTPChecker {
	return &HTTPChecker{client: &http.Client{
		Timeout: timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("stopped after 5 redirects")
			}
			return nil
		},
	}}
}

// Check GETs url and reports success only on a final status of exactly 200.
func (c *HTTPChecker) Check(ctx context.Context, url string) store.CheckResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return failure(nil, err.Error())
	}
	req.Header.Set("User-Agent", "health-monitor/1.0")
	resp, err := c.client.Do(req)
	if err != nil {
		return failure(nil, err.Error())
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
	code := resp.StatusCode
	if code == http.StatusOK {
		return store.CheckResult{OK: true, StatusCode: &code}
	}
	return failure(&code, fmt.Sprintf("unexpected status %d", code))
}

func failure(code *int, msg string) store.CheckResult {
	return store.CheckResult{StatusCode: code, Err: &msg}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/poller/ -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: http checker with timeout, redirect cap, and body limit"
```

---

### Task 7: Alert dispatcher

**Files:**
- Create: `internal/alert/alert.go`, `internal/alert/alert_test.go`

**Interfaces:**
- Produces: `alert.Alert{TargetID, URL string; WebhookURL *string; ConsecutiveFailures int; StatusCode *int; Err *string; OccurredAt time.Time}`, `alert.New(fallbackURL string, timeout time.Duration, log *slog.Logger) *Dispatcher`, `(*Dispatcher).Dispatch(ctx, a Alert) error`, exported field `Dispatcher.Backoff []time.Duration` (default `1s, 2s, 4s`).

- [ ] **Step 1: Write the failing tests**

`internal/alert/alert_test.go`:

```go
package alert

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func newTestDispatcher(fallback string) *Dispatcher {
	d := New(fallback, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	d.Backoff = []time.Duration{0, 0, 0}
	return d
}

func ptr[T any](v T) *T { return &v }

func sample(webhook *string) Alert {
	return Alert{TargetID: "t1", URL: "https://example.com", WebhookURL: webhook,
		ConsecutiveFailures: 2, StatusCode: ptr(503), OccurredAt: time.Date(2026, 9, 13, 18, 0, 15, 0, time.UTC)}
}

func TestDispatchDeliversPayload(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("bad request shape: %s %s", r.Method, r.Header.Get("Content-Type"))
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	if err := newTestDispatcher("").Dispatch(context.Background(), sample(ptr(srv.URL))); err != nil {
		t.Fatal(err)
	}
	if got["event"] != "target.down" || got["target_id"] != "t1" || got["status"] != "DOWN" ||
		got["consecutive_failures"] != float64(2) || got["last_status_code"] != float64(503) ||
		got["last_error"] != nil || got["occurred_at"] != "2026-09-13T18:00:15Z" || got["url"] != "https://example.com" {
		t.Errorf("payload %v", got)
	}
}

func TestDispatchUsesFallback(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer srv.Close()
	if err := newTestDispatcher(srv.URL).Dispatch(context.Background(), sample(nil)); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Errorf("hits = %d", hits.Load())
	}
}

func TestDispatchNoWebhookIsNoop(t *testing.T) {
	if err := newTestDispatcher("").Dispatch(context.Background(), sample(nil)); err != nil {
		t.Fatal(err)
	}
}

func TestDispatchRetriesThenSucceeds(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) < 3 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()
	if err := newTestDispatcher("").Dispatch(context.Background(), sample(ptr(srv.URL))); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 3 {
		t.Errorf("hits = %d, want 3", hits.Load())
	}
}

func TestDispatchGivesUpAfterThreeRetries(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(500)
	}))
	defer srv.Close()
	err := newTestDispatcher("").Dispatch(context.Background(), sample(ptr(srv.URL)))
	if err == nil {
		t.Fatal("expected error")
	}
	if hits.Load() != 4 {
		t.Errorf("hits = %d, want 4 (1 try + 3 retries)", hits.Load())
	}
}

func TestDispatchUnreachable(t *testing.T) {
	err := newTestDispatcher("").Dispatch(context.Background(), sample(ptr("http://127.0.0.1:1")))
	if err == nil {
		t.Fatal("expected error")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/alert/`
Expected: build failure, `undefined: New`.

- [ ] **Step 3: Implement alert.go**

`internal/alert/alert.go`:

```go
// Package alert posts webhook notifications when a target goes DOWN.
package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type Alert struct {
	TargetID            string
	URL                 string
	WebhookURL          *string
	ConsecutiveFailures int
	StatusCode          *int
	Err                 *string
	OccurredAt          time.Time
}

type payload struct {
	Event               string    `json:"event"`
	TargetID            string    `json:"target_id"`
	URL                 string    `json:"url"`
	Status              string    `json:"status"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	LastStatusCode      *int      `json:"last_status_code"`
	LastError           *string   `json:"last_error"`
	OccurredAt          time.Time `json:"occurred_at"`
}

type Dispatcher struct {
	client   *http.Client
	fallback string
	log      *slog.Logger
	// Backoff holds the wait before each retry; its length is the retry count.
	Backoff []time.Duration
}

func New(fallbackURL string, timeout time.Duration, log *slog.Logger) *Dispatcher {
	return &Dispatcher{
		client:   &http.Client{Timeout: timeout},
		fallback: fallbackURL,
		log:      log,
		Backoff:  []time.Duration{time.Second, 2 * time.Second, 4 * time.Second},
	}
}

// Dispatch POSTs the alert to the target's webhook or the fallback. Any 2xx
// counts as delivered. After the retries are exhausted it logs and returns
// the last error; the caller has already committed the state change.
func (d *Dispatcher) Dispatch(ctx context.Context, a Alert) error {
	url := d.fallback
	if a.WebhookURL != nil && *a.WebhookURL != "" {
		url = *a.WebhookURL
	}
	if url == "" {
		d.log.Warn("target down but no webhook configured", "target", a.TargetID, "url", a.URL)
		return nil
	}
	body, err := json.Marshal(payload{
		Event: "target.down", TargetID: a.TargetID, URL: a.URL, Status: "DOWN",
		ConsecutiveFailures: a.ConsecutiveFailures, LastStatusCode: a.StatusCode,
		LastError: a.Err, OccurredAt: a.OccurredAt.UTC(),
	})
	if err != nil {
		return err
	}
	var last error
	for attempt := 0; attempt <= len(d.Backoff); attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d.Backoff[attempt-1]):
			}
		}
		last = d.post(ctx, url, body)
		if last == nil {
			d.log.Info("webhook delivered", "target", a.TargetID, "attempt", attempt+1)
			return nil
		}
		d.log.Warn("webhook attempt failed", "target", a.TargetID, "attempt", attempt+1, "err", last)
	}
	d.log.Error("webhook delivery abandoned", "target", a.TargetID, "webhook", url, "err", last)
	return last
}

func (d *Dispatcher) post(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("webhook returned %d", resp.StatusCode)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/alert/ -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: webhook dispatcher with retry and fallback"
```

---

### Task 8: Poller loop and worker mode

**Files:**
- Create: `internal/poller/poller.go`, `internal/poller/poller_test.go`, `scripts/run-worker.sh`
- Modify: `cmd/monitor/main.go` (replace `runWorker`)

**Interfaces:**
- Consumes: `store.Target`, `store.CheckResult`, `store.Transition`, `store.ErrNotFound`, `alert.Alert`, `(*alert.Dispatcher).Dispatch`, `poller.NewHTTPChecker`.
- Produces: `poller.Store`, `poller.Checker`, `poller.Dispatcher` interfaces; `poller.New(s Store, c Checker, d Dispatcher, interval time.Duration, maxConcurrent int, log *slog.Logger) *Poller`; `(*Poller).Run(ctx) error`; `(*Poller).RunOnce(ctx) (int, error)`; `(*Poller).Wait()`.

- [ ] **Step 1: Write the failing tests**

`internal/poller/poller_test.go`:

```go
package poller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ductringuyen-0618/health-monitor/internal/alert"
	"github.com/ductringuyen-0618/health-monitor/internal/store"
)

type fakeStore struct {
	mu       sync.Mutex
	due      []store.Target
	claimErr error
	recorded map[string][]store.CheckResult
	// transitions is returned in order per target; defaults to no transition.
	transitions map[string][]store.Transition
	deleted     map[string]bool
}

func newFakeStore(due ...store.Target) *fakeStore {
	return &fakeStore{due: due, recorded: map[string][]store.CheckResult{},
		transitions: map[string][]store.Transition{}, deleted: map[string]bool{}}
}

func (f *fakeStore) ClaimDue(_ context.Context, limit int, _ time.Duration) ([]store.Target, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	n := min(limit, len(f.due))
	out := f.due[:n]
	f.due = f.due[n:]
	return out, nil
}

func (f *fakeStore) CountDue(context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.due), nil
}

func (f *fakeStore) RecordCheck(_ context.Context, id string, r store.CheckResult) (store.Transition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleted[id] {
		return store.Transition{}, store.ErrNotFound
	}
	f.recorded[id] = append(f.recorded[id], r)
	q := f.transitions[id]
	if len(q) == 0 {
		return store.Transition{From: "UP", To: "UP"}, nil
	}
	f.transitions[id] = q[1:]
	return q[0], nil
}

type fakeDispatcher struct {
	mu     sync.Mutex
	alerts []alert.Alert
}

func (d *fakeDispatcher) Dispatch(_ context.Context, a alert.Alert) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.alerts = append(d.alerts, a)
	return nil
}

type panicChecker struct{}

func (panicChecker) Check(context.Context, string) store.CheckResult { panic("boom") }

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func serverWith(code int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) }))
}

func TestRunOnceChecksAndRecords(t *testing.T) {
	ok := serverWith(200)
	defer ok.Close()
	bad := serverWith(500)
	defer bad.Close()
	fs := newFakeStore(store.Target{ID: "a", URL: ok.URL}, store.Target{ID: "b", URL: bad.URL})
	p := New(fs, NewHTTPChecker(time.Second), &fakeDispatcher{}, 15*time.Second, 5, quiet())
	n, err := p.RunOnce(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	p.Wait()
	if r := fs.recorded["a"]; len(r) != 1 || !r[0].OK {
		t.Errorf("a recorded %+v", r)
	}
	if r := fs.recorded["b"]; len(r) != 1 || r[0].OK || *r[0].StatusCode != 500 {
		t.Errorf("b recorded %+v", r)
	}
}

func TestRunOnceClaimsOnlyFreeSlots(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer slow.Close()
	var due []store.Target
	for _, id := range []string{"1", "2", "3", "4", "5"} {
		due = append(due, store.Target{ID: id, URL: slow.URL})
	}
	fs := newFakeStore(due...)
	p := New(fs, NewHTTPChecker(2*time.Second), &fakeDispatcher{}, 15*time.Second, 2, quiet())
	n, _ := p.RunOnce(context.Background())
	if n != 2 {
		t.Fatalf("first claim %d, want 2", n)
	}
	n, _ = p.RunOnce(context.Background())
	if n != 0 {
		t.Fatalf("claim with full semaphore %d, want 0", n)
	}
	p.Wait()
	n, _ = p.RunOnce(context.Background())
	if n != 2 {
		t.Fatalf("after drain %d, want 2", n)
	}
	p.Wait()
}

func TestDispatchesOnlyOnTransitionToDown(t *testing.T) {
	bad := serverWith(503)
	defer bad.Close()
	fs := newFakeStore(store.Target{ID: "x", URL: bad.URL}, store.Target{ID: "y", URL: bad.URL}, store.Target{ID: "z", URL: bad.URL})
	fs.transitions["x"] = []store.Transition{{From: "PENDING", To: "DOWN", ConsecutiveFailures: 2}}
	fs.transitions["y"] = []store.Transition{{From: "DOWN", To: "DOWN", ConsecutiveFailures: 3}}
	fs.transitions["z"] = []store.Transition{{From: "UP", To: "UP", ConsecutiveFailures: 1}}
	d := &fakeDispatcher{}
	p := New(fs, NewHTTPChecker(time.Second), d, 15*time.Second, 5, quiet())
	if _, err := p.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.Wait()
	if len(d.alerts) != 1 || d.alerts[0].TargetID != "x" || d.alerts[0].ConsecutiveFailures != 2 ||
		d.alerts[0].StatusCode == nil || *d.alerts[0].StatusCode != 503 || d.alerts[0].OccurredAt.IsZero() {
		t.Errorf("alerts %+v", d.alerts)
	}
}

func TestPanicInCheckDoesNotStopOthers(t *testing.T) {
	fs := newFakeStore(store.Target{ID: "p", URL: "http://x"}, store.Target{ID: "q", URL: "http://y"})
	p := New(fs, panicChecker{}, &fakeDispatcher{}, 15*time.Second, 5, quiet())
	if _, err := p.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.Wait() // must return: the semaphore slots were released despite the panics
	if len(fs.recorded) != 0 {
		t.Errorf("panicking checks should record nothing, got %+v", fs.recorded)
	}
}

func TestDeletedTargetIsIgnored(t *testing.T) {
	ok := serverWith(200)
	defer ok.Close()
	fs := newFakeStore(store.Target{ID: "gone", URL: ok.URL})
	fs.deleted["gone"] = true
	p := New(fs, NewHTTPChecker(time.Second), &fakeDispatcher{}, 15*time.Second, 5, quiet())
	if _, err := p.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.Wait()
}

func TestRunOnceReturnsClaimError(t *testing.T) {
	fs := newFakeStore()
	fs.claimErr = errors.New("db down")
	p := New(fs, NewHTTPChecker(time.Second), &fakeDispatcher{}, 15*time.Second, 5, quiet())
	if _, err := p.RunOnce(context.Background()); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	ok := serverWith(200)
	defer ok.Close()
	fs := newFakeStore(store.Target{ID: "a", URL: ok.URL})
	p := New(fs, NewHTTPChecker(time.Second), &fakeDispatcher{}, 15*time.Second, 5, quiet())
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := p.Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run returned %v", err)
	}
	if len(fs.recorded["a"]) != 1 {
		t.Errorf("expected one check before cancel, got %+v", fs.recorded)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/poller/`
Expected: build failure, `undefined: New`.

- [ ] **Step 3: Implement poller.go**

`internal/poller/poller.go`:

```go
package poller

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/ductringuyen-0618/health-monitor/internal/alert"
	"github.com/ductringuyen-0618/health-monitor/internal/store"
)

type Store interface {
	ClaimDue(ctx context.Context, limit int, interval time.Duration) ([]store.Target, error)
	CountDue(ctx context.Context) (int, error)
	RecordCheck(ctx context.Context, id string, r store.CheckResult) (store.Transition, error)
}

type Checker interface {
	Check(ctx context.Context, url string) store.CheckResult
}

type Dispatcher interface {
	Dispatch(ctx context.Context, a alert.Alert) error
}

const (
	idleSleep    = time.Second
	fullSleep    = 100 * time.Millisecond
	minBackoff   = time.Second
	maxBackoff   = 30 * time.Second
)

type Poller struct {
	store    Store
	checker  Checker
	dispatch Dispatcher
	interval time.Duration
	sem      chan struct{}
	wg       sync.WaitGroup
	log      *slog.Logger
}

func New(s Store, c Checker, d Dispatcher, interval time.Duration, maxConcurrent int, log *slog.Logger) *Poller {
	return &Poller{store: s, checker: c, dispatch: d, interval: interval,
		sem: make(chan struct{}, maxConcurrent), log: log}
}

// Run drains due targets until ctx is cancelled, then waits for in-flight
// checks. Store errors back off from 1s to 30s; the loop never exits on them.
func (p *Poller) Run(ctx context.Context) error {
	backoff := minBackoff
	for {
		if ctx.Err() != nil {
			p.wg.Wait()
			return ctx.Err()
		}
		claimed, err := p.RunOnce(ctx)
		var wait time.Duration
		switch {
		case err != nil:
			p.log.Error("poll iteration failed", "err", err, "retry_in", backoff)
			wait, backoff = backoff, min(backoff*2, maxBackoff)
		case claimed > 0:
			backoff = minBackoff
			continue
		case len(p.sem) == cap(p.sem):
			wait, backoff = fullSleep, minBackoff
		default:
			wait, backoff = idleSleep, minBackoff
		}
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
}

// RunOnce claims as many due targets as there are free semaphore slots and
// starts a check goroutine for each. It returns the number claimed.
func (p *Poller) RunOnce(ctx context.Context) (int, error) {
	free := cap(p.sem) - len(p.sem)
	if free == 0 {
		return 0, nil
	}
	targets, err := p.store.ClaimDue(ctx, free, p.interval)
	if err != nil {
		return 0, err
	}
	for _, t := range targets {
		p.sem <- struct{}{}
		p.wg.Add(1)
		go func(t store.Target) {
			defer p.wg.Done()
			defer func() { <-p.sem }()
			p.handle(ctx, t)
		}(t)
	}
	if len(targets) > 0 {
		backlog, _ := p.store.CountDue(ctx)
		p.log.Info("claimed", "count", len(targets), "backlog", backlog, "in_flight", len(p.sem))
	}
	return len(targets), nil
}

// Wait blocks until every in-flight check has finished. Used by tests and by
// Run during shutdown.
func (p *Poller) Wait() { p.wg.Wait() }

func (p *Poller) handle(ctx context.Context, t store.Target) {
	defer func() {
		if r := recover(); r != nil {
			p.log.Error("check panicked", "target", t.ID, "url", t.URL, "panic", r)
		}
	}()
	res := p.checker.Check(ctx, t.URL)
	tr, err := p.store.RecordCheck(ctx, t.ID, res)
	if errors.Is(err, store.ErrNotFound) {
		return
	}
	if err != nil {
		p.log.Error("record check failed", "target", t.ID, "err", err)
		return
	}
	p.log.Info("checked", "target", t.ID, "url", t.URL, "ok", res.OK, "status", tr.To, "failures", tr.ConsecutiveFailures)
	if tr.From != "DOWN" && tr.To == "DOWN" {
		a := alert.Alert{TargetID: t.ID, URL: t.URL, WebhookURL: t.WebhookURL,
			ConsecutiveFailures: tr.ConsecutiveFailures, StatusCode: res.StatusCode, Err: res.Err,
			OccurredAt: time.Now()}
		if err := p.dispatch.Dispatch(ctx, a); err != nil {
			p.log.Error("alert dispatch failed", "target", t.ID, "err", err)
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/poller/ -v -race`
Expected: all PASS with no race reports.

- [ ] **Step 5: Wire worker mode into main.go**

Replace the `runWorker` stub in `cmd/monitor/main.go` with:

```go
func runWorker(ctx context.Context, cfg config.Config, st *store.Store, log *slog.Logger) error {
	p := poller.New(
		st,
		poller.NewHTTPChecker(cfg.CheckTimeout),
		alert.New(cfg.DefaultWebhookURL, 5*time.Second, log),
		cfg.PollInterval,
		cfg.MaxConcurrentChecks,
		log,
	)
	log.Info("worker started", "interval", cfg.PollInterval, "max_concurrent", cfg.MaxConcurrentChecks)
	err := p.Run(ctx)
	log.Info("worker stopped")
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
```

Add to the import block: `"github.com/ductringuyen-0618/health-monitor/internal/alert"` and `"github.com/ductringuyen-0618/health-monitor/internal/poller"`.

`scripts/run-worker.sh`:

```bash
#!/usr/bin/env bash
# Run the poller against the local compose Postgres.
set -euo pipefail
cd "$(dirname "$0")/.."
export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5433/monitor?sslmode=disable}"
export MODE=worker
exec go run ./cmd/monitor
```

`chmod +x scripts/run-worker.sh`

- [ ] **Step 6: Smoke test both modes together**

Terminal 1: `./scripts/run-api.sh`. Terminal 2: `./scripts/run-worker.sh`. Terminal 3:

```bash
curl -s -X POST localhost:8080/targets -H 'Content-Type: application/json' -d '{"url":"https://httpbin.org/status/500"}'
sleep 35
curl -s localhost:8080/targets
```

Expected: the worker log shows `checked` lines, and after two intervals the target reports `"status":"DOWN"` with `"consecutive_failures":2` and a `webhook` warning line saying no webhook is configured. Stop both with Ctrl+C.

- [ ] **Step 7: Commit**

```bash
./scripts/test.sh -race
git add -A
git commit -m "feat: poller drain loop and worker mode"
```

---

### Task 9: Container, App Platform spec, demo tooling, README

**Files:**
- Create: `Dockerfile`, `deployments/app-platform.yaml`, `cmd/demo-sink/main.go`, `scripts/demo.sh`, `README.md`

**Interfaces:**
- Consumes: the API from Task 4 and the worker from Task 8.
- Produces: a buildable image and a demo script used again in Task 10 against the live URL.

- [ ] **Step 1: Dockerfile**

```dockerfile
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/monitor ./cmd/monitor

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/monitor /monitor
EXPOSE 8080
ENTRYPOINT ["/monitor"]
```

Verify: `docker build -t health-monitor .` succeeds, then

```bash
docker run --rm -p 8081:8080 -e DATABASE_URL='postgres://postgres:postgres@host.docker.internal:5433/monitor?sslmode=disable' health-monitor
```

and `curl -s localhost:8081/healthz` returns `db":"up"`. Stop the container with Ctrl+C.

- [ ] **Step 2: App Platform spec**

`deployments/app-platform.yaml`:

```yaml
name: health-monitor
region: nyc
services:
  - name: api
    github:
      repo: ductringuyen-0618/health-monitor
      branch: main
      deploy_on_push: true
    dockerfile_path: Dockerfile
    http_port: 8080
    instance_count: 1
    instance_size_slug: apps-s-1vcpu-0.5gb
    health_check:
      http_path: /healthz
      initial_delay_seconds: 10
    envs:
      - key: MODE
        value: api
      - key: DATABASE_URL
        value: ${db.DATABASE_URL}
workers:
  - name: poller
    github:
      repo: ductringuyen-0618/health-monitor
      branch: main
      deploy_on_push: true
    dockerfile_path: Dockerfile
    instance_count: 1
    instance_size_slug: apps-s-1vcpu-0.5gb
    envs:
      - key: MODE
        value: worker
      - key: DATABASE_URL
        value: ${db.DATABASE_URL}
      - key: POLL_INTERVAL
        value: 15s
databases:
  - name: db
    engine: PG
    version: "16"
    production: false
```

- [ ] **Step 3: Demo sink**

`cmd/demo-sink/main.go`:

```go
// Command demo-sink is a local stand-in for a monitored host and a webhook
// receiver: /ok returns 200, /bad returns 500, POST /hook records the body,
// GET /hook lists recorded bodies.
package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
)

func main() {
	var mu sync.Mutex
	var received []json.RawMessage
	mux := http.NewServeMux()
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/bad", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) })
	mux.HandleFunc("/hook", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodPost {
			b, _ := io.ReadAll(r.Body)
			received = append(received, json.RawMessage(b))
			log.Printf("webhook received: %s", b)
			w.WriteHeader(202)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if received == nil {
			w.Write([]byte("[]"))
			return
		}
		_ = json.NewEncoder(w).Encode(received)
	})
	port := os.Getenv("SINK_PORT")
	if port == "" {
		port = "9090"
	}
	log.Printf("demo-sink listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
```

- [ ] **Step 4: Demo script**

`scripts/demo.sh`:

```bash
#!/usr/bin/env bash
# Register one healthy and one failing target, then wait for the failing one
# to go DOWN. Works locally (defaults) and against the live app:
#   BASE=https://<app>.ondigitalocean.app OK_URL=https://httpbin.org/status/200 \
#   BAD_URL=https://httpbin.org/status/500 HOOK_URL=https://webhook.site/<id> ./scripts/demo.sh
set -euo pipefail
BASE="${BASE:-http://localhost:8080}"
OK_URL="${OK_URL:-http://localhost:9090/ok}"
BAD_URL="${BAD_URL:-http://localhost:9090/bad}"
HOOK_URL="${HOOK_URL:-http://localhost:9090/hook}"
SUFFIX="$(date +%s)"

json_field() { python -c "import sys,json; print(json.load(sys.stdin)['$1'])"; }

echo "== healthz"; curl -sf "$BASE/healthz"; echo
echo "== register healthy target"
OK_ID=$(curl -sf -X POST "$BASE/targets" -H 'Content-Type: application/json' \
  -d "{\"url\":\"$OK_URL?d=$SUFFIX\",\"webhook_url\":\"$HOOK_URL\"}" | json_field id)
echo "== register failing target"
BAD_ID=$(curl -sf -X POST "$BASE/targets" -H 'Content-Type: application/json' \
  -d "{\"url\":\"$BAD_URL?d=$SUFFIX\",\"webhook_url\":\"$HOOK_URL\"}" | json_field id)
echo "healthy=$OK_ID failing=$BAD_ID"

for i in $(seq 1 40); do
  STATUS=$(curl -sf "$BASE/targets/$BAD_ID" | json_field status)
  echo "t+$((i*3))s failing target status: $STATUS"
  if [ "$STATUS" = "DOWN" ]; then
    echo "== failing target"; curl -sf "$BASE/targets/$BAD_ID"; echo
    echo "== healthy target"; curl -sf "$BASE/targets/$OK_ID"; echo
    if [[ "$HOOK_URL" == http://localhost* ]]; then
      echo "== webhooks received by sink"; curl -sf "$HOOK_URL"; echo
    else
      echo "check $HOOK_URL for the target.down payload"
    fi
    exit 0
  fi
  sleep 3
done
echo "failing target never went DOWN" >&2
exit 1
```

`chmod +x scripts/demo.sh`

- [ ] **Step 5: Run the local end-to-end demo**

Four terminals: `./scripts/run-api.sh`, `./scripts/run-worker.sh`, `go run ./cmd/demo-sink`, then `./scripts/demo.sh`.

Expected: within about 35 seconds the script prints `failing target status: DOWN`, the failing target body shows `"consecutive_failures":2`, the healthy one shows `"status":"UP"`, and the sink lists exactly one payload with `"event":"target.down"`. Save the script output to `docs/evidence/local-demo.txt`.

- [ ] **Step 6: README**

`README.md`:

```markdown
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
```

- [ ] **Step 7: Commit**

```bash
./scripts/test.sh
git add -A
git commit -m "feat: dockerfile, app platform spec, demo tooling, and readme"
```

---

### Task 10: Deploy and produce evidence

**Files:**
- Create: `docs/evidence/deploy.txt`, `docs/evidence/live-demo.txt`, `docs/evidence/two-workers.txt`

**Interfaces:**
- Consumes: everything above.
- Produces: a live URL and evidence files.

- [ ] **Step 1: Push to GitHub**

```bash
gh repo create ductringuyen-0618/health-monitor --public --source=. --remote=origin --push
git branch --show-current   # must print main
```

- [ ] **Step 2: Create the app**

```bash
doctl apps create --spec deployments/app-platform.yaml --format ID,DefaultIngress
```

Note the app ID. Poll until the deployment is ACTIVE:

```bash
APP_ID=<id>
doctl apps list-deployments "$APP_ID" --format ID,Phase,Progress
```

If the build fails, read `doctl apps logs "$APP_ID" --type build` and fix. If the api component restarts, read `doctl apps logs "$APP_ID" api --type run`.

- [ ] **Step 3: Verify live health**

```bash
LIVE=$(doctl apps get "$APP_ID" --format DefaultIngress --no-header)
curl -s "$LIVE/healthz"
```

Expected: `{"status":"ok","db":"up"}`. Save the command and output to `docs/evidence/deploy.txt` together with `doctl apps get "$APP_ID"` output.

- [ ] **Step 4: Run the live demo**

Open https://webhook.site in a browser and copy the unique URL. Then:

```bash
BASE="$LIVE" OK_URL=https://httpbin.org/status/200 BAD_URL=https://httpbin.org/status/500 \
HOOK_URL=https://webhook.site/<id> ./scripts/demo.sh | tee docs/evidence/live-demo.txt
```

Expected: the failing target reaches DOWN within about 35 seconds and webhook.site shows one `target.down` request. Paste the payload body from webhook.site into the bottom of `docs/evidence/live-demo.txt`.

- [ ] **Step 5: Prove two workers do not double-check**

Edit `deployments/app-platform.yaml`: set the poller `instance_count: 2`. Then:

```bash
doctl apps update "$APP_ID" --spec deployments/app-platform.yaml
# wait for ACTIVE, then watch one interval
doctl apps logs "$APP_ID" poller --type run --tail 200 > docs/evidence/two-workers.txt
```

In the file, count `checked` lines per `target` within any 15 second window: each target appears once. Both instances should show `claimed` lines. Leave `instance_count: 2` if the account allows it, otherwise set it back to 1 and update the app again. Commit the yaml change either way.

- [ ] **Step 6: Commit evidence**

```bash
git add -A
git commit -m "docs: deployment and live demo evidence"
git push
```

Confirm the push triggers a new deployment that goes ACTIVE (`deploy_on_push`), and `curl "$LIVE/healthz"` still answers.

---

## Self-review

**Spec coverage.** Scope decisions: timeouts count as failures (Task 6), 200-only success (Task 6), one-success recovery and two-strikes DOWN (Task 3 SQL), fire-once on transition (Task 8 `handle`), per-target webhook with fallback and no-webhook no-op (Task 7), 3 retries with backoff (Task 7), multi-worker claim (Task 3). Components and layout: Tasks 1 to 9. Data model: Task 2 migration. Store operations: Tasks 2 and 3. API table, validation, JSON shape, error body, middleware, graceful shutdown: Tasks 4 and 5. Poller drain loop, free-slot claiming, backlog log, checker rules, panic recovery: Tasks 6 and 8. Dispatcher payload and retry: Task 7. Configuration table: Task 1. Dockerfile, App Platform spec, compose on 5433, demo script: Task 9. Error handling: DB unreachable (Task 5 healthz 503 and Task 8 backoff), deleted mid-check (Tasks 3 and 8). Testing list: store integration tests use compose Postgres through `TEST_DATABASE_URL` instead of testcontainers, a deliberate simplification recorded in Global Constraints. Evidence: Task 10.

**Placeholder scan.** No TBD, no "similar to Task N", every code step has code.

**Type consistency.** `store.CheckResult{OK, StatusCode *int, Err *string}` is used identically in Tasks 3, 6, 8. `store.Transition{From, To, ConsecutiveFailures}` matches Tasks 3 and 8. `alert.Alert` fields match between Tasks 7 and 8. `poller.New(s, c, d, interval, maxConcurrent, log)` matches Task 8 tests and the `runWorker` wiring. `api.NewRouter(s, log)` matches Tasks 4 and 5. `NewHTTPChecker(timeout)` matches Tasks 6 and 8.
