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
