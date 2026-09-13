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
