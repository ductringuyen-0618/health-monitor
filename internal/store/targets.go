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
