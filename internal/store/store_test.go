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
