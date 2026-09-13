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
