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
