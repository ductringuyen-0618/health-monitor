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

func (f *fakeStore) RecordCheck(ctx context.Context, id string, r store.CheckResult) (store.Transition, error) {
	if ctx.Err() != nil {
		return store.Transition{}, ctx.Err()
	}
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

func TestRunLetsInFlightCheckFinishAfterCancel(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer slow.Close()
	fs := newFakeStore(store.Target{ID: "a", URL: slow.URL})
	p := New(fs, NewHTTPChecker(2*time.Second), &fakeDispatcher{}, 15*time.Second, 5, quiet())
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- p.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
	if r := fs.recorded["a"]; len(r) != 1 || !r[0].OK {
		t.Errorf("expected the in-flight check to finish and record OK once, got %+v", r)
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
