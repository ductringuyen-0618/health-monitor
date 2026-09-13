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
