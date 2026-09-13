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
