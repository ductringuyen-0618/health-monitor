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
	res := NewHTTPChecker(2*time.Second).Check(context.Background(), srv.URL)
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
		res := NewHTTPChecker(2*time.Second).Check(context.Background(), srv.URL)
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
	res := NewHTTPChecker(2*time.Second).Check(context.Background(), srv.URL+"/start")
	if !res.OK {
		t.Errorf("got %+v", res)
	}
}

func TestCheckTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
	}))
	defer srv.Close()
	res := NewHTTPChecker(100*time.Millisecond).Check(context.Background(), srv.URL)
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
