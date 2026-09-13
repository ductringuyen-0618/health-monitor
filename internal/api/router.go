// Package api serves the target management REST API.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/ductringuyen-0618/health-monitor/internal/store"
)

// TargetStore is what the API needs from persistence.
type TargetStore interface {
	Create(ctx context.Context, url string, webhookURL *string) (store.Target, error)
	List(ctx context.Context) ([]store.Target, error)
	Get(ctx context.Context, id string) (store.Target, error)
	Delete(ctx context.Context, id string) error
	Ping(ctx context.Context) error
}

func NewRouter(s TargetStore, log *slog.Logger) http.Handler {
	h := &handlers{store: s, log: log}
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(requestLogger(log))
	r.Use(middleware.Recoverer)
	r.Get("/healthz", h.healthz)
	r.Route("/targets", func(r chi.Router) {
		r.Post("/", h.create)
		r.Get("/", h.list)
		r.Get("/{id}", h.get)
		r.Delete("/{id}", h.delete)
	})
	return r
}

func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			log.Info("request",
				"id", middleware.GetReqID(r.Context()),
				"method", r.Method, "path", r.URL.Path,
				"status", ww.Status(), "ms", time.Since(start).Milliseconds())
		})
	}
}
