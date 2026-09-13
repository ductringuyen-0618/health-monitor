package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"

	"github.com/ductringuyen-0618/health-monitor/internal/store"
)

type handlers struct {
	store TargetStore
	log   *slog.Logger
}

type createRequest struct {
	URL        string  `json:"url"`
	WebhookURL *string `json:"webhook_url"`
}

func (h *handlers) healthz(w http.ResponseWriter, r *http.Request) {
	if err := h.store.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "degraded", "db": "down"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "db": "up"})
}

func (h *handlers) create(w http.ResponseWriter, r *http.Request) {
	var in createRequest
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "body must be a JSON object")
		return
	}
	if err := validateURL(in.URL); err != nil {
		writeError(w, http.StatusBadRequest, "url: "+err.Error())
		return
	}
	if in.WebhookURL != nil {
		if err := validateURL(*in.WebhookURL); err != nil {
			writeError(w, http.StatusBadRequest, "webhook_url: "+err.Error())
			return
		}
	}
	t, err := h.store.Create(r.Context(), in.URL, in.WebhookURL)
	if errors.Is(err, store.ErrConflict) {
		writeError(w, http.StatusConflict, "url already registered")
		return
	}
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

func (h *handlers) list(w http.ResponseWriter, r *http.Request) {
	targets, err := h.store.List(r.Context())
	if err != nil {
		h.fail(w, err)
		return
	}
	if targets == nil {
		targets = []store.Target{}
	}
	writeJSON(w, http.StatusOK, targets)
}

func (h *handlers) get(w http.ResponseWriter, r *http.Request) {
	t, err := h.store.Get(r.Context(), chi.URLParam(r, "id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "target not found")
		return
	}
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (h *handlers) delete(w http.ResponseWriter, r *http.Request) {
	err := h.store.Delete(r.Context(), chi.URLParam(r, "id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "target not found")
		return
	}
	if err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// fail logs the underlying error and answers 503: every non-domain error the
// store returns is a database problem.
func (h *handlers) fail(w http.ResponseWriter, err error) {
	h.log.Error("store error", "err", err)
	writeError(w, http.StatusServiceUnavailable, "database unavailable")
}

func validateURL(raw string) error {
	if raw == "" {
		return fmt.Errorf("is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("is not a valid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https")
	}
	if u.Host == "" {
		return fmt.Errorf("must include a host")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
