// Package alert posts webhook notifications when a target goes DOWN.
package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type Alert struct {
	TargetID            string
	URL                 string
	WebhookURL          *string
	ConsecutiveFailures int
	StatusCode          *int
	Err                 *string
	OccurredAt          time.Time
}

type payload struct {
	Event               string    `json:"event"`
	TargetID            string    `json:"target_id"`
	URL                 string    `json:"url"`
	Status              string    `json:"status"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	LastStatusCode      *int      `json:"last_status_code"`
	LastError           *string   `json:"last_error"`
	OccurredAt          time.Time `json:"occurred_at"`
}

type Dispatcher struct {
	client   *http.Client
	fallback string
	log      *slog.Logger
	// Backoff holds the wait before each retry; its length is the retry count.
	Backoff []time.Duration
}

func New(fallbackURL string, timeout time.Duration, log *slog.Logger) *Dispatcher {
	return &Dispatcher{
		client:   &http.Client{Timeout: timeout},
		fallback: fallbackURL,
		log:      log,
		Backoff:  []time.Duration{time.Second, 2 * time.Second, 4 * time.Second},
	}
}

// Dispatch POSTs the alert to the target's webhook or the fallback. Any 2xx
// counts as delivered. After the retries are exhausted it logs and returns
// the last error; the caller has already committed the state change.
func (d *Dispatcher) Dispatch(ctx context.Context, a Alert) error {
	url := d.fallback
	if a.WebhookURL != nil && *a.WebhookURL != "" {
		url = *a.WebhookURL
	}
	if url == "" {
		d.log.Warn("target down but no webhook configured", "target", a.TargetID, "url", a.URL)
		return nil
	}
	body, err := json.Marshal(payload{
		Event: "target.down", TargetID: a.TargetID, URL: a.URL, Status: "DOWN",
		ConsecutiveFailures: a.ConsecutiveFailures, LastStatusCode: a.StatusCode,
		LastError: a.Err, OccurredAt: a.OccurredAt.UTC(),
	})
	if err != nil {
		return err
	}
	var last error
	for attempt := 0; attempt <= len(d.Backoff); attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d.Backoff[attempt-1]):
			}
		}
		last = d.post(ctx, url, body)
		if last == nil {
			d.log.Info("webhook delivered", "target", a.TargetID, "attempt", attempt+1)
			return nil
		}
		d.log.Warn("webhook attempt failed", "target", a.TargetID, "attempt", attempt+1, "err", last)
	}
	d.log.Error("webhook delivery abandoned", "target", a.TargetID, "webhook", url, "err", last)
	return last
}

func (d *Dispatcher) post(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("webhook returned %d", resp.StatusCode)
	}
	return nil
}
