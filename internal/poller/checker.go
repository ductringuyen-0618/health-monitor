// Package poller claims due targets, checks them, and records the outcome.
package poller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ductringuyen-0618/health-monitor/internal/store"
)

const maxBodyBytes = 64 << 10

type HTTPChecker struct {
	client *http.Client
}

// NewHTTPChecker builds a checker whose timeout covers connect, headers, and
// body read, and which follows at most five redirects.
func NewHTTPChecker(timeout time.Duration) *HTTPChecker {
	return &HTTPChecker{client: &http.Client{
		Timeout: timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}}
}

// Check GETs url and reports success only on a final status of exactly 200.
func (c *HTTPChecker) Check(ctx context.Context, url string) store.CheckResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return failure(nil, err.Error())
	}
	req.Header.Set("User-Agent", "health-monitor/1.0")
	resp, err := c.client.Do(req)
	if err != nil {
		return failure(nil, err.Error())
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
	code := resp.StatusCode
	if code == http.StatusOK {
		return store.CheckResult{OK: true, StatusCode: &code}
	}
	return failure(&code, fmt.Sprintf("unexpected status %d", code))
}

func failure(code *int, msg string) store.CheckResult {
	return store.CheckResult{StatusCode: code, Err: &msg}
}
