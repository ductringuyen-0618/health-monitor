package poller

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/ductringuyen-0618/health-monitor/internal/alert"
	"github.com/ductringuyen-0618/health-monitor/internal/store"
)

type Store interface {
	ClaimDue(ctx context.Context, limit int, interval time.Duration) ([]store.Target, error)
	CountDue(ctx context.Context) (int, error)
	RecordCheck(ctx context.Context, id string, r store.CheckResult) (store.Transition, error)
}

type Checker interface {
	Check(ctx context.Context, url string) store.CheckResult
}

type Dispatcher interface {
	Dispatch(ctx context.Context, a alert.Alert) error
}

const (
	idleSleep  = time.Second
	fullSleep  = 100 * time.Millisecond
	minBackoff = time.Second
	maxBackoff = 30 * time.Second
	// inFlightBudget bounds a check goroutine after Run's context is
	// cancelled: it covers the checker timeout plus webhook retries plus the
	// DB write, so an in-flight check can finish instead of being aborted.
	inFlightBudget = 30 * time.Second
)

type Poller struct {
	store    Store
	checker  Checker
	dispatch Dispatcher
	interval time.Duration
	sem      chan struct{}
	wg       sync.WaitGroup
	log      *slog.Logger
}

func New(s Store, c Checker, d Dispatcher, interval time.Duration, maxConcurrent int, log *slog.Logger) *Poller {
	return &Poller{store: s, checker: c, dispatch: d, interval: interval,
		sem: make(chan struct{}, maxConcurrent), log: log}
}

// Run drains due targets until ctx is cancelled, then waits for in-flight
// checks. Store errors back off from 1s to 30s; the loop never exits on them.
func (p *Poller) Run(ctx context.Context) error {
	backoff := minBackoff
	for {
		if ctx.Err() != nil {
			p.wg.Wait()
			return ctx.Err()
		}
		claimed, err := p.RunOnce(ctx)
		var wait time.Duration
		switch {
		case err != nil:
			p.log.Error("poll iteration failed", "err", err, "retry_in", backoff)
			wait, backoff = backoff, min(backoff*2, maxBackoff)
		case claimed > 0:
			backoff = minBackoff
			continue
		case len(p.sem) == cap(p.sem):
			wait, backoff = fullSleep, minBackoff
		default:
			wait, backoff = idleSleep, minBackoff
		}
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
}

// RunOnce claims as many due targets as there are free semaphore slots and
// starts a check goroutine for each. It returns the number claimed.
func (p *Poller) RunOnce(ctx context.Context) (int, error) {
	free := cap(p.sem) - len(p.sem)
	if free == 0 {
		return 0, nil
	}
	targets, err := p.store.ClaimDue(ctx, free, p.interval)
	if err != nil {
		return 0, err
	}
	for _, t := range targets {
		p.sem <- struct{}{}
		p.wg.Add(1)
		go func(t store.Target) {
			defer p.wg.Done()
			defer func() { <-p.sem }()
			hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), inFlightBudget)
			defer cancel()
			p.handle(hctx, t)
		}(t)
	}
	if len(targets) > 0 {
		backlog, _ := p.store.CountDue(ctx)
		p.log.Info("claimed", "count", len(targets), "backlog", backlog, "in_flight", len(p.sem))
	}
	return len(targets), nil
}

// Wait blocks until every in-flight check has finished. Used by tests and by
// Run during shutdown.
func (p *Poller) Wait() { p.wg.Wait() }

func (p *Poller) handle(ctx context.Context, t store.Target) {
	defer func() {
		if r := recover(); r != nil {
			p.log.Error("check panicked", "target", t.ID, "url", t.URL, "panic", r)
		}
	}()
	res := p.checker.Check(ctx, t.URL)
	tr, err := p.store.RecordCheck(ctx, t.ID, res)
	if errors.Is(err, store.ErrNotFound) {
		return
	}
	if err != nil {
		p.log.Error("record check failed", "target", t.ID, "err", err)
		return
	}
	p.log.Info("checked", "target", t.ID, "url", t.URL, "ok", res.OK, "status", tr.To, "failures", tr.ConsecutiveFailures)
	if tr.From != "DOWN" && tr.To == "DOWN" {
		a := alert.Alert{TargetID: t.ID, URL: t.URL, WebhookURL: t.WebhookURL,
			ConsecutiveFailures: tr.ConsecutiveFailures, StatusCode: res.StatusCode, Err: res.Err,
			OccurredAt: time.Now()}
		if err := p.dispatch.Dispatch(ctx, a); err != nil {
			p.log.Error("alert dispatch failed", "target", t.ID, "err", err)
		}
	}
}
