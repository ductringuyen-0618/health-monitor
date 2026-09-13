// Command monitor runs either the REST API (MODE=api) or the polling worker
// (MODE=worker) against the same Postgres database.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ductringuyen-0618/health-monitor/internal/api"
	"github.com/ductringuyen-0618/health-monitor/internal/config"
	"github.com/ductringuyen-0618/health-monitor/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := migrateWithRetry(ctx, st, log); err != nil {
		return err
	}

	log.Info("starting", "mode", cfg.Mode)
	switch cfg.Mode {
	case "api":
		return runAPI(ctx, cfg, st, log)
	case "worker":
		return runWorker(ctx, cfg, st, log)
	}
	return fmt.Errorf("unknown mode %q", cfg.Mode)
}

// migrateWithRetry gives a freshly provisioned database a minute to come up
// before the process gives up and lets the platform restart it.
func migrateWithRetry(ctx context.Context, st *store.Store, log *slog.Logger) error {
	var err error
	for attempt := 1; attempt <= 12; attempt++ {
		if err = st.Migrate(ctx); err == nil {
			return nil
		}
		log.Warn("migrate failed, retrying", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
	return fmt.Errorf("migrate: %w", err)
}

func runAPI(ctx context.Context, cfg config.Config, st *store.Store, log *slog.Logger) error {
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           api.NewRouter(st, log),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Info("api listening", "port", cfg.Port)
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	log.Info("api stopped")
	return nil
}

func runWorker(ctx context.Context, cfg config.Config, st *store.Store, log *slog.Logger) error {
	return fmt.Errorf("worker mode not implemented yet")
}
