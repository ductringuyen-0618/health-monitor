// Package config reads process configuration from the environment.
package config

import (
	"fmt"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL         string
	Mode                string
	Port                string
	PollInterval        time.Duration
	CheckTimeout        time.Duration
	MaxConcurrentChecks int
	DefaultWebhookURL   string
}

const (
	minPollInterval = 10 * time.Second
	maxPollInterval = 30 * time.Second
)

// Load builds a Config from getenv (normally os.Getenv). Missing values take
// defaults; POLL_INTERVAL is clamped to [10s, 30s].
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		DatabaseURL:         getenv("DATABASE_URL"),
		Mode:                orDefault(getenv("MODE"), "api"),
		Port:                orDefault(getenv("PORT"), "8080"),
		PollInterval:        15 * time.Second,
		CheckTimeout:        5 * time.Second,
		MaxConcurrentChecks: 20,
		DefaultWebhookURL:   getenv("DEFAULT_WEBHOOK_URL"),
	}
	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.Mode != "api" && cfg.Mode != "worker" {
		return cfg, fmt.Errorf("MODE must be api or worker, got %q", cfg.Mode)
	}
	var err error
	if cfg.PollInterval, err = duration(getenv("POLL_INTERVAL"), cfg.PollInterval); err != nil {
		return cfg, fmt.Errorf("POLL_INTERVAL: %w", err)
	}
	cfg.PollInterval = min(max(cfg.PollInterval, minPollInterval), maxPollInterval)
	if cfg.CheckTimeout, err = duration(getenv("CHECK_TIMEOUT"), cfg.CheckTimeout); err != nil {
		return cfg, fmt.Errorf("CHECK_TIMEOUT: %w", err)
	}
	if v := getenv("MAX_CONCURRENT_CHECKS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return cfg, fmt.Errorf("MAX_CONCURRENT_CHECKS must be a positive integer, got %q", v)
		}
		cfg.MaxConcurrentChecks = n
	}
	return cfg, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func duration(v string, def time.Duration) (time.Duration, error) {
	if v == "" {
		return def, nil
	}
	return time.ParseDuration(v)
}
