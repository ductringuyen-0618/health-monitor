package config

import (
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{"DATABASE_URL": "postgres://x"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Mode != "api" || cfg.Port != "8080" {
		t.Errorf("mode/port defaults wrong: %+v", cfg)
	}
	if cfg.PollInterval != 15*time.Second || cfg.CheckTimeout != 5*time.Second {
		t.Errorf("duration defaults wrong: %+v", cfg)
	}
	if cfg.MaxConcurrentChecks != 20 || cfg.DefaultWebhookURL != "" {
		t.Errorf("other defaults wrong: %+v", cfg)
	}
}

func TestLoadRequiresDatabaseURL(t *testing.T) {
	if _, err := Load(env(map[string]string{})); err == nil {
		t.Fatal("expected error for missing DATABASE_URL")
	}
}

func TestLoadRejectsBadMode(t *testing.T) {
	if _, err := Load(env(map[string]string{"DATABASE_URL": "x", "MODE": "cron"})); err == nil {
		t.Fatal("expected error for bad MODE")
	}
}

func TestLoadClampsPollInterval(t *testing.T) {
	cases := map[string]time.Duration{"2s": 10 * time.Second, "20s": 20 * time.Second, "5m": 30 * time.Second}
	for in, want := range cases {
		cfg, err := Load(env(map[string]string{"DATABASE_URL": "x", "POLL_INTERVAL": in}))
		if err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if cfg.PollInterval != want {
			t.Errorf("%s: got %v want %v", in, cfg.PollInterval, want)
		}
	}
}

func TestLoadParsesOverrides(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"DATABASE_URL": "x", "MODE": "worker", "PORT": "9000",
		"CHECK_TIMEOUT": "2s", "MAX_CONCURRENT_CHECKS": "50", "DEFAULT_WEBHOOK_URL": "https://h",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != "worker" || cfg.Port != "9000" || cfg.CheckTimeout != 2*time.Second ||
		cfg.MaxConcurrentChecks != 50 || cfg.DefaultWebhookURL != "https://h" {
		t.Errorf("overrides not applied: %+v", cfg)
	}
}

func TestLoadRejectsBadNumbers(t *testing.T) {
	for _, m := range []map[string]string{
		{"DATABASE_URL": "x", "MAX_CONCURRENT_CHECKS": "0"},
		{"DATABASE_URL": "x", "MAX_CONCURRENT_CHECKS": "abc"},
		{"DATABASE_URL": "x", "CHECK_TIMEOUT": "soon"},
	} {
		if _, err := Load(env(m)); err == nil {
			t.Errorf("expected error for %v", m)
		}
	}
}
