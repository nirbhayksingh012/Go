package config

import (
	"os"
	"testing"
	"time"
)

func TestConfigLoadDefaults(t *testing.T) {
	// Clear any overrides for test isolation
	os.Unsetenv("PORT")
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("WORKER_COUNT")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected valid default config, got error: %v", err)
	}

	if cfg.ServerPort != "8080" {
		t.Errorf("expected default port 8080, got %s", cfg.ServerPort)
	}
	if cfg.WorkerCount != 4 {
		t.Errorf("expected default worker count 4, got %d", cfg.WorkerCount)
	}
	if cfg.PollInterval != 500*time.Millisecond {
		t.Errorf("expected default poll interval 500ms, got %v", cfg.PollInterval)
	}
	if cfg.LeaseDuration != 30*time.Second {
		t.Errorf("expected default lease duration 30s, got %v", cfg.LeaseDuration)
	}
}

func TestConfigValidationErrors(t *testing.T) {
	t.Run("invalid port", func(t *testing.T) {
		t.Setenv("PORT", "99999")
		_, err := Load()
		if err == nil {
			t.Fatal("expected error for port out of range, got nil")
		}
	})

	t.Run("non-numeric port", func(t *testing.T) {
		t.Setenv("PORT", "not-a-port")
		_, err := Load()
		if err == nil {
			t.Fatal("expected error for non-numeric port, got nil")
		}
	})

	t.Run("invalid worker count", func(t *testing.T) {
		t.Setenv("PORT", "8080")
		t.Setenv("WORKER_COUNT", "0")
		_, err := Load()
		if err == nil {
			t.Fatal("expected error for worker count < 1, got nil")
		}
	})
}
