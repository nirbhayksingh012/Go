package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/snowstream/snowstream/internal/api"
	"github.com/snowstream/snowstream/internal/db"
	"github.com/snowstream/snowstream/internal/worker"
)

// Ensure DB implements api.DBChecker, api.StreamStore, and worker.JobQueue
var (
	_ api.DBChecker   = (*db.DB)(nil)
	_ api.StreamStore = (*db.DB)(nil)
	_ worker.JobQueue = (*db.DB)(nil)
)

func TestDefaultConfig(t *testing.T) {
	url := "postgres://postgres:postgres@localhost:5432/snowstream_dev?sslmode=disable"
	cfg := db.DefaultConfig(url)

	if cfg.URL != url {
		t.Errorf("expected URL %s, got %s", url, cfg.URL)
	}
	if cfg.MaxConns != 25 {
		t.Errorf("expected MaxConns 25, got %d", cfg.MaxConns)
	}
	if cfg.MinConns != 5 {
		t.Errorf("expected MinConns 5, got %d", cfg.MinConns)
	}
	if cfg.MaxConnLifetime != 1*time.Hour {
		t.Errorf("expected MaxConnLifetime 1h, got %v", cfg.MaxConnLifetime)
	}
	if cfg.MaxConnIdleTime != 30*time.Minute {
		t.Errorf("expected MaxConnIdleTime 30m, got %v", cfg.MaxConnIdleTime)
	}
	if cfg.HealthCheckPeriod != 1*time.Minute {
		t.Errorf("expected HealthCheckPeriod 1m, got %v", cfg.HealthCheckPeriod)
	}
}

func TestNew_EmptyURL(t *testing.T) {
	ctx := context.Background()
	_, err := db.New(ctx, db.Config{}, nil)
	if err == nil {
		t.Fatal("expected error when URL is empty, got nil")
	}
}

func TestNew_InvalidURL(t *testing.T) {
	ctx := context.Background()
	_, err := db.New(ctx, db.Config{URL: "::not-a-valid-url::"}, nil)
	if err == nil {
		t.Fatal("expected error when URL is invalid, got nil")
	}
}

func TestDB_NilPool(t *testing.T) {
	d := &db.DB{}
	if err := d.Ping(); err == nil {
		t.Fatal("expected error pinging nil pool, got nil")
	}
	if d.Pool() != nil {
		t.Fatal("expected Pool() to return nil")
	}
	if d.Stat() != nil {
		t.Fatal("expected Stat() to return nil")
	}
	// Close should be safe on nil pool
	d.Close()
}
