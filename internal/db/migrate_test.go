package db_test

import (
	"context"
	"strings"
	"testing"

	"github.com/snowstream/snowstream/internal/db"
)

func TestLoadMigrations(t *testing.T) {
	migrations, err := db.LoadMigrations()
	if err != nil {
		t.Fatalf("unexpected error loading migrations: %v", err)
	}

	if len(migrations) != 2 {
		t.Fatalf("expected 2 migrations, found %d", len(migrations))
	}

	// Verify Migration 1
	m1 := migrations[0]
	if m1.Version != 1 {
		t.Errorf("expected migration 1 version to be 1, got %d", m1.Version)
	}
	if m1.Name != "create_streams_and_events" {
		t.Errorf("expected name 'create_streams_and_events', got '%s'", m1.Name)
	}
	if !strings.Contains(m1.UpSQL, "CREATE TABLE IF NOT EXISTS streams") {
		t.Errorf("expected m1.UpSQL to contain streams table creation, got: %s", m1.UpSQL)
	}
	if !strings.Contains(m1.UpSQL, "CREATE TABLE IF NOT EXISTS events") {
		t.Errorf("expected m1.UpSQL to contain events table creation, got: %s", m1.UpSQL)
	}
	if !strings.Contains(m1.UpSQL, "CREATE TABLE IF NOT EXISTS materialized_views") {
		t.Errorf("expected m1.UpSQL to contain materialized_views table creation, got: %s", m1.UpSQL)
	}
	if !strings.Contains(m1.DownSQL, "DROP TABLE IF EXISTS streams") {
		t.Errorf("expected m1.DownSQL to drop streams table, got: %s", m1.DownSQL)
	}

	// Verify Migration 2
	m2 := migrations[1]
	if m2.Version != 2 {
		t.Errorf("expected migration 2 version to be 2, got %d", m2.Version)
	}
	if m2.Name != "create_refresh_jobs" {
		t.Errorf("expected name 'create_refresh_jobs', got '%s'", m2.Name)
	}
	if !strings.Contains(m2.UpSQL, "CREATE TABLE IF NOT EXISTS refresh_jobs") {
		t.Errorf("expected m2.UpSQL to contain refresh_jobs table creation, got: %s", m2.UpSQL)
	}
	if !strings.Contains(m2.UpSQL, "idx_refresh_jobs_queue") {
		t.Errorf("expected m2.UpSQL to contain idx_refresh_jobs_queue index, got: %s", m2.UpSQL)
	}
	if !strings.Contains(m2.DownSQL, "DROP TABLE IF EXISTS refresh_jobs") {
		t.Errorf("expected m2.DownSQL to drop refresh_jobs table, got: %s", m2.DownSQL)
	}
}

func TestMigrate_NilPool(t *testing.T) {
	d := &db.DB{}
	ctx := context.Background()

	if err := d.Migrate(ctx); err == nil {
		t.Fatal("expected error calling Migrate on nil pool, got nil")
	}

	if _, err := d.GetAppliedMigrations(ctx); err == nil {
		t.Fatal("expected error calling GetAppliedMigrations on nil pool, got nil")
	}
}
