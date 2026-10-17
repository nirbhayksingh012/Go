package db

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"github.com/snowstream/snowstream/internal/db/migrations"
)

// MigrationLockID is a constant 64-bit integer used for postgres advisory locking during migrations.
const MigrationLockID = 742918823

// Migration represents a database migration pair (up/down).
type Migration struct {
	Version int
	Name    string
	UpSQL   string
	DownSQL string
}

// LoadMigrations reads and parses embedded SQL migration files from migrations.FS.
func LoadMigrations() ([]Migration, error) {
	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations embedded directory: %w", err)
	}

	migrationsMap := make(map[int]*Migration)

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		filename := entry.Name()
		// Format: 000001_create_streams_and_events.up.sql
		parts := strings.Split(filename, ".")
		if len(parts) < 3 {
			continue
		}

		direction := parts[len(parts)-2] // "up" or "down"
		baseName := strings.Join(parts[:len(parts)-2], ".")

		nameParts := strings.SplitN(baseName, "_", 2)
		if len(nameParts) < 2 {
			continue
		}

		version, err := strconv.Atoi(nameParts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid migration version in filename %s: %w", filename, err)
		}

		desc := nameParts[1]
		content, err := migrations.FS.ReadFile(filename)
		if err != nil {
			return nil, fmt.Errorf("failed to read migration file %s: %w", filename, err)
		}

		m, exists := migrationsMap[version]
		if !exists {
			m = &Migration{
				Version: version,
				Name:    desc,
			}
			migrationsMap[version] = m
		}

		if direction == "up" {
			m.UpSQL = string(content)
		} else if direction == "down" {
			m.DownSQL = string(content)
		}
	}

	result := make([]Migration, 0, len(migrationsMap))
	for _, m := range migrationsMap {
		result = append(result, *m)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Version < result[j].Version
	})

	return result, nil
}

// Migrate executes all unapplied database migrations sequentially within transactions.
// It acquires a PostgreSQL advisory lock to ensure cluster-safe execution.
func (d *DB) Migrate(ctx context.Context) error {
	if d.pool == nil {
		return fmt.Errorf("database pool is not initialized")
	}

	allMigrations, err := LoadMigrations()
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
	}
	if len(allMigrations) == 0 {
		return nil
	}

	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection for migrations: %w", err)
	}
	defer conn.Release()

	// Acquire cluster-wide advisory lock
	var lockAcquired bool
	err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", MigrationLockID).Scan(&lockAcquired)
	if err != nil {
		return fmt.Errorf("failed to check advisory lock: %w", err)
	}
	if !lockAcquired {
		return fmt.Errorf("another migration process is currently running (advisory lock %d held)", MigrationLockID)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", MigrationLockID)
	}()

	// Ensure schema_migrations table exists
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
	`
	if _, err := conn.Exec(ctx, createTableSQL); err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	// Fetch already applied migrations
	rows, err := conn.Query(ctx, "SELECT version FROM schema_migrations ORDER BY version ASC")
	if err != nil {
		return fmt.Errorf("failed to fetch applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var ver int
		if err := rows.Scan(&ver); err != nil {
			return fmt.Errorf("failed to scan migration row: %w", err)
		}
		applied[ver] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading applied migrations: %w", err)
	}

	// Apply pending migrations in order
	for _, m := range allMigrations {
		if applied[m.Version] {
			continue
		}

		if d.logger != nil {
			d.logger.Info("applying database migration",
				slog.Int("version", m.Version),
				slog.String("name", m.Name),
			)
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction for migration %d: %w", m.Version, err)
		}

		if _, err := tx.Exec(ctx, m.UpSQL); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("failed executing migration %d (%s): %w", m.Version, m.Name, err)
		}

		recordSQL := "INSERT INTO schema_migrations (version, name, applied_at) VALUES ($1, $2, NOW())"
		if _, err := tx.Exec(ctx, recordSQL, m.Version, m.Name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("failed recording migration %d in schema_migrations: %w", m.Version, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed committing migration %d: %w", m.Version, err)
		}

		if d.logger != nil {
			d.logger.Info("migration applied successfully",
				slog.Int("version", m.Version),
				slog.String("name", m.Name),
			)
		}
	}

	return nil
}

// GetAppliedMigrations returns a list of version numbers of all currently applied migrations.
func (d *DB) GetAppliedMigrations(ctx context.Context) ([]int, error) {
	if d.pool == nil {
		return nil, fmt.Errorf("database pool is not initialized")
	}

	rows, err := d.pool.Query(ctx, "SELECT version FROM schema_migrations ORDER BY version ASC")
	if err != nil {
		return nil, fmt.Errorf("failed to query schema_migrations: %w", err)
	}
	defer rows.Close()

	var versions []int
	for rows.Next() {
		var ver int
		if err := rows.Scan(&ver); err != nil {
			return nil, err
		}
		versions = append(versions, ver)
	}
	return versions, rows.Err()
}
