package db

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config holds configuration parameters for the PostgreSQL connection pool.
type Config struct {
	URL               string
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

// DefaultConfig returns production-ready defaults for the connection pool.
func DefaultConfig(databaseURL string) Config {
	return Config{
		URL:               databaseURL,
		MaxConns:          25,
		MinConns:          5,
		MaxConnLifetime:   1 * time.Hour,
		MaxConnIdleTime:   30 * time.Minute,
		HealthCheckPeriod: 1 * time.Minute,
	}
}

// DB encapsulates the pgxpool connection pool and associated logging.
type DB struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// New initializes and connects a new PostgreSQL connection pool.
func New(ctx context.Context, cfg Config, logger *slog.Logger) (*DB, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("database URL must not be empty")
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database config: %w", err)
	}

	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	}
	if cfg.HealthCheckPeriod > 0 {
		poolCfg.HealthCheckPeriod = cfg.HealthCheckPeriod
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	return &DB{
		pool:   pool,
		logger: logger,
	}, nil
}

// Ping checks if the database is reachable within a default 2-second timeout.
// It implements api.DBChecker.
func (d *DB) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return d.PingContext(ctx)
}

// PingContext checks database connectivity using the provided context.
func (d *DB) PingContext(ctx context.Context) error {
	if d.pool == nil {
		return fmt.Errorf("database pool is not initialized")
	}
	return d.pool.Ping(ctx)
}

// Pool returns the underlying pgxpool.Pool for query execution.
func (d *DB) Pool() *pgxpool.Pool {
	return d.pool
}

// Close gracefully closes all connections in the pool.
func (d *DB) Close() {
	if d.pool != nil {
		if d.logger != nil {
			d.logger.Info("closing database connection pool")
		}
		d.pool.Close()
	}
}

// Stat returns connection pool statistics.
func (d *DB) Stat() *pgxpool.Stat {
	if d.pool == nil {
		return nil
	}
	return d.pool.Stat()
}
