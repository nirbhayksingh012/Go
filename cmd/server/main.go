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

	"github.com/snowstream/snowstream/internal/api"
	"github.com/snowstream/snowstream/internal/config"
	"github.com/snowstream/snowstream/internal/db"
	"github.com/snowstream/snowstream/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize structured logging
	var logHandler slog.Handler
	logOpts := &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}
	if cfg.Environment == "production" {
		logHandler = slog.NewJSONHandler(os.Stdout, logOpts)
	} else {
		logHandler = slog.NewTextHandler(os.Stdout, logOpts)
	}
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	logger.Info("starting SnowStream server",
		slog.String("environment", cfg.Environment),
		slog.String("port", cfg.ServerPort),
		slog.Int("worker_count", cfg.WorkerCount),
	)

	// Initialize PostgreSQL connection pool
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	dbPool, err := db.New(dbCtx, db.DefaultConfig(cfg.DatabaseURL), logger)
	if err != nil {
		logger.Error("failed to initialize database pool", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer dbPool.Close()

	// Initial connectivity probe (non-blocking for development if DB is not yet booted)
	if err := dbPool.PingContext(dbCtx); err != nil {
		logger.Warn("initial database ping failed (health checks will report DEGRADED until DB is online)",
			slog.String("error", err.Error()),
			slog.String("database_url", cfg.DatabaseURL),
		)
	} else {
		logger.Info("database connection established successfully")

		// Apply database schema migrations
		if err := dbPool.Migrate(dbCtx); err != nil {
			logger.Error("failed to apply database migrations", slog.String("error", err.Error()))
			os.Exit(1)
		}
		logger.Info("database schema migrations applied successfully")
	}

	// Initialize background worker pool
	workerPool := worker.NewPool(worker.Config{
		WorkerCount:   cfg.WorkerCount,
		PollInterval:  cfg.PollInterval,
		LeaseDuration: cfg.LeaseDuration,
	}, dbPool, logger)

	workerPool.RegisterHandler(worker.JobTypeMaterializedViewRefresh, worker.DefaultMaterializedViewRefreshHandler(logger))
	workerPool.RegisterHandler(worker.JobTypeRetentionCleanup, worker.DefaultRetentionCleanupHandler(logger))

	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()
	workerPool.Start(workerCtx)

	router := api.NewRouter(api.RouterDeps{
		Logger:    logger,
		DBChecker: dbPool,
		Store:     dbPool,
	})

	server := &http.Server{
		Addr:         ":" + cfg.ServerPort,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in background goroutine
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("HTTP server listening", slog.String("addr", server.Addr))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
		}
	}()

	// Listen for shutdown signals
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		logger.Error("server failed to start", slog.String("error", err.Error()))
		workerPool.Stop()
		os.Exit(1)
	case sig := <-shutdownSignal:
		logger.Info("shutdown signal received", slog.String("signal", sig.String()))

		// Gracefully drain background workers
		workerPool.Stop()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			logger.Error("graceful server shutdown failed", slog.String("error", err.Error()))
			_ = server.Close()
		}
		logger.Info("server shutdown complete")
	}
}

func parseLogLevel(lvl string) slog.Level {
	switch lvl {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
