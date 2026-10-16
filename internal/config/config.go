package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds the complete configuration settings for SnowStream.
// Every setting can be overridden via environment variables for cloud-native deployment.
type Config struct {
	Environment    string        // "development", "test", or "production"
	ServerPort     string        // HTTP server port (e.g., "8080")
	DatabaseURL    string        // PostgreSQL connection string
	WorkerCount    int           // Number of concurrent refresh workers
	JobQueueSize   int           // Max capacity of in-memory worker job buffer
	PollInterval   time.Duration // Polling interval for scheduler when idle
	LeaseDuration  time.Duration // Lease expiration for active refresh jobs
	MaxJobAttempts int           // Max retry attempts for failed jobs
	LogLevel       string        // "debug", "info", "warn", "error"
}

// Load loads configuration from environment variables, falling back to sensible defaults.
// It performs validation to ensure critical parameters are non-empty and well-formed.
func Load() (*Config, error) {
	cfg := &Config{
		Environment:    getEnv("ENVIRONMENT", "development"),
		ServerPort:     getEnv("PORT", "8080"),
		DatabaseURL:    getEnv("DATABASE_URL", "postgres://postgres@localhost:5432/snowstream_dev?sslmode=disable"),
		WorkerCount:    getEnvAsInt("WORKER_COUNT", 4),
		JobQueueSize:   getEnvAsInt("JOB_QUEUE_SIZE", 100),
		PollInterval:   getEnvAsDuration("POLL_INTERVAL", 500*time.Millisecond),
		LeaseDuration:  getEnvAsDuration("LEASE_DURATION", 30*time.Second),
		MaxJobAttempts: getEnvAsInt("MAX_JOB_ATTEMPTS", 5),
		LogLevel:       getEnv("LOG_LEVEL", "info"),
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return cfg, nil
}

// Validate ensures that configuration values meet necessary operational bounds.
func (c *Config) Validate() error {
	if c.ServerPort == "" {
		return fmt.Errorf("PORT must not be empty")
	}
	portNum, err := strconv.Atoi(c.ServerPort)
	if err != nil || portNum < 1 || portNum > 65535 {
		return fmt.Errorf("PORT must be a valid integer between 1 and 65535, got: %s", c.ServerPort)
	}

	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL must not be empty")
	}

	if c.WorkerCount < 1 {
		return fmt.Errorf("WORKER_COUNT must be at least 1, got: %d", c.WorkerCount)
	}

	if c.JobQueueSize < 1 {
		return fmt.Errorf("JOB_QUEUE_SIZE must be at least 1, got: %d", c.JobQueueSize)
	}

	if c.PollInterval <= 0 {
		return fmt.Errorf("POLL_INTERVAL must be positive, got: %v", c.PollInterval)
	}

	if c.LeaseDuration <= 0 {
		return fmt.Errorf("LEASE_DURATION must be positive, got: %v", c.LeaseDuration)
	}

	if c.MaxJobAttempts < 1 {
		return fmt.Errorf("MAX_JOB_ATTEMPTS must be at least 1, got: %d", c.MaxJobAttempts)
	}

	return nil
}

// Helper functions for reading environment variables with typed fallbacks.

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	if valStr, ok := os.LookupEnv(key); ok && valStr != "" {
		if val, err := strconv.Atoi(valStr); err == nil {
			return val
		}
	}
	return defaultVal
}

func getEnvAsDuration(key string, defaultVal time.Duration) time.Duration {
	if valStr, ok := os.LookupEnv(key); ok && valStr != "" {
		if val, err := time.ParseDuration(valStr); err == nil {
			return val
		}
	}
	return defaultVal
}
