package api

import (
	"encoding/json"
	"net/http"
	"time"
)

// HealthResponse represents the payload returned by GET /health.
type HealthResponse struct {
	Status    string    `json:"status"`
	Service   string    `json:"service"`
	Version   string    `json:"version"`
	Timestamp time.Time `json:"timestamp"`
	Database  string    `json:"database,omitempty"`
}

// DBChecker is an optional interface to probe database connectivity during health checks.
type DBChecker interface {
	Ping() error
}

// HealthHandler returns an http.HandlerFunc that performs the health check.
func HealthHandler(checker DBChecker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := "UP"
		dbStatus := "UNKNOWN"

		if checker != nil {
			if err := checker.Ping(); err != nil {
				status = "DEGRADED"
				dbStatus = "DOWN"
			} else {
				dbStatus = "CONNECTED"
			}
		}

		resp := HealthResponse{
			Status:    status,
			Service:   "SnowStream Mini Data Warehouse",
			Version:   "1.0.0",
			Timestamp: time.Now().UTC(),
		}
		if checker != nil {
			resp.Database = dbStatus
		}

		w.Header().Set("Content-Type", "application/json")
		if status == "DEGRADED" {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}

		_ = json.NewEncoder(w).Encode(resp)
	}
}
