package db

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Stream represents a named event stream / dataset table.
type Stream struct {
	ID               uuid.UUID       `json:"id"`
	Name             string          `json:"name"`
	Description      string          `json:"description"`
	RetentionHours   int             `json:"retention_hours"`
	SchemaDefinition json.RawMessage `json:"schema_definition"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

// Event represents an append-only ingested record within a stream.
type Event struct {
	ID             int64           `json:"id"`
	StreamID       uuid.UUID       `json:"stream_id"`
	StreamName     string          `json:"stream_name"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	Payload        json.RawMessage `json:"payload"`
	Headers        json.RawMessage `json:"headers,omitempty"`
	IngestedAt     time.Time       `json:"ingested_at"`
}

// RefreshJob represents a task in the distributed asynchronous worker queue.
type RefreshJob struct {
	ID          uuid.UUID       `json:"id"`
	JobType     string          `json:"job_type"`
	StreamID    *uuid.UUID      `json:"stream_id,omitempty"`
	ViewID      *uuid.UUID      `json:"view_id,omitempty"`
	Status      string          `json:"status"` // pending, running, completed, failed
	Payload     json.RawMessage `json:"payload"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	LastError   string          `json:"last_error,omitempty"`
	LockedBy    *string         `json:"locked_by,omitempty"`
	LockedUntil *time.Time      `json:"locked_until,omitempty"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// MaterializedView represents downstream aggregation metadata.
type MaterializedView struct {
	ID                     uuid.UUID  `json:"id"`
	Name                   string     `json:"name"`
	SourceStreamID         uuid.UUID  `json:"source_stream_id"`
	QueryDefinition        string     `json:"query_definition"`
	RefreshIntervalSeconds int        `json:"refresh_interval_seconds"`
	LastRefreshedAt        *time.Time `json:"last_refreshed_at,omitempty"`
	LastEventID            int64      `json:"last_event_id"`
	Status                 string     `json:"status"`
	CreatedAt              time.Time  `json:"created_at"`
	UpdatedAt              time.Time  `json:"updated_at"`
}
