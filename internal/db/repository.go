package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Standard domain repository errors
var (
	ErrStreamNotFound      = errors.New("stream not found")
	ErrStreamAlreadyExists  = errors.New("stream already exists")
	ErrInvalidInput        = errors.New("invalid input data")
)

// IngestEventInput represents the input data for ingesting an event into a stream.
type IngestEventInput struct {
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	Payload        json.RawMessage `json:"payload"`
	Headers        json.RawMessage `json:"headers,omitempty"`
}

// CreateStream inserts a new stream into the database.
func (d *DB) CreateStream(ctx context.Context, stream *Stream) error {
	if d.pool == nil {
		return fmt.Errorf("database pool is not initialized")
	}

	if stream.Name == "" {
		return fmt.Errorf("%w: stream name must not be empty", ErrInvalidInput)
	}

	if stream.RetentionHours <= 0 {
		stream.RetentionHours = 168 // Default to 7 days
	}

	if len(stream.SchemaDefinition) == 0 {
		stream.SchemaDefinition = json.RawMessage("{}")
	}

	query := `
		INSERT INTO streams (name, description, retention_hours, schema_definition, created_at, updated_at)
		VALUES ($1, $2, $3, $4, NOW(), NOW())
		RETURNING id, created_at, updated_at;
	`

	err := d.pool.QueryRow(ctx, query,
		stream.Name,
		stream.Description,
		stream.RetentionHours,
		stream.SchemaDefinition,
	).Scan(&stream.ID, &stream.CreatedAt, &stream.UpdatedAt)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
			return fmt.Errorf("%w: stream '%s' already exists", ErrStreamAlreadyExists, stream.Name)
		}
		return fmt.Errorf("failed to create stream: %w", err)
	}

	return nil
}

// GetStreamByName fetches a single stream by its unique name.
func (d *DB) GetStreamByName(ctx context.Context, name string) (*Stream, error) {
	if d.pool == nil {
		return nil, fmt.Errorf("database pool is not initialized")
	}

	query := `
		SELECT id, name, description, retention_hours, schema_definition, created_at, updated_at
		FROM streams
		WHERE name = $1;
	`

	s := &Stream{}
	err := d.pool.QueryRow(ctx, query, name).Scan(
		&s.ID,
		&s.Name,
		&s.Description,
		&s.RetentionHours,
		&s.SchemaDefinition,
		&s.CreatedAt,
		&s.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrStreamNotFound
		}
		return nil, fmt.Errorf("failed to query stream '%s': %w", name, err)
	}

	return s, nil
}

// ListStreams returns all registered streams ordered by creation date descending.
func (d *DB) ListStreams(ctx context.Context) ([]Stream, error) {
	if d.pool == nil {
		return nil, fmt.Errorf("database pool is not initialized")
	}

	query := `
		SELECT id, name, description, retention_hours, schema_definition, created_at, updated_at
		FROM streams
		ORDER BY created_at DESC;
	`

	rows, err := d.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list streams: %w", err)
	}
	defer rows.Close()

	var streams []Stream
	for rows.Next() {
		var s Stream
		err := rows.Scan(
			&s.ID,
			&s.Name,
			&s.Description,
			&s.RetentionHours,
			&s.SchemaDefinition,
			&s.CreatedAt,
			&s.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan stream row: %w", err)
		}
		streams = append(streams, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading stream rows: %w", err)
	}

	if streams == nil {
		streams = []Stream{}
	}

	return streams, nil
}

// IngestEvents appends a batch of events to a stream within a transaction.
// Events with duplicate idempotency keys for the same stream are safely ignored.
func (d *DB) IngestEvents(ctx context.Context, stream *Stream, inputs []IngestEventInput) ([]Event, error) {
	if d.pool == nil {
		return nil, fmt.Errorf("database pool is not initialized")
	}
	if len(inputs) == 0 {
		return []Event{}, nil
	}

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin ingestion transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var insertedEvents []Event

	for _, in := range inputs {
		if len(in.Payload) == 0 {
			in.Payload = json.RawMessage("{}")
		}
		if len(in.Headers) == 0 {
			in.Headers = json.RawMessage("{}")
		}

		query := `
			INSERT INTO events (stream_id, stream_name, idempotency_key, payload, headers, ingested_at)
			VALUES ($1, $2, $3, $4, $5, NOW())
			ON CONFLICT (stream_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
			RETURNING id, stream_id, stream_name, idempotency_key, payload, headers, ingested_at;
		`

		var evt Event
		err := tx.QueryRow(ctx, query,
			stream.ID,
			stream.Name,
			in.IdempotencyKey,
			in.Payload,
			in.Headers,
		).Scan(
			&evt.ID,
			&evt.StreamID,
			&evt.StreamName,
			&evt.IdempotencyKey,
			&evt.Payload,
			&evt.Headers,
			&evt.IngestedAt,
		)

		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Duplicate idempotency key was encountered and ignored via ON CONFLICT DO NOTHING
				continue
			}
			return nil, fmt.Errorf("failed to insert event: %w", err)
		}

		insertedEvents = append(insertedEvents, evt)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit ingestion batch: %w", err)
	}

	if insertedEvents == nil {
		insertedEvents = []Event{}
	}

	return insertedEvents, nil
}

// QueryEvents returns events for a stream starting strictly after a given sequence ID.
func (d *DB) QueryEvents(ctx context.Context, streamName string, afterID int64, limit int) ([]Event, error) {
	if d.pool == nil {
		return nil, fmt.Errorf("database pool is not initialized")
	}

	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	query := `
		SELECT id, stream_id, stream_name, idempotency_key, payload, headers, ingested_at
		FROM events
		WHERE stream_name = $1 AND id > $2
		ORDER BY id ASC
		LIMIT $3;
	`

	rows, err := d.pool.Query(ctx, query, streamName, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var evt Event
		err := rows.Scan(
			&evt.ID,
			&evt.StreamID,
			&evt.StreamName,
			&evt.IdempotencyKey,
			&evt.Payload,
			&evt.Headers,
			&evt.IngestedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan event: %w", err)
		}
		events = append(events, evt)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading event rows: %w", err)
	}

	if events == nil {
		events = []Event{}
	}

	return events, nil
}
