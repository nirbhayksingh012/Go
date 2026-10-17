package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/snowstream/snowstream/internal/db"
)

var validStreamNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

// StreamStore defines the persistence interface required by the stream & ingestion API.
type StreamStore interface {
	CreateStream(ctx context.Context, stream *db.Stream) error
	GetStreamByName(ctx context.Context, name string) (*db.Stream, error)
	ListStreams(ctx context.Context) ([]db.Stream, error)
	IngestEvents(ctx context.Context, stream *db.Stream, inputs []db.IngestEventInput) ([]db.Event, error)
	QueryEvents(ctx context.Context, streamName string, afterID int64, limit int) ([]db.Event, error)
}

// CreateStreamRequest defines the payload for creating a new stream.
type CreateStreamRequest struct {
	Name             string          `json:"name"`
	Description      string          `json:"description,omitempty"`
	RetentionHours   int             `json:"retention_hours,omitempty"`
	SchemaDefinition json.RawMessage `json:"schema_definition,omitempty"`
}

// CreateStreamHandler registers a new stream in SnowStream.
func CreateStreamHandler(store StreamStore, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateStreamRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
			return
		}

		if !validStreamNameRegex.MatchString(req.Name) {
			respondError(w, http.StatusBadRequest, "stream name must be 1-128 alphanumeric characters, underscores, or hyphens")
			return
		}

		if req.RetentionHours <= 0 {
			req.RetentionHours = 168 // 7 days default
		}
		if len(req.SchemaDefinition) == 0 {
			req.SchemaDefinition = json.RawMessage("{}")
		}

		stream := &db.Stream{
			Name:             req.Name,
			Description:      req.Description,
			RetentionHours:   req.RetentionHours,
			SchemaDefinition: req.SchemaDefinition,
		}

		if err := store.CreateStream(r.Context(), stream); err != nil {
			if errors.Is(err, db.ErrStreamAlreadyExists) {
				respondError(w, http.StatusConflict, fmt.Sprintf("stream '%s' already exists", req.Name))
				return
			}
			if errors.Is(err, db.ErrInvalidInput) {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			logger.Error("failed to create stream", slog.String("stream", req.Name), slog.String("error", err.Error()))
			respondError(w, http.StatusInternalServerError, "failed to create stream")
			return
		}

		respondJSON(w, http.StatusCreated, stream)
	}
}

// ListStreamsHandler returns all active streams.
func ListStreamsHandler(store StreamStore, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		streams, err := store.ListStreams(r.Context())
		if err != nil {
			logger.Error("failed to list streams", slog.String("error", err.Error()))
			respondError(w, http.StatusInternalServerError, "failed to list streams")
			return
		}

		UpdateStreamsCount(len(streams))
		respondJSON(w, http.StatusOK, map[string]interface{}{
			"count":   len(streams),
			"streams": streams,
		})
	}
}

// GetStreamHandler returns metadata for a single stream by name.
func GetStreamHandler(store StreamStore, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := chi.URLParam(r, "name")
		if name == "" {
			respondError(w, http.StatusBadRequest, "stream name parameter is required")
			return
		}

		stream, err := store.GetStreamByName(r.Context(), name)
		if err != nil {
			if errors.Is(err, db.ErrStreamNotFound) {
				respondError(w, http.StatusNotFound, fmt.Sprintf("stream '%s' not found", name))
				return
			}
			logger.Error("failed to fetch stream", slog.String("stream", name), slog.String("error", err.Error()))
			respondError(w, http.StatusInternalServerError, "failed to fetch stream")
			return
		}

		respondJSON(w, http.StatusOK, stream)
	}
}

// IngestBatchRequest represents an envelope containing a list of events.
type IngestBatchRequest struct {
	Events []db.IngestEventInput `json:"events"`
}

// IngestEventsHandler handles high-throughput ingestion of events into a stream.
// Supports single event payload or batched event payload.
func IngestEventsHandler(store StreamStore, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		streamName := chi.URLParam(r, "name")
		if streamName == "" {
			respondError(w, http.StatusBadRequest, "stream name parameter is required")
			return
		}

		stream, err := store.GetStreamByName(r.Context(), streamName)
		if err != nil {
			if errors.Is(err, db.ErrStreamNotFound) {
				respondError(w, http.StatusNotFound, fmt.Sprintf("stream '%s' not found", streamName))
				return
			}
			logger.Error("failed to lookup stream for ingestion", slog.String("stream", streamName), slog.String("error", err.Error()))
			respondError(w, http.StatusInternalServerError, "failed to verify stream existence")
			return
		}

		// Read raw body to inspect format
		var raw json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			respondError(w, http.StatusBadRequest, "invalid JSON payload: "+err.Error())
			return
		}

		var inputs []db.IngestEventInput

		// 1. Try decoding as direct array of events [ {...}, {...} ]
		if err := json.Unmarshal(raw, &inputs); err == nil && len(inputs) > 0 {
			// Decoded as array
		} else {
			// 2. Try decoding as batch envelope { "events": [ ... ] }
			var batchReq IngestBatchRequest
			if err := json.Unmarshal(raw, &batchReq); err == nil && len(batchReq.Events) > 0 {
				inputs = batchReq.Events
			} else {
				// 3. Try decoding as a single event object { "payload": ... }
				var singleEvent db.IngestEventInput
				if err := json.Unmarshal(raw, &singleEvent); err == nil && len(singleEvent.Payload) > 0 {
					inputs = []db.IngestEventInput{singleEvent}
				} else {
					respondError(w, http.StatusBadRequest, "payload must contain non-empty event data or 'events' array")
					return
				}
			}
		}

		// Validate payloads
		for i, in := range inputs {
			if len(in.Payload) == 0 || string(in.Payload) == "null" {
				respondError(w, http.StatusBadRequest, fmt.Sprintf("event at index %d has empty payload", i))
				return
			}
		}

		startTime := time.Now()
		inserted, err := store.IngestEvents(r.Context(), stream, inputs)
		duration := time.Since(startTime)

		if err != nil {
			RecordIngestMetrics(streamName, "error", len(inputs), duration)
			logger.Error("event ingestion failed", slog.String("stream", streamName), slog.String("error", err.Error()))
			respondError(w, http.StatusInternalServerError, "failed to ingest events: "+err.Error())
			return
		}

		RecordIngestMetrics(streamName, "success", len(inserted), duration)

		respondJSON(w, http.StatusCreated, map[string]interface{}{
			"status":         "success",
			"stream":         streamName,
			"received_count": len(inputs),
			"ingested_count": len(inserted),
			"events":         inserted,
		})
	}
}

// QueryEventsHandler fetches events sequentially from a stream for consumers or analytics.
func QueryEventsHandler(store StreamStore, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		streamName := chi.URLParam(r, "name")
		if streamName == "" {
			respondError(w, http.StatusBadRequest, "stream name parameter is required")
			return
		}

		// Verify stream exists
		if _, err := store.GetStreamByName(r.Context(), streamName); err != nil {
			if errors.Is(err, db.ErrStreamNotFound) {
				respondError(w, http.StatusNotFound, fmt.Sprintf("stream '%s' not found", streamName))
				return
			}
			respondError(w, http.StatusInternalServerError, "failed to check stream")
			return
		}

		afterID := int64(0)
		if val := r.URL.Query().Get("after_id"); val != "" {
			if parsed, err := strconv.ParseInt(val, 10, 64); err == nil && parsed >= 0 {
				afterID = parsed
			}
		}

		limit := 100
		if val := r.URL.Query().Get("limit"); val != "" {
			if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
				limit = parsed
			}
		}

		events, err := store.QueryEvents(r.Context(), streamName, afterID, limit)
		if err != nil {
			logger.Error("failed to query stream events", slog.String("stream", streamName), slog.String("error", err.Error()))
			respondError(w, http.StatusInternalServerError, "failed to query events")
			return
		}

		nextCursor := afterID
		if len(events) > 0 {
			nextCursor = events[len(events)-1].ID
		}

		respondJSON(w, http.StatusOK, map[string]interface{}{
			"stream":      streamName,
			"count":       len(events),
			"events":      events,
			"next_cursor": nextCursor,
		})
	}
}

// JSON response helpers

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{
		"error": message,
	})
}
