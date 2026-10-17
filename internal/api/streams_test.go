package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/snowstream/snowstream/internal/api"
	"github.com/snowstream/snowstream/internal/db"
)

// mockStreamStore provides an in-memory implementation of api.StreamStore for testing.
type mockStreamStore struct {
	streams map[string]*db.Stream
	events  map[string][]db.Event
	nextID  int64
}

func newMockStreamStore() *mockStreamStore {
	return &mockStreamStore{
		streams: make(map[string]*db.Stream),
		events:  make(map[string][]db.Event),
		nextID:  1,
	}
}

func (m *mockStreamStore) CreateStream(_ context.Context, s *db.Stream) error {
	if _, exists := m.streams[s.Name]; exists {
		return db.ErrStreamAlreadyExists
	}
	s.ID = uuid.New()
	s.CreatedAt = time.Now().UTC()
	s.UpdatedAt = time.Now().UTC()
	m.streams[s.Name] = s
	m.events[s.Name] = []db.Event{}
	return nil
}

func (m *mockStreamStore) GetStreamByName(_ context.Context, name string) (*db.Stream, error) {
	s, exists := m.streams[name]
	if !exists {
		return nil, db.ErrStreamNotFound
	}
	return s, nil
}

func (m *mockStreamStore) ListStreams(_ context.Context) ([]db.Stream, error) {
	list := make([]db.Stream, 0, len(m.streams))
	for _, s := range m.streams {
		list = append(list, *s)
	}
	return list, nil
}

func (m *mockStreamStore) IngestEvents(_ context.Context, stream *db.Stream, inputs []db.IngestEventInput) ([]db.Event, error) {
	var inserted []db.Event
	existing := m.events[stream.Name]

	for _, in := range inputs {
		// Check idempotency key deduplication
		if in.IdempotencyKey != nil && *in.IdempotencyKey != "" {
			duplicate := false
			for _, prev := range existing {
				if prev.IdempotencyKey != nil && *prev.IdempotencyKey == *in.IdempotencyKey {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
		}

		evt := db.Event{
			ID:             m.nextID,
			StreamID:       stream.ID,
			StreamName:     stream.Name,
			IdempotencyKey: in.IdempotencyKey,
			Payload:        in.Payload,
			Headers:        in.Headers,
			IngestedAt:     time.Now().UTC(),
		}
		m.nextID++
		existing = append(existing, evt)
		inserted = append(inserted, evt)
	}

	m.events[stream.Name] = existing
	return inserted, nil
}

func (m *mockStreamStore) QueryEvents(_ context.Context, streamName string, afterID int64, limit int) ([]db.Event, error) {
	var result []db.Event
	for _, evt := range m.events[streamName] {
		if evt.ID > afterID {
			result = append(result, evt)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func setupTestServer(store api.StreamStore) http.Handler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return api.NewRouter(api.RouterDeps{
		Logger: logger,
		Store:  store,
	})
}

func TestCreateStream(t *testing.T) {
	store := newMockStreamStore()
	handler := setupTestServer(store)

	t.Run("successful stream creation", func(t *testing.T) {
		body := `{"name": "user_clicks", "description": "User clicks stream", "retention_hours": 72}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/streams", bytes.NewBufferString(body))
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusCreated {
			t.Fatalf("expected status 201, got %d: %s", rr.Code, rr.Body.String())
		}

		var resp db.Stream
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if resp.Name != "user_clicks" || resp.RetentionHours != 72 {
			t.Errorf("unexpected response content: %+v", resp)
		}
	})

	t.Run("duplicate stream conflict", func(t *testing.T) {
		body := `{"name": "user_clicks"}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/streams", bytes.NewBufferString(body))
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusConflict {
			t.Fatalf("expected status 409, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("invalid stream name", func(t *testing.T) {
		body := `{"name": "invalid name with spaces!"}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/streams", bytes.NewBufferString(body))
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400 for invalid name, got %d", rr.Code)
		}
	})
}

func TestListAndGetStreams(t *testing.T) {
	store := newMockStreamStore()
	_ = store.CreateStream(context.Background(), &db.Stream{Name: "events_a"})
	_ = store.CreateStream(context.Background(), &db.Stream{Name: "events_b"})
	handler := setupTestServer(store)

	t.Run("list all streams", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/streams", nil)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rr.Code)
		}

		var resp struct {
			Count   int         `json:"count"`
			Streams []db.Stream `json:"streams"`
		}
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode list response: %v", err)
		}

		if resp.Count != 2 || len(resp.Streams) != 2 {
			t.Errorf("expected 2 streams, got count %d, len %d", resp.Count, len(resp.Streams))
		}
	})

	t.Run("get existing stream by name", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/streams/events_a", nil)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rr.Code)
		}
	})

	t.Run("get non-existent stream", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/streams/non_existent", nil)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusNotFound {
			t.Fatalf("expected status 404, got %d", rr.Code)
		}
	})
}

func TestIngestAndQueryEvents(t *testing.T) {
	store := newMockStreamStore()
	_ = store.CreateStream(context.Background(), &db.Stream{Name: "telemetry"})
	handler := setupTestServer(store)

	t.Run("ingest single event payload", func(t *testing.T) {
		body := `{
			"idempotency_key": "k-1",
			"payload": {"temp": 21.5, "device_id": "sensor-101"}
		}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/streams/telemetry/events", bytes.NewBufferString(body))
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusCreated {
			t.Fatalf("expected status 201, got %d: %s", rr.Code, rr.Body.String())
		}

		var resp struct {
			Status        string `json:"status"`
			IngestedCount int    `json:"ingested_count"`
		}
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if resp.Status != "success" || resp.IngestedCount != 1 {
			t.Errorf("unexpected ingest response: %+v", resp)
		}
	})

	t.Run("ingest batch envelope events", func(t *testing.T) {
		body := `{
			"events": [
				{"idempotency_key": "k-2", "payload": {"temp": 22.0}},
				{"idempotency_key": "k-3", "payload": {"temp": 23.1}}
			]
		}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/streams/telemetry/events", bytes.NewBufferString(body))
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusCreated {
			t.Fatalf("expected status 201, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("query events with cursor", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/streams/telemetry/events?after_id=0&limit=10", nil)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rr.Code)
		}

		var resp struct {
			Count      int        `json:"count"`
			Events     []db.Event `json:"events"`
			NextCursor int64      `json:"next_cursor"`
		}
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode query response: %v", err)
		}

		if resp.Count != 3 {
			t.Errorf("expected 3 total events, got %d", resp.Count)
		}
		if resp.NextCursor <= 0 {
			t.Errorf("expected valid positive next_cursor, got %d", resp.NextCursor)
		}
	})

	t.Run("ingest to non-existent stream fails with 404", func(t *testing.T) {
		body := `{"payload": {"test": true}}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/streams/unknown_stream/events", bytes.NewBufferString(body))
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusNotFound {
			t.Fatalf("expected status 404, got %d", rr.Code)
		}
	})

	t.Run("ingest event with empty payload fails with 400", func(t *testing.T) {
		body := `{"payload": null}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/streams/telemetry/events", bytes.NewBufferString(body))
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", rr.Code)
		}
	})
}
