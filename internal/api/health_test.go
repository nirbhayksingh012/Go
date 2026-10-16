package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type mockDBChecker struct {
	err error
}

func (m *mockDBChecker) Ping() error {
	return m.err
}

func TestHealthHandler(t *testing.T) {
	t.Run("healthy without db checker", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rr := httptest.NewRecorder()

		handler := HealthHandler(nil)
		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rr.Code)
		}

		var resp HealthResponse
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if resp.Status != "UP" {
			t.Errorf("expected status UP, got %s", resp.Status)
		}
		if resp.Service != "SnowStream Mini Data Warehouse" {
			t.Errorf("unexpected service name: %s", resp.Service)
		}
	})

	t.Run("healthy with connected db", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rr := httptest.NewRecorder()

		handler := HealthHandler(&mockDBChecker{err: nil})
		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rr.Code)
		}

		var resp HealthResponse
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if resp.Status != "UP" || resp.Database != "CONNECTED" {
			t.Errorf("expected UP and CONNECTED, got %s and %s", resp.Status, resp.Database)
		}
	})

	t.Run("degraded with failing db", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rr := httptest.NewRecorder()

		handler := HealthHandler(&mockDBChecker{err: errors.New("connection refused")})
		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusServiceUnavailable {
			t.Errorf("expected status 503, got %d", rr.Code)
		}

		var resp HealthResponse
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if resp.Status != "DEGRADED" || resp.Database != "DOWN" {
			t.Errorf("expected DEGRADED and DOWN, got %s and %s", resp.Status, resp.Database)
		}
	})
}
