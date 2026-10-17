package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RouterDeps encapsulates all dependencies required to wire the API routes.
type RouterDeps struct {
	Logger    *slog.Logger
	DBChecker DBChecker
	Store     StreamStore
}

// NewRouter sets up the Chi HTTP router with production-ready middleware and routes.
func NewRouter(deps RouterDeps) *chi.Mux {
	r := chi.NewRouter()

	// Essential middleware stack
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(slogMiddleware(deps.Logger))
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	// Health check endpoint
	r.Get("/health", HealthHandler(deps.DBChecker))

	// Prometheus Metrics endpoint
	r.Handle("/api/v1/metrics", promhttp.Handler())

	// Streams and Ingestion API endpoints
	if deps.Store != nil {
		r.Route("/api/v1", func(apiRouter chi.Router) {
			apiRouter.Post("/streams", CreateStreamHandler(deps.Store, deps.Logger))
			apiRouter.Get("/streams", ListStreamsHandler(deps.Store, deps.Logger))
			apiRouter.Get("/streams/{name}", GetStreamHandler(deps.Store, deps.Logger))
			apiRouter.Post("/streams/{name}/events", IngestEventsHandler(deps.Store, deps.Logger))
			apiRouter.Get("/streams/{name}/events", QueryEventsHandler(deps.Store, deps.Logger))
		})
	}

	return r
}

// slogMiddleware logs incoming HTTP requests using Go's structured logger (log/slog).
func slogMiddleware(logger *slog.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			defer func() {
				logger.Info("http request completed",
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.Int("status", ww.Status()),
					slog.Int("bytes", ww.BytesWritten()),
					slog.Duration("duration", time.Since(start)),
					slog.String("request_id", middleware.GetReqID(r.Context())),
				)
			}()

			next.ServeHTTP(ww, r)
		})
	}
}
