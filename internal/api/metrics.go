package api

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	metricsOnce sync.Once

	eventsIngestedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "snowstream",
			Subsystem: "ingest",
			Name:      "events_total",
			Help:      "Total number of events processed by the ingestion engine",
		},
		[]string{"stream", "status"},
	)

	eventIngestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "snowstream",
			Subsystem: "ingest",
			Name:      "duration_seconds",
			Help:      "Duration of event ingestion batches in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
		},
		[]string{"stream"},
	)

	streamsCountGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "snowstream",
			Subsystem: "metadata",
			Name:      "streams_total",
			Help:      "Total number of active event streams registered",
		},
	)
)

// initMetrics registers all Prometheus metrics with the default registry once.
func initMetrics() {
	metricsOnce.Do(func() {
		prometheus.MustRegister(eventsIngestedTotal)
		prometheus.MustRegister(eventIngestDuration)
		prometheus.MustRegister(streamsCountGauge)
	})
}

// RecordIngestMetrics updates Prometheus counters and histograms for an ingestion call.
func RecordIngestMetrics(stream string, status string, count int, duration time.Duration) {
	initMetrics()
	eventsIngestedTotal.WithLabelValues(stream, status).Add(float64(count))
	eventIngestDuration.WithLabelValues(stream).Observe(duration.Seconds())
}

// UpdateStreamsCount updates the gauge metric with current stream count.
func UpdateStreamsCount(count int) {
	initMetrics()
	streamsCountGauge.Set(float64(count))
}
