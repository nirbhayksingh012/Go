package worker

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	metricsOnce sync.Once

	jobsProcessedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "snowstream",
			Subsystem: "worker",
			Name:      "jobs_total",
			Help:      "Total number of background jobs processed by the worker pool",
		},
		[]string{"job_type", "status"},
	)

	jobExecutionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "snowstream",
			Subsystem: "worker",
			Name:      "duration_seconds",
			Help:      "Duration of background job executions in seconds",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 30.0, 60.0},
		},
		[]string{"job_type"},
	)

	activeWorkersGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "snowstream",
			Subsystem: "worker",
			Name:      "active_count",
			Help:      "Current count of active running worker goroutines",
		},
	)
)

func initMetrics() {
	metricsOnce.Do(func() {
		prometheus.MustRegister(jobsProcessedTotal)
		prometheus.MustRegister(jobExecutionDuration)
		prometheus.MustRegister(activeWorkersGauge)
	})
}

// RecordJobMetrics records Prometheus metrics for a completed or failed background job.
func RecordJobMetrics(jobType string, status string, duration time.Duration) {
	initMetrics()
	jobsProcessedTotal.WithLabelValues(jobType, status).Inc()
	jobExecutionDuration.WithLabelValues(jobType).Observe(duration.Seconds())
}

// SetActiveWorkers updates the active workers count gauge.
func SetActiveWorkers(count int) {
	initMetrics()
	activeWorkersGauge.Set(float64(count))
}
