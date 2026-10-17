package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/snowstream/snowstream/internal/db"
)

// JobQueue defines the interface for distributed job acquisition and state transitions.
type JobQueue interface {
	AcquireJob(ctx context.Context, workerID string, leaseDuration time.Duration) (*db.RefreshJob, error)
	CompleteJob(ctx context.Context, jobID uuid.UUID) error
	FailJob(ctx context.Context, jobID uuid.UUID, errMsg string) error
}

// Config configures the background worker pool execution.
type Config struct {
	WorkerCount  int           // Number of concurrent worker goroutines
	PollInterval time.Duration // Time to sleep when no jobs are pending
	LeaseDuration time.Duration // Duration to lock a job for processing
}

// Pool coordinates concurrent background workers processing jobs from the queue.
type Pool struct {
	cfg      Config
	queue    JobQueue
	logger   *slog.Logger
	handlers map[string]JobHandler
	mu       sync.RWMutex
	wg       sync.WaitGroup
	cancel   context.CancelFunc
}

// NewPool initializes a new worker pool.
func NewPool(cfg Config, queue JobQueue, logger *slog.Logger) *Pool {
	if cfg.WorkerCount < 1 {
		cfg.WorkerCount = 4
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}

	return &Pool{
		cfg:      cfg,
		queue:    queue,
		logger:   logger,
		handlers: make(map[string]JobHandler),
	}
}

// RegisterHandler binds an execution function to a specific job type.
func (p *Pool) RegisterHandler(jobType string, handler JobHandler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers[jobType] = handler
}

// Start launches the background worker goroutines.
func (p *Pool) Start(ctx context.Context) {
	workerCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	p.logger.Info("starting background worker pool",
		slog.Int("worker_count", p.cfg.WorkerCount),
		slog.Duration("poll_interval", p.cfg.PollInterval),
		slog.Duration("lease_duration", p.cfg.LeaseDuration),
	)

	SetActiveWorkers(p.cfg.WorkerCount)

	for i := 1; i <= p.cfg.WorkerCount; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		p.wg.Add(1)
		go p.runWorker(workerCtx, workerID)
	}
}

// Stop signals all workers to finish their current job and wait for graceful termination.
func (p *Pool) Stop() {
	if p.cancel != nil {
		p.logger.Info("stopping background worker pool gracefully")
		p.cancel()
		p.wg.Wait()
		SetActiveWorkers(0)
		p.logger.Info("all background workers stopped")
	}
}

func (p *Pool) runWorker(ctx context.Context, workerID string) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		job, err := p.queue.AcquireJob(ctx, workerID, p.cfg.LeaseDuration)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.logger.Error("worker failed to acquire job from queue",
				slog.String("worker_id", workerID),
				slog.String("error", err.Error()),
			)
			p.sleepOrDone(ctx, p.cfg.PollInterval)
			continue
		}

		if job == nil {
			// Queue is empty, pause before next poll
			p.sleepOrDone(ctx, p.cfg.PollInterval)
			continue
		}

		// Execute the acquired job
		p.executeJob(ctx, workerID, job)
	}
}

func (p *Pool) executeJob(ctx context.Context, workerID string, job *db.RefreshJob) {
	p.logger.Info("worker executing job",
		slog.String("worker_id", workerID),
		slog.String("job_id", job.ID.String()),
		slog.String("job_type", job.JobType),
		slog.Int("attempt", job.Attempts),
	)

	p.mu.RLock()
	handler, exists := p.handlers[job.JobType]
	p.mu.RUnlock()

	startTime := time.Now()
	var execErr error

	if !exists {
		p.logger.Warn("no handler registered for job type; marking completed",
			slog.String("job_type", job.JobType),
			slog.String("job_id", job.ID.String()),
		)
	} else {
		execErr = handler(ctx, job)
	}

	duration := time.Since(startTime)

	// Use separate context with timeout for status updates to ensure completion commits even during shutdown
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cleanupCancel()

	if execErr != nil {
		RecordJobMetrics(job.JobType, "failed", duration)
		p.logger.Error("job execution failed",
			slog.String("job_id", job.ID.String()),
			slog.String("job_type", job.JobType),
			slog.String("error", execErr.Error()),
			slog.Duration("duration", duration),
		)
		if err := p.queue.FailJob(cleanupCtx, job.ID, execErr.Error()); err != nil {
			p.logger.Error("failed to mark job as failed in queue",
				slog.String("job_id", job.ID.String()),
				slog.String("error", err.Error()),
			)
		}
	} else {
		RecordJobMetrics(job.JobType, "completed", duration)
		p.logger.Info("job execution completed successfully",
			slog.String("job_id", job.ID.String()),
			slog.String("job_type", job.JobType),
			slog.Duration("duration", duration),
		)
		if err := p.queue.CompleteJob(cleanupCtx, job.ID); err != nil {
			p.logger.Error("failed to mark job as completed in queue",
				slog.String("job_id", job.ID.String()),
				slog.String("error", err.Error()),
			)
		}
	}
}

func (p *Pool) sleepOrDone(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
