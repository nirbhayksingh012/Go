package worker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/snowstream/snowstream/internal/db"
	"github.com/snowstream/snowstream/internal/worker"
)

// mockQueue provides a thread-safe in-memory queue for testing the worker pool.
type mockQueue struct {
	mu        sync.Mutex
	jobs      map[uuid.UUID]*db.RefreshJob
	order     []uuid.UUID
	completed []uuid.UUID
	failed    []uuid.UUID
}

func newMockQueue() *mockQueue {
	return &mockQueue{
		jobs:      make(map[uuid.UUID]*db.RefreshJob),
		completed: make([]uuid.UUID, 0),
		failed:    make([]uuid.UUID, 0),
	}
}

func (q *mockQueue) enqueue(job *db.RefreshJob) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	job.Status = "pending"
	q.jobs[job.ID] = job
	q.order = append(q.order, job.ID)
}

func (q *mockQueue) AcquireJob(_ context.Context, workerID string, leaseDuration time.Duration) (*db.RefreshJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, id := range q.order {
		job := q.jobs[id]
		if job.Status == "pending" {
			job.Status = "running"
			job.LockedBy = &workerID
			job.Attempts++
			return job, nil
		}
	}
	return nil, nil
}

func (q *mockQueue) CompleteJob(_ context.Context, jobID uuid.UUID) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return errors.New("job not found")
	}
	job.Status = "completed"
	q.completed = append(q.completed, jobID)
	return nil
}

func (q *mockQueue) FailJob(_ context.Context, jobID uuid.UUID, errMsg string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return errors.New("job not found")
	}
	job.Status = "failed"
	job.LastError = errMsg
	q.failed = append(q.failed, jobID)
	return nil
}

func TestWorkerPool_ExecutionSuccess(t *testing.T) {
	queue := newMockQueue()
	jobID := uuid.New()
	queue.enqueue(&db.RefreshJob{
		ID:      jobID,
		JobType: "test_job",
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := worker.Config{
		WorkerCount:  2,
		PollInterval: 10 * time.Millisecond,
		LeaseDuration: 5 * time.Second,
	}

	pool := worker.NewPool(cfg, queue, logger)

	jobDone := make(chan struct{})
	pool.RegisterHandler("test_job", func(ctx context.Context, job *db.RefreshJob) error {
		if job.ID == jobID {
			close(jobDone)
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)
	defer pool.Stop()

	select {
	case <-jobDone:
		// Job was processed
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to process job")
	}

	// Verify job is marked as completed in queue
	time.Sleep(50 * time.Millisecond)
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.completed) != 1 || queue.completed[0] != jobID {
		t.Errorf("expected job %s to be completed, got %v", jobID, queue.completed)
	}
}

func TestWorkerPool_ExecutionFailure(t *testing.T) {
	queue := newMockQueue()
	jobID := uuid.New()
	queue.enqueue(&db.RefreshJob{
		ID:      jobID,
		JobType: "failing_job",
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := worker.Config{
		WorkerCount:  1,
		PollInterval: 10 * time.Millisecond,
		LeaseDuration: 5 * time.Second,
	}

	pool := worker.NewPool(cfg, queue, logger)

	failureHandled := make(chan struct{})
	pool.RegisterHandler("failing_job", func(ctx context.Context, job *db.RefreshJob) error {
		defer close(failureHandled)
		return errors.New("simulated processing error")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)
	defer pool.Stop()

	select {
	case <-failureHandled:
		// Handler executed and failed
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to process failing job")
	}

	// Verify job is recorded as failed in queue
	time.Sleep(50 * time.Millisecond)
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.failed) != 1 || queue.failed[0] != jobID {
		t.Errorf("expected job %s to be failed, got %v", jobID, queue.failed)
	}
}

func TestWorkerPool_GracefulStop(t *testing.T) {
	queue := newMockQueue()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := worker.Config{
		WorkerCount:  3,
		PollInterval: 10 * time.Millisecond,
		LeaseDuration: 5 * time.Second,
	}

	pool := worker.NewPool(cfg, queue, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)
	time.Sleep(20 * time.Millisecond)

	// Stop should cleanly wait for workers without hanging
	stopDone := make(chan struct{})
	go func() {
		pool.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
		// Successfully stopped
	case <-time.After(2 * time.Second):
		t.Fatal("pool.Stop() took too long to terminate")
	}
}
