package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// EnqueueJob inserts a new refresh or maintenance job into the queue.
func (d *DB) EnqueueJob(ctx context.Context, job *RefreshJob) error {
	if d.pool == nil {
		return fmt.Errorf("database pool is not initialized")
	}

	if job.JobType == "" {
		return fmt.Errorf("%w: job_type must not be empty", ErrInvalidInput)
	}

	if job.MaxAttempts <= 0 {
		job.MaxAttempts = 5
	}

	if len(job.Payload) == 0 {
		job.Payload = json.RawMessage("{}")
	}

	if job.ScheduledAt.IsZero() {
		job.ScheduledAt = time.Now().UTC()
	}

	query := `
		INSERT INTO refresh_jobs (
			job_type, stream_id, view_id, status, payload,
			attempts, max_attempts, last_error, scheduled_at,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, 'pending', $4,
			0, $5, '', $6,
			NOW(), NOW()
		) RETURNING id, status, created_at, updated_at;
	`

	err := d.pool.QueryRow(ctx, query,
		job.JobType,
		job.StreamID,
		job.ViewID,
		job.Payload,
		job.MaxAttempts,
		job.ScheduledAt,
	).Scan(&job.ID, &job.Status, &job.CreatedAt, &job.UpdatedAt)

	if err != nil {
		return fmt.Errorf("failed to enqueue job: %w", err)
	}

	return nil
}

// AcquireJob atomically leases the next pending or expired job using SELECT FOR UPDATE SKIP LOCKED.
func (d *DB) AcquireJob(ctx context.Context, workerID string, leaseDuration time.Duration) (*RefreshJob, error) {
	if d.pool == nil {
		return nil, fmt.Errorf("database pool is not initialized")
	}

	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}

	query := `
		WITH next_job AS (
			SELECT id
			FROM refresh_jobs
			WHERE (status = 'pending' AND scheduled_at <= NOW())
			   OR (status = 'running' AND locked_until < NOW())
			ORDER BY scheduled_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE refresh_jobs
		SET status = 'running',
			locked_by = $1,
			locked_until = NOW() + $2::interval,
			attempts = attempts + 1,
			updated_at = NOW()
		WHERE id IN (SELECT id FROM next_job)
		RETURNING id, job_type, stream_id, view_id, status, payload,
		          attempts, max_attempts, last_error, locked_by, locked_until,
		          scheduled_at, created_at, updated_at;
	`

	intervalStr := fmt.Sprintf("%d seconds", int(leaseDuration.Seconds()))
	var job RefreshJob
	err := d.pool.QueryRow(ctx, query, workerID, intervalStr).Scan(
		&job.ID,
		&job.JobType,
		&job.StreamID,
		&job.ViewID,
		&job.Status,
		&job.Payload,
		&job.Attempts,
		&job.MaxAttempts,
		&job.LastError,
		&job.LockedBy,
		&job.LockedUntil,
		&job.ScheduledAt,
		&job.CreatedAt,
		&job.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No pending or reclaimable jobs in the queue
			return nil, nil
		}
		return nil, fmt.Errorf("failed to acquire job from queue: %w", err)
	}

	return &job, nil
}

// CompleteJob marks an active job as completed and clears lease locks.
func (d *DB) CompleteJob(ctx context.Context, jobID uuid.UUID) error {
	if d.pool == nil {
		return fmt.Errorf("database pool is not initialized")
	}

	query := `
		UPDATE refresh_jobs
		SET status = 'completed',
			locked_by = NULL,
			locked_until = NULL,
			updated_at = NOW()
		WHERE id = $1;
	`

	res, err := d.pool.Exec(ctx, query, jobID)
	if err != nil {
		return fmt.Errorf("failed to complete job %s: %w", jobID, err)
	}

	if res.RowsAffected() == 0 {
		return fmt.Errorf("job %s not found", jobID)
	}

	return nil
}

// FailJob records failure details for a job. If attempts < max_attempts, it reschedules
// with exponential backoff; otherwise it marks the job permanently failed.
func (d *DB) FailJob(ctx context.Context, jobID uuid.UUID, errMsg string) error {
	if d.pool == nil {
		return fmt.Errorf("database pool is not initialized")
	}

	query := `
		UPDATE refresh_jobs
		SET status = CASE
				WHEN attempts >= max_attempts THEN 'failed'
				ELSE 'pending'
			END,
			last_error = $2,
			locked_by = NULL,
			locked_until = NULL,
			scheduled_at = CASE
				WHEN attempts >= max_attempts THEN scheduled_at
				ELSE NOW() + (attempts * INTERVAL '10 seconds')
			END,
			updated_at = NOW()
		WHERE id = $1;
	`

	res, err := d.pool.Exec(ctx, query, jobID, errMsg)
	if err != nil {
		return fmt.Errorf("failed to fail job %s: %w", jobID, err)
	}

	if res.RowsAffected() == 0 {
		return fmt.Errorf("job %s not found", jobID)
	}

	return nil
}
