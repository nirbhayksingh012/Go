-- Refresh Jobs Queue for Async Processing, View Compaction & Materialization
CREATE TABLE IF NOT EXISTS refresh_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type VARCHAR(64) NOT NULL,
    stream_id UUID REFERENCES streams(id) ON DELETE CASCADE,
    view_id UUID REFERENCES materialized_views(id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL DEFAULT 'pending', -- 'pending', 'running', 'completed', 'failed'
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    last_error TEXT DEFAULT '',
    locked_by VARCHAR(128),
    locked_until TIMESTAMPTZ,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for worker polling using SELECT FOR UPDATE SKIP LOCKED
CREATE INDEX IF NOT EXISTS idx_refresh_jobs_queue ON refresh_jobs (status, scheduled_at, locked_until)
    WHERE status IN ('pending', 'running');

CREATE INDEX IF NOT EXISTS idx_refresh_jobs_stream_id ON refresh_jobs (stream_id);
CREATE INDEX IF NOT EXISTS idx_refresh_jobs_view_id ON refresh_jobs (view_id);
