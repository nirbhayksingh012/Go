-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Streams Table
CREATE TABLE IF NOT EXISTS streams (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(128) NOT NULL UNIQUE,
    description TEXT DEFAULT '',
    retention_hours INT NOT NULL DEFAULT 168, -- default 7 days
    schema_definition JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_streams_name ON streams (name);

-- Events Table (Append-only Streaming Log)
CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    stream_id UUID NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    stream_name VARCHAR(128) NOT NULL,
    idempotency_key VARCHAR(256),
    payload JSONB NOT NULL,
    headers JSONB DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Optimize queries by stream and time range
CREATE INDEX IF NOT EXISTS idx_events_stream_ingested ON events (stream_name, ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_stream_id ON events (stream_id, id);

-- Unique idempotency key per stream to avoid duplicate writes
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_idempotency ON events (stream_id, idempotency_key) WHERE idempotency_key IS NOT NULL;

-- GIN index for high-performance JSONB filtering within payloads
CREATE INDEX IF NOT EXISTS idx_events_payload_gin ON events USING GIN (payload);

-- Materialized Views Definition Table
CREATE TABLE IF NOT EXISTS materialized_views (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(128) NOT NULL UNIQUE,
    source_stream_id UUID NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    query_definition TEXT NOT NULL,
    refresh_interval_seconds INT NOT NULL DEFAULT 60,
    last_refreshed_at TIMESTAMPTZ,
    last_event_id BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_matviews_status ON materialized_views (status);
