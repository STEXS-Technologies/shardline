CREATE TABLE IF NOT EXISTS shardline_reliability_events (
    operation_kind TEXT NOT NULL DEFAULT 'Upload',
    operation_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    event_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (operation_kind, operation_id, sequence)
);

CREATE INDEX IF NOT EXISTS shardline_reliability_events_created_at_idx
    ON shardline_reliability_events (created_at);
