CREATE TABLE IF NOT EXISTS shardline_reliability_events (
    operation_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_json TEXT NOT NULL,
    created_at_unix_seconds INTEGER NOT NULL,
    PRIMARY KEY (operation_id, sequence)
);

CREATE INDEX IF NOT EXISTS shardline_reliability_events_created_at_idx
    ON shardline_reliability_events (created_at_unix_seconds);
