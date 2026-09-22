CREATE TABLE shardline_reliability_events_without_kind (
    operation_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_json TEXT NOT NULL,
    created_at_unix_seconds INTEGER NOT NULL,
    PRIMARY KEY (operation_id, sequence)
);

INSERT INTO shardline_reliability_events_without_kind
    (operation_id, sequence, event_json, created_at_unix_seconds)
SELECT operation_id, sequence, event_json, created_at_unix_seconds
FROM shardline_reliability_events;

DROP TABLE shardline_reliability_events;
ALTER TABLE shardline_reliability_events_without_kind RENAME TO shardline_reliability_events;

CREATE INDEX shardline_reliability_events_created_at_idx
    ON shardline_reliability_events (created_at_unix_seconds);
