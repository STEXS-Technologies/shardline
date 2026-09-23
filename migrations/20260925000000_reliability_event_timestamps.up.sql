ALTER TABLE shardline_reliability_events
    ADD COLUMN IF NOT EXISTS created_at_unix_seconds BIGINT;

UPDATE shardline_reliability_events
SET created_at_unix_seconds = EXTRACT(EPOCH FROM created_at)::BIGINT
WHERE created_at_unix_seconds IS NULL;

ALTER TABLE shardline_reliability_events
    ALTER COLUMN created_at_unix_seconds SET NOT NULL;

DROP INDEX IF EXISTS shardline_reliability_events_created_at_idx;

CREATE INDEX IF NOT EXISTS shardline_reliability_events_created_at_idx
    ON shardline_reliability_events (created_at_unix_seconds);
