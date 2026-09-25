DROP INDEX IF EXISTS shardline_reliability_events_created_at_idx;

ALTER TABLE shardline_reliability_events
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;

UPDATE shardline_reliability_events
SET created_at = to_timestamp(created_at_unix_seconds)
WHERE created_at IS NULL;

ALTER TABLE shardline_reliability_events
    ALTER COLUMN created_at SET NOT NULL;

ALTER TABLE shardline_reliability_events
    DROP COLUMN IF EXISTS created_at_unix_seconds;

CREATE INDEX IF NOT EXISTS shardline_reliability_events_created_at_idx
    ON shardline_reliability_events (created_at);
