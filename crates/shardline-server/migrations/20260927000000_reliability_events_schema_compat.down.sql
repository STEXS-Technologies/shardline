DROP INDEX IF EXISTS shardline_reliability_events_created_at_unix_seconds_idx;
ALTER TABLE shardline_reliability_events
    DROP COLUMN IF EXISTS created_at_unix_seconds;
