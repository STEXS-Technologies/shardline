-- Older pre-release databases created the reliability journal with a
-- timestamptz `created_at` column. The released schema uses an integer Unix
-- timestamp for the same durable ordering metadata. Upgrade the existing
-- table in place without rewriting event payloads.
ALTER TABLE shardline_reliability_events
    ADD COLUMN IF NOT EXISTS created_at_unix_seconds BIGINT;

UPDATE shardline_reliability_events
SET created_at_unix_seconds = EXTRACT(EPOCH FROM created_at)::BIGINT
WHERE created_at_unix_seconds IS NULL;

ALTER TABLE shardline_reliability_events
    ALTER COLUMN created_at_unix_seconds SET NOT NULL;

CREATE INDEX IF NOT EXISTS shardline_reliability_events_created_at_unix_seconds_idx
    ON shardline_reliability_events (created_at_unix_seconds);
