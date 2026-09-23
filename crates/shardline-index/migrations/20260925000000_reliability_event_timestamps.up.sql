-- SQLite reliability events already carry the canonical Unix timestamp from
-- the initial journal migration. Keep the backend migration histories aligned
-- without rewriting the existing materialized state.
CREATE INDEX IF NOT EXISTS shardline_reliability_events_created_at_idx
    ON shardline_reliability_events (created_at_unix_seconds);
