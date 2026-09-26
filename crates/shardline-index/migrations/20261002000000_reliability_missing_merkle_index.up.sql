CREATE INDEX IF NOT EXISTS shardline_reliability_events_missing_merkle_idx
    ON shardline_reliability_events (operation_kind, operation_id, sequence)
    WHERE merkle_commit_json IS NULL;
