ALTER TABLE shardline_reliability_events
    ADD COLUMN IF NOT EXISTS merkle_commit_json JSONB;
