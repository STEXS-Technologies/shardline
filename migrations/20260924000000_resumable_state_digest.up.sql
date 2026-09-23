ALTER TABLE shardline_resumable_sessions
    ADD COLUMN IF NOT EXISTS state_digest TEXT;
