ALTER TABLE shardline_provider_repository_states
    DROP COLUMN IF EXISTS last_drift_checked_at_unix_seconds;

ALTER TABLE shardline_provider_repository_states
    DROP COLUMN IF EXISTS last_authorization_rechecked_at_unix_seconds;

ALTER TABLE shardline_provider_repository_states
    DROP COLUMN IF EXISTS last_cache_invalidated_at_unix_seconds;
