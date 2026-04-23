ALTER TABLE shardline_provider_repository_states
    ADD COLUMN last_cache_invalidated_at_unix_seconds INTEGER CHECK (
        last_cache_invalidated_at_unix_seconds IS NULL
        OR last_cache_invalidated_at_unix_seconds >= 0
    );

ALTER TABLE shardline_provider_repository_states
    ADD COLUMN last_authorization_rechecked_at_unix_seconds INTEGER CHECK (
        last_authorization_rechecked_at_unix_seconds IS NULL
        OR last_authorization_rechecked_at_unix_seconds >= 0
    );

ALTER TABLE shardline_provider_repository_states
    ADD COLUMN last_drift_checked_at_unix_seconds INTEGER CHECK (
        last_drift_checked_at_unix_seconds IS NULL
        OR last_drift_checked_at_unix_seconds >= 0
    );
