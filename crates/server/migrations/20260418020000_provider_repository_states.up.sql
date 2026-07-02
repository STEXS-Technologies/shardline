CREATE TABLE IF NOT EXISTS shardline_provider_repository_states (
    provider TEXT NOT NULL CHECK (length(trim(provider)) > 0),
    owner TEXT NOT NULL CHECK (length(trim(owner)) > 0),
    repo TEXT NOT NULL CHECK (length(trim(repo)) > 0),
    last_access_changed_at_unix_seconds BIGINT CHECK (
        last_access_changed_at_unix_seconds IS NULL
        OR last_access_changed_at_unix_seconds >= 0
    ),
    last_revision_pushed_at_unix_seconds BIGINT CHECK (
        last_revision_pushed_at_unix_seconds IS NULL
        OR last_revision_pushed_at_unix_seconds >= 0
    ),
    last_pushed_revision TEXT,
    last_cache_invalidated_at_unix_seconds BIGINT CHECK (
        last_cache_invalidated_at_unix_seconds IS NULL
        OR last_cache_invalidated_at_unix_seconds >= 0
    ),
    last_authorization_rechecked_at_unix_seconds BIGINT CHECK (
        last_authorization_rechecked_at_unix_seconds IS NULL
        OR last_authorization_rechecked_at_unix_seconds >= 0
    ),
    last_drift_checked_at_unix_seconds BIGINT CHECK (
        last_drift_checked_at_unix_seconds IS NULL
        OR last_drift_checked_at_unix_seconds >= 0
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, owner, repo)
);

CREATE INDEX IF NOT EXISTS shardline_provider_repository_states_updated_at_idx
    ON shardline_provider_repository_states (updated_at, provider, owner, repo);
