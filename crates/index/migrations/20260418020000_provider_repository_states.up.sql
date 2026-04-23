CREATE TABLE IF NOT EXISTS shardline_provider_repository_states (
    provider TEXT NOT NULL CHECK (length(trim(provider)) > 0),
    owner TEXT NOT NULL CHECK (length(trim(owner)) > 0),
    repo TEXT NOT NULL CHECK (length(trim(repo)) > 0),
    last_access_changed_at_unix_seconds INTEGER CHECK (
        last_access_changed_at_unix_seconds IS NULL
        OR last_access_changed_at_unix_seconds >= 0
    ),
    last_revision_pushed_at_unix_seconds INTEGER CHECK (
        last_revision_pushed_at_unix_seconds IS NULL
        OR last_revision_pushed_at_unix_seconds >= 0
    ),
    last_pushed_revision TEXT,
    created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
    updated_at_unix_seconds INTEGER NOT NULL CHECK (updated_at_unix_seconds >= 0),
    PRIMARY KEY (provider, owner, repo)
);

CREATE INDEX IF NOT EXISTS shardline_provider_repository_states_updated_at_idx
    ON shardline_provider_repository_states (updated_at_unix_seconds, provider, owner, repo);
