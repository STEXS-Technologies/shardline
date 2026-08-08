CREATE TABLE IF NOT EXISTS shardline_tree_entries (
    provider TEXT NOT NULL,
    owner TEXT NOT NULL,
    repo TEXT NOT NULL,
    revision TEXT NOT NULL,
    path TEXT NOT NULL,
    file_id TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    updated_at_unix_seconds BIGINT NOT NULL CHECK (updated_at_unix_seconds >= 0),
    PRIMARY KEY (provider, owner, repo, revision, path)
);

CREATE TABLE IF NOT EXISTS shardline_revisions (
    provider TEXT NOT NULL,
    owner TEXT NOT NULL,
    repo TEXT NOT NULL,
    revision TEXT NOT NULL,
    created_at_unix_seconds BIGINT NOT NULL CHECK (created_at_unix_seconds >= 0),
    updated_at_unix_seconds BIGINT NOT NULL CHECK (updated_at_unix_seconds >= 0),
    PRIMARY KEY (provider, owner, repo, revision)
);
