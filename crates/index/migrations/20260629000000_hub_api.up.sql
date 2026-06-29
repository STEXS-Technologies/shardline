CREATE TABLE IF NOT EXISTS shardline_hub_repos (
    repo_id TEXT PRIMARY KEY,
    repo_type TEXT NOT NULL CHECK (repo_type IN ('model', 'dataset', 'space')),
    private INTEGER NOT NULL DEFAULT 0 CHECK (private IN (0, 1)),
    default_branch TEXT NOT NULL,
    created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
    updated_at_unix_seconds INTEGER NOT NULL CHECK (updated_at_unix_seconds >= 0)
);

CREATE TABLE IF NOT EXISTS shardline_hub_revisions (
    repo_id TEXT NOT NULL,
    ref_name TEXT NOT NULL,
    sha TEXT NOT NULL,
    parent_sha TEXT,
    message TEXT,
    created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
    PRIMARY KEY (repo_id, sha),
    FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos(repo_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS shardline_hub_revisions_repo_ref_idx
    ON shardline_hub_revisions (repo_id, ref_name);

CREATE TABLE IF NOT EXISTS shardline_hub_file_entries (
    commit_sha TEXT NOT NULL,
    path TEXT NOT NULL,
    size INTEGER NOT NULL CHECK (size >= 0),
    sha TEXT NOT NULL,
    is_lfs INTEGER NOT NULL DEFAULT 0 CHECK (is_lfs IN (0, 1)),
    PRIMARY KEY (commit_sha, path)
);

CREATE TABLE IF NOT EXISTS shardline_hub_lfs_objects (
    oid TEXT PRIMARY KEY,
    data BLOB NOT NULL,
    size INTEGER NOT NULL CHECK (size >= 0),
    created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0)
);
