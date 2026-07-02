CREATE TABLE IF NOT EXISTS shardline_hub_webhooks (
    id TEXT PRIMARY KEY,
    repo_id TEXT NOT NULL,
    url TEXT NOT NULL,
    events TEXT NOT NULL DEFAULT 'push',
    secret TEXT,
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
    FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos(repo_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS shardline_hub_webhooks_repo_idx ON shardline_hub_webhooks (repo_id);
