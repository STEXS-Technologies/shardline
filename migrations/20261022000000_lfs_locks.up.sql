CREATE TABLE IF NOT EXISTS shardline_lfs_locks (
    scope_namespace TEXT NOT NULL,
    id TEXT NOT NULL,
    path TEXT NOT NULL,
    owner_key TEXT NOT NULL,
    owner_name TEXT NOT NULL,
    locked_at TEXT NOT NULL,
    PRIMARY KEY (scope_namespace, id),
    UNIQUE (scope_namespace, path)
);
