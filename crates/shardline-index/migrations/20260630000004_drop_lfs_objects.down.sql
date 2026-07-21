CREATE TABLE IF NOT EXISTS shardline_hub_lfs_objects (
    oid TEXT PRIMARY KEY,
    data BLOB NOT NULL,
    size INTEGER NOT NULL CHECK (size >= 0),
    created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0)
);
