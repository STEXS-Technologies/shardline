CREATE TABLE IF NOT EXISTS shardline_hub_lfs_objects (
    oid TEXT PRIMARY KEY,
    data BYTEA NOT NULL,
    size BIGINT NOT NULL CHECK (size >= 0),
    created_at_unix_seconds BIGINT NOT NULL CHECK (created_at_unix_seconds >= 0)
);
