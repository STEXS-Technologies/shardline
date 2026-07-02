CREATE TABLE IF NOT EXISTS shardline_dedupe_shards (
    chunk_hash TEXT PRIMARY KEY,
    shard_object_key TEXT NOT NULL,
    updated_at_unix_seconds INTEGER NOT NULL CHECK (updated_at_unix_seconds >= 0)
);
