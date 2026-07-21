CREATE TABLE IF NOT EXISTS shardline_dedupe_shards (
    chunk_hash TEXT PRIMARY KEY,
    shard_object_key TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
