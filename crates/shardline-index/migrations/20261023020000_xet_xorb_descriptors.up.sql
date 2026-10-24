CREATE TABLE IF NOT EXISTS shardline_xet_xorbs (
    xorb_hash TEXT PRIMARY KEY,
    descriptor TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shardline_xet_chunks (
    xorb_hash TEXT NOT NULL REFERENCES shardline_xet_xorbs(xorb_hash) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
    chunk_hash TEXT NOT NULL,
    PRIMARY KEY (xorb_hash, chunk_index)
);
CREATE INDEX IF NOT EXISTS shardline_xet_chunks_hash_idx
    ON shardline_xet_chunks(chunk_hash, xorb_hash, chunk_index);
