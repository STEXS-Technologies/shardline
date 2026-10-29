CREATE TABLE IF NOT EXISTS shardline_s3_multipart_counts (
    scope_namespace TEXT NOT NULL,
    object_key TEXT NOT NULL,
    file_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    part_count INTEGER NOT NULL CHECK (part_count >= 0),
    updated_at_unix_seconds INTEGER NOT NULL,
    proof TEXT NOT NULL,
    PRIMARY KEY (scope_namespace, object_key)
);
