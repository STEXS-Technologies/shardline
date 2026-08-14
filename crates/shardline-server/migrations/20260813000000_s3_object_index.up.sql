CREATE TABLE IF NOT EXISTS shardline_s3_objects (
    scope_namespace TEXT NOT NULL,
    object_key TEXT NOT NULL,
    file_id TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    content_hash TEXT NOT NULL,
    updated_at_unix_seconds BIGINT NOT NULL,
    PRIMARY KEY (scope_namespace, object_key)
);

CREATE INDEX IF NOT EXISTS shardline_s3_objects_scope_key_idx
    ON shardline_s3_objects (scope_namespace, object_key);
