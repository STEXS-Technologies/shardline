CREATE TABLE IF NOT EXISTS shardline_resumable_sessions (
    session_id TEXT PRIMARY KEY,
    protocol TEXT NOT NULL CHECK (protocol IN ('lfs_patch', 'oci_blob', 's3_multipart')),
    scope_namespace TEXT NOT NULL,
    target_key TEXT NOT NULL,
    attributes_json TEXT NOT NULL DEFAULT '{}',
    state TEXT NOT NULL CHECK (state IN ('active', 'completing', 'completed', 'aborted', 'expired')),
    generation INTEGER NOT NULL DEFAULT 1 CHECK (generation > 0),
    fence_epoch INTEGER NOT NULL DEFAULT 1 CHECK (fence_epoch > 0),
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS shardline_resumable_sessions_expiry_idx
    ON shardline_resumable_sessions (state, expires_at);

CREATE TABLE IF NOT EXISTS shardline_resumable_session_parts (
    session_id TEXT NOT NULL REFERENCES shardline_resumable_sessions(session_id) ON DELETE CASCADE,
    part_number INTEGER NOT NULL CHECK (part_number > 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    staging_key TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    etag TEXT,
    range_start INTEGER,
    range_end INTEGER,
    CHECK ((range_start IS NULL AND range_end IS NULL) OR
           (range_start >= 0 AND range_end > range_start)),
    PRIMARY KEY (session_id, part_number)
);
