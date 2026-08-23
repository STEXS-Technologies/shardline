CREATE TABLE IF NOT EXISTS shardline_resumable_sessions (
    session_id TEXT PRIMARY KEY,
    protocol TEXT NOT NULL CHECK (protocol IN ('lfs_patch', 'oci_blob', 's3_multipart')),
    scope_namespace TEXT NOT NULL,
    target_key TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'completing', 'completed', 'aborted', 'expired')),
    generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
    fence_epoch BIGINT NOT NULL DEFAULT 1 CHECK (fence_epoch > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS shardline_resumable_sessions_expiry_idx
    ON shardline_resumable_sessions (state, expires_at);

CREATE TABLE IF NOT EXISTS shardline_resumable_session_parts (
    session_id TEXT NOT NULL REFERENCES shardline_resumable_sessions(session_id) ON DELETE CASCADE,
    part_number BIGINT NOT NULL CHECK (part_number > 0),
    generation BIGINT NOT NULL CHECK (generation > 0),
    staging_key TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    etag TEXT,
    PRIMARY KEY (session_id, part_number)
);
