CREATE TABLE IF NOT EXISTS shardline_file_reconstructions (
    file_id TEXT PRIMARY KEY,
    terms JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shardline_xorbs (
    xorb_hash TEXT PRIMARY KEY,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shardline_quarantine_candidates (
    object_key TEXT PRIMARY KEY,
    observed_length BIGINT NOT NULL CHECK (observed_length >= 0),
    first_seen_unreachable_at_unix_seconds BIGINT NOT NULL
        CHECK (first_seen_unreachable_at_unix_seconds >= 0),
    delete_after_unix_seconds BIGINT NOT NULL CHECK (delete_after_unix_seconds >= 0),
    CHECK (delete_after_unix_seconds >= first_seen_unreachable_at_unix_seconds)
);

CREATE TABLE IF NOT EXISTS shardline_file_records (
    record_key TEXT PRIMARY KEY,
    record_kind TEXT NOT NULL CHECK (record_kind IN ('latest', 'version')),
    scope_key TEXT NOT NULL,
    file_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    record JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS shardline_file_records_kind_key_idx
    ON shardline_file_records (record_kind, record_key);

CREATE INDEX IF NOT EXISTS shardline_file_records_scope_file_idx
    ON shardline_file_records (scope_key, file_id);

CREATE INDEX IF NOT EXISTS shardline_quarantine_delete_after_idx
    ON shardline_quarantine_candidates (delete_after_unix_seconds, object_key);
