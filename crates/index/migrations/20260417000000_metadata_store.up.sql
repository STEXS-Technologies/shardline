CREATE TABLE IF NOT EXISTS shardline_local_metadata_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shardline_file_reconstructions (
    file_id TEXT PRIMARY KEY,
    terms TEXT NOT NULL,
    updated_at_unix_seconds INTEGER NOT NULL CHECK (updated_at_unix_seconds >= 0)
);

CREATE TABLE IF NOT EXISTS shardline_xorbs (
    xorb_hash TEXT PRIMARY KEY,
    registered_at_unix_seconds INTEGER NOT NULL CHECK (registered_at_unix_seconds >= 0)
);

CREATE TABLE IF NOT EXISTS shardline_quarantine_candidates (
    object_key TEXT PRIMARY KEY,
    observed_length INTEGER NOT NULL CHECK (observed_length >= 0),
    first_seen_unreachable_at_unix_seconds INTEGER NOT NULL
        CHECK (first_seen_unreachable_at_unix_seconds >= 0),
    delete_after_unix_seconds INTEGER NOT NULL CHECK (delete_after_unix_seconds >= 0),
    updated_at_unix_seconds INTEGER NOT NULL CHECK (updated_at_unix_seconds >= 0),
    CHECK (delete_after_unix_seconds >= first_seen_unreachable_at_unix_seconds)
);

CREATE TABLE IF NOT EXISTS shardline_file_records (
    record_key TEXT PRIMARY KEY,
    record_kind TEXT NOT NULL CHECK (record_kind IN ('latest', 'version')),
    scope_key TEXT NOT NULL,
    file_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    record TEXT NOT NULL,
    updated_at_unix_seconds INTEGER NOT NULL CHECK (updated_at_unix_seconds >= 0)
);

CREATE INDEX IF NOT EXISTS shardline_file_records_kind_key_idx
    ON shardline_file_records (record_kind, record_key);

CREATE INDEX IF NOT EXISTS shardline_file_records_scope_file_idx
    ON shardline_file_records (scope_key, file_id);

CREATE INDEX IF NOT EXISTS shardline_quarantine_delete_after_idx
    ON shardline_quarantine_candidates (delete_after_unix_seconds, object_key);
