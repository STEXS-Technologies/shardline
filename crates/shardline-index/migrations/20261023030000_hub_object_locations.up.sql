CREATE TABLE IF NOT EXISTS shardline_hub_object_locations (
    repository_key TEXT NOT NULL,
    object_kind TEXT NOT NULL CHECK (object_kind IN ('file', 'git-archive')),
    object_id TEXT NOT NULL,
    storage_revision TEXT NOT NULL,
    PRIMARY KEY (repository_key, object_kind, object_id)
);
CREATE INDEX IF NOT EXISTS shardline_file_records_file_scope_idx
    ON shardline_file_records (file_id, scope_key, record_kind);
CREATE INDEX IF NOT EXISTS shardline_xet_file_identities_file_scope_idx
    ON shardline_xet_file_identities (file_id, scope_key);
CREATE INDEX IF NOT EXISTS shardline_hub_file_entries_hash_idx
    ON shardline_hub_file_entries (commit_sha, sha, is_lfs, size);
