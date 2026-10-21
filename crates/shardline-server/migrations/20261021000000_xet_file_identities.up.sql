CREATE TABLE IF NOT EXISTS shardline_xet_file_identities (
    scope_key TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    file_id TEXT NOT NULL,
    total_bytes BIGINT NOT NULL CHECK (total_bytes >= 0),
    PRIMARY KEY (scope_key, sha256)
);

CREATE INDEX IF NOT EXISTS shardline_xet_file_identities_file_id_idx
    ON shardline_xet_file_identities (scope_key, file_id);
