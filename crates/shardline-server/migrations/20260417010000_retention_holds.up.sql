CREATE TABLE IF NOT EXISTS shardline_retention_holds (
    object_key TEXT PRIMARY KEY,
    reason TEXT NOT NULL CHECK (length(trim(reason)) > 0),
    held_at_unix_seconds BIGINT NOT NULL CHECK (held_at_unix_seconds >= 0),
    release_after_unix_seconds BIGINT NULL CHECK (release_after_unix_seconds >= 0),
    CHECK (
        release_after_unix_seconds IS NULL
        OR release_after_unix_seconds >= held_at_unix_seconds
    )
);

CREATE INDEX IF NOT EXISTS shardline_retention_holds_release_after_idx
    ON shardline_retention_holds (release_after_unix_seconds, object_key);
