CREATE TABLE IF NOT EXISTS shardline_upload_intents (
    intent_id TEXT PRIMARY KEY,
    object_key TEXT NOT NULL,
    object_hash TEXT NOT NULL,
    object_length INTEGER NOT NULL CHECK (object_length >= 0),
    state TEXT NOT NULL DEFAULT 'created'
        CHECK (state IN ('created', 'storing', 'stored', 'metadata_committed', 'visible', 'failed')),
    created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
    updated_at_unix_seconds INTEGER NOT NULL CHECK (updated_at_unix_seconds >= 0)
);

CREATE INDEX IF NOT EXISTS shardline_upload_intents_state_idx
    ON shardline_upload_intents (state);

CREATE INDEX IF NOT EXISTS shardline_upload_intents_created_at_idx
    ON shardline_upload_intents (created_at_unix_seconds);
