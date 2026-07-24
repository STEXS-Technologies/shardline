CREATE TABLE IF NOT EXISTS shardline_upload_intents (
    intent_id TEXT PRIMARY KEY,
    object_key TEXT NOT NULL,
    object_hash TEXT NOT NULL,
    object_length BIGINT NOT NULL,
    state TEXT NOT NULL DEFAULT 'created'
        CHECK (state IN ('created', 'storing', 'stored', 'metadata_committed', 'visible', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS shardline_upload_intents_state_idx
    ON shardline_upload_intents (state);

CREATE INDEX IF NOT EXISTS shardline_upload_intents_created_at_idx
    ON shardline_upload_intents (created_at);
