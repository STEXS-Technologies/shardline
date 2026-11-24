-- Hub ref receipt outbox. Additive and OFF by default; this does not change
-- legacy success semantics or constitute an external recovery guarantee.
CREATE TABLE IF NOT EXISTS shardline_hub_recovery_config (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    mode TEXT NOT NULL CHECK (mode IN ('off', 'shadow')),
    deployment_id TEXT,
    CHECK ((mode = 'off' AND deployment_id IS NULL)
        OR (mode = 'shadow' AND deployment_id IS NOT NULL AND length(deployment_id) > 0))
);
INSERT INTO shardline_hub_recovery_config (singleton_id, mode, deployment_id)
VALUES (1, 'off', NULL) ON CONFLICT (singleton_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS shardline_hub_recovery_sequences (
    repo_id TEXT PRIMARY KEY,
    last_sequence INTEGER NOT NULL CHECK (last_sequence >= 0),
    last_archived_sequence INTEGER NOT NULL DEFAULT 0 CHECK (last_archived_sequence >= 0 AND last_archived_sequence <= last_sequence)
);

CREATE TABLE IF NOT EXISTS shardline_hub_recovery_outbox (
    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id TEXT NOT NULL,
    domain_sequence INTEGER NOT NULL CHECK (domain_sequence > 0),
    transaction_id TEXT NOT NULL UNIQUE,
    event_json TEXT NOT NULL,
    created_at_unix_seconds INTEGER NOT NULL,
    archived_at_unix_seconds INTEGER,
    archive_digest TEXT,
    UNIQUE (repo_id, domain_sequence),
    CHECK ((archived_at_unix_seconds IS NULL AND archive_digest IS NULL)
        OR (archived_at_unix_seconds IS NOT NULL AND archive_digest IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS shardline_hub_recovery_pending_idx
    ON shardline_hub_recovery_outbox (repo_id, domain_sequence)
    WHERE archived_at_unix_seconds IS NULL;
