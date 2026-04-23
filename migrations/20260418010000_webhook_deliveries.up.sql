CREATE TABLE IF NOT EXISTS shardline_webhook_deliveries (
    provider TEXT NOT NULL CHECK (length(trim(provider)) > 0),
    owner TEXT NOT NULL CHECK (length(trim(owner)) > 0),
    repo TEXT NOT NULL CHECK (length(trim(repo)) > 0),
    delivery_id TEXT NOT NULL CHECK (length(trim(delivery_id)) > 0),
    processed_at_unix_seconds BIGINT NOT NULL CHECK (processed_at_unix_seconds >= 0),
    PRIMARY KEY (provider, owner, repo, delivery_id)
);

CREATE INDEX IF NOT EXISTS shardline_webhook_deliveries_processed_at_idx
    ON shardline_webhook_deliveries (processed_at_unix_seconds, provider, owner, repo, delivery_id);
