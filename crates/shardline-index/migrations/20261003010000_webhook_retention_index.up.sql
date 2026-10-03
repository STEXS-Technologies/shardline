-- Support webhook expiration selection without scanning retained deliveries.
CREATE INDEX IF NOT EXISTS shardline_webhook_deliveries_retention_idx
    ON shardline_webhook_deliveries
       (processed_at_unix_seconds, provider, owner, repo, delivery_id);
