-- PostgreSQL retention locks selected heap rows and only filters by timestamp.
-- A narrow index also avoids adding key width to schema-valid legacy receipts.
CREATE INDEX IF NOT EXISTS shardline_webhook_deliveries_retention_idx
    ON shardline_webhook_deliveries (processed_at_unix_seconds);
