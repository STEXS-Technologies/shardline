-- Add index for list_revisions() which orders by created_at_unix_seconds DESC.
-- Without this, Postgres/SQLite must sort in memory after filtering by repo_id.
CREATE INDEX IF NOT EXISTS shardline_hub_revisions_repo_created_idx
    ON shardline_hub_revisions (repo_id, created_at_unix_seconds DESC);

-- Drop unused secondary indexes. These were created proactively for future
-- cleanup jobs but no query in the codebase filters on their leading column.
-- They carry write overhead on INSERT/UPDATE/DELETE with zero read benefit.
DROP INDEX IF EXISTS shardline_quarantine_delete_after_idx;
DROP INDEX IF EXISTS shardline_retention_holds_release_after_idx;
DROP INDEX IF EXISTS shardline_webhook_deliveries_processed_at_idx;
DROP INDEX IF EXISTS shardline_provider_repository_states_updated_at_idx;
