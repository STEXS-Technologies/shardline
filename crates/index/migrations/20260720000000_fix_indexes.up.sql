-- Add index for list_revisions() ordering
CREATE INDEX IF NOT EXISTS shardline_hub_revisions_repo_created_idx
    ON shardline_hub_revisions (repo_id, created_at_unix_seconds DESC);

-- Drop unused secondary indexes
DROP INDEX IF EXISTS shardline_quarantine_delete_after_idx;
DROP INDEX IF EXISTS shardline_retention_holds_release_after_idx;
DROP INDEX IF EXISTS shardline_webhook_deliveries_processed_at_idx;
DROP INDEX IF EXISTS shardline_provider_repository_states_updated_at_idx;
