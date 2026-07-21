-- Rollback: re-create the indexes we dropped, remove the new one.
CREATE INDEX IF NOT EXISTS shardline_quarantine_delete_after_idx
    ON shardline_quarantine_candidates (delete_after_unix_seconds, object_key);
CREATE INDEX IF NOT EXISTS shardline_retention_holds_release_after_idx
    ON shardline_retention_holds (release_after_unix_seconds, object_key);
CREATE INDEX IF NOT EXISTS shardline_webhook_deliveries_processed_at_idx
    ON shardline_webhook_deliveries (processed_at_unix_seconds, provider, owner, repo, delivery_id);
CREATE INDEX IF NOT EXISTS shardline_provider_repository_states_updated_at_idx
    ON shardline_provider_repository_states (updated_at, provider, owner, repo);
DROP INDEX IF EXISTS shardline_hub_revisions_repo_created_idx;
