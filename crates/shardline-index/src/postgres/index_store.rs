use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use shardline_protocol::{ChunkRange, RepositoryProvider, ShardlineHash};
use shardline_storage::ObjectKey;
use sqlx::{Row, postgres::PgRow, query, query_scalar, types::Json};

use super::{PostgresMetadataStoreError, i64_to_u64, u64_to_i64};
use crate::{
    AsyncIndexStore, DedupeShardMapping, FileId, FileReconstruction, IndexStoreFuture,
    ProviderRepositoryState, QuarantineCandidate, ReconstructionTerm, RepoKey, RetentionHold,
    StoredObjectId, TreeStore, WebhookDelivery, WebhookDeliveryError, parse_xet_hash_hex,
    provider::parse_repository_provider,
    upload_intent::{UploadIntent, UploadIntentState, UploadIntentStore},
    xet_hash_hex_string,
};

impl AsyncIndexStore for super::PostgresIndexStore {
    type Error = PostgresMetadataStoreError;

    fn reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, Option<FileReconstruction>, Self::Error> {
        Box::pin(async move {
            let row = query("SELECT terms FROM shardline_file_reconstructions WHERE file_id = $1")
                .bind(xet_hash_hex_string(file_id.hash()))
                .fetch_optional(&self.pool)
                .await?;

            let Some(row) = row else {
                return Ok(None);
            };
            let Json(record) = row.try_get::<Json<PostgresFileReconstructionRecord>, _>("terms")?;
            Ok(Some(record.into_domain()?))
        })
    }

    fn insert_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
        reconstruction: &'operation FileReconstruction,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let record = PostgresFileReconstructionRecord::from_domain(reconstruction);
            query(
                "INSERT INTO shardline_file_reconstructions (file_id, terms)
                 VALUES ($1, $2)
                 ON CONFLICT (file_id)
                 DO UPDATE SET terms = EXCLUDED.terms, updated_at = now()",
            )
            .bind(xet_hash_hex_string(file_id.hash()))
            .bind(Json(record))
            .execute(&self.pool)
            .await?;
            Ok(())
        })
    }

    fn list_reconstruction_file_ids(&self) -> IndexStoreFuture<'_, Vec<FileId>, Self::Error> {
        Box::pin(async move {
            let rows = query("SELECT file_id FROM shardline_file_reconstructions ORDER BY file_id")
                .fetch_all(&self.pool)
                .await?;

            rows.iter()
                .map(|row| {
                    let file_id = row.try_get::<String, _>("file_id")?;
                    let hash = parse_xet_hash_hex(&file_id)?;
                    Ok(FileId::new(hash))
                })
                .collect::<Result<Vec<_>, PostgresMetadataStoreError>>()
        })
    }

    fn delete_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let result = query("DELETE FROM shardline_file_reconstructions WHERE file_id = $1")
                .bind(xet_hash_hex_string(file_id.hash()))
                .execute(&self.pool)
                .await?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn contains_object<'operation>(
        &'operation self,
        object_id: &'operation StoredObjectId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let exists = query_scalar::<_, bool>(
                "SELECT EXISTS(
                    SELECT 1 FROM shardline_stored_objects WHERE object_hash = $1
                 )",
            )
            .bind(xet_hash_hex_string(object_id.hash()))
            .fetch_one(&self.pool)
            .await?;
            Ok(exists)
        })
    }

    fn insert_object<'operation>(
        &'operation self,
        object_id: &'operation StoredObjectId,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            query(
                "INSERT INTO shardline_stored_objects (object_hash)
                 VALUES ($1)
                 ON CONFLICT (object_hash) DO NOTHING",
            )
            .bind(xet_hash_hex_string(object_id.hash()))
            .execute(&self.pool)
            .await?;
            Ok(())
        })
    }

    fn dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, Option<DedupeShardMapping>, Self::Error> {
        Box::pin(async move {
            let row = query(
                "SELECT chunk_hash, shard_object_key
                 FROM shardline_dedupe_shards
                 WHERE chunk_hash = $1",
            )
            .bind(xet_hash_hex_string(chunk_hash))
            .fetch_optional(&self.pool)
            .await?;

            row.as_ref().map(dedupe_shard_mapping_from_row).transpose()
        })
    }

    fn list_dedupe_shard_mappings(
        &self,
    ) -> IndexStoreFuture<'_, Vec<DedupeShardMapping>, Self::Error> {
        Box::pin(async move {
            let rows = query(
                "SELECT chunk_hash, shard_object_key
                 FROM shardline_dedupe_shards
                 ORDER BY chunk_hash",
            )
            .fetch_all(&self.pool)
            .await?;

            rows.iter()
                .map(dedupe_shard_mapping_from_row)
                .collect::<Result<Vec<_>, _>>()
        })
    }

    fn visit_dedupe_shard_mappings<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(DedupeShardMapping) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            let mut rows = query(
                "SELECT chunk_hash, shard_object_key
                 FROM shardline_dedupe_shards
                 ORDER BY chunk_hash",
            )
            .fetch(&self.pool);

            while let Some(row) = rows
                .try_next()
                .await
                .map_err(Self::Error::from)
                .map_err(Into::<VisitorError>::into)?
            {
                let mapping = dedupe_shard_mapping_from_row(&row).map_err(Into::into)?;
                visitor(mapping)?;
            }

            Ok(())
        })
    }

    fn upsert_dedupe_shard_mapping<'operation>(
        &'operation self,
        mapping: &'operation DedupeShardMapping,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            query(
                "INSERT INTO shardline_dedupe_shards (chunk_hash, shard_object_key)
                 VALUES ($1, $2)
                 ON CONFLICT (chunk_hash)
                 DO UPDATE SET
                    shard_object_key = EXCLUDED.shard_object_key,
                    updated_at = now()",
            )
            .bind(xet_hash_hex_string(mapping.chunk_hash()))
            .bind(mapping.shard_object_key().as_str())
            .execute(&self.pool)
            .await?;
            Ok(())
        })
    }

    fn delete_dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let result = query("DELETE FROM shardline_dedupe_shards WHERE chunk_hash = $1")
                .bind(xet_hash_hex_string(chunk_hash))
                .execute(&self.pool)
                .await?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<QuarantineCandidate>, Self::Error> {
        Box::pin(async move {
            let row = query(
                "SELECT object_key,
                        observed_length,
                        first_seen_unreachable_at_unix_seconds,
                        delete_after_unix_seconds
                 FROM shardline_quarantine_candidates
                 WHERE object_key = $1",
            )
            .bind(object_key.as_str())
            .fetch_optional(&self.pool)
            .await?;

            row.as_ref().map(quarantine_candidate_from_row).transpose()
        })
    }

    fn list_quarantine_candidates(
        &self,
    ) -> IndexStoreFuture<'_, Vec<QuarantineCandidate>, Self::Error> {
        Box::pin(async move {
            let rows = query(
                "SELECT object_key,
                        observed_length,
                        first_seen_unreachable_at_unix_seconds,
                        delete_after_unix_seconds
                 FROM shardline_quarantine_candidates
                 ORDER BY object_key",
            )
            .fetch_all(&self.pool)
            .await?;

            rows.iter()
                .map(quarantine_candidate_from_row)
                .collect::<Result<Vec<_>, _>>()
        })
    }

    fn visit_quarantine_candidates<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(QuarantineCandidate) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            let mut rows = query(
                "SELECT object_key,
                        observed_length,
                        first_seen_unreachable_at_unix_seconds,
                        delete_after_unix_seconds
                 FROM shardline_quarantine_candidates
                 ORDER BY object_key",
            )
            .fetch(&self.pool);

            while let Some(row) = rows
                .try_next()
                .await
                .map_err(Self::Error::from)
                .map_err(Into::<VisitorError>::into)?
            {
                let candidate = quarantine_candidate_from_row(&row).map_err(Into::into)?;
                visitor(candidate)?;
            }

            Ok(())
        })
    }

    fn upsert_quarantine_candidate<'operation>(
        &'operation self,
        candidate: &'operation QuarantineCandidate,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            query(
                "INSERT INTO shardline_quarantine_candidates (
                    object_key,
                    observed_length,
                    first_seen_unreachable_at_unix_seconds,
                    delete_after_unix_seconds
                 )
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (object_key)
                 DO UPDATE SET
                    observed_length = EXCLUDED.observed_length,
                    first_seen_unreachable_at_unix_seconds =
                        EXCLUDED.first_seen_unreachable_at_unix_seconds,
                    delete_after_unix_seconds = EXCLUDED.delete_after_unix_seconds",
            )
            .bind(candidate.object_key().as_str())
            .bind(u64_to_i64(candidate.observed_length())?)
            .bind(u64_to_i64(
                candidate.first_seen_unreachable_at_unix_seconds(),
            )?)
            .bind(u64_to_i64(candidate.delete_after_unix_seconds())?)
            .execute(&self.pool)
            .await?;
            Ok(())
        })
    }

    fn delete_quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let result = query("DELETE FROM shardline_quarantine_candidates WHERE object_key = $1")
                .bind(object_key.as_str())
                .execute(&self.pool)
                .await?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<RetentionHold>, Self::Error> {
        Box::pin(async move {
            let row = query(
                "SELECT object_key,
                        reason,
                        held_at_unix_seconds,
                        release_after_unix_seconds
                 FROM shardline_retention_holds
                 WHERE object_key = $1",
            )
            .bind(object_key.as_str())
            .fetch_optional(&self.pool)
            .await?;

            row.as_ref().map(retention_hold_from_row).transpose()
        })
    }

    fn list_retention_holds(&self) -> IndexStoreFuture<'_, Vec<RetentionHold>, Self::Error> {
        Box::pin(async move {
            let rows = query(
                "SELECT object_key,
                        reason,
                        held_at_unix_seconds,
                        release_after_unix_seconds
                 FROM shardline_retention_holds
                 ORDER BY object_key",
            )
            .fetch_all(&self.pool)
            .await?;

            rows.iter()
                .map(retention_hold_from_row)
                .collect::<Result<Vec<_>, _>>()
        })
    }

    fn visit_retention_holds<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(RetentionHold) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            let mut rows = query(
                "SELECT object_key,
                        reason,
                        held_at_unix_seconds,
                        release_after_unix_seconds
                 FROM shardline_retention_holds
                 ORDER BY object_key",
            )
            .fetch(&self.pool);

            while let Some(row) = rows
                .try_next()
                .await
                .map_err(Self::Error::from)
                .map_err(Into::<VisitorError>::into)?
            {
                let hold = retention_hold_from_row(&row).map_err(Into::into)?;
                visitor(hold)?;
            }

            Ok(())
        })
    }

    fn upsert_retention_hold<'operation>(
        &'operation self,
        hold: &'operation RetentionHold,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            query(
                "INSERT INTO shardline_retention_holds (
                    object_key,
                    reason,
                    held_at_unix_seconds,
                    release_after_unix_seconds
                 )
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (object_key)
                 DO UPDATE SET
                    reason = EXCLUDED.reason,
                    held_at_unix_seconds = EXCLUDED.held_at_unix_seconds,
                    release_after_unix_seconds = EXCLUDED.release_after_unix_seconds",
            )
            .bind(hold.object_key().as_str())
            .bind(hold.reason())
            .bind(u64_to_i64(hold.held_at_unix_seconds())?)
            .bind(
                hold.release_after_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
            )
            .execute(&self.pool)
            .await?;
            Ok(())
        })
    }

    fn delete_retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let result = query("DELETE FROM shardline_retention_holds WHERE object_key = $1")
                .bind(object_key.as_str())
                .execute(&self.pool)
                .await?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn record_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let result = query(
                "INSERT INTO shardline_webhook_deliveries (
                    provider,
                    owner,
                    repo,
                    delivery_id,
                    processed_at_unix_seconds
                 )
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (provider, owner, repo, delivery_id)
                 DO NOTHING",
            )
            .bind(delivery.provider().as_str())
            .bind(delivery.owner())
            .bind(delivery.repo())
            .bind(delivery.delivery_id())
            .bind(u64_to_i64(delivery.processed_at_unix_seconds())?)
            .execute(&self.pool)
            .await?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn list_webhook_deliveries(&self) -> IndexStoreFuture<'_, Vec<WebhookDelivery>, Self::Error> {
        Box::pin(async move {
            let rows = query(
                "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
                 FROM shardline_webhook_deliveries
                 ORDER BY provider, owner, repo, delivery_id",
            )
            .fetch_all(&self.pool)
            .await?;
            rows.into_iter()
                .map(|row| webhook_delivery_from_row(&row))
                .collect::<Result<Vec<_>, _>>()
        })
    }

    fn delete_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let result = query(
                "DELETE FROM shardline_webhook_deliveries
                 WHERE provider = $1 AND owner = $2 AND repo = $3 AND delivery_id = $4",
            )
            .bind(delivery.provider().as_str())
            .bind(delivery.owner())
            .bind(delivery.repo())
            .bind(delivery.delivery_id())
            .execute(&self.pool)
            .await?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn purge_webhook_deliveries_older_than<'operation>(
        &'operation self,
        older_than_unix_seconds: u64,
    ) -> IndexStoreFuture<'operation, u64, Self::Error> {
        Box::pin(async move {
            let result = query(
                "DELETE FROM shardline_webhook_deliveries
                 WHERE processed_at_unix_seconds < $1",
            )
            .bind(u64_to_i64(older_than_unix_seconds)?)
            .execute(&self.pool)
            .await?;
            Ok(result.rows_affected())
        })
    }

    fn provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, Option<ProviderRepositoryState>, Self::Error> {
        Box::pin(async move {
            let row = query(
                "SELECT provider,
                        owner,
                        repo,
                        last_access_changed_at_unix_seconds,
                        last_revision_pushed_at_unix_seconds,
                        last_pushed_revision,
                        last_cache_invalidated_at_unix_seconds,
                        last_authorization_rechecked_at_unix_seconds,
                        last_drift_checked_at_unix_seconds
                 FROM shardline_provider_repository_states
                 WHERE provider = $1 AND owner = $2 AND repo = $3",
            )
            .bind(provider.as_str())
            .bind(owner)
            .bind(repo)
            .fetch_optional(&self.pool)
            .await?;

            row.as_ref()
                .map(provider_repository_state_from_row)
                .transpose()
        })
    }

    fn list_provider_repository_states(
        &self,
    ) -> IndexStoreFuture<'_, Vec<ProviderRepositoryState>, Self::Error> {
        Box::pin(async move {
            let rows = query(
                "SELECT provider,
                        owner,
                        repo,
                        last_access_changed_at_unix_seconds,
                        last_revision_pushed_at_unix_seconds,
                        last_pushed_revision,
                        last_cache_invalidated_at_unix_seconds,
                        last_authorization_rechecked_at_unix_seconds,
                        last_drift_checked_at_unix_seconds
                 FROM shardline_provider_repository_states
                 ORDER BY provider, owner, repo",
            )
            .fetch_all(&self.pool)
            .await?;
            rows.into_iter()
                .map(|row| provider_repository_state_from_row(&row))
                .collect::<Result<Vec<_>, _>>()
        })
    }

    fn upsert_provider_repository_state<'operation>(
        &'operation self,
        state: &'operation ProviderRepositoryState,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            query(
                "INSERT INTO shardline_provider_repository_states (
                    provider,
                    owner,
                    repo,
                    last_access_changed_at_unix_seconds,
                    last_revision_pushed_at_unix_seconds,
                    last_pushed_revision,
                    last_cache_invalidated_at_unix_seconds,
                    last_authorization_rechecked_at_unix_seconds,
                    last_drift_checked_at_unix_seconds
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT (provider, owner, repo)
                 DO UPDATE SET
                    last_access_changed_at_unix_seconds = CASE
                        WHEN EXCLUDED.last_access_changed_at_unix_seconds IS NULL
                            THEN shardline_provider_repository_states.last_access_changed_at_unix_seconds
                        WHEN shardline_provider_repository_states.last_access_changed_at_unix_seconds IS NULL
                          OR EXCLUDED.last_access_changed_at_unix_seconds >= shardline_provider_repository_states.last_access_changed_at_unix_seconds
                            THEN EXCLUDED.last_access_changed_at_unix_seconds
                        ELSE shardline_provider_repository_states.last_access_changed_at_unix_seconds
                    END,
                    last_pushed_revision = CASE
                        WHEN EXCLUDED.last_revision_pushed_at_unix_seconds IS NOT NULL
                         AND (shardline_provider_repository_states.last_revision_pushed_at_unix_seconds IS NULL
                           OR EXCLUDED.last_revision_pushed_at_unix_seconds >= shardline_provider_repository_states.last_revision_pushed_at_unix_seconds)
                            THEN EXCLUDED.last_pushed_revision
                        ELSE shardline_provider_repository_states.last_pushed_revision
                    END,
                    last_revision_pushed_at_unix_seconds = CASE
                        WHEN EXCLUDED.last_revision_pushed_at_unix_seconds IS NULL
                            THEN shardline_provider_repository_states.last_revision_pushed_at_unix_seconds
                        WHEN shardline_provider_repository_states.last_revision_pushed_at_unix_seconds IS NULL
                          OR EXCLUDED.last_revision_pushed_at_unix_seconds >= shardline_provider_repository_states.last_revision_pushed_at_unix_seconds
                            THEN EXCLUDED.last_revision_pushed_at_unix_seconds
                        ELSE shardline_provider_repository_states.last_revision_pushed_at_unix_seconds
                    END,
                    last_cache_invalidated_at_unix_seconds = CASE
                        WHEN EXCLUDED.last_cache_invalidated_at_unix_seconds IS NULL
                            THEN shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds
                        WHEN shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds IS NULL
                          OR EXCLUDED.last_cache_invalidated_at_unix_seconds >= shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds
                            THEN EXCLUDED.last_cache_invalidated_at_unix_seconds
                        ELSE shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds
                    END,
                    last_authorization_rechecked_at_unix_seconds = CASE
                        WHEN EXCLUDED.last_authorization_rechecked_at_unix_seconds IS NULL
                            THEN shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds
                        WHEN shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds IS NULL
                          OR EXCLUDED.last_authorization_rechecked_at_unix_seconds >= shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds
                            THEN EXCLUDED.last_authorization_rechecked_at_unix_seconds
                        ELSE shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds
                    END,
                    last_drift_checked_at_unix_seconds = CASE
                        WHEN EXCLUDED.last_drift_checked_at_unix_seconds IS NULL
                            THEN shardline_provider_repository_states.last_drift_checked_at_unix_seconds
                        WHEN shardline_provider_repository_states.last_drift_checked_at_unix_seconds IS NULL
                          OR EXCLUDED.last_drift_checked_at_unix_seconds >= shardline_provider_repository_states.last_drift_checked_at_unix_seconds
                            THEN EXCLUDED.last_drift_checked_at_unix_seconds
                        ELSE shardline_provider_repository_states.last_drift_checked_at_unix_seconds
                    END,
                    updated_at = now()",
            )
            .bind(state.provider().as_str())
            .bind(state.owner())
            .bind(state.repo())
            .bind(
                state
                    .last_access_changed_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
            )
            .bind(
                state
                    .last_revision_pushed_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
            )
            .bind(state.last_pushed_revision())
            .bind(
                state
                    .last_cache_invalidated_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
            )
            .bind(
                state
                    .last_authorization_rechecked_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
            )
            .bind(
                state
                    .last_drift_checked_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
            )
            .execute(&self.pool)
            .await?;
            Ok(())
        })
    }

    fn delete_provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let result = query(
                "DELETE FROM shardline_provider_repository_states
                 WHERE provider = $1 AND owner = $2 AND repo = $3",
            )
            .bind(provider.as_str())
            .bind(owner)
            .bind(repo)
            .execute(&self.pool)
            .await?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn prune_revisions_over_cap<'operation>(
        &'operation self,
        key: &'operation RepoKey,
        max_revisions: usize,
    ) -> IndexStoreFuture<'operation, u64, Self::Error> {
        let store = self.clone();
        let key = key.clone();
        Box::pin(
            async move { TreeStore::prune_revisions_over_cap(&store, &key, max_revisions).await },
        )
    }

    fn list_revision_repo_keys(&self) -> IndexStoreFuture<'_, Vec<RepoKey>, Self::Error> {
        let store = self.clone();
        Box::pin(async move { TreeStore::list_revision_repo_keys(&store).await })
    }
}

#[async_trait::async_trait]
impl UploadIntentStore for super::PostgresIndexStore {
    type Error = PostgresMetadataStoreError;

    async fn create_intent(&self, intent: &UploadIntent) -> Result<(), Self::Error> {
        let result = sqlx::query(
            "INSERT INTO shardline_upload_intents (
                intent_id, object_key, object_hash, object_length, state, created_at, updated_at
             )
             VALUES ($1, $2, $3, $4, $5, now(), now())
             ON CONFLICT (intent_id) DO UPDATE SET intent_id = EXCLUDED.intent_id
             WHERE shardline_upload_intents.object_key = EXCLUDED.object_key
               AND shardline_upload_intents.object_hash = EXCLUDED.object_hash
               AND shardline_upload_intents.object_length = EXCLUDED.object_length",
        )
        .bind(intent.intent_id())
        .bind(intent.object_key())
        .bind(intent.object_hash())
        .bind(intent.object_length() as i64)
        .bind(intent.state().as_str())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(crate::UploadIntentConflictError::new(intent.intent_id()).into());
        }
        Ok(())
    }

    async fn transition_intent(
        &self,
        intent_id: &str,
        new_state: UploadIntentState,
    ) -> Result<bool, Self::Error> {
        let current = self.intent_by_id(intent_id).await?;
        let Some(current) = current else {
            return Ok(false);
        };
        if current.state() == new_state {
            // Idempotent: already in the target state (concurrent duplicate
            // caller performing the same transition).
            return Ok(true);
        }
        if !current.state().can_transition_to(new_state) {
            return Ok(false);
        }
        let rows = sqlx::query(
            "UPDATE shardline_upload_intents SET state = $1, updated_at = now() WHERE intent_id = $2 AND state = $3"
        )
        .bind(new_state.as_str())
        .bind(intent_id)
        .bind(current.state().as_str())
        .execute(&self.pool)
        .await?;
        if rows.rows_affected() > 0 {
            return Ok(true);
        }
        // Race: a concurrent caller advanced the state between our read and the
        // conditional UPDATE, so zero rows matched. If the intent is now already
        // in the target state, the transition is effectively complete — report
        // success instead of a spurious invalid transition.
        let now = self.intent_by_id(intent_id).await?;
        Ok(now.is_some_and(|intent| intent.state() == new_state))
    }

    async fn intent_by_id(&self, intent_id: &str) -> Result<Option<UploadIntent>, Self::Error> {
        let row = sqlx::query_as::<_, (String, String, String, i64, String, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            "SELECT intent_id, object_key, object_hash, object_length, state, created_at, updated_at FROM shardline_upload_intents WHERE intent_id = $1"
        )
        .bind(intent_id)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some((id, key, hash, length, state_str, created, updated)) => {
                let state = UploadIntentState::parse(&state_str).ok_or_else(|| {
                    PostgresMetadataStoreError::InvalidUploadIntentState(state_str.clone())
                })?;
                let created_dur = std::time::Duration::from_secs(created.timestamp() as u64);
                let updated_dur = std::time::Duration::from_secs(updated.timestamp() as u64);
                Ok(Some(UploadIntent::from_parts(
                    id,
                    key,
                    hash,
                    length as u64,
                    state,
                    created_dur,
                    updated_dur,
                )))
            }
            None => Ok(None),
        }
    }

    async fn intents_by_state(
        &self,
        state: UploadIntentState,
    ) -> Result<Vec<UploadIntent>, Self::Error> {
        let rows = sqlx::query_as::<_, (String, String, String, i64, String, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            "SELECT intent_id, object_key, object_hash, object_length, state, created_at, updated_at FROM shardline_upload_intents WHERE state = $1 ORDER BY created_at"
        )
        .bind(state.as_str())
        .fetch_all(&self.pool)
        .await?;
        let intents = rows
            .into_iter()
            .map(|(id, key, hash, length, state_str, created, updated)| {
                let s = UploadIntentState::parse(&state_str).ok_or_else(|| {
                    PostgresMetadataStoreError::InvalidUploadIntentState(state_str.clone())
                })?;
                Ok(UploadIntent::from_parts(
                    id,
                    key,
                    hash,
                    length as u64,
                    s,
                    std::time::Duration::from_secs(created.timestamp() as u64),
                    std::time::Duration::from_secs(updated.timestamp() as u64),
                ))
            })
            .collect::<Result<Vec<_>, PostgresMetadataStoreError>>()?;
        Ok(intents)
    }

    async fn stale_intents(
        &self,
        state: UploadIntentState,
        older_than: std::time::Duration,
    ) -> Result<Vec<UploadIntent>, Self::Error> {
        let duration = chrono::Duration::from_std(older_than).map_err(|_e| {
            PostgresMetadataStoreError::InvalidUploadIntentState("invalid duration".into())
        })?;
        let cutoff = chrono::Utc::now()
            .checked_sub_signed(duration)
            .ok_or_else(|| {
                PostgresMetadataStoreError::InvalidUploadIntentState("invalid duration".into())
            })?;
        let rows = sqlx::query_as::<_, (String, String, String, i64, String, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            "SELECT intent_id, object_key, object_hash, object_length, state, created_at, updated_at FROM shardline_upload_intents WHERE state = $1 AND created_at < $2 ORDER BY created_at"
        )
        .bind(state.as_str())
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;
        let intents = rows
            .into_iter()
            .map(|(id, key, hash, length, state_str, created, updated)| {
                let s = UploadIntentState::parse(&state_str).ok_or_else(|| {
                    PostgresMetadataStoreError::InvalidUploadIntentState(state_str.clone())
                })?;
                Ok(UploadIntent::from_parts(
                    id,
                    key,
                    hash,
                    length as u64,
                    s,
                    std::time::Duration::from_secs(created.timestamp() as u64),
                    std::time::Duration::from_secs(updated.timestamp() as u64),
                ))
            })
            .collect::<Result<Vec<_>, PostgresMetadataStoreError>>()?;
        Ok(intents)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PostgresFileReconstructionRecord {
    terms: Vec<PostgresReconstructionTermRecord>,
}

impl PostgresFileReconstructionRecord {
    pub fn from_domain(reconstruction: &FileReconstruction) -> Self {
        Self {
            terms: reconstruction
                .terms()
                .iter()
                .map(PostgresReconstructionTermRecord::from_domain)
                .collect::<Vec<_>>(),
        }
    }

    pub fn into_domain(self) -> Result<FileReconstruction, PostgresMetadataStoreError> {
        let terms = self
            .terms
            .into_iter()
            .map(PostgresReconstructionTermRecord::into_domain)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FileReconstruction::new(terms))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostgresReconstructionTermRecord {
    object_hash: String,
    chunk_start: u32,
    chunk_end_exclusive: u32,
    unpacked_length: u64,
}

impl PostgresReconstructionTermRecord {
    fn from_domain(term: &ReconstructionTerm) -> Self {
        Self {
            object_hash: xet_hash_hex_string(term.object_id().hash()),
            chunk_start: term.chunk_range().start(),
            chunk_end_exclusive: term.chunk_range().end_exclusive(),
            unpacked_length: term.unpacked_length(),
        }
    }

    fn into_domain(self) -> Result<ReconstructionTerm, PostgresMetadataStoreError> {
        let hash = parse_xet_hash_hex(&self.object_hash)?;
        let range = ChunkRange::new(self.chunk_start, self.chunk_end_exclusive)?;
        Ok(ReconstructionTerm::new(
            StoredObjectId::new(hash),
            range,
            self.unpacked_length,
        ))
    }
}

fn quarantine_candidate_from_row(
    row: &PgRow,
) -> Result<QuarantineCandidate, PostgresMetadataStoreError> {
    let object_key = ObjectKey::parse(row.try_get::<String, _>("object_key")?.as_str())?;
    let observed_length = i64_to_u64(row.try_get::<i64, _>("observed_length")?)?;
    let first_seen = i64_to_u64(row.try_get::<i64, _>("first_seen_unreachable_at_unix_seconds")?)?;
    let delete_after = i64_to_u64(row.try_get::<i64, _>("delete_after_unix_seconds")?)?;
    QuarantineCandidate::new(object_key, observed_length, first_seen, delete_after)
        .map_err(PostgresMetadataStoreError::from)
}

fn dedupe_shard_mapping_from_row(
    row: &PgRow,
) -> Result<DedupeShardMapping, PostgresMetadataStoreError> {
    let chunk_hash = parse_xet_hash_hex(row.try_get::<String, _>("chunk_hash")?.as_str())?;
    let shard_object_key =
        ObjectKey::parse(row.try_get::<String, _>("shard_object_key")?.as_str())?;
    Ok(DedupeShardMapping::new(chunk_hash, shard_object_key))
}

fn retention_hold_from_row(row: &PgRow) -> Result<RetentionHold, PostgresMetadataStoreError> {
    let object_key = ObjectKey::parse(row.try_get::<String, _>("object_key")?.as_str())?;
    let reason = row.try_get::<String, _>("reason")?;
    let held_at_unix_seconds = i64_to_u64(row.try_get::<i64, _>("held_at_unix_seconds")?)?;
    let release_after_unix_seconds = row
        .try_get::<Option<i64>, _>("release_after_unix_seconds")?
        .map(i64_to_u64)
        .transpose()?;
    RetentionHold::new(
        object_key,
        reason,
        held_at_unix_seconds,
        release_after_unix_seconds,
    )
    .map_err(PostgresMetadataStoreError::from)
}

fn webhook_delivery_from_row(row: &PgRow) -> Result<WebhookDelivery, PostgresMetadataStoreError> {
    let provider_name = row.try_get::<String, _>("provider")?;
    let provider = parse_repository_provider(&provider_name, |_| {
        PostgresMetadataStoreError::WebhookDelivery(WebhookDeliveryError::InvalidProvider)
    })?;
    let owner = row.try_get::<String, _>("owner")?;
    let repo = row.try_get::<String, _>("repo")?;
    let delivery_id = row.try_get::<String, _>("delivery_id")?;
    let processed_at_unix_seconds =
        i64_to_u64(row.try_get::<i64, _>("processed_at_unix_seconds")?)?;
    WebhookDelivery::new(
        provider,
        owner,
        repo,
        delivery_id,
        processed_at_unix_seconds,
    )
    .map_err(PostgresMetadataStoreError::from)
}

fn provider_repository_state_from_row(
    row: &PgRow,
) -> Result<ProviderRepositoryState, PostgresMetadataStoreError> {
    let provider_name = row.try_get::<String, _>("provider")?;
    let provider = parse_repository_provider(&provider_name, |_| {
        PostgresMetadataStoreError::InvalidRepoType(provider_name.clone())
    })?;
    Ok(ProviderRepositoryState::new(
        provider,
        row.try_get("owner")?,
        row.try_get("repo")?,
        row.try_get::<Option<i64>, _>("last_access_changed_at_unix_seconds")?
            .map(i64_to_u64)
            .transpose()?,
        row.try_get::<Option<i64>, _>("last_revision_pushed_at_unix_seconds")?
            .map(i64_to_u64)
            .transpose()?,
        row.try_get("last_pushed_revision")?,
    )
    .with_reconciliation(
        row.try_get::<Option<i64>, _>("last_cache_invalidated_at_unix_seconds")?
            .map(i64_to_u64)
            .transpose()?,
        row.try_get::<Option<i64>, _>("last_authorization_rechecked_at_unix_seconds")?
            .map(i64_to_u64)
            .transpose()?,
        row.try_get::<Option<i64>, _>("last_drift_checked_at_unix_seconds")?
            .map(i64_to_u64)
            .transpose()?,
    ))
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_in_result,
        clippy::arithmetic_side_effects,
        clippy::option_if_let_else,
        clippy::unreachable,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use
    )]
    use std::{
        io::{Read, Write},
        net::{Shutdown, TcpListener, TcpStream},
        str::FromStr,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, JoinHandle},
        time::Duration,
    };

    use shardline_protocol::{ChunkRange, HashParseError, RepositoryProvider, ShardlineHash};
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};

    use super::{PostgresFileReconstructionRecord, PostgresReconstructionTermRecord};
    use crate::{
        AsyncIndexStore, FileReconstruction, ProviderRepositoryState, ReconstructionTerm,
        StoredObjectId,
    };

    struct CommitResponseLossProxy {
        port: u16,
        response_dropped: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl CommitResponseLossProxy {
        fn start(upstream: String) -> std::io::Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let port = listener.local_addr()?.port();
            let response_dropped = Arc::new(AtomicBool::new(false));
            let worker_response_dropped = Arc::clone(&response_dropped);
            let thread = thread::spawn(move || {
                let Ok((mut client, _address)) = listener.accept() else {
                    return;
                };
                let Ok(mut provider) = TcpStream::connect(upstream) else {
                    return;
                };
                let Ok(mut client_requests) = client.try_clone() else {
                    return;
                };
                let Ok(mut provider_requests) = provider.try_clone() else {
                    return;
                };
                let request_thread = thread::spawn(move || {
                    std::io::copy(&mut client_requests, &mut provider_requests).ok();
                });
                forward_postgres_responses(&mut provider, &mut client, &worker_response_dropped)
                    .ok();
                client.shutdown(Shutdown::Both).ok();
                provider.shutdown(Shutdown::Both).ok();
                request_thread.join().ok();
            });
            Ok(Self {
                port,
                response_dropped,
                thread: Some(thread),
            })
        }

        const fn port(&self) -> u16 {
            self.port
        }

        fn response_was_dropped(&self) -> bool {
            self.response_dropped.load(Ordering::Acquire)
        }
    }

    impl Drop for CommitResponseLossProxy {
        fn drop(&mut self) {
            if let Some(thread) = self.thread.take() {
                thread.join().unwrap();
            }
        }
    }

    fn forward_postgres_responses(
        provider: &mut TcpStream,
        client: &mut TcpStream,
        response_dropped: &AtomicBool,
    ) -> std::io::Result<()> {
        const MAX_BACKEND_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
        loop {
            let mut message_type = [0_u8; 1];
            match provider.read_exact(&mut message_type) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(error) => return Err(error),
            }
            let mut encoded_length = [0_u8; 4];
            provider.read_exact(&mut encoded_length)?;
            let encoded_length = u32::from_be_bytes(encoded_length);
            let payload_length = encoded_length.checked_sub(4).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid PostgreSQL backend message length",
                )
            })?;
            let payload_length = usize::try_from(payload_length)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            if payload_length > MAX_BACKEND_MESSAGE_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "PostgreSQL backend message exceeds proxy limit",
                ));
            }
            let mut payload = vec![0_u8; payload_length];
            provider.read_exact(&mut payload)?;
            if message_type == *b"C" && payload == b"COMMIT\0" {
                response_dropped.store(true, Ordering::Release);
                client.shutdown(Shutdown::Both)?;
                return Ok(());
            }
            client.write_all(&message_type)?;
            client.write_all(&encoded_length.to_be_bytes())?;
            client.write_all(&payload)?;
        }
    }

    // ------------------------------------------------------------------
    // PostgresReconstructionTermRecord: private type, tested in-module
    // ------------------------------------------------------------------
    #[test]
    fn reconstruction_term_record_roundtrips() {
        let hash = ShardlineHash::from_bytes([42; 32]);
        let range = ChunkRange::new(2, 5).unwrap();
        let term = ReconstructionTerm::new(StoredObjectId::new(hash), range, 512);

        let record = PostgresReconstructionTermRecord::from_domain(&term);
        assert_eq!(record.object_hash.len(), 64);
        assert_eq!(record.chunk_start, 2);
        assert_eq!(record.chunk_end_exclusive, 5);
        assert_eq!(record.unpacked_length, 512);

        let restored = record.into_domain().expect("valid reconstruction term");
        assert_eq!(restored, term);
    }

    #[test]
    fn reconstruction_term_record_invalid_hash_returns_error() {
        let record = PostgresReconstructionTermRecord {
            object_hash: "not-a-valid-hex-string".into(),
            chunk_start: 0,
            chunk_end_exclusive: 1,
            unpacked_length: 100,
        };
        let result = record.into_domain();
        assert!(result.is_err());
        // "not-a-valid-hex-string" has length 22 (< 64), so it fails with InvalidLength
        assert!(matches!(
            result,
            Err(super::PostgresMetadataStoreError::HashParse(
                HashParseError::InvalidLength
            ))
        ));
    }

    #[test]
    fn reconstruction_term_record_invalid_hash_char_returns_error() {
        // 64 characters, but contains uppercase
        let hex_hash = "A".repeat(64);
        let record = PostgresReconstructionTermRecord {
            object_hash: hex_hash,
            chunk_start: 0,
            chunk_end_exclusive: 1,
            unpacked_length: 100,
        };
        let result = record.into_domain();
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(super::PostgresMetadataStoreError::HashParse(
                HashParseError::InvalidCharacter(_)
            ))
        ));
    }

    #[test]
    fn reconstruction_term_record_invalid_range_returns_error() {
        let hex_hash = "a".repeat(64);
        let record = PostgresReconstructionTermRecord {
            object_hash: hex_hash,
            chunk_start: 5,
            chunk_end_exclusive: 3,
            unpacked_length: 100,
        };
        let result = record.into_domain();
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(super::PostgresMetadataStoreError::Range(_))
        ));
    }

    // ------------------------------------------------------------------
    // PostgresFileReconstructionRecord: pub(super) type
    // ------------------------------------------------------------------
    #[test]
    fn file_reconstruction_record_multiple_terms_roundtrips() {
        let hash_a = ShardlineHash::from_bytes([1; 32]);
        let hash_b = ShardlineHash::from_bytes([2; 32]);
        let range_a = ChunkRange::new(0, 1).unwrap();
        let range_b = ChunkRange::new(1, 3).unwrap();
        let reconstruction = FileReconstruction::new(vec![
            ReconstructionTerm::new(StoredObjectId::new(hash_a), range_a, 64),
            ReconstructionTerm::new(StoredObjectId::new(hash_b), range_b, 128),
        ]);
        let record = PostgresFileReconstructionRecord::from_domain(&reconstruction);
        let restored = record.into_domain().expect("valid reconstruction");
        assert_eq!(restored.terms().len(), 2);
    }

    #[test]
    fn file_reconstruction_record_empty_terms() {
        let reconstruction = FileReconstruction::new(vec![]);
        let record = PostgresFileReconstructionRecord::from_domain(&reconstruction);
        let restored = record.into_domain().expect("empty terms is valid");
        assert!(restored.terms().is_empty());
    }

    #[test]
    fn file_reconstruction_record_invalid_hash_in_terms_returns_error() {
        let record = PostgresFileReconstructionRecord {
            terms: vec![PostgresReconstructionTermRecord {
                object_hash: "bad".into(),
                chunk_start: 0,
                chunk_end_exclusive: 1,
                unpacked_length: 0,
            }],
        };
        let result = record.into_domain();
        assert!(matches!(
            result,
            Err(super::PostgresMetadataStoreError::HashParse(_))
        ));
    }

    // ── Postgres UploadIntentStore integration tests ──────────────────────

    use crate::upload_intent::{UploadIntent, UploadIntentState, UploadIntentStore};
    async fn connect_postgres() -> Option<sqlx::PgPool> {
        let url = std::env::var("DATABASE_URL").ok()?;
        sqlx::PgPool::connect(&url).await.ok()
    }

    fn postgres_upstream(database_url: &str) -> Option<String> {
        let parsed = url::Url::parse(database_url).ok()?;
        let host = parsed.host_str()?;
        let port = parsed.port().unwrap_or(5432);
        Some(format!("{host}:{port}"))
    }

    fn make_pg_store(pool: sqlx::PgPool) -> crate::PostgresIndexStore {
        crate::PostgresIndexStore::new(pool)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_provider_repository_state_concurrent_partial_updates_are_merged() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        sqlx::query(
            "DELETE FROM shardline_provider_repository_states
             WHERE provider = $1 AND owner = $2 AND repo = $3",
        )
        .bind(RepositoryProvider::GitHub.as_str())
        .bind("concurrent-team")
        .bind("concurrent-state")
        .execute(&pool)
        .await
        .expect("clean provider state fixture");

        let access_store = make_pg_store(pool.clone());
        let revision_store = make_pg_store(pool);
        let access = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "concurrent-team".into(),
            "concurrent-state".into(),
            Some(150),
            None,
            None,
        )
        .with_reconciliation(Some(170), None, Some(190));
        let revision = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "concurrent-team".into(),
            "concurrent-state".into(),
            None,
            Some(200),
            Some("refs/heads/main".into()),
        )
        .with_reconciliation(None, Some(180), None);

        let (access_result, revision_result) = tokio::join!(
            access_store.upsert_provider_repository_state(&access),
            revision_store.upsert_provider_repository_state(&revision),
        );
        access_result.expect("access state upsert");
        revision_result.expect("revision state upsert");

        let loaded = access_store
            .provider_repository_state(
                RepositoryProvider::GitHub,
                "concurrent-team",
                "concurrent-state",
            )
            .await
            .expect("load merged provider state")
            .expect("merged provider state");
        assert_eq!(loaded.last_access_changed_at_unix_seconds(), Some(150));
        assert_eq!(loaded.last_revision_pushed_at_unix_seconds(), Some(200));
        assert_eq!(loaded.last_pushed_revision(), Some("refs/heads/main"));
        assert_eq!(loaded.last_cache_invalidated_at_unix_seconds(), Some(170));
        assert_eq!(
            loaded.last_authorization_rechecked_at_unix_seconds(),
            Some(180)
        );
        assert_eq!(loaded.last_drift_checked_at_unix_seconds(), Some(190));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_create_and_retrieve() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = make_pg_store(pool);
        let intent = UploadIntent::new(
            "test-intent-1".into(),
            "test/key".into(),
            "ab".repeat(32),
            128,
        );
        store.create_intent(&intent).await.expect("create_intent");
        let loaded = store
            .intent_by_id("test-intent-1")
            .await
            .expect("intent_by_id");
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.intent_id(), "test-intent-1");
        assert_eq!(loaded.state(), UploadIntentState::Created);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_create_idempotent() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = make_pg_store(pool);
        let intent = UploadIntent::new(
            "test-intent-idempotent".into(),
            "test/key2".into(),
            "cd".repeat(32),
            64,
        );
        store.create_intent(&intent).await.expect("first create");
        // Second create with same ID should not error (ON CONFLICT DO NOTHING)
        store
            .create_intent(&intent)
            .await
            .expect("second create (idempotent)");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_recovers_when_commit_response_is_lost() {
        let Ok(database_url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let Some(direct_pool) = connect_postgres().await else {
            eprintln!("skipping: cannot connect to DATABASE_URL");
            return;
        };
        let intent = UploadIntent::new(
            "test-intent-lost-commit-response".into(),
            "test/lost-commit-response".into(),
            "9a".repeat(32),
            4096,
        );
        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind(intent.intent_id())
            .execute(&direct_pool)
            .await
            .expect("clean commit-response-loss fixture");

        let upstream = postgres_upstream(&database_url).expect("Postgres upstream address");
        let proxy = CommitResponseLossProxy::start(upstream).expect("start Postgres fault proxy");
        let connect_options = PgConnectOptions::from_str(&database_url)
            .expect("parse DATABASE_URL")
            .host("127.0.0.1")
            .port(proxy.port())
            .ssl_mode(PgSslMode::Disable);
        let proxy_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .expect("connect through Postgres fault proxy");
        let mut transaction = proxy_pool.begin().await.expect("begin transaction");
        sqlx::query(
            "INSERT INTO shardline_upload_intents (
                intent_id, object_key, object_hash, object_length, state, created_at, updated_at
             ) VALUES ($1, $2, $3, $4, $5, now(), now())",
        )
        .bind(intent.intent_id())
        .bind(intent.object_key())
        .bind(intent.object_hash())
        .bind(intent.object_length() as i64)
        .bind(intent.state().as_str())
        .execute(&mut *transaction)
        .await
        .expect("insert upload intent before ambiguous COMMIT");

        let commit = transaction.commit().await;
        assert!(
            proxy.response_was_dropped(),
            "proxy must observe PostgreSQL commit before dropping its completion message"
        );
        assert!(
            commit.is_err(),
            "client must see an ambiguous outcome when COMMIT completion is lost"
        );
        proxy_pool.close().await;

        let store = make_pg_store(direct_pool.clone());
        store
            .create_intent(&intent)
            .await
            .expect("idempotent retry after ambiguous COMMIT");
        let loaded = store
            .intent_by_id(intent.intent_id())
            .await
            .expect("read durable intent after ambiguous COMMIT")
            .expect("committed intent exists");
        assert_eq!(loaded.object_key(), intent.object_key());
        assert_eq!(loaded.object_hash(), intent.object_hash());
        assert_eq!(loaded.object_length(), intent.object_length());
        assert_eq!(loaded.state(), UploadIntentState::Created);

        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind(intent.intent_id())
            .execute(&direct_pool)
            .await
            .expect("clean commit-response-loss fixture");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_pool_exhaustion_is_bounded_and_recovers() {
        let Ok(database_url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(100))
            .connect(&database_url)
            .await
            .expect("connect bounded Postgres pool");
        let held_connection = pool.acquire().await.expect("hold only pool connection");
        let exhausted = pool.acquire().await;
        assert!(
            matches!(exhausted, Err(sqlx::Error::PoolTimedOut)),
            "exhausted pool must fail within its configured bound: {exhausted:?}"
        );
        drop(held_connection);
        let recovered = pool.acquire().await;
        assert!(
            recovered.is_ok(),
            "pool must recover after capacity is released: {recovered:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_statement_timeout_is_retryable_after_lock_release() {
        let Ok(database_url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(|connection, _metadata| {
                Box::pin(async move {
                    sqlx::query("SET statement_timeout = '100ms'")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(&database_url)
            .await
            .expect("connect statement-timeout Postgres pool");
        let store = make_pg_store(pool.clone());
        let intent = UploadIntent::new(
            "test-intent-statement-timeout".into(),
            "test/statement-timeout".into(),
            "7b".repeat(32),
            2048,
        );
        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind(intent.intent_id())
            .execute(&pool)
            .await
            .expect("clean statement-timeout fixture");
        store
            .create_intent(&intent)
            .await
            .expect("create statement-timeout fixture");

        let mut lock_transaction = pool.begin().await.expect("begin row-lock transaction");
        sqlx::query(
            "SELECT intent_id FROM shardline_upload_intents WHERE intent_id = $1 FOR UPDATE",
        )
        .bind(intent.intent_id())
        .fetch_one(&mut *lock_transaction)
        .await
        .expect("lock upload-intent row");
        let timed_out = store
            .transition_intent(intent.intent_id(), UploadIntentState::Storing)
            .await;
        assert!(
            matches!(timed_out, Err(super::PostgresMetadataStoreError::Sqlx(_))),
            "blocked transition must surface the statement timeout: {timed_out:?}"
        );
        lock_transaction
            .rollback()
            .await
            .expect("release upload-intent row lock");

        assert!(
            store
                .transition_intent(intent.intent_id(), UploadIntentState::Storing)
                .await
                .expect("retry transition after lock release"),
            "retry must advance the intent after the transient lock clears"
        );
        let loaded = store
            .intent_by_id(intent.intent_id())
            .await
            .expect("load transitioned upload intent")
            .expect("transitioned upload intent exists");
        assert_eq!(loaded.state(), UploadIntentState::Storing);

        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind(intent.intent_id())
            .execute(&pool)
            .await
            .expect("clean statement-timeout fixture");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_rejects_id_reuse_for_different_object() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind("test-intent-conflict")
            .execute(&pool)
            .await
            .expect("clean leftover intent");
        let store = make_pg_store(pool);
        let original = UploadIntent::new(
            "test-intent-conflict".into(),
            "test/original".into(),
            "ab".repeat(32),
            64,
        );
        let conflicting = UploadIntent::new(
            "test-intent-conflict".into(),
            "test/conflicting".into(),
            "cd".repeat(32),
            128,
        );
        store.create_intent(&original).await.unwrap();
        store.create_intent(&original).await.unwrap();
        assert!(matches!(
            store.create_intent(&conflicting).await,
            Err(super::PostgresMetadataStoreError::UploadIntentConflict(_))
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_transition_to_visible() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        // Robust against a persistent Postgres: a previous run may have left
        // this fixed-id intent in a terminal state, which would make the forward
        // transition chain below fail. Clean the row so the test starts fresh
        // (CI's ephemeral database does not surface this).
        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind("test-intent-transition")
            .execute(&pool)
            .await
            .expect("clean leftover intent");
        let store = make_pg_store(pool);
        let intent = UploadIntent::new(
            "test-intent-transition".into(),
            "test/key3".into(),
            "ef".repeat(32),
            256,
        );
        store.create_intent(&intent).await.expect("create_intent");
        for state in [
            UploadIntentState::Storing,
            UploadIntentState::Stored,
            UploadIntentState::MetadataCommitted,
            UploadIntentState::Visible,
        ] {
            let transitioned = store
                .transition_intent("test-intent-transition", state)
                .await
                .expect("transition_intent");
            assert!(transitioned, "transition to {state:?} should succeed");
        }
        let loaded = store
            .intent_by_id("test-intent-transition")
            .await
            .expect("intent_by_id");
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().state(), UploadIntentState::Visible);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_transition_missing_returns_false() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = make_pg_store(pool);
        let result = store
            .transition_intent("nonexistent-intent", UploadIntentState::Failed)
            .await
            .expect("transition_intent");
        assert!(!result, "transitioning missing intent should return false");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_query_by_state() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = make_pg_store(pool);
        // Create two intents with different states
        let a = UploadIntent::new("query-state-a".into(), "test/a".into(), "01".repeat(32), 10);
        let b = UploadIntent::new("query-state-b".into(), "test/b".into(), "02".repeat(32), 20);
        store.create_intent(&a).await.expect("create a");
        store.create_intent(&b).await.expect("create b");
        store
            .transition_intent("query-state-b", UploadIntentState::Failed)
            .await
            .expect("transition b");

        let created = store
            .intents_by_state(UploadIntentState::Created)
            .await
            .expect("query created");
        let failed = store
            .intents_by_state(UploadIntentState::Failed)
            .await
            .expect("query failed");
        assert!(
            created.iter().any(|i| i.intent_id() == "query-state-a"),
            "a should be in Created"
        );
        assert!(
            failed.iter().any(|i| i.intent_id() == "query-state-b"),
            "b should be in Failed"
        );
    }
}
