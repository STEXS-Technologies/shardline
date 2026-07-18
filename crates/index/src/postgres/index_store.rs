use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use shardline_protocol::{ChunkRange, RepositoryProvider, ShardlineHash};
use shardline_storage::ObjectKey;
use sqlx::{Row, postgres::PgRow, query, query_scalar, types::Json};

use super::{PostgresMetadataStoreError, i64_to_u64, u64_to_i64};
use crate::{
    AsyncIndexStore, DedupeShardMapping, FileId, FileReconstruction, IndexStoreFuture,
    ProviderRepositoryState, QuarantineCandidate, ReconstructionTerm, RetentionHold,
    StoredObjectId, WebhookDelivery, WebhookDeliveryError, parse_xet_hash_hex,
    provider::parse_repository_provider, xet_hash_hex_string,
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
                    last_access_changed_at_unix_seconds = EXCLUDED.last_access_changed_at_unix_seconds,
                    last_revision_pushed_at_unix_seconds = EXCLUDED.last_revision_pushed_at_unix_seconds,
                    last_pushed_revision = EXCLUDED.last_pushed_revision,
                    last_cache_invalidated_at_unix_seconds = EXCLUDED.last_cache_invalidated_at_unix_seconds,
                    last_authorization_rechecked_at_unix_seconds = EXCLUDED.last_authorization_rechecked_at_unix_seconds,
                    last_drift_checked_at_unix_seconds = EXCLUDED.last_drift_checked_at_unix_seconds,
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
    use shardline_protocol::{ChunkRange, HashParseError, ShardlineHash};

    use super::{PostgresFileReconstructionRecord, PostgresReconstructionTermRecord};
    use crate::{FileReconstruction, ReconstructionTerm, StoredObjectId, XorbId};

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
}
