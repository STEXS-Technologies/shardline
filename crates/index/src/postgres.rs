use std::time::Duration;

use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Error as JsonError, to_vec};
use shardline_protocol::{
    ChunkRange, HashParseError, RangeError, RepositoryProvider, ShardlineHash,
};
use shardline_storage::{ObjectKey, ObjectKeyError};
use sqlx::{
    Error as SqlxError, PgPool, Postgres, Row, Transaction, postgres::PgRow, query, query_scalar,
    types::Json,
};
use thiserror::Error;

use crate::{
    AsyncIndexStore, DedupeShardMapping, FileId, FileReconstruction, FileRecord, IndexStoreFuture,
    ProviderRepositoryState, QuarantineCandidate, QuarantineCandidateError, ReconstructionTerm,
    RecordStore, RecordStoreFuture, RepositoryRecordScope, RetentionHold, RetentionHoldError,
    StoredObjectId, StoredRecord, WebhookDelivery, WebhookDeliveryError, parse_xet_hash_hex,
    provider::parse_repository_provider,
    record_key::record_key as shared_record_key,
    record_key::{
        repository_record_scope_key as shared_repository_record_scope_key,
        repository_scope_key as shared_repository_scope_key,
    },
    xet_hash_hex_string,
};

/// Postgres-compatible implementation of the asynchronous index-store contract.
#[derive(Debug, Clone)]
pub struct PostgresIndexStore {
    pool: PgPool,
}

impl PostgresIndexStore {
    /// Creates a Postgres index-store adapter from an existing pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Returns the underlying connection pool.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

impl AsyncIndexStore for PostgresIndexStore {
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

/// Opaque Postgres file-record locator.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PostgresRecordLocator {
    record_key: String,
    kind: PostgresRecordKind,
    scope_key: String,
    file_id: String,
    content_hash: Option<String>,
}

impl PostgresRecordLocator {
    /// Returns the stable record key for this locator.
    #[must_use]
    pub fn record_key(&self) -> &str {
        &self.record_key
    }

    /// Returns the file identifier associated with this locator.
    #[must_use]
    pub fn file_id(&self) -> &str {
        &self.file_id
    }

    /// Returns the immutable content hash when this locator points at a version record.
    #[must_use]
    pub fn content_hash(&self) -> Option<&str> {
        self.content_hash.as_deref()
    }
}

/// Postgres-compatible implementation of the record-store contract.
#[derive(Debug, Clone)]
pub struct PostgresRecordStore {
    pool: PgPool,
}

impl PostgresRecordStore {
    /// Creates a Postgres record-store adapter from an existing pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Returns the underlying connection pool.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Inserts or replaces an immutable version record.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresMetadataStoreError`] when the row cannot be written.
    pub async fn insert_version_record(
        &self,
        record: &FileRecord,
    ) -> Result<(), PostgresMetadataStoreError> {
        let locator = self.version_record_locator(record);
        self.upsert_record(&locator, record).await
    }

    /// Atomically commits one file-version record and its latest-file alias.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresMetadataStoreError`] when either row cannot be written. No
    /// latest-file alias is made visible unless the immutable version record is also
    /// committed.
    pub async fn commit_file_version_metadata(
        &self,
        record: &FileRecord,
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut transaction = self.pool.begin().await?;
        let version_locator = self.version_record_locator(record);
        upsert_record_in_transaction(&mut transaction, &version_locator, record).await?;
        let latest_locator = self.latest_record_locator(record);
        upsert_record_in_transaction(&mut transaction, &latest_locator, record).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Atomically commits native shard metadata.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresMetadataStoreError`] when any row in the metadata set cannot
    /// be written. No visible latest-file record or dedupe-shard mapping is committed
    /// unless the full set commits.
    pub async fn commit_native_shard_metadata(
        &self,
        records: &[FileRecord],
        dedupe_mappings: &[DedupeShardMapping],
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut transaction = self.pool.begin().await?;
        for record in records {
            let locator = self.version_record_locator(record);
            upsert_record_in_transaction(&mut transaction, &locator, record).await?;
        }
        for mapping in dedupe_mappings {
            upsert_dedupe_shard_mapping_in_transaction(&mut transaction, mapping).await?;
        }
        for record in records {
            let locator = self.latest_record_locator(record);
            upsert_record_in_transaction(&mut transaction, &locator, record).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn upsert_record(
        &self,
        locator: &PostgresRecordLocator,
        record: &FileRecord,
    ) -> Result<(), PostgresMetadataStoreError> {
        query(
            "INSERT INTO shardline_file_records (
                record_key,
                record_kind,
                scope_key,
                file_id,
                content_hash,
                record
             )
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (record_key)
             DO UPDATE SET
                record_kind = EXCLUDED.record_kind,
                scope_key = EXCLUDED.scope_key,
                file_id = EXCLUDED.file_id,
                content_hash = EXCLUDED.content_hash,
                record = EXCLUDED.record,
                updated_at = now()",
        )
        .bind(&locator.record_key)
        .bind(locator.kind.as_str())
        .bind(&locator.scope_key)
        .bind(&locator.file_id)
        .bind(&record.content_hash)
        .bind(Json(record.clone()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn read_record(
        &self,
        locator: &PostgresRecordLocator,
    ) -> Result<Option<FileRecord>, PostgresMetadataStoreError> {
        let row = query("SELECT record FROM shardline_file_records WHERE record_key = $1")
            .bind(&locator.record_key)
            .fetch_optional(&self.pool)
            .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let Json(record) = row.try_get::<Json<FileRecord>, _>("record")?;
        Ok(Some(record))
    }
}

async fn upsert_record_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    locator: &PostgresRecordLocator,
    record: &FileRecord,
) -> Result<(), PostgresMetadataStoreError> {
    query(
        "INSERT INTO shardline_file_records (
            record_key,
            record_kind,
            scope_key,
            file_id,
            content_hash,
            record
         )
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (record_key)
         DO UPDATE SET
            record_kind = EXCLUDED.record_kind,
            scope_key = EXCLUDED.scope_key,
            file_id = EXCLUDED.file_id,
            content_hash = EXCLUDED.content_hash,
            record = EXCLUDED.record,
            updated_at = now()",
    )
    .bind(&locator.record_key)
    .bind(locator.kind.as_str())
    .bind(&locator.scope_key)
    .bind(&locator.file_id)
    .bind(&record.content_hash)
    .bind(Json(record.clone()))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn upsert_dedupe_shard_mapping_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    mapping: &DedupeShardMapping,
) -> Result<(), PostgresMetadataStoreError> {
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
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

impl RecordStore for PostgresRecordStore {
    type Error = PostgresMetadataStoreError;
    type Locator = PostgresRecordLocator;

    fn list_latest_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move { self.list_record_locators(PostgresRecordKind::Latest).await })
    }

    fn visit_latest_record_locators<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(Self::Locator) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            self.visit_record_locators(PostgresRecordKind::Latest, &mut visitor)
                .await
        })
    }

    fn visit_latest_records<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            self.visit_records(PostgresRecordKind::Latest, &mut visitor)
                .await
        })
    }

    fn list_version_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move { self.list_record_locators(PostgresRecordKind::Version).await })
    }

    fn list_repository_latest_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            self.list_repository_record_locators(PostgresRecordKind::Latest, repository)
                .await
        })
    }

    fn visit_repository_latest_records<'operation, Visitor, VisitorError>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            self.visit_repository_records(PostgresRecordKind::Latest, repository, &mut visitor)
                .await
        })
    }

    fn list_repository_version_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            self.list_repository_record_locators(PostgresRecordKind::Version, repository)
                .await
        })
    }

    fn visit_version_record_locators<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(Self::Locator) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            self.visit_record_locators(PostgresRecordKind::Version, &mut visitor)
                .await
        })
    }

    fn visit_version_records<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            self.visit_records(PostgresRecordKind::Version, &mut visitor)
                .await
        })
    }

    fn visit_repository_version_records<'operation, Visitor, VisitorError>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            self.visit_repository_records(PostgresRecordKind::Version, repository, &mut visitor)
                .await
        })
    }

    fn read_record_bytes<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Vec<u8>, Self::Error> {
        Box::pin(async move {
            let record = self
                .read_record(locator)
                .await?
                .ok_or(PostgresMetadataStoreError::RecordNotFound)?;
            Ok(to_vec(&record)?)
        })
    }

    fn read_latest_record_bytes<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, Option<Vec<u8>>, Self::Error> {
        Box::pin(async move {
            let locator = self.latest_record_locator(record);
            self.read_record(&locator)
                .await?
                .map(|stored_record| {
                    to_vec(&stored_record).map_err(PostgresMetadataStoreError::from)
                })
                .transpose()
        })
    }

    fn write_version_record<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let locator = self.version_record_locator(record);
            self.upsert_record(&locator, record).await
        })
    }

    fn write_latest_record<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let locator = self.latest_record_locator(record);
            self.upsert_record(&locator, record).await
        })
    }

    fn delete_record_locator<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let result = query("DELETE FROM shardline_file_records WHERE record_key = $1")
                .bind(&locator.record_key)
                .execute(&self.pool)
                .await?;
            if result.rows_affected() > 0 {
                return Ok(());
            }

            Err(PostgresMetadataStoreError::RecordNotFound)
        })
    }

    fn record_locator_exists<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let exists = query_scalar::<_, bool>(
                "SELECT EXISTS(
                    SELECT 1 FROM shardline_file_records WHERE record_key = $1
                 )",
            )
            .bind(&locator.record_key)
            .fetch_one(&self.pool)
            .await?;
            Ok(exists)
        })
    }

    fn prune_empty_latest_records(&self) -> RecordStoreFuture<'_, (), Self::Error> {
        Box::pin(async move { Ok(()) })
    }

    fn modified_since_epoch<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Duration, Self::Error> {
        Box::pin(async move {
            let value = query_scalar::<_, i64>(
                "SELECT FLOOR(EXTRACT(EPOCH FROM updated_at))::BIGINT
                 FROM shardline_file_records
                 WHERE record_key = $1",
            )
            .bind(&locator.record_key)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(PostgresMetadataStoreError::RecordNotFound)?;
            Ok(Duration::from_secs(i64_to_u64(value)?))
        })
    }

    fn latest_record_locator(&self, record: &FileRecord) -> Self::Locator {
        record_locator(PostgresRecordKind::Latest, record, None)
    }

    fn version_record_locator(&self, record: &FileRecord) -> Self::Locator {
        record_locator(
            PostgresRecordKind::Version,
            record,
            Some(record.content_hash.clone()),
        )
    }
}

impl PostgresRecordStore {
    async fn list_record_locators(
        &self,
        kind: PostgresRecordKind,
    ) -> Result<Vec<PostgresRecordLocator>, PostgresMetadataStoreError> {
        let rows = query(
            "SELECT record_key, record_kind, scope_key, file_id, content_hash
             FROM shardline_file_records
             WHERE record_kind = $1
             ORDER BY record_key",
        )
        .bind(kind.as_str())
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(record_locator_from_row)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn visit_record_locators<Visitor, VisitorError>(
        &self,
        kind: PostgresRecordKind,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        PostgresMetadataStoreError: Into<VisitorError>,
        Visitor: FnMut(PostgresRecordLocator) -> Result<(), VisitorError>,
    {
        let mut rows = query(
            "SELECT record_key, record_kind, scope_key, file_id, content_hash
             FROM shardline_file_records
             WHERE record_kind = $1
             ORDER BY record_key",
        )
        .bind(kind.as_str())
        .fetch(&self.pool);

        while let Some(row) = rows
            .try_next()
            .await
            .map_err(PostgresMetadataStoreError::from)
            .map_err(Into::into)?
        {
            visitor(record_locator_from_row(&row).map_err(Into::into)?)?;
        }

        Ok(())
    }

    async fn visit_records<Visitor, VisitorError>(
        &self,
        kind: PostgresRecordKind,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        PostgresMetadataStoreError: Into<VisitorError>,
        Visitor: FnMut(StoredRecord<PostgresRecordLocator>) -> Result<(), VisitorError>,
    {
        let mut rows = query(
            "SELECT record_key,
                    record_kind,
                    scope_key,
                    file_id,
                    content_hash,
                    record,
                    FLOOR(EXTRACT(EPOCH FROM updated_at))::BIGINT AS modified_since_epoch
             FROM shardline_file_records
             WHERE record_kind = $1
             ORDER BY record_key",
        )
        .bind(kind.as_str())
        .fetch(&self.pool);

        while let Some(row) = rows
            .try_next()
            .await
            .map_err(PostgresMetadataStoreError::from)
            .map_err(Into::into)?
        {
            visitor(stored_record_from_row(&row).map_err(Into::into)?)?;
        }

        Ok(())
    }

    async fn list_repository_record_locators(
        &self,
        kind: PostgresRecordKind,
        repository: &RepositoryRecordScope,
    ) -> Result<Vec<PostgresRecordLocator>, PostgresMetadataStoreError> {
        let scope_key = shared_repository_record_scope_key(repository);
        let scope_prefix = format!("{scope_key}%");
        let rows = query(
            "SELECT record_key, record_kind, scope_key, file_id, content_hash
             FROM shardline_file_records
             WHERE record_kind = $1
               AND (scope_key = $2 OR scope_key LIKE $3)
             ORDER BY record_key",
        )
        .bind(kind.as_str())
        .bind(&scope_key)
        .bind(&scope_prefix)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(record_locator_from_row)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn visit_repository_records<Visitor, VisitorError>(
        &self,
        kind: PostgresRecordKind,
        repository: &RepositoryRecordScope,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        PostgresMetadataStoreError: Into<VisitorError>,
        Visitor: FnMut(StoredRecord<PostgresRecordLocator>) -> Result<(), VisitorError>,
    {
        let scope_key = shared_repository_record_scope_key(repository);
        let scope_prefix = format!("{scope_key}%");
        let mut rows = query(
            "SELECT record_key,
                    record_kind,
                    scope_key,
                    file_id,
                    content_hash,
                    record,
                    FLOOR(EXTRACT(EPOCH FROM updated_at))::BIGINT AS modified_since_epoch
             FROM shardline_file_records
             WHERE record_kind = $1
               AND (scope_key = $2 OR scope_key LIKE $3)
             ORDER BY record_key",
        )
        .bind(kind.as_str())
        .bind(&scope_key)
        .bind(&scope_prefix)
        .fetch(&self.pool);

        while let Some(row) = rows
            .try_next()
            .await
            .map_err(PostgresMetadataStoreError::from)
            .map_err(Into::into)?
        {
            visitor(stored_record_from_row(&row).map_err(Into::into)?)?;
        }

        Ok(())
    }
}

/// Postgres metadata-store failure.
#[derive(Debug, Error)]
pub enum PostgresMetadataStoreError {
    /// Postgres access failed.
    #[error("postgres metadata store operation failed")]
    Sqlx(#[source] Box<SqlxError>),
    /// JSON serialization or deserialization failed.
    #[error("postgres metadata json operation failed")]
    Json(#[from] JsonError),
    /// A stored hash value was invalid.
    #[error("stored hash value was invalid")]
    HashParse(#[from] HashParseError),
    /// A stored object key was invalid.
    #[error("stored object key was invalid")]
    ObjectKey(#[from] ObjectKeyError),
    /// A stored chunk range was invalid.
    #[error("stored chunk range was invalid")]
    Range(#[from] RangeError),
    /// A stored retention hold was invalid.
    #[error("stored retention hold was invalid")]
    RetentionHold(#[from] RetentionHoldError),
    /// A stored quarantine candidate was invalid.
    #[error("stored quarantine candidate was invalid")]
    QuarantineCandidate(#[from] QuarantineCandidateError),
    /// A stored webhook delivery was invalid.
    #[error("stored webhook delivery was invalid")]
    WebhookDelivery(#[from] WebhookDeliveryError),
    /// A stored integer exceeded the supported range.
    #[error("stored integer exceeded the supported range")]
    IntegerOutOfRange,
    /// The requested record locator does not exist.
    #[error("postgres record locator was not found")]
    RecordNotFound,
    /// A stored record kind was invalid.
    #[error("stored record kind was invalid")]
    InvalidRecordKind,
}

impl From<SqlxError> for PostgresMetadataStoreError {
    fn from(value: SqlxError) -> Self {
        Self::Sqlx(Box::new(value))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostgresFileReconstructionRecord {
    terms: Vec<PostgresReconstructionTermRecord>,
}

impl PostgresFileReconstructionRecord {
    fn from_domain(reconstruction: &FileReconstruction) -> Self {
        Self {
            terms: reconstruction
                .terms()
                .iter()
                .map(PostgresReconstructionTermRecord::from_domain)
                .collect::<Vec<_>>(),
        }
    }

    fn into_domain(self) -> Result<FileReconstruction, PostgresMetadataStoreError> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum PostgresRecordKind {
    Latest,
    Version,
}

impl PostgresRecordKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::Version => "version",
        }
    }

    fn parse(value: &str) -> Result<Self, PostgresMetadataStoreError> {
        match value {
            "latest" => Ok(Self::Latest),
            "version" => Ok(Self::Version),
            _other => Err(PostgresMetadataStoreError::InvalidRecordKind),
        }
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
    let provider = parse_repository_provider(&provider_name, || {
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
    let provider = parse_repository_provider(&provider_name, || {
        PostgresMetadataStoreError::WebhookDelivery(WebhookDeliveryError::InvalidProvider)
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

fn record_locator_from_row(
    row: &PgRow,
) -> Result<PostgresRecordLocator, PostgresMetadataStoreError> {
    let kind = PostgresRecordKind::parse(row.try_get::<String, _>("record_kind")?.as_str())?;
    let content_hash = match kind {
        PostgresRecordKind::Latest => None,
        PostgresRecordKind::Version => Some(row.try_get::<String, _>("content_hash")?),
    };
    Ok(PostgresRecordLocator {
        record_key: row.try_get("record_key")?,
        kind,
        scope_key: row.try_get("scope_key")?,
        file_id: row.try_get("file_id")?,
        content_hash,
    })
}

fn stored_record_from_row(
    row: &PgRow,
) -> Result<StoredRecord<PostgresRecordLocator>, PostgresMetadataStoreError> {
    let locator = record_locator_from_row(row)?;
    let Json(record) = row.try_get::<Json<FileRecord>, _>("record")?;
    let modified_since_epoch =
        Duration::from_secs(i64_to_u64(row.try_get::<i64, _>("modified_since_epoch")?)?);
    Ok(StoredRecord {
        locator,
        bytes: to_vec(&record)?,
        modified_since_epoch,
    })
}

fn record_locator(
    kind: PostgresRecordKind,
    record: &FileRecord,
    content_hash: Option<String>,
) -> PostgresRecordLocator {
    let scope_key = shared_repository_scope_key(record.repository_scope.as_ref());
    let record_key = shared_record_key(
        kind.as_str(),
        &scope_key,
        &record.file_id,
        content_hash.as_deref(),
    );
    PostgresRecordLocator {
        record_key,
        kind,
        scope_key,
        file_id: record.file_id.clone(),
        content_hash,
    }
}

fn u64_to_i64(value: u64) -> Result<i64, PostgresMetadataStoreError> {
    i64::try_from(value).map_err(|_error| PostgresMetadataStoreError::IntegerOutOfRange)
}

fn i64_to_u64(value: i64) -> Result<u64, PostgresMetadataStoreError> {
    u64::try_from(value).map_err(|_error| PostgresMetadataStoreError::IntegerOutOfRange)
}

#[cfg(test)]
mod tests {
    use shardline_protocol::{ChunkRange, RepositoryProvider, RepositoryScope, ShardlineHash};

    use super::{PostgresFileReconstructionRecord, PostgresRecordKind};
    use crate::{
        FileId, FileReconstruction, FileRecord, ReconstructionTerm, RepositoryRecordScope, XorbId,
        record_key::record_key as shared_record_key,
        record_key::{
            repository_record_scope_key as shared_repository_record_scope_key,
            repository_scope_key as shared_repository_scope_key,
        },
        xet_hash_hex_string,
    };

    #[test]
    fn postgres_reconstruction_record_roundtrips_domain_terms() {
        let hash = ShardlineHash::from_bytes([11; 32]);
        let range = ChunkRange::new(1, 4);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };
        let reconstruction =
            FileReconstruction::new(vec![ReconstructionTerm::new(XorbId::new(hash), range, 256)]);

        let record = PostgresFileReconstructionRecord::from_domain(&reconstruction);
        let restored = record.into_domain();

        assert!(matches!(restored, Ok(ref restored) if restored == &reconstruction));
    }

    #[test]
    fn postgres_record_keys_distinguish_scope_file_and_kind_without_parsing() {
        let first_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team:a", "asset", Some("main"));
        let second_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "a:asset", Some("main"));
        assert!(first_scope.is_ok());
        assert!(second_scope.is_ok());
        let (Ok(first_scope), Ok(second_scope)) = (first_scope, second_scope) else {
            return;
        };

        let first_key = shared_repository_scope_key(Some(&first_scope));
        let second_key = shared_repository_scope_key(Some(&second_scope));

        assert_ne!(first_key, second_key);
        assert_ne!(
            shared_record_key(
                PostgresRecordKind::Latest.as_str(),
                &first_key,
                "file",
                None
            ),
            shared_record_key(
                PostgresRecordKind::Version.as_str(),
                &first_key,
                "file",
                Some("a".repeat(64).as_str())
            )
        );
    }

    #[test]
    fn postgres_latest_locator_ignores_content_hash_for_stable_head_keys() {
        let scope = RepositoryScope::new(RepositoryProvider::GitLab, "team", "assets", None);
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let first = file_record(scope.clone(), "a");
        let second = file_record(scope, "b");
        let first_key = super::record_locator(PostgresRecordKind::Latest, &first, None);
        let second_key = super::record_locator(PostgresRecordKind::Latest, &second, None);

        assert_eq!(first_key, second_key);
    }

    #[test]
    fn postgres_repository_scope_key_prefix_matches_all_repository_revisions_only() {
        let repository = RepositoryRecordScope::new(RepositoryProvider::GitHub, "team", "assets");
        let revisionless = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None);
        let revisioned =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        let other = RepositoryScope::new(RepositoryProvider::GitHub, "team", "other", Some("main"));
        assert!(revisionless.is_ok());
        assert!(revisioned.is_ok());
        assert!(other.is_ok());
        let (Ok(revisionless), Ok(revisioned), Ok(other)) = (revisionless, revisioned, other)
        else {
            return;
        };

        let repository_key = shared_repository_record_scope_key(&repository);
        let revisionless_key = shared_repository_scope_key(Some(&revisionless));
        let revisioned_key = shared_repository_scope_key(Some(&revisioned));
        let other_key = shared_repository_scope_key(Some(&other));

        assert_eq!(repository_key, revisionless_key);
        assert!(revisioned_key.starts_with(&repository_key));
        assert!(!other_key.starts_with(&repository_key));
    }

    #[test]
    fn postgres_lifecycle_migrations_reject_inverted_timelines() {
        let metadata_migration =
            include_str!("../../../migrations/20260417000000_metadata_store.up.sql");
        let retention_migration =
            include_str!("../../../migrations/20260417010000_retention_holds.up.sql");

        assert!(metadata_migration.contains(
            "CHECK (delete_after_unix_seconds >= first_seen_unreachable_at_unix_seconds)"
        ));
        assert!(retention_migration.contains("release_after_unix_seconds >= held_at_unix_seconds"));
    }

    fn file_record(scope: RepositoryScope, content_seed: &str) -> FileRecord {
        let hash = ShardlineHash::from_bytes([12; 32]);
        let file_id = xet_hash_hex_string(FileId::new(hash).hash());
        FileRecord {
            file_id,
            content_hash: content_seed.repeat(64),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: Some(scope),
            chunks: Vec::new(),
        }
    }
}
