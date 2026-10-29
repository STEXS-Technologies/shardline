use std::sync::{Arc, OnceLock};
use std::time::Duration;
use std::{collections::HashSet, str::FromStr};

mod candidate_batch;
mod first_candidate;
mod repository_candidates;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use futures_util::TryStreamExt;
use serde_json::to_vec;
use shardline_reliability::resumable_session_event;
use sqlx::{
    Connection as _, PgConnection, Postgres, QueryBuilder, Row, Transaction, postgres::PgRow,
    query, query_scalar, types::Json,
};

use super::{
    PostgresMetadataStoreError, PostgresRecordLocator, RecordKind, i64_to_u64,
    insert_reliability_event, next_reliability_sequence,
};
use crate::{
    DedupeShardMapping, FileRecord, RecordMutation, RecordStoreFuture, RecordTraversal,
    RepositoryRecordScope, ResumableCompletionFence, S3ObjectEntry, S3PublishCondition,
    StoredRecord,
    record_key::record_key as shared_record_key,
    record_key::{
        repository_record_scope_key as shared_repository_record_scope_key,
        repository_scope_key as shared_repository_scope_key,
    },
    xet_hash_hex_string,
};

/// Keep bulk dedupe publication well below PostgreSQL's bind-parameter ceiling.
const POSTGRES_DEDUPE_MAPPING_BATCH_SIZE: usize = 4_096;

// Admission precedes polling an owned read row or cloning a write payload,
// bounding conversion work across read and write paths and store instances.
fn record_conversion_budget() -> &'static Arc<Semaphore> {
    static BUDGET: OnceLock<Arc<Semaphore>> = OnceLock::new();
    BUDGET.get_or_init(|| Arc::new(Semaphore::new(8)))
}

pub(crate) async fn admit_record_conversion()
-> Result<OwnedSemaphorePermit, PostgresMetadataStoreError> {
    Arc::clone(record_conversion_budget())
        .acquire_owned()
        .await
        .map_err(|error| PostgresMetadataStoreError::Unsupported(error.to_string()))
}

pub(crate) async fn run_record_conversion<T: Send + 'static>(
    permit: OwnedSemaphorePermit,
    convert: impl FnOnce() -> Result<T, PostgresMetadataStoreError> + Send + 'static,
) -> Result<T, PostgresMetadataStoreError> {
    // Returning the permit with the result keeps admission charged for queued,
    // running and completed-but-unclaimed work, even if the waiter is canceled.
    let (result, _permit) = tokio::task::spawn_blocking(move || {
        let result = convert();
        (result, permit)
    })
    .await
    .map_err(|error| PostgresMetadataStoreError::Unsupported(error.to_string()))?;
    // No yield after taking ownership of the completed result.
    result
}

fn canonical_record_bytes_from_row(row: &PgRow) -> Result<Vec<u8>, PostgresMetadataStoreError> {
    let Json(record) = row.try_get::<Json<FileRecord>, _>("record")?;
    Ok(to_vec(&record)?)
}

async fn prepare_record_upsert(
    locator: &PostgresRecordLocator,
    record: &FileRecord,
) -> Result<
    sqlx::query::Query<'static, Postgres, sqlx::postgres::PgArguments>,
    PostgresMetadataStoreError,
> {
    // Charge admission before owning the write payload. SQLx performs JSON
    // encoding during bind, so build the entire argument set off the executor.
    let permit = admit_record_conversion().await?;
    let locator = locator.clone();
    let record = record.clone();
    run_record_conversion(permit, move || {
        Ok(query(
            "INSERT INTO shardline_file_records (
                record_key, record_kind, scope_key, file_id, content_hash, record
             ) VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (record_key)
             DO UPDATE SET
                record_kind = EXCLUDED.record_kind,
                scope_key = EXCLUDED.scope_key,
                file_id = EXCLUDED.file_id,
                content_hash = EXCLUDED.content_hash,
                -- Keep the existing TOASTed payload on an exact replay while
                -- still advancing updated_at and firing the normal row triggers.
                record = CASE
                    WHEN shardline_file_records.record IS DISTINCT FROM EXCLUDED.record
                        THEN EXCLUDED.record
                    ELSE shardline_file_records.record
                END,
                updated_at = now()",
        )
        .bind(locator.record_key)
        .bind(locator.kind.as_str())
        .bind(locator.scope_key)
        .bind(locator.file_id)
        .bind(record.content_hash.clone())
        .bind(Json(record)))
    })
    .await
}

/// Both aliases carry the same record. Bind its JSON once, while preserving
/// version-before-latest row processing and the existing per-row triggers.
/// Unchanged payloads retain their existing TOAST storage; timestamps and
/// reference-protection triggers still refresh on every publication.
async fn upsert_file_aliases_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    version: &PostgresRecordLocator,
    latest: &PostgresRecordLocator,
    record: &FileRecord,
) -> Result<(), PostgresMetadataStoreError> {
    let permit = admit_record_conversion().await?;
    let version = version.clone();
    let latest = latest.clone();
    let record = record.clone();
    let statement = run_record_conversion(permit, move || {
        Ok(query(
            "INSERT INTO shardline_file_records (
                record_key, record_kind, scope_key, file_id, content_hash, record
             ) VALUES ($1, $2, $3, $4, $5, $6), ($7, $8, $3, $4, $5, $6)
             ON CONFLICT (record_key)
             DO UPDATE SET
                record_kind = EXCLUDED.record_kind,
                scope_key = EXCLUDED.scope_key,
                file_id = EXCLUDED.file_id,
                content_hash = EXCLUDED.content_hash,
                -- Keep the existing TOASTed payload on an exact replay while
                -- still advancing updated_at and firing the normal row triggers.
                record = CASE
                    WHEN shardline_file_records.record IS DISTINCT FROM EXCLUDED.record
                        THEN EXCLUDED.record
                    ELSE shardline_file_records.record
                END,
                updated_at = now()",
        )
        .bind(version.record_key)
        .bind(version.kind.as_str())
        .bind(version.scope_key)
        .bind(version.file_id)
        .bind(record.content_hash.clone())
        .bind(Json(record))
        .bind(latest.record_key)
        .bind(latest.kind.as_str()))
    })
    .await?;
    statement.execute(&mut **transaction).await?;
    Ok(())
}

impl super::PostgresRecordStore {
    /// Reads on the caller's ownership connection without replacing it.
    ///
    /// # Errors
    /// Returns a missing-record or metadata error, including connection loss.
    pub async fn read_record_bytes_on_connection(
        &self,
        connection: &mut PgConnection,
        locator: &PostgresRecordLocator,
    ) -> Result<Vec<u8>, PostgresMetadataStoreError> {
        let permit = admit_record_conversion().await?;
        let row = query("SELECT record FROM shardline_file_records WHERE record_key = $1")
            .bind(&locator.record_key)
            .fetch_optional(connection)
            .await?
            .ok_or(PostgresMetadataStoreError::RecordNotFound)?;
        run_record_conversion(permit, move || canonical_record_bytes_from_row(&row)).await
    }
    /// Inserts or replaces an immutable version record.
    ///
    /// This writes metadata through the store's pool. It does not check backing
    /// bytes, preserve a caller-owned connection or publish the latest-file
    /// alias. Storage-backed callers must retain their operation's protection
    /// while validating content. Use `commit_file_version_metadata_on_connection`
    /// when an existing ownership session must publish a version and latest alias.
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
    /// This method commits metadata only. It does not inspect object storage or
    /// retain an ownership connection held by the caller. Callers publishing
    /// storage-backed content must validate that content under their operation's
    /// storage/reference protection. Use the connection-bound variant when an
    /// existing session owns the operation.
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
        let mut connection = self.pool.acquire().await?;
        self.commit_file_version_metadata_on_connection(&mut connection, record)
            .await
    }

    /// Commits a file version and latest alias on the caller's ownership session.
    ///
    /// Keeping the session preserves its locks; this method does not validate
    /// backing bytes in object storage. The caller must perform that validation
    /// while its reference protection remains held through publication.
    ///
    /// # Errors
    ///
    /// Returns a database or metadata error without replacing a lost connection.
    pub async fn commit_file_version_metadata_on_connection(
        &self,
        connection: &mut PgConnection,
        record: &FileRecord,
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        self.stage_file_version_metadata_in_transaction(&mut transaction, record)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Stages version and latest aliases in the caller's publication transaction.
    ///
    /// The caller must retain ownership and backing-object protection through
    /// commit. Unlike the connection-based method, an error belongs to the
    /// caller's transaction and requires its rollback.
    ///
    /// # Errors
    ///
    /// Returns conversion or database errors without committing the transaction.
    pub async fn stage_file_version_metadata_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        record: &FileRecord,
    ) -> Result<(), PostgresMetadataStoreError> {
        let version_locator = self.version_record_locator(record);
        let latest_locator = self.latest_record_locator(record);
        upsert_file_aliases_in_transaction(transaction, &version_locator, &latest_locator, record)
            .await?;
        Ok(())
    }

    /// Atomically publishes an S3 record version, its latest alias, and the
    /// listing row through the caller's Postgres connection.
    ///
    /// This method publishes metadata only. It does not acquire the caller's
    /// operation ownership or inspect object storage. The caller must retain
    /// its ownership/reference protection and validate backing content through
    /// the commit. A supplied completion fence additionally checks the resumable
    /// session; it does not replace those storage and ownership checks.
    ///
    /// # Errors
    ///
    /// Returns an error when the entry does not describe `record`, metadata
    /// serialization fails, or Postgres cannot commit the transaction.
    pub async fn publish_s3_object_on_connection(
        &self,
        connection: &mut PgConnection,
        record: &FileRecord,
        entry: &S3ObjectEntry,
        condition: &S3PublishCondition,
        completion_fence: Option<&ResumableCompletionFence>,
    ) -> Result<bool, PostgresMetadataStoreError> {
        self.publish_s3_object_on_connection_with_multipart_count(
            connection,
            record,
            entry,
            condition,
            completion_fence,
            None,
        )
        .await
    }

    /// Like [`Self::publish_s3_object_on_connection`], with optional S3
    /// multipart metadata committed in the same transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the entry does not describe `record`, metadata
    /// serialization fails, or Postgres cannot commit the transaction.
    pub async fn publish_s3_object_on_connection_with_multipart_count(
        &self,
        connection: &mut PgConnection,
        record: &FileRecord,
        entry: &S3ObjectEntry,
        condition: &S3PublishCondition,
        completion_fence: Option<&ResumableCompletionFence>,
        multipart_part_count: Option<u32>,
    ) -> Result<bool, PostgresMetadataStoreError> {
        if entry.file_id != record.file_id
            || entry.content_hash != record.content_hash
            || entry.size_bytes != record.total_bytes
        {
            return Err(PostgresMetadataStoreError::S3PublicationMismatch);
        }
        let metadata = serde_json::to_string(&entry.user_metadata)?;
        let mut transaction = connection.begin().await?;
        let previous_s3_entry = super::s3_objects::current_s3_object_on_connection(
            &mut transaction,
            &entry.scope_namespace,
            &entry.object_key,
        )
        .await?;
        let completion_identity = if let Some(fence) = completion_fence {
            let owns_completion = query(
                "SELECT scope_namespace, target_key FROM shardline_resumable_sessions
                 WHERE session_id = $1 AND state = 'completing' AND fence_epoch = $2
                 AND expires_at > clock_timestamp()
                 FOR UPDATE",
            )
            .bind(fence.session_id())
            .bind(super::u64_to_i64(fence.epoch().get())?)
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(owns_completion) = owns_completion else {
                transaction.rollback().await?;
                return Ok(false);
            };
            let scope_namespace: String = owns_completion.try_get("scope_namespace")?;
            let target_key: String = owns_completion.try_get("target_key")?;
            Some((scope_namespace, target_key))
        } else {
            None
        };
        let version = self.version_record_locator(record);
        let latest = self.latest_record_locator(record);
        upsert_file_aliases_in_transaction(&mut transaction, &version, &latest, record).await?;
        let publication = match condition {
            S3PublishCondition::Unconditional => {
                query(
                    "INSERT INTO shardline_s3_objects (
                 scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
                 user_metadata, updated_at_unix_seconds
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (scope_namespace, object_key)
             DO UPDATE SET file_id = EXCLUDED.file_id, size_bytes = EXCLUDED.size_bytes,
                 content_hash = EXCLUDED.content_hash, etag = EXCLUDED.etag,
                 user_metadata = EXCLUDED.user_metadata,
                 updated_at_unix_seconds = EXCLUDED.updated_at_unix_seconds",
                )
                .bind(&entry.scope_namespace)
                .bind(&entry.object_key)
                .bind(&entry.file_id)
                .bind(super::u64_to_i64(entry.size_bytes)?)
                .bind(&entry.content_hash)
                .bind(&entry.etag)
                .bind(&metadata)
                .bind(entry.updated_at_unix_seconds)
                .execute(&mut *transaction)
                .await?
            }
            S3PublishCondition::IfUnchanged(Some(expected)) => {
                let expected_metadata = serde_json::to_string(&expected.user_metadata)?;
                query(
                    "UPDATE shardline_s3_objects
                     SET file_id = $3, size_bytes = $4, content_hash = $5, etag = $6,
                         user_metadata = $7, updated_at_unix_seconds = $8
                     WHERE scope_namespace = $1 AND object_key = $2
                       AND file_id = $9 AND size_bytes = $10 AND content_hash = $11
                       AND etag = $12 AND user_metadata = $13
                       AND updated_at_unix_seconds = $14",
                )
                .bind(&entry.scope_namespace)
                .bind(&entry.object_key)
                .bind(&entry.file_id)
                .bind(super::u64_to_i64(entry.size_bytes)?)
                .bind(&entry.content_hash)
                .bind(&entry.etag)
                .bind(&metadata)
                .bind(entry.updated_at_unix_seconds)
                .bind(&expected.file_id)
                .bind(super::u64_to_i64(expected.size_bytes)?)
                .bind(&expected.content_hash)
                .bind(&expected.etag)
                .bind(expected_metadata)
                .bind(expected.updated_at_unix_seconds)
                .execute(&mut *transaction)
                .await?
            }
            S3PublishCondition::IfUnchanged(None) => {
                query(
                    "INSERT INTO shardline_s3_objects (
                     scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
                     user_metadata, updated_at_unix_seconds
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT (scope_namespace, object_key) DO NOTHING",
                )
                .bind(&entry.scope_namespace)
                .bind(&entry.object_key)
                .bind(&entry.file_id)
                .bind(super::u64_to_i64(entry.size_bytes)?)
                .bind(&entry.content_hash)
                .bind(&entry.etag)
                .bind(&metadata)
                .bind(entry.updated_at_unix_seconds)
                .execute(&mut *transaction)
                .await?
            }
        };
        if publication.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(false);
        }
        super::s3_objects::persist_multipart_count(&mut transaction, entry, multipart_part_count)
            .await?;
        super::s3_objects::record_s3_object_transition(
            &mut transaction,
            previous_s3_entry.as_ref(),
            Some(entry),
        )
        .await?;
        if let Some(fence) = completion_fence {
            let completed = query(
                "UPDATE shardline_resumable_sessions
                 SET state = 'completed', updated_at = now()
                 WHERE session_id = $1 AND state = 'completing' AND fence_epoch = $2",
            )
            .bind(fence.session_id())
            .bind(super::u64_to_i64(fence.epoch().get())?)
            .execute(&mut *transaction)
            .await?;
            if completed.rows_affected() != 1 {
                transaction.rollback().await?;
                return Ok(false);
            }
            super::refresh_resumable_state_digest(&mut transaction, fence.session_id()).await?;
            let Some((scope_namespace, target_key)) = completion_identity else {
                transaction.rollback().await?;
                return Ok(false);
            };
            let sequence = next_reliability_sequence(
                transaction.as_mut(),
                shardline_reliability::OperationKind::ResumableSession,
                fence.session_id(),
            )
            .await?;
            let event = resumable_session_event(
                scope_namespace,
                fence.session_id(),
                target_key,
                sequence,
                crate::ResumableSessionState::Completing,
                crate::ResumableSessionState::Completed,
            )?;
            insert_reliability_event(transaction.as_mut(), &event).await?;
        }
        transaction.commit().await?;
        Ok(true)
    }

    /// Atomically removes an S3 listing row and the record version it made
    /// visible through a lock-owning Postgres connection.
    ///
    /// When no listing row exists, the current global latest alias is removed
    /// for compatibility with objects written before the S3 listing index.
    /// Immutable chunks remain for writer-excluding GC.
    ///
    /// # Errors
    ///
    /// Returns an error when Postgres cannot commit the deletion transaction.
    pub async fn delete_s3_object_on_connection(
        &self,
        connection: &mut PgConnection,
        scope_namespace: &str,
        object_key: &str,
        fallback_file_id: &str,
    ) -> Result<bool, PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        let deleted_entry = super::s3_objects::remove_s3_object_on_connection(
            &mut transaction,
            scope_namespace,
            object_key,
        )
        .await?;
        let scope_key = shared_repository_scope_key(None);
        let (file_id, visible_hash) = if let Some(row) = deleted_entry.as_ref() {
            (row.file_id.clone(), Some(row.content_hash.clone()))
        } else {
            let hash = query_scalar::<_, String>(
                "SELECT content_hash FROM shardline_file_records
                 WHERE scope_key = $1 AND file_id = $2 AND record_kind = 'latest'
                 FOR UPDATE",
            )
            .bind(&scope_key)
            .bind(fallback_file_id)
            .fetch_optional(&mut *transaction)
            .await?;
            (fallback_file_id.to_owned(), hash)
        };
        let deleted_records = if let Some(content_hash) = visible_hash {
            query(
                "DELETE FROM shardline_file_records
                 WHERE scope_key = $1 AND file_id = $2
                   AND (record_kind = 'latest'
                        OR (record_kind = 'version' AND content_hash = $3))",
            )
            .bind(scope_key)
            .bind(file_id)
            .bind(content_hash)
            .execute(&mut *transaction)
            .await?
            .rows_affected()
        } else {
            0
        };
        if deleted_entry.is_some() {
            super::s3_objects::record_s3_object_transition(
                &mut transaction,
                deleted_entry.as_ref(),
                None,
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(deleted_entry.is_some() || deleted_records > 0)
    }

    /// Atomically removes a visible file reference and its immutable version record.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresMetadataStoreError`] when the transaction cannot be completed.
    pub async fn delete_file_version_metadata(
        &self,
        record: &FileRecord,
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut connection = self.pool.acquire().await?;
        self.delete_file_version_metadata_on_connection(&mut connection, record)
            .await
    }

    /// Removes metadata on the caller's non-replaceable ownership connection.
    ///
    /// # Errors
    /// Returns a metadata error, including connection loss.
    pub async fn delete_file_version_metadata_on_connection(
        &self,
        connection: &mut PgConnection,
        record: &FileRecord,
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        let latest = self.latest_record_locator(record);
        let version = self.version_record_locator(record);
        query("DELETE FROM shardline_file_records WHERE record_key = $1 OR record_key = $2")
            .bind(&latest.record_key)
            .bind(&version.record_key)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Removes a file reference only if its latest hash still matches `record`.
    /// A concurrent publisher's latest alias and the losing version are retained
    /// when the condition fails.
    ///
    /// # Errors
    /// Returns a metadata error if the transaction cannot complete.
    pub async fn delete_file_version_metadata_if_latest(
        &self,
        record: &FileRecord,
    ) -> Result<bool, PostgresMetadataStoreError> {
        let mut connection = self.pool.acquire().await?;
        self.delete_file_version_metadata_if_latest_on_connection(&mut connection, record)
            .await
    }

    /// Conditionally removes metadata on the caller's ownership connection.
    ///
    /// # Errors
    /// Returns a metadata error, including connection loss.
    pub async fn delete_file_version_metadata_if_latest_on_connection(
        &self,
        connection: &mut PgConnection,
        record: &FileRecord,
    ) -> Result<bool, PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        let latest = self.latest_record_locator(record);
        let removed = query(
            "DELETE FROM shardline_file_records
             WHERE record_key = $1 AND content_hash = $2",
        )
        .bind(&latest.record_key)
        .bind(&record.content_hash)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if removed == 0 {
            transaction.rollback().await?;
            return Ok(false);
        }
        let version = self.version_record_locator(record);
        query("DELETE FROM shardline_file_records WHERE record_key = $1")
            .bind(&version.record_key)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(true)
    }

    /// Atomically commits native shard metadata.
    ///
    /// This compatibility entry point supplies no additional physical object
    /// keys. Metadata reference checks cannot discover keys hidden in xorb
    /// payloads or prove that backing bytes exist. Storage-backed publishers
    /// should derive physical keys from validated payloads and use the explicit
    /// reference variant, retaining protection while checking storage presence.
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
        self.commit_native_shard_metadata_with_remote_delete_references(
            records,
            dedupe_mappings,
            &[],
        )
        .await
    }

    /// Publishes native metadata with the physical references derived from
    /// validated xorb payloads. Pending-delete checks and all metadata writes
    /// share one transaction. The existing method retains its signature.
    /// This checks delete coordination for the supplied keys, not storage
    /// presence or completeness of the supplied reference set. It acquires a
    /// connection from this store; use the connection-bound variant to retain
    /// an existing ownership session.
    ///
    /// # Errors
    ///
    /// Returns a retryable pending-delete error or a database failure. On
    /// schemas predating the compatibility guard, behavior remains unchanged;
    /// guarded GC refuses remote-delete admission until the owner-run upgrade.
    pub async fn commit_native_shard_metadata_with_remote_delete_references(
        &self,
        records: &[FileRecord],
        dedupe_mappings: &[DedupeShardMapping],
        references: &[shardline_storage::ObjectKey],
    ) -> Result<(), PostgresMetadataStoreError> {
        self.commit_native_shard_metadata_with_xet_identities(
            records,
            dedupe_mappings,
            references,
            &[],
        )
        .await
    }

    /// Atomically commits native shard metadata and Hub SHA-256 identities.
    ///
    /// # Errors
    ///
    /// Returns a metadata/database error or a conflict when a repository-scoped
    /// SHA-256 identity already has a different byte length.
    pub async fn commit_native_shard_metadata_with_xet_identities(
        &self,
        records: &[FileRecord],
        dedupe_mappings: &[DedupeShardMapping],
        references: &[shardline_storage::ObjectKey],
        identities: &[crate::XetFileIdentity],
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut connection = self.pool.acquire().await?;
        self.commit_native_shard_metadata_on_connection_with_xet_identities(
            &mut connection,
            records,
            dedupe_mappings,
            references,
            identities,
        )
        .await
    }

    /// Publishes native records, mappings and reference checks on one ownership session.
    ///
    /// The caller supplies all physical references and validates their backing
    /// bytes while protection remains held. This metadata-only method does not
    /// read object storage or establish that the reference set is complete.
    ///
    /// # Errors
    ///
    /// Returns a pending-delete, metadata or database error. A lost connection is
    /// never replaced by a connection from the store's pool.
    pub async fn commit_native_shard_metadata_on_connection(
        &self,
        connection: &mut PgConnection,
        records: &[FileRecord],
        dedupe_mappings: &[DedupeShardMapping],
        references: &[shardline_storage::ObjectKey],
    ) -> Result<(), PostgresMetadataStoreError> {
        self.commit_native_shard_metadata_on_connection_with_xet_identities(
            connection,
            records,
            dedupe_mappings,
            references,
            &[],
        )
        .await
    }

    /// Publishes native shard records, mappings, references, and Hub identities
    /// in one transaction on the existing owner session.
    ///
    /// # Errors
    ///
    /// Returns a pending-delete, metadata/database error, or a conflict when a
    /// repository-scoped SHA-256 identity has a different byte length.
    pub async fn commit_native_shard_metadata_on_connection_with_xet_identities(
        &self,
        connection: &mut PgConnection,
        records: &[FileRecord],
        dedupe_mappings: &[DedupeShardMapping],
        references: &[shardline_storage::ObjectKey],
        identities: &[crate::XetFileIdentity],
    ) -> Result<(), PostgresMetadataStoreError> {
        self.commit_native_shard_metadata_on_connection_with_descriptors(
            connection,
            records,
            dedupe_mappings,
            references,
            identities,
            &[],
        )
        .await
    }

    /// Publishes verified native descriptors on the retained ownership session,
    /// atomically with records, mappings, reference checks and Hub identities.
    ///
    /// # Errors
    /// Returns an error for invalid/conflicting metadata or database failure.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit_native_shard_metadata_on_connection_with_descriptors(
        &self,
        connection: &mut PgConnection,
        records: &[FileRecord],
        dedupe_mappings: &[DedupeShardMapping],
        references: &[shardline_storage::ObjectKey],
        identities: &[crate::XetFileIdentity],
        descriptors: &[crate::XetXorbDescriptor],
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        super::xet_xorb::stage_descriptors(&mut transaction, descriptors).await?;
        if !references.is_empty()
            && query_scalar::<_, bool>(
                "SELECT to_regprocedure('shardline_reference_keys_remote_delete_v1(text,text[])') IS NOT NULL",
            ).fetch_one(&mut *transaction).await?
        {
            let keys: Vec<_> = references.iter().map(shardline_storage::ObjectKey::as_str).collect();
            query("SELECT shardline_reference_keys_remote_delete_v1(current_schema(), $1)")
                .bind(keys).execute(&mut *transaction).await?;
        }
        for record in records {
            let locator = self.version_record_locator(record);
            upsert_record_in_transaction(&mut transaction, &locator, record).await?;
        }
        upsert_dedupe_shard_mappings_in_transaction(&mut transaction, dedupe_mappings).await?;
        for identity in identities {
            let scope_key = identity.repository_key();
            let total_bytes = i64::try_from(identity.total_bytes).map_err(|_error| {
                PostgresMetadataStoreError::IntegerOutOfRange("file size exceeds i64".to_owned())
            })?;
            let result = query(
                "INSERT INTO shardline_xet_file_identities
                    (scope_key, sha256, file_id, total_bytes)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (scope_key, sha256) DO UPDATE SET
                    file_id = EXCLUDED.file_id,
                    total_bytes = EXCLUDED.total_bytes
                 WHERE shardline_xet_file_identities.total_bytes = EXCLUDED.total_bytes
                   AND shardline_xet_file_identities.file_id <> EXCLUDED.file_id",
            )
            .bind(&scope_key)
            .bind(&identity.sha256)
            .bind(&identity.file_id)
            .bind(total_bytes)
            .execute(&mut *transaction)
            .await?;
            if result.rows_affected() == 0 {
                // ON CONFLICT retains the conflicting row lock even when its
                // WHERE clause skips the update. Check the locked identity in
                // this transaction, including concurrent identical insertions.
                let replay: bool = query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM shardline_xet_file_identities
                     WHERE scope_key=$1 AND sha256=$2 AND file_id=$3 AND total_bytes=$4)",
                )
                .bind(&scope_key)
                .bind(&identity.sha256)
                .bind(&identity.file_id)
                .bind(total_bytes)
                .fetch_one(&mut *transaction)
                .await?;
                if !replay {
                    return Err(PostgresMetadataStoreError::XetFileIdentityConflict);
                }
            }
        }
        for record in records {
            let locator = self.latest_record_locator(record);
            upsert_record_in_transaction(&mut transaction, &locator, record).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Looks up a Hub SHA-256 identity for a repository, independent of revision.
    ///
    /// # Errors
    ///
    /// Returns a metadata/database error or a range error if the stored size is invalid.
    pub async fn xet_file_identity(
        &self,
        repository_scope: &shardline_protocol::RepositoryScope,
        sha256: &str,
    ) -> Result<Option<crate::XetFileIdentity>, PostgresMetadataStoreError> {
        let row = query(
            "SELECT file_id, total_bytes FROM shardline_xet_file_identities
             WHERE scope_key = $1 AND sha256 = $2",
        )
        .bind(crate::XetFileIdentity::repository_key_for_scope(
            repository_scope,
        ))
        .bind(sha256)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let total_bytes: i64 = row.try_get("total_bytes")?;
            Ok(crate::XetFileIdentity {
                repository_scope: repository_scope.clone(),
                sha256: sha256.to_owned(),
                file_id: row.try_get("file_id")?,
                total_bytes: u64::try_from(total_bytes).map_err(|_error| {
                    PostgresMetadataStoreError::IntegerOutOfRange(
                        "stored file size is negative".to_owned(),
                    )
                })?,
            })
        })
        .transpose()
    }

    async fn upsert_record(
        &self,
        locator: &PostgresRecordLocator,
        record: &FileRecord,
    ) -> Result<(), PostgresMetadataStoreError> {
        prepare_record_upsert(locator, record)
            .await?
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn read_record_bytes_optional(
        &self,
        locator: &PostgresRecordLocator,
    ) -> Result<Option<Vec<u8>>, PostgresMetadataStoreError> {
        let permit = admit_record_conversion().await?;
        let row = query("SELECT record FROM shardline_file_records WHERE record_key = $1")
            .bind(&locator.record_key)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        run_record_conversion(permit, move || canonical_record_bytes_from_row(&row))
            .await
            .map(Some)
    }

    async fn list_record_locators(
        &self,
        kind: RecordKind,
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
        kind: RecordKind,
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
        kind: RecordKind,
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

        loop {
            let permit = admit_record_conversion().await.map_err(Into::into)?;
            let Some(row) = rows
                .try_next()
                .await
                .map_err(PostgresMetadataStoreError::from)
                .map_err(Into::into)?
            else {
                break;
            };
            let record = run_record_conversion(permit, move || stored_record_from_row(&row))
                .await
                .map_err(Into::into)?;
            visitor(record)?;
        }

        Ok(())
    }

    async fn list_repository_record_locators(
        &self,
        kind: RecordKind,
        repository: &RepositoryRecordScope,
    ) -> Result<Vec<PostgresRecordLocator>, PostgresMetadataStoreError> {
        let scope_key = shared_repository_record_scope_key(repository);
        let escaped_prefix = format!(
            "{}%",
            scope_key
                .replace('\\', "\\\\")
                .replace('_', "\\_")
                .replace('%', "\\%")
        );
        let rows = query(
            "SELECT record_key, record_kind, scope_key, file_id, content_hash
             FROM shardline_file_records
             WHERE record_kind = $1
               AND (scope_key = $2 OR scope_key LIKE $3 ESCAPE '\\')
             ORDER BY record_key",
        )
        .bind(kind.as_str())
        .bind(&scope_key)
        .bind(&escaped_prefix)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(record_locator_from_row)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn visit_repository_records<Visitor, VisitorError>(
        &self,
        kind: RecordKind,
        repository: &RepositoryRecordScope,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        PostgresMetadataStoreError: Into<VisitorError>,
        Visitor: FnMut(StoredRecord<PostgresRecordLocator>) -> Result<(), VisitorError>,
    {
        let scope_key = shared_repository_record_scope_key(repository);
        let escaped_prefix = format!(
            "{}%",
            scope_key
                .replace('\\', "\\\\")
                .replace('_', "\\_")
                .replace('%', "\\%")
        );
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
               AND (scope_key = $2 OR scope_key LIKE $3 ESCAPE '\\')
             ORDER BY record_key",
        )
        .bind(kind.as_str())
        .bind(&scope_key)
        .bind(&escaped_prefix)
        .fetch(&self.pool);

        loop {
            let permit = admit_record_conversion().await.map_err(Into::into)?;
            let Some(row) = rows
                .try_next()
                .await
                .map_err(PostgresMetadataStoreError::from)
                .map_err(Into::into)?
            else {
                break;
            };
            let record = run_record_conversion(permit, move || stored_record_from_row(&row))
                .await
                .map_err(Into::into)?;
            visitor(record)?;
        }

        Ok(())
    }
}

impl RecordTraversal for super::PostgresRecordStore {
    type Error = PostgresMetadataStoreError;
    type Locator = PostgresRecordLocator;

    fn native_chunk_reference_page(
        &self,
        after: Option<(String, i64)>,
    ) -> RecordStoreFuture<'_, Vec<(String, i64, String)>, Self::Error> {
        Box::pin(async move {
            if after.is_none() {
                // Optional owner-installed acceleration must not be required
                // by ordinary GC against a released metadata schema. Check
                // once per traversal; actual query/permission errors propagate.
                let present: bool =
                    sqlx::query_scalar("SELECT to_regclass('shardline_xet_chunks') IS NOT NULL")
                        .fetch_one(&self.pool)
                        .await?;
                if !present {
                    return Ok(Vec::new());
                }
            }
            let (hash, ordinal) = after.unwrap_or_else(|| (String::new(), -1));
            sqlx::query_as("SELECT xorb_hash,chunk_index,chunk_hash FROM shardline_xet_chunks WHERE (xorb_hash,chunk_index)>($1,$2) ORDER BY xorb_hash,chunk_index LIMIT 256")
                .bind(hash).bind(ordinal).fetch_all(&self.pool).await.map_err(Into::into)
        })
    }

    fn visit_repository_hash_records<'operation, Visitor, VisitorError>(
        &'operation self,
        repository: &'operation shardline_protocol::RepositoryScope,
        hash_hex: &'operation str,
        visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(
            async move { repository_candidates::visit(self, repository, hash_hex, visitor).await },
        )
    }
    fn first_repository_hash_record<'operation>(
        &'operation self,
        repository: &'operation shardline_protocol::RepositoryScope,
        hash_hex: &'operation str,
    ) -> RecordStoreFuture<'operation, Option<StoredRecord<Self::Locator>>, Self::Error>
    where
        Self::Error: Send + 'operation,
    {
        first_candidate::read(self, repository, hash_hex)
    }

    fn list_latest_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move { self.list_record_locators(RecordKind::Latest).await })
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
            self.visit_record_locators(RecordKind::Latest, &mut visitor)
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
        Box::pin(async move { self.visit_records(RecordKind::Latest, &mut visitor).await })
    }

    fn list_version_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move { self.list_record_locators(RecordKind::Version).await })
    }

    fn list_repository_latest_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            self.list_repository_record_locators(RecordKind::Latest, repository)
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
            self.visit_repository_records(RecordKind::Latest, repository, &mut visitor)
                .await
        })
    }

    fn list_repository_version_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            self.list_repository_record_locators(RecordKind::Version, repository)
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
            self.visit_record_locators(RecordKind::Version, &mut visitor)
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
        Box::pin(async move { self.visit_records(RecordKind::Version, &mut visitor).await })
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
            self.visit_repository_records(RecordKind::Version, repository, &mut visitor)
                .await
        })
    }

    fn read_record_bytes<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Vec<u8>, Self::Error> {
        Box::pin(async move {
            self.read_record_bytes_optional(locator)
                .await?
                .ok_or(PostgresMetadataStoreError::RecordNotFound)
        })
    }

    fn read_latest_record_bytes<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, Option<Vec<u8>>, Self::Error> {
        Box::pin(async move {
            self.read_record_bytes_optional(&self.latest_record_locator(record))
                .await
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
        record_locator(RecordKind::Latest, record, None)
    }

    fn version_record_locator(&self, record: &FileRecord) -> Self::Locator {
        record_locator(
            RecordKind::Version,
            record,
            Some(record.content_hash.clone()),
        )
    }
}

impl RecordMutation for super::PostgresRecordStore {
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

    fn prune_empty_latest_records(&self) -> RecordStoreFuture<'_, (), Self::Error> {
        Box::pin(async move { Ok(()) })
    }
}

pub(super) async fn upsert_record_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    locator: &PostgresRecordLocator,
    record: &FileRecord,
) -> Result<(), PostgresMetadataStoreError> {
    prepare_record_upsert(locator, record)
        .await?
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
            updated_at = now()
         WHERE shardline_dedupe_shards.shard_object_key
               IS DISTINCT FROM EXCLUDED.shard_object_key",
    )
    .bind(xet_hash_hex_string(mapping.chunk_hash()))
    .bind(mapping.shard_object_key().as_str())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn upsert_dedupe_shard_mappings_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    mappings: &[DedupeShardMapping],
) -> Result<(), PostgresMetadataStoreError> {
    let mut seen = HashSet::with_capacity(mappings.len());
    let has_duplicate_hashes = mappings
        .iter()
        .any(|mapping| !seen.insert(mapping.chunk_hash()));
    if has_duplicate_hashes {
        // Preserve the established last-write-wins behavior for callers that
        // supply the same chunk more than once. The native shard parser emits
        // unique hashes, so normal uploads use the batched path below.
        for mapping in mappings {
            upsert_dedupe_shard_mapping_in_transaction(transaction, mapping).await?;
        }
        return Ok(());
    }

    for batch in mappings.chunks(POSTGRES_DEDUPE_MAPPING_BATCH_SIZE) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO shardline_dedupe_shards (chunk_hash, shard_object_key) ",
        );
        builder.push_values(batch, |mut row, mapping| {
            row.push_bind(xet_hash_hex_string(mapping.chunk_hash()))
                .push_bind(mapping.shard_object_key().as_str());
        });
        builder.push(
            " ON CONFLICT (chunk_hash)
              DO UPDATE SET
                 shard_object_key = EXCLUDED.shard_object_key,
                 updated_at = now()
              WHERE shardline_dedupe_shards.shard_object_key
                    IS DISTINCT FROM EXCLUDED.shard_object_key",
        );
        builder.build().execute(&mut **transaction).await?;
    }
    Ok(())
}

fn record_locator_from_row(
    row: &PgRow,
) -> Result<PostgresRecordLocator, PostgresMetadataStoreError> {
    let kind = RecordKind::from_str(row.try_get::<String, _>("record_kind")?.as_str())
        .map_err(|_err| PostgresMetadataStoreError::InvalidRecordKind)?;
    let content_hash = match kind {
        RecordKind::Latest => None,
        RecordKind::Version => Some(row.try_get::<String, _>("content_hash")?),
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

pub(super) fn record_locator(
    kind: RecordKind,
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
    use std::time::Duration;

    use shardline_protocol::{RepositoryProvider, RepositoryScope, ShardlineHash};
    use shardline_storage::ObjectKey;

    use super::RecordKind;
    use super::record_locator;
    use crate::{
        DedupeShardMapping, FileChunkRecord, FileRecord, ResumableSession,
        ResumableSessionProtocol, S3ObjectEntry, S3PublishCondition,
    };
    use crate::{RecordMutation, RecordTraversal};

    fn sample_record(repo_scope: Option<RepositoryScope>) -> FileRecord {
        FileRecord {
            file_id: "test.bin".to_owned(),
            content_hash: "a".repeat(64),
            total_bytes: 100,
            chunk_size: 50,
            storage_repr: crate::StorageRepresentation::FixedChunkV1,
            repository_scope: repo_scope,
            chunks: vec![FileChunkRecord {
                hash: "b".repeat(64),
                offset: 0,
                length: 50,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 50,
            }],
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pg_xet_identity_replay_preserves_row_and_conflict_rollback() {
        let Some(pool) = super::super::test_support::connect_isolated_postgres().await else {
            return;
        };
        // This fixture clones the released database's tables. Install the new
        // identity table in its isolated schema before exercising the bridge.
        sqlx::raw_sql(include_str!(
            "../../migrations/20261021000000_xet_file_identities.up.sql"
        ))
        .execute(&pool)
        .await
        .unwrap();
        let store = super::super::PostgresRecordStore::new(pool.clone());
        let mut identity = crate::XetFileIdentity {
            repository_scope: RepositoryScope::new(
                RepositoryProvider::GitHub,
                "team",
                "assets",
                Some("main"),
            )
            .unwrap(),
            sha256: "a".repeat(64),
            file_id: "b".repeat(64),
            total_bytes: 42,
        };
        let identities = std::slice::from_ref(&identity);
        let (first, second) = tokio::join!(
            store.commit_native_shard_metadata_with_xet_identities(&[], &[], &[], identities),
            store.commit_native_shard_metadata_with_xet_identities(&[], &[], &[], identities),
        );
        first.unwrap();
        second.unwrap();
        // Upgrade an existing binding, rather than exercising only fresh writes.
        sqlx::raw_sql(include_str!(
            "../../../../migrations/20261023040000_xet_file_aliases.up.sql"
        ))
        .execute(&pool)
        .await
        .unwrap();
        let before: String =
            sqlx::query_scalar("SELECT xmin::text FROM shardline_xet_file_identities")
                .fetch_one(&pool)
                .await
                .unwrap();
        store
            .commit_native_shard_metadata_with_xet_identities(&[], &[], &[], identities)
            .await
            .unwrap();
        let after: String =
            sqlx::query_scalar("SELECT xmin::text FROM shardline_xet_file_identities")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            before, after,
            "exact replay must not replace the row version"
        );
        let alias_before: String =
            sqlx::query_scalar("SELECT xmin::text FROM shardline_xet_file_aliases")
                .fetch_one(&pool)
                .await
                .unwrap();
        store
            .commit_native_shard_metadata_with_xet_identities(&[], &[], &[], identities)
            .await
            .unwrap();
        let alias_after: String =
            sqlx::query_scalar("SELECT xmin::text FROM shardline_xet_file_aliases")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            alias_before, alias_after,
            "exact replay must not rewrite aliases"
        );
        identity.file_id = "c".repeat(64);
        store
            .commit_native_shard_metadata_with_xet_identities(
                &[],
                &[],
                &[],
                std::slice::from_ref(&identity),
            )
            .await
            .unwrap();
        let aliases: Vec<String> =
            sqlx::query_scalar("SELECT file_id FROM shardline_xet_file_aliases ORDER BY file_id")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(aliases, vec!["b".repeat(64), "c".repeat(64)]);
        let mut conflicting_alias = identity.clone();
        conflicting_alias.file_id = "b".repeat(64);
        conflicting_alias.sha256 = "e".repeat(64);
        assert!(
            store
                .commit_native_shard_metadata_with_xet_identities(
                    &[],
                    &[],
                    &[],
                    &[conflicting_alias.clone()]
                )
                .await
                .is_err()
        );
        assert!(
            store
                .xet_file_identity(
                    &conflicting_alias.repository_scope,
                    &conflicting_alias.sha256
                )
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .xet_file_identity(&identity.repository_scope, &identity.sha256)
                .await
                .unwrap(),
            Some(identity.clone())
        );
        let hub = crate::hub::BoxedHubStore::from_store(super::super::PostgresIndexStore::new(
            pool.clone(),
        ));
        assert_eq!(
            hub.lookup_xet_file_identity(&identity.repository_scope, &identity.sha256)
                .unwrap(),
            Some(identity.clone())
        );
        for (owner, repository) in [("team", "other"), ("other", "assets")] {
            let scope =
                RepositoryScope::new(RepositoryProvider::GitHub, owner, repository, Some("main"))
                    .unwrap();
            assert_eq!(
                hub.lookup_xet_file_identity(&scope, &identity.sha256)
                    .unwrap(),
                None
            );
        }
        // Content identities are repository-wide; revision authorization is
        // enforced when looking up the committed file, not by duplicating this mapping.
        let mut historical = identity.clone();
        historical.repository_scope = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "team",
            "assets",
            Some("historical"),
        )
        .unwrap();
        assert_eq!(
            hub.lookup_xet_file_identity(&historical.repository_scope, &historical.sha256)
                .unwrap(),
            Some(historical)
        );
        let mut new_identity = identity.clone();
        new_identity.sha256 = "d".repeat(64);
        new_identity.file_id = "f".repeat(64);
        let mut conflict = identity.clone();
        conflict.total_bytes = 43;
        assert!(matches!(
            store
                .commit_native_shard_metadata_with_xet_identities(
                    &[],
                    &[],
                    &[],
                    &[new_identity.clone(), conflict]
                )
                .await,
            Err(super::super::PostgresMetadataStoreError::XetFileIdentityConflict)
        ));
        assert_eq!(
            store
                .xet_file_identity(&new_identity.repository_scope, &new_identity.sha256)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .xet_file_identity(&identity.repository_scope, &identity.sha256)
                .await
                .unwrap(),
            Some(identity.clone())
        );

        // Force both replacements to wait on the same existing row. Merely
        // joining two futures can otherwise exercise sequential publications.
        let mut blocker = pool.begin().await.unwrap();
        sqlx::query("SELECT sha256 FROM shardline_xet_file_identities FOR UPDATE")
            .fetch_one(&mut *blocker)
            .await
            .unwrap();
        let mut left = pool.acquire().await.unwrap();
        let mut right = pool.acquire().await.unwrap();
        let left_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *left)
            .await
            .unwrap();
        let right_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *right)
            .await
            .unwrap();
        let mut left_identity = identity.clone();
        left_identity.file_id = "1".repeat(64);
        let mut right_identity = identity.clone();
        right_identity.file_id = "2".repeat(64);
        let release = async {
            let both_waiting = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let waiting: i64 = sqlx::query_scalar(
                        "SELECT COUNT(*) FROM pg_stat_activity \
                         WHERE pid = ANY($1) AND wait_event_type = 'Lock'",
                    )
                    .bind(vec![left_pid, right_pid])
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                    if waiting == 2 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await;
            blocker.rollback().await.unwrap();
            both_waiting.expect("both identity writers must overlap on the locked row");
        };
        let (left_result, right_result, ()) =
            tokio::time::timeout(Duration::from_secs(20), async {
                tokio::join!(
                    store.commit_native_shard_metadata_on_connection_with_xet_identities(
                        &mut left,
                        &[],
                        &[],
                        &[],
                        std::slice::from_ref(&left_identity),
                    ),
                    store.commit_native_shard_metadata_on_connection_with_xet_identities(
                        &mut right,
                        &[],
                        &[],
                        &[],
                        std::slice::from_ref(&right_identity),
                    ),
                    release,
                )
            })
            .await
            .expect("contending identity publications must finish after lock release");
        left_result.unwrap();
        right_result.unwrap();
        let canonical = store
            .xet_file_identity(&identity.repository_scope, &identity.sha256)
            .await
            .unwrap()
            .unwrap();
        assert!(canonical == left_identity || canonical == right_identity);
        let aliases: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT file_id, sha256, total_bytes FROM shardline_xet_file_aliases ORDER BY file_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            aliases,
            ["1", "2", "b", "c"]
                .map(|digit| (digit.repeat(64), identity.sha256.clone(), 42))
                .to_vec(),
            "neither concurrent publication may lose an earlier or competing alias",
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pg_native_shard_dedupe_mapping_batches_preserve_replay_and_duplicate_semantics() {
        let Some(pool) = super::super::test_support::connect_isolated_postgres().await else {
            return;
        };
        let store = super::super::PostgresRecordStore::new(pool.clone());
        let mappings: Vec<_> = (0..5_000_u64)
            .map(|index| {
                let mut hash_bytes = [0_u8; 32];
                hash_bytes[..8].copy_from_slice(&index.to_be_bytes());
                DedupeShardMapping::new(
                    ShardlineHash::from_bytes(hash_bytes),
                    ObjectKey::parse(&format!("shards/{index:08x}/fixture.shard")).unwrap(),
                )
            })
            .collect();

        store
            .commit_native_shard_metadata_with_xet_identities(&[], &mappings, &[], &[])
            .await
            .unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM shardline_dedupe_shards")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, mappings.len() as i64);

        let checked_mapping = &mappings[4_096];
        let hash = crate::xet_hash_hex_string(checked_mapping.chunk_hash());
        let xmin_before: String = sqlx::query_scalar(
            "SELECT xmin::text FROM shardline_dedupe_shards WHERE chunk_hash = $1",
        )
        .bind(&hash)
        .fetch_one(&pool)
        .await
        .unwrap();
        store
            .commit_native_shard_metadata_with_xet_identities(&[], &mappings, &[], &[])
            .await
            .unwrap();
        let xmin_after: String = sqlx::query_scalar(
            "SELECT xmin::text FROM shardline_dedupe_shards WHERE chunk_hash = $1",
        )
        .bind(&hash)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(xmin_before, xmin_after, "an exact replay rewrote the row");

        let remapped = [DedupeShardMapping::new(
            checked_mapping.chunk_hash(),
            ObjectKey::parse("shards/remapped/fixture.shard").unwrap(),
        )];
        store
            .commit_native_shard_metadata_with_xet_identities(&[], &remapped, &[], &[])
            .await
            .unwrap();
        let remapped_key: String = sqlx::query_scalar(
            "SELECT shard_object_key FROM shardline_dedupe_shards WHERE chunk_hash = $1",
        )
        .bind(&hash)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(remapped_key, "shards/remapped/fixture.shard");

        let duplicate_hash = mappings[0].chunk_hash();
        let duplicate_mappings = [
            DedupeShardMapping::new(
                duplicate_hash,
                ObjectKey::parse("shards/duplicate/first.shard").unwrap(),
            ),
            DedupeShardMapping::new(
                duplicate_hash,
                ObjectKey::parse("shards/duplicate/last.shard").unwrap(),
            ),
        ];
        store
            .commit_native_shard_metadata_with_xet_identities(&[], &duplicate_mappings, &[], &[])
            .await
            .unwrap();
        let final_key: String = sqlx::query_scalar(
            "SELECT shard_object_key FROM shardline_dedupe_shards WHERE chunk_hash = $1",
        )
        .bind(crate::xet_hash_hex_string(duplicate_hash))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(final_key, "shards/duplicate/last.shard");
        pool.close().await;
    }

    #[test]
    fn conversion_budget_survives_queued_running_and_completed_cancellation() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let budget = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
            let (release, blocked) = std::sync::mpsc::channel();
            let (started, entered) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                blocked.recv().unwrap();
            });
            entered.await.unwrap();
            let permit = budget.clone().acquire_owned().await.unwrap();
            let (finished, finish) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(super::run_record_conversion(permit, move || {
                finished.send(()).unwrap();
                Ok::<_, super::PostgresMetadataStoreError>(())
            }));
            tokio::task::yield_now().await;
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert_eq!(budget.available_permits(), 0);
            release.send(()).unwrap();
            blocker.await.unwrap();
            finish.await.unwrap();
            let permit = budget.clone().acquire_owned().await.unwrap();
            let (release, blocked) = std::sync::mpsc::channel();
            let (started, entered) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(super::run_record_conversion(permit, move || {
                started.send(()).unwrap();
                blocked.recv().unwrap();
                Ok::<_, super::PostgresMetadataStoreError>(())
            }));
            entered.await.unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert_eq!(budget.available_permits(), 0);
            // The runtime keeps progressing with its single blocking worker busy.
            tokio::time::timeout(std::time::Duration::from_secs(1), tokio::task::yield_now())
                .await
                .unwrap();
            release.send(()).unwrap();
            let permit = budget.clone().acquire_owned().await.unwrap();
            let (started, entered) = tokio::sync::oneshot::channel();
            let task = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                (vec![0u8; 6_549_097], permit)
            });
            entered.await.unwrap();
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
            assert_eq!(budget.available_permits(), 0);
            drop(task);
            let permit = budget.clone().acquire_owned().await.unwrap();
            let (release, blocked) = std::sync::mpsc::channel();
            let (finished, finish) = tokio::sync::oneshot::channel();
            let mut conversion = Box::pin(super::run_record_conversion(permit, move || {
                blocked.recv().unwrap();
                let query =
                    sqlx::query("SELECT $1::jsonb").bind(sqlx::types::Json(sample_record(None)));
                finished.send(()).unwrap();
                Ok::<_, super::PostgresMetadataStoreError>(query)
            }));
            assert!(futures_util::poll!(conversion.as_mut()).is_pending());
            release.send(()).unwrap();
            finish.await.unwrap();
            assert_eq!(budget.available_permits(), 0);
            drop(conversion);
            let _permit = budget.clone().acquire_owned().await.unwrap();
        });
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pg_record_conversion_preserves_public_bytes_and_errors() {
        let Some(pool) = super::super::test_support::connect_isolated_postgres().await else {
            return;
        };
        let store = super::super::PostgresRecordStore::new(pool.clone());
        let scope = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "cpu-team",
            "cpu-repo",
            Some("main"),
        )
        .unwrap();
        let mut record = sample_record(Some(scope.clone()));
        record.total_bytes = 50;
        record.validate_reconstruction_plan().unwrap();
        store.commit_file_version_metadata(&record).await.unwrap();
        let bytes = serde_json::to_vec(&record).unwrap();
        let repository = crate::RepositoryRecordScope::from_repository_scope(&scope);
        for mode in 0..4 {
            let mut seen = Vec::new();
            let callback = |entry: crate::StoredRecord<super::super::PostgresRecordLocator>| {
                seen.push(entry.bytes);
                Ok::<_, super::super::PostgresMetadataStoreError>(())
            };
            match mode {
                0 => store.visit_latest_records(callback).await.unwrap(),
                1 => store.visit_version_records(callback).await.unwrap(),
                2 => store
                    .visit_repository_latest_records(&repository, callback)
                    .await
                    .unwrap(),
                _ => store
                    .visit_repository_version_records(&repository, callback)
                    .await
                    .unwrap(),
            }
            assert_eq!(seen, vec![bytes.clone()]);
            let mut calls = 0;
            let callback = |_| {
                calls += 1;
                Err::<(), _>(super::super::PostgresMetadataStoreError::IntegerOutOfRange(
                    "visitor sentinel".into(),
                ))
            };
            let result = match mode {
                0 => store.visit_latest_records(callback).await,
                1 => store.visit_version_records(callback).await,
                2 => {
                    store
                        .visit_repository_latest_records(&repository, callback)
                        .await
                }
                _ => {
                    store
                        .visit_repository_version_records(&repository, callback)
                        .await
                }
            };
            assert!(
                matches!(result, Err(super::super::PostgresMetadataStoreError::IntegerOutOfRange(ref text)) if text == "visitor sentinel")
            );
            assert_eq!(calls, 1);
        }
        let locator = store.latest_record_locator(&record);
        assert_eq!(store.read_record_bytes(&locator).await.unwrap(), bytes);
        assert_eq!(
            store.read_latest_record_bytes(&record).await.unwrap(),
            Some(bytes)
        );
        sqlx::query("UPDATE shardline_file_records SET record='{}'::jsonb")
            .execute(&pool)
            .await
            .unwrap();
        assert!(matches!(
            store.read_record_bytes(&locator).await,
            Err(super::super::PostgresMetadataStoreError::Sqlx(_))
        ));
        let mut calls = 0;
        assert!(matches!(
            store
                .visit_latest_records(|_| {
                    calls += 1;
                    Ok::<_, super::super::PostgresMetadataStoreError>(())
                })
                .await,
            Err(super::super::PostgresMetadataStoreError::Sqlx(_))
        ));
        assert_eq!(calls, 0);
        sqlx::query("DELETE FROM shardline_file_records")
            .execute(&pool)
            .await
            .unwrap();
        assert!(matches!(
            store.read_record_bytes(&locator).await,
            Err(super::super::PostgresMetadataStoreError::RecordNotFound)
        ));
        assert_eq!(store.read_latest_record_bytes(&record).await.unwrap(), None);
        pool.close().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unchanged_record_publication_refreshes_age_and_changed_payloads_remain_visible() {
        use crate::RecordMutation;
        let Some(pool) = super::super::test_support::connect_isolated_postgres().await else {
            return;
        };
        let store = super::super::PostgresRecordStore::new(pool.clone());
        let mut record = sample_record(None);
        record.total_bytes = 50;
        store.commit_file_version_metadata(&record).await.unwrap();
        for separate in [false, true] {
            sqlx::query("UPDATE shardline_file_records SET updated_at = to_timestamp(1)")
                .execute(&pool)
                .await
                .unwrap();
            if separate {
                store.write_version_record(&record).await.unwrap();
                store.write_latest_record(&record).await.unwrap();
            } else {
                store.commit_file_version_metadata(&record).await.unwrap();
            }
            let refreshed: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM shardline_file_records WHERE updated_at > to_timestamp(1)",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(refreshed, 2);
            for locator in [
                store.latest_record_locator(&record),
                store.version_record_locator(&record),
            ] {
                assert_eq!(
                    store.read_record_bytes(&locator).await.unwrap(),
                    serde_json::to_vec(&record).unwrap()
                );
            }
            record.chunk_size += 1;
            if separate {
                store.write_version_record(&record).await.unwrap();
                store.write_latest_record(&record).await.unwrap();
            } else {
                store.commit_file_version_metadata(&record).await.unwrap();
            }
            for locator in [
                store.latest_record_locator(&record),
                store.version_record_locator(&record),
            ] {
                assert_eq!(
                    store.read_record_bytes(&locator).await.unwrap(),
                    serde_json::to_vec(&record).unwrap()
                );
            }
        }
        pool.close().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pg_version_replay_refreshes_row_and_latest_replay_updates() {
        let Some(pool) = super::super::test_support::connect_isolated_postgres().await else {
            return;
        };
        let store = super::super::PostgresRecordStore::new(pool.clone());
        let record = sample_record(None);

        RecordMutation::write_version_record(&store, &record)
            .await
            .unwrap();
        let version = store.version_record_locator(&record);
        let version_xmin_before: String = sqlx::query_scalar(
            "SELECT xmin::text FROM shardline_file_records WHERE record_key = $1",
        )
        .bind(&version.record_key)
        .fetch_one(&pool)
        .await
        .unwrap();
        RecordMutation::write_version_record(&store, &record)
            .await
            .unwrap();
        let version_xmin_after: String = sqlx::query_scalar(
            "SELECT xmin::text FROM shardline_file_records WHERE record_key = $1",
        )
        .bind(&version.record_key)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_ne!(version_xmin_before, version_xmin_after);

        let mut changed_record = record.clone();
        changed_record.total_bytes += 1;
        RecordMutation::write_version_record(&store, &changed_record)
            .await
            .unwrap();
        let changed_version_xmin: String = sqlx::query_scalar(
            "SELECT xmin::text FROM shardline_file_records WHERE record_key = $1",
        )
        .bind(&version.record_key)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_ne!(version_xmin_after, changed_version_xmin);

        RecordMutation::write_latest_record(&store, &record)
            .await
            .unwrap();
        let latest = store.latest_record_locator(&record);
        let latest_xmin_before: String = sqlx::query_scalar(
            "SELECT xmin::text FROM shardline_file_records WHERE record_key = $1",
        )
        .bind(&latest.record_key)
        .fetch_one(&pool)
        .await
        .unwrap();
        RecordMutation::write_latest_record(&store, &record)
            .await
            .unwrap();
        let latest_xmin_after: String = sqlx::query_scalar(
            "SELECT xmin::text FROM shardline_file_records WHERE record_key = $1",
        )
        .bind(&latest.record_key)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_ne!(latest_xmin_before, latest_xmin_after);
        pool.close().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn staged_aliases_remain_private_until_the_publication_transaction_commits() {
        let Some(pool) = super::super::test_support::connect_isolated_postgres().await else {
            return;
        };
        let store = super::super::PostgresRecordStore::new(pool.clone());
        let mut record = sample_record(None);
        record.total_bytes = 50;
        for commit in [false, true] {
            let mut publication = pool.begin().await.unwrap();
            store
                .stage_file_version_metadata_in_transaction(&mut publication, &record)
                .await
                .unwrap();
            let private_rows: i64 =
                sqlx::query_scalar("SELECT count(*) FROM shardline_file_records")
                    .fetch_one(&mut *publication)
                    .await
                    .unwrap();
            assert_eq!(private_rows, 2);
            let visible_rows: i64 =
                sqlx::query_scalar("SELECT count(*) FROM shardline_file_records")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(visible_rows, 0);
            if commit {
                publication.commit().await.unwrap();
            } else {
                publication.rollback().await.unwrap();
            }
            let visible_rows: i64 =
                sqlx::query_scalar("SELECT count(*) FROM shardline_file_records")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(visible_rows, if commit { 2 } else { 0 });
        }
        pool.close().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_latest_alias_trigger_rolls_back_version_and_preserves_parent_transaction() {
        let Some(pool) = super::super::test_support::connect_isolated_postgres().await else {
            return;
        };
        let store = super::super::PostgresRecordStore::new(pool.clone());
        let mut previous = sample_record(None);
        previous.total_bytes = 50;
        store.commit_file_version_metadata(&previous).await.unwrap();
        sqlx::raw_sql(
            "CREATE FUNCTION reject_latest_alias() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
               IF NEW.record_kind = 'latest' THEN
                 RAISE EXCEPTION 'latest alias rejected by fixture';
               END IF;
               RETURN NEW;
             END $$;
             CREATE TRIGGER reject_latest_alias BEFORE INSERT OR UPDATE
             ON shardline_file_records FOR EACH ROW EXECUTE FUNCTION reject_latest_alias()",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut replacement = previous.clone();
        replacement.content_hash = "c".repeat(64);
        let mut parent = pool.begin().await.unwrap();
        let error = store
            .commit_file_version_metadata_on_connection(&mut parent, &replacement)
            .await
            .unwrap_err();
        let super::PostgresMetadataStoreError::Sqlx(error) = error else {
            panic!("unexpected publication error: {error:?}");
        };
        assert_eq!(
            error.as_database_error().unwrap().message(),
            "latest alias rejected by fixture"
        );
        // The failed inner publication must leave the caller's transaction
        // usable, with neither a replacement version nor a changed latest.
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT record_kind, content_hash FROM shardline_file_records ORDER BY record_kind",
        )
        .fetch_all(&mut *parent)
        .await
        .unwrap();
        assert_eq!(
            rows,
            vec![
                ("latest".into(), previous.content_hash.clone()),
                ("version".into(), previous.content_hash)
            ]
        );
        parent.commit().await.unwrap();
        pool.close().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn conditional_file_cleanup_preserves_a_newer_publisher() {
        let Some(pool) = super::super::test_support::connect_isolated_postgres().await else {
            return;
        };
        let store = super::super::PostgresRecordStore::new(pool.clone());
        let mut loser = sample_record(None);
        loser.total_bytes = 50;
        store.commit_file_version_metadata(&loser).await.unwrap();
        // Cleanup has read the loser; another node publishes before deletion.
        let mut winner = loser.clone();
        winner.content_hash = "c".repeat(64);
        let mut publication = pool.begin().await.unwrap();
        let publisher_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *publication)
            .await
            .unwrap();
        store
            .commit_file_version_metadata_on_connection(&mut publication, &winner)
            .await
            .unwrap();
        let cleanup_pool = pool.clone();
        let cleanup_record = loser.clone();
        let cleanup = tokio::spawn(async move {
            super::super::PostgresRecordStore::new(cleanup_pool)
                .delete_file_version_metadata_if_latest(&cleanup_record)
                .await
        });
        // Observe the actual database lock wait, rather than assuming a sleep
        // is long enough for the cleanup transaction to reach its DELETE.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                     WHERE $1 = ANY(pg_blocking_pids(pid)))",
                )
                .bind(publisher_pid)
                .fetch_one(&pool)
                .await
                .unwrap();
                if waiting {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cleanup must wait on the concurrent publication");
        assert!(!cleanup.is_finished());
        publication.commit().await.unwrap();
        assert!(!cleanup.await.unwrap().unwrap());
        assert!(
            !store
                .delete_file_version_metadata_if_latest(&loser)
                .await
                .unwrap()
        );
        assert_eq!(
            store.read_latest_record_bytes(&loser).await.unwrap(),
            Some(serde_json::to_vec(&winner).unwrap())
        );
        assert_eq!(
            store
                .read_record_bytes(&store.version_record_locator(&loser))
                .await
                .unwrap(),
            serde_json::to_vec(&loser).unwrap()
        );
        assert!(
            store
                .delete_file_version_metadata_if_latest(&winner)
                .await
                .unwrap()
        );
        assert!(
            store
                .read_latest_record_bytes(&winner)
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            store
                .read_record_bytes(&store.version_record_locator(&winner))
                .await,
            Err(super::super::PostgresMetadataStoreError::RecordNotFound)
        ));
        assert!(
            !store
                .delete_file_version_metadata_if_latest(&winner)
                .await
                .unwrap()
        );
        pool.close().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pg_prepared_record_writes_keep_atomicity_and_error_categories() {
        let Some(pool) = super::super::test_support::connect_isolated_postgres().await else {
            return;
        };
        let store = super::super::PostgresRecordStore::new(pool.clone());
        let mut record = sample_record(None);
        record.total_bytes = 50;
        record.validate_reconstruction_plan().unwrap();
        store.commit_file_version_metadata(&record).await.unwrap();
        let canonical = serde_json::to_vec(&record).unwrap();
        assert_eq!(
            store
                .read_record_bytes(&store.latest_record_locator(&record))
                .await
                .unwrap(),
            canonical
        );
        assert_eq!(
            store
                .read_record_bytes(&store.version_record_locator(&record))
                .await
                .unwrap(),
            canonical
        );
        // The version write succeeds first; the latest write is rejected. Both
        // must roll back, including when the shared helper awaits preparation.
        sqlx::query("ALTER TABLE shardline_file_records ADD CONSTRAINT audit_reject_latest CHECK (NOT (file_id='rollback-proof' AND record_kind='latest'))").execute(&pool).await.unwrap();
        record.file_id = "rollback-proof".into();
        assert!(matches!(
            store.commit_file_version_metadata(&record).await,
            Err(super::super::PostgresMetadataStoreError::Sqlx(_))
        ));
        assert!(
            !store
                .record_locator_exists(&store.version_record_locator(&record))
                .await
                .unwrap()
        );
        assert!(
            !store
                .record_locator_exists(&store.latest_record_locator(&record))
                .await
                .unwrap()
        );
        assert!(matches!(
            store
                .commit_native_shard_metadata(&[record.clone()], &[])
                .await,
            Err(super::super::PostgresMetadataStoreError::Sqlx(_))
        ));
        assert!(
            !store
                .record_locator_exists(&store.version_record_locator(&record))
                .await
                .unwrap()
        );
        sqlx::query("ALTER TABLE shardline_file_records DROP CONSTRAINT audit_reject_latest")
            .execute(&pool)
            .await
            .unwrap();
        store
            .commit_native_shard_metadata(&[record.clone()], &[])
            .await
            .unwrap();
        assert!(
            store
                .record_locator_exists(&store.latest_record_locator(&record))
                .await
                .unwrap()
        );
        pool.close().await;
    }

    #[test]
    fn record_locator_produces_latest_locator_without_content_hash() {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "repo", Some("main")).unwrap();
        let record = sample_record(Some(scope));
        let locator = record_locator(RecordKind::Latest, &record, None);

        assert_eq!(locator.file_id(), "test.bin");
        assert!(locator.content_hash().is_none());
        // Latest locator key should not include content hash
        assert!(!locator.record_key().contains(&"a".repeat(64)));
    }

    #[test]
    fn record_locator_produces_version_locator_with_content_hash() {
        let record = sample_record(None);
        let locator = record_locator(
            RecordKind::Version,
            &record,
            Some(record.content_hash.clone()),
        );

        assert_eq!(locator.file_id(), "test.bin");
        assert_eq!(locator.content_hash(), Some(record.content_hash.as_str()));
        // Version locator key should include content hash
        assert!(locator.record_key().contains(&record.content_hash));
    }

    #[test]
    fn record_locator_latest_and_version_have_different_keys() {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitLab, "team", "assets", None).unwrap();
        let record = sample_record(Some(scope));
        let latest = record_locator(RecordKind::Latest, &record, None);
        let version = record_locator(
            RecordKind::Version,
            &record,
            Some(record.content_hash.clone()),
        );

        assert_ne!(latest.record_key(), version.record_key());
        assert_eq!(latest.file_id(), version.file_id());
    }

    #[test]
    fn record_locator_uses_repository_scope_key() {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None).unwrap();
        let record = sample_record(Some(scope));
        let locator = record_locator(RecordKind::Latest, &record, None);

        // The locator key should contain scope information
        assert!(locator.record_key().contains("github"));
    }

    #[test]
    fn record_locator_without_repository_scope() {
        let record = sample_record(None);
        let locator = record_locator(RecordKind::Latest, &record, None);

        // Without scope, the key should still be valid
        assert!(!locator.record_key().is_empty());
        assert_eq!(locator.file_id(), "test.bin");
    }

    #[tokio::test]
    async fn s3_publication_rolls_back_records_when_condition_loses() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = super::super::PostgresRecordStore::new(pool.clone());
        let suffix = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let mut record = sample_record(None);
        record.file_id = format!("atomic-s3-{}-{suffix}", std::process::id());
        let entry = S3ObjectEntry {
            scope_namespace: format!("scope-{suffix}"),
            object_key: "models/weights.bin".to_owned(),
            file_id: record.file_id.clone(),
            size_bytes: record.total_bytes,
            content_hash: record.content_hash.clone(),
            etag: "etag-new".to_owned(),
            user_metadata: vec![("owner".to_owned(), "alice".to_owned())],
            updated_at_unix_seconds: 1,
        };
        let mut expected = entry.clone();
        expected.etag = "etag-that-does-not-exist".to_owned();
        let mut connection = pool.acquire().await.unwrap();
        assert!(
            !store
                .publish_s3_object_on_connection(
                    &mut connection,
                    &record,
                    &entry,
                    &S3PublishCondition::IfUnchanged(Some(expected)),
                    None,
                )
                .await
                .unwrap()
        );
        let record_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM shardline_file_records WHERE file_id = $1")
                .bind(&record.file_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(record_count, 0);

        assert!(
            store
                .publish_s3_object_on_connection(
                    &mut connection,
                    &record,
                    &entry,
                    &S3PublishCondition::Unconditional,
                    None,
                )
                .await
                .unwrap()
        );
        let record_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM shardline_file_records WHERE file_id = $1")
                .bind(&record.file_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(record_count, 2);

        assert!(
            store
                .delete_s3_object_on_connection(
                    &mut connection,
                    &entry.scope_namespace,
                    &entry.object_key,
                    &entry.file_id,
                )
                .await
                .unwrap()
        );
        let record_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM shardline_file_records WHERE file_id = $1")
                .bind(&record.file_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(record_count, 0);
        let listing_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM shardline_s3_objects
             WHERE scope_namespace = $1 AND object_key = $2",
        )
        .bind(&entry.scope_namespace)
        .bind(&entry.object_key)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(listing_count, 0);
    }

    #[tokio::test]
    async fn stale_multipart_completion_cannot_publish() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let record_store = super::super::PostgresRecordStore::new(pool.clone());
        let index_store = super::super::PostgresIndexStore::new(pool.clone());
        let suffix = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let session = ResumableSession::new(
            format!("fenced-completion-{suffix}"),
            ResumableSessionProtocol::S3Multipart,
            format!("scope-{suffix}"),
            "large.bin".to_owned(),
            std::time::Duration::from_secs(
                u64::try_from(chrono::Utc::now().timestamp()).unwrap() + 3_600,
            ),
        );
        assert!(
            index_store
                .create_resumable_session(&session)
                .await
                .unwrap()
        );
        let first = index_store
            .begin_resumable_completion(session.session_id())
            .await
            .unwrap()
            .unwrap()
            .0;
        let second = index_store
            .begin_resumable_completion(session.session_id())
            .await
            .unwrap()
            .unwrap()
            .0;
        assert!(second.fence_epoch() > first.fence_epoch());

        let mut record = sample_record(None);
        record.file_id = format!("fenced-record-{suffix}");
        let entry = S3ObjectEntry {
            scope_namespace: session.scope_namespace().to_owned(),
            object_key: session.target_key().to_owned(),
            file_id: record.file_id.clone(),
            size_bytes: record.total_bytes,
            content_hash: record.content_hash.clone(),
            etag: "etag".to_owned(),
            user_metadata: Vec::new(),
            updated_at_unix_seconds: 1,
        };
        let mut connection = pool.acquire().await.unwrap();
        assert!(
            !record_store
                .publish_s3_object_on_connection(
                    &mut connection,
                    &record,
                    &entry,
                    &S3PublishCondition::Unconditional,
                    Some(&first.completion_fence()),
                )
                .await
                .unwrap()
        );
        assert!(
            record_store
                .publish_s3_object_on_connection(
                    &mut connection,
                    &record,
                    &entry,
                    &S3PublishCondition::Unconditional,
                    Some(&second.completion_fence()),
                )
                .await
                .unwrap()
        );
        assert_eq!(
            index_store
                .resumable_session_by_id(session.session_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            crate::ResumableSessionState::Completed
        );
        let events = index_store
            .resumable_reliability_events(session.session_id())
            .await
            .unwrap();
        shardline_reliability::verify_state_transition_chain(&events).unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| (event.before.as_str(), event.after.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("active", "active"),
                ("active", "completing"),
                ("completing", "completing"),
                ("completing", "completed"),
            ]
        );
    }
}
