use shardline_storage::ObjectKey;
use sqlx::{Postgres, Transaction, query, query_scalar};
use thiserror::Error;

use super::{PostgresIndexStore, PostgresMetadataStoreError};

// Distinct domains preserve existing resource fences. Completed per-object
// rows can be removed: one persistent sequence prevents reuse of old permits.
#[cfg(test)]
const REMOTE_DELETE_DOMAIN: &str = "maintenance-remote-delete";
#[cfg(test)]
const REMOTE_DELETE_SEQUENCE_DOMAIN: &str = "maintenance-remote-delete-sequence";

/// Identity of one durably admitted remote delete.
///
/// Dropping this value does not clear the pending marker. It must not expire
/// because a process died, a database connection closed, or a timer elapsed.
#[derive(Debug)]
#[must_use = "retain this permit until the remote delete is proved settled"]
pub struct PostgresRemoteDeletePermit {
    object_key: ObjectKey,
    epoch: i64,
}

impl PostgresRemoteDeletePermit {
    /// Returns the object covered by this permit.
    #[must_use]
    pub const fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }

    /// Returns the monotonically increasing attempt identity.
    #[must_use]
    pub const fn epoch(&self) -> i64 {
        self.epoch
    }
}

/// A later mutation cannot safely protect or publish this object yet.
#[derive(Debug, Error)]
#[error("object has an unresolved remote delete; retry after reconciliation")]
pub struct RemoteDeletePending {
    object_key: ObjectKey,
}

impl RemoteDeletePending {
    /// Returns the affected object for trusted diagnostics.
    #[must_use]
    pub const fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }
}

impl PostgresMetadataStoreError {
    /// Returns whether an unresolved remote delete requires a retryable response.
    #[must_use]
    pub fn is_remote_delete_pending(&self) -> bool {
        if let Self::Sqlx(error) = self
            && let sqlx::Error::Io(error) = error.as_ref()
        {
            return error
                .get_ref()
                .is_some_and(|source| source.is::<RemoteDeletePending>());
        }
        if let Self::Sqlx(error) = self
            && let sqlx::Error::Database(error) = error.as_ref()
        {
            return error.code().as_deref() == Some("SLD01");
        }
        false
    }
}

impl PostgresIndexStore {
    /// Installs the compatibility guard using the existing migration-owner role.
    ///
    /// Called by `db migrate up` after the released schema is complete. This
    /// preserves the migration catalog and existing writer grants. The functions
    /// validate existing permissions and their installed schema with fixed
    /// search paths; the trigger also protects older retention writers.
    /// Installation commits atomically. Runtime writer roles need no DDL rights.
    ///
    /// # Errors
    ///
    /// Returns a database failure, including insufficient migration-owner rights.
    pub async fn initialize_remote_delete_guard(&self) -> Result<(), sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let schema = query_scalar::<_, String>("SELECT current_schema()")
            .fetch_one(&mut *transaction)
            .await?;
        let quoted_schema = format!("\"{}\"", schema.replace('"', "\"\""));
        let schema_literal = format!("E'{}'", schema.replace('\\', "\\\\").replace('\'', "\\'"));
        // Escaped identifiers and literals replace SQL header placeholders;
        // no catalog text is interpolated into dollar-quoted function bodies.
        // Expand only tokens from the original template. A schema name may
        // itself contain either token; never expand inserted catalog text.
        let sql = include_str!("remote_delete_guard.sql")
            .split("@SCHEMA@")
            .map(|part| part.replace("@SCHEMA_LITERAL@", &schema_literal))
            .collect::<Vec<_>>()
            .join(&quoted_schema);
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await
    }

    /// Commits a pending marker before any request for this delete is issued.
    ///
    /// The caller must own the deployment's exclusive maintenance barrier and
    /// use its non-replaceable work pool. These store primitives do not acquire
    /// that barrier or send a remote request. An earlier pending marker rejects
    /// admission. One persistent global sequence prevents reuse of completed
    /// identities without retaining a completed row for every object.
    ///
    /// # Errors
    ///
    /// Returns a pending-delete error, an exhausted epoch, or a database failure.
    pub async fn begin_remote_delete(
        &self,
        object_key: &ObjectKey,
    ) -> Result<PostgresRemoteDeletePermit, PostgresMetadataStoreError> {
        let mut transaction = self.pool.begin().await?;
        let initialized = query_scalar::<_, bool>(
            "SELECT to_regprocedure('shardline_remote_delete_guard_v1(text,text)') IS NOT NULL
             AND to_regprocedure('shardline_remote_delete_begin_v1(text,text)') IS NOT NULL
             AND to_regprocedure('shardline_remote_delete_finish_v1(text,text,bigint)') IS NOT NULL
             AND EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='shardline_retention_holds'::regclass
                        AND tgname='shardline_retention_remote_delete_v1' AND tgenabled='O')
             AND EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='shardline_dedupe_shards'::regclass
                        AND tgname='shardline_dedupe_remote_delete_v1' AND tgenabled='O')
             AND (SELECT count(*) FROM pg_trigger
                  WHERE tgrelid IN ('shardline_file_records'::regclass,
                                    'shardline_file_reconstructions'::regclass,
                                    'shardline_stored_objects'::regclass)
                  AND tgname='shardline_reference_remote_delete_v1' AND tgenabled='O') = 3",
        )
        .fetch_one(&mut *transaction)
        .await?;
        if !initialized {
            return Err(PostgresMetadataStoreError::Unsupported(
                "remote delete protection requires the existing db migrate up command".to_owned(),
            ));
        }
        let epoch =
            query_scalar::<_, i64>("SELECT shardline_remote_delete_begin_v1(current_schema(), $1)")
                .bind(object_key.as_str())
                .fetch_one(&mut *transaction)
                .await
                .map_err(|error| {
                    if error
                        .as_database_error()
                        .and_then(|error| error.code())
                        .as_deref()
                        == Some("SLD02")
                    {
                        PostgresMetadataStoreError::IntegerOutOfRange(
                            "remote delete epoch".to_owned(),
                        )
                    } else {
                        PostgresMetadataStoreError::from(error)
                    }
                })?;
        transaction.commit().await?;
        Ok(PostgresRemoteDeletePermit {
            object_key: object_key.clone(),
            epoch,
        })
    }

    /// Marks this exact attempt settled after every issued request has settled.
    ///
    /// A successful response to a retry is insufficient if a previous request
    /// can still apply later. Callers must prevent transparent mutation retries
    /// or supply stronger remote evidence. Use the original ownership-bound pool:
    /// connection loss must leave the marker pending rather than reconnecting.
    /// This method verifies the attempt identity, not the remote evidence itself.
    ///
    /// # Errors
    ///
    /// Returns a database failure. Returns `false` for an already settled or
    /// superseded permit; an old permit cannot clear a newer pending attempt.
    pub async fn finish_remote_delete(
        &self,
        permit: &PostgresRemoteDeletePermit,
    ) -> Result<bool, PostgresMetadataStoreError> {
        let mut transaction = self.pool.begin().await?;
        let changed = query_scalar::<_, bool>(
            "SELECT shardline_remote_delete_finish_v1(current_schema(), $1, $2)",
        )
        .bind(permit.object_key().as_str())
        .bind(permit.epoch())
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(changed)
    }

    /// Rejects a pending delete in the same transaction as a later publication.
    ///
    /// Hold the transaction until the protected mutation commits. An independent
    /// preflight check leaves a race between checking and publishing. This only
    /// protects writers that use this boundary. The installed retention trigger
    /// also protects older retention writers; other publication paths need their
    /// own transactional check.
    ///
    /// # Errors
    ///
    /// Returns a pending-delete error or a database failure.
    pub async fn require_remote_delete_settled(
        transaction: &mut Transaction<'_, Postgres>,
        object_key: &ObjectKey,
    ) -> Result<(), PostgresMetadataStoreError> {
        let initialized = query_scalar::<_, bool>(
            "SELECT to_regprocedure('shardline_remote_delete_guard_v1(text,text)') IS NOT NULL",
        )
        .fetch_one(&mut **transaction)
        .await?;
        if !initialized {
            // Legacy schemas admit no new remote-delete permits. Preserve old
            // writer credentials until the normal migration owner installs the guard.
            return Ok(());
        }
        query("SELECT shardline_remote_delete_guard_v1(current_schema(), $1)")
            .bind(object_key.as_str())
            .execute(&mut **transaction)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::{AsyncIndexStore, RecordTraversal, RetentionHold};

    #[tokio::test]
    async fn file_publication_serializes_delete_admission_before_any_marker_exists() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let hash = "ac".repeat(32);
        let key = ObjectKey::parse(&format!("ac/{hash}")).unwrap();
        let mut publication = pool.begin().await.unwrap();
        query("INSERT INTO shardline_file_records(record_key,record_kind,scope_key,file_id,content_hash,record) VALUES ('race','version','global','file','content',$1)")
            .bind(sqlx::types::Json(serde_json::json!({"chunk_size": 1, "chunks": [{"hash": hash}]})))
            .execute(&mut *publication).await.unwrap();
        let mut admission = Box::pin(store.begin_remote_delete(&key));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut admission)
                .await
                .is_err()
        );
        publication.rollback().await.unwrap();
        let permit = tokio::time::timeout(std::time::Duration::from_secs(5), admission)
            .await
            .unwrap()
            .unwrap();
        assert!(store.finish_remote_delete(&permit).await.unwrap());
    }

    #[tokio::test]
    async fn large_file_publication_uses_bounded_delete_locks() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let chunks: Vec<_> = (0..20_000)
            .map(|ordinal| serde_json::json!({"hash": format!("{ordinal:064x}")}))
            .collect();
        let record = serde_json::json!({"chunk_size": 1, "chunks": chunks});
        let mut transaction = pool.begin().await.unwrap();
        query("INSERT INTO shardline_file_records(record_key,record_kind,scope_key,file_id,content_hash,record) VALUES ('large','version','global','large','content',$1)")
            .bind(sqlx::types::Json(record)).execute(&mut *transaction).await.unwrap();
        query("INSERT INTO shardline_dedupe_shards(chunk_hash, shard_object_key)
               SELECT ordinal::text, 'large/shard/' || ordinal::text FROM generate_series(1, 20000) ordinal")
            .execute(&mut *transaction).await.unwrap();
        let locks: i64 = query_scalar(
            "SELECT count(*) FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory'",
        )
        .fetch_one(&mut *transaction)
        .await
        .unwrap();
        assert!(locks > 0 && locks <= 256);
        transaction.commit().await.unwrap();
    }

    #[tokio::test]
    async fn file_version_publication_preserves_latest_and_version_on_pending_delete() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let index = PostgresIndexStore::new(pool.clone());
        let records = super::super::PostgresRecordStore::new(pool.clone());
        let mut record = crate::FileRecord {
            file_id: "publication.bin".to_owned(),
            content_hash: "cc".repeat(32),
            total_bytes: 1,
            chunk_size: 1,
            storage_repr: crate::StorageRepresentation::FixedChunkV1,
            repository_scope: None,
            chunks: vec![crate::FileChunkRecord {
                hash: "dd".repeat(32),
                offset: 0,
                length: 1,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 1,
            }],
        };
        record.validate_reconstruction_plan().unwrap();
        records.commit_file_version_metadata(&record).await.unwrap();
        let original = serde_json::to_vec(&record).unwrap();
        let locator = records.latest_record_locator(&record);
        record.content_hash = "ee".repeat(32);
        let chunk = record.chunks.first_mut().unwrap();
        chunk.hash = "ff".repeat(32);
        let key = ObjectKey::parse(&format!("ff/{}", chunk.hash)).unwrap();
        let permit = index.begin_remote_delete(&key).await.unwrap();
        assert!(
            records
                .commit_file_version_metadata(&record)
                .await
                .unwrap_err()
                .is_remote_delete_pending()
        );
        let entry = crate::S3ObjectEntry {
            scope_namespace: "scope".to_owned(),
            object_key: "publication.bin".to_owned(),
            file_id: record.file_id.clone(),
            size_bytes: record.total_bytes,
            content_hash: record.content_hash.clone(),
            etag: "etag".to_owned(),
            user_metadata: Vec::new(),
            updated_at_unix_seconds: 1,
        };
        let mut connection = pool.acquire().await.unwrap();
        assert!(
            records
                .publish_s3_object_on_connection(
                    &mut connection,
                    &record,
                    &entry,
                    &crate::S3PublishCondition::Unconditional,
                    None
                )
                .await
                .unwrap_err()
                .is_remote_delete_pending()
        );
        drop(connection);
        assert_eq!(
            query_scalar::<_, i64>("SELECT count(*) FROM shardline_s3_objects")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(records.read_record_bytes(&locator).await.unwrap(), original);
        assert_eq!(
            query_scalar::<_, i64>("SELECT count(*) FROM shardline_file_records")
                .fetch_one(&pool)
                .await
                .unwrap(),
            2
        );
        assert!(index.finish_remote_delete(&permit).await.unwrap());
        records.commit_file_version_metadata(&record).await.unwrap();
        assert_eq!(
            records.read_record_bytes(&locator).await.unwrap(),
            serde_json::to_vec(&record).unwrap()
        );
    }

    #[tokio::test]
    async fn direct_file_references_reject_pending_chunk_container_cache_and_dedupe_keys() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let hash = "aa".repeat(32);
        query("INSERT INTO shardline_dedupe_shards(chunk_hash, shard_object_key) VALUES ($1, 'mapped/shard')")
            .bind(&hash).execute(&pool).await.unwrap();
        let insert = "INSERT INTO shardline_file_records
                      (record_key, record_kind, scope_key, file_id, content_hash, record)
                      VALUES ($1, $2, 'global', 'file', 'content', $3)
                      ON CONFLICT(record_key) DO UPDATE SET record=EXCLUDED.record";
        for physical in [
            format!("aa/{hash}"),
            format!("xorbs/default/aa/{hash}.xorb"),
            format!("_xorb_chunks/aa/{hash}"),
            "mapped/shard".to_owned(),
        ] {
            let key = ObjectKey::parse(&physical).unwrap();
            let permit = store.begin_remote_delete(&key).await.unwrap();
            for kind in ["latest", "version"] {
                let record = serde_json::json!({"chunk_size": 1, "chunks": [{"hash": hash}]});
                let error = query(insert)
                    .bind(kind)
                    .bind(kind)
                    .bind(sqlx::types::Json(record))
                    .execute(&pool)
                    .await
                    .unwrap_err();
                assert!(PostgresMetadataStoreError::from(error).is_remote_delete_pending());
            }
            let count: i64 = query_scalar("SELECT count(*) FROM shardline_file_records")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(count, 0);
            assert!(store.finish_remote_delete(&permit).await.unwrap());
        }
        // A term-based record references a container, not a same-hash chunk.
        let key = ObjectKey::parse(&format!("aa/{hash}")).unwrap();
        let permit = store.begin_remote_delete(&key).await.unwrap();
        let record = serde_json::json!({"chunk_size": 0, "chunks": [{"hash": hash}]});
        query(insert)
            .bind("term")
            .bind("version")
            .bind(sqlx::types::Json(&record))
            .execute(&pool)
            .await
            .unwrap();
        assert!(store.finish_remote_delete(&permit).await.unwrap());
        let container_key = ObjectKey::parse(&format!("xorbs/default/aa/{hash}.xorb")).unwrap();
        let container_permit = store.begin_remote_delete(&container_key).await.unwrap();
        let error = query(insert)
            .bind("term")
            .bind("version")
            .bind(sqlx::types::Json(record))
            .execute(&pool)
            .await
            .unwrap_err();
        assert!(PostgresMetadataStoreError::from(error).is_remote_delete_pending());
        assert!(store.finish_remote_delete(&container_permit).await.unwrap());
    }

    #[tokio::test]
    async fn reconstruction_and_object_registration_reject_pending_container_and_cache() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let hash = "bb".repeat(32);
        for physical in [
            format!("xorbs/default/bb/{hash}.xorb"),
            format!("_xorb_chunks/bb/{hash}"),
        ] {
            let key = ObjectKey::parse(&physical).unwrap();
            let permit = store.begin_remote_delete(&key).await.unwrap();
            let terms = serde_json::json!({"terms": [{"object_hash": hash}]});
            let error = query(
                "INSERT INTO shardline_file_reconstructions(file_id, terms) VALUES ('file', $1)",
            )
            .bind(sqlx::types::Json(terms))
            .execute(&pool)
            .await
            .unwrap_err();
            assert!(PostgresMetadataStoreError::from(error).is_remote_delete_pending());
            let registration_error =
                query("INSERT INTO shardline_stored_objects(object_hash) VALUES ($1)")
                    .bind(&hash)
                    .execute(&pool)
                    .await
                    .unwrap_err();
            assert!(
                PostgresMetadataStoreError::from(registration_error).is_remote_delete_pending()
            );
            assert!(store.finish_remote_delete(&permit).await.unwrap());
        }
        query("INSERT INTO shardline_file_reconstructions(file_id, terms) VALUES ('file', $1)")
            .bind(sqlx::types::Json(
                serde_json::json!({"terms": [{"object_hash": hash}]}),
            ))
            .execute(&pool)
            .await
            .unwrap();
        query("INSERT INTO shardline_stored_objects(object_hash) VALUES ($1)")
            .bind(&hash)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn dedupe_publication_holds_the_delete_lock_until_transaction_completion() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let key = ObjectKey::parse("shards/default/aa/concurrent.shard").unwrap();
        let mut publication = pool.begin().await.unwrap();
        query("INSERT INTO shardline_dedupe_shards(chunk_hash, shard_object_key) VALUES ('concurrent', $1)")
            .bind(key.as_str()).execute(&mut *publication).await.unwrap();
        let mut admission = Box::pin(store.begin_remote_delete(&key));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut admission)
                .await
                .is_err()
        );
        publication.rollback().await.unwrap();
        let permit = tokio::time::timeout(std::time::Duration::from_secs(5), admission)
            .await
            .unwrap()
            .unwrap();
        assert!(store.finish_remote_delete(&permit).await.unwrap());
    }

    #[tokio::test]
    async fn legacy_dedupe_publication_rejects_pending_delete_and_preserves_previous_mapping() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let key = ObjectKey::parse("shards/default/aa/pending.shard").unwrap();
        let insert = "INSERT INTO shardline_dedupe_shards(chunk_hash, shard_object_key)
                      VALUES ($1, $2) ON CONFLICT(chunk_hash)
                      DO UPDATE SET shard_object_key=EXCLUDED.shard_object_key";
        query(insert)
            .bind("existing")
            .bind("safe/shard")
            .execute(&pool)
            .await
            .unwrap();
        let permit = store.begin_remote_delete(&key).await.unwrap();
        for hash in ["new", "existing"] {
            let error = query(insert)
                .bind(hash)
                .bind(key.as_str())
                .execute(&pool)
                .await
                .unwrap_err();
            assert!(PostgresMetadataStoreError::from(error).is_remote_delete_pending());
        }
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT chunk_hash, shard_object_key FROM shardline_dedupe_shards")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(rows, vec![("existing".to_owned(), "safe/shard".to_owned())]);
        let error = query(
            "INSERT INTO shardline_dedupe_shards(chunk_hash, shard_object_key)
             VALUES ('batch-safe', 'safe/batch'), ('batch-pending', $1)",
        )
        .bind(key.as_str())
        .execute(&pool)
        .await
        .unwrap_err();
        assert!(PostgresMetadataStoreError::from(error).is_remote_delete_pending());
        let batch_rows: i64 = query_scalar(
            "SELECT COUNT(*) FROM shardline_dedupe_shards WHERE chunk_hash LIKE 'batch-%'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(batch_rows, 0);
        query(insert)
            .bind("unrelated")
            .bind("safe/other")
            .execute(&pool)
            .await
            .unwrap();
        assert!(store.finish_remote_delete(&permit).await.unwrap());
        query(insert)
            .bind("new")
            .bind(key.as_str())
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn reference_writers_need_no_new_retention_or_fence_privileges() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let schema: String = query_scalar("SELECT current_schema()")
            .fetch_one(&pool)
            .await
            .unwrap();
        let role = format!("{schema}_writer");
        let identifier = format!("\"{}\"", schema.replace('"', "\"\""));
        let role_identifier = format!("\"{}\"", role.replace('"', "\"\""));
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE {role_identifier};
             GRANT USAGE ON SCHEMA {identifier} TO {role_identifier};
             GRANT SELECT, INSERT, UPDATE ON {identifier}.shardline_dedupe_shards,
                 {identifier}.shardline_file_records,
                 {identifier}.shardline_file_reconstructions,
                 {identifier}.shardline_stored_objects TO {role_identifier};"
        )))
        .execute(&pool)
        .await
        .unwrap();
        let key = ObjectKey::parse("shards/default/aa/limited.shard").unwrap();
        let permit = store.begin_remote_delete(&key).await.unwrap();
        let hash = "ab".repeat(32);
        let container = ObjectKey::parse(&format!("xorbs/default/ab/{hash}.xorb")).unwrap();
        let container_permit = store.begin_remote_delete(&container).await.unwrap();
        let mut connection = pool.acquire().await.unwrap();
        query(sqlx::AssertSqlSafe(format!(
            "SET SESSION AUTHORIZATION {role_identifier}"
        )))
        .execute(&mut *connection)
        .await
        .unwrap();
        let insert =
            "INSERT INTO shardline_dedupe_shards(chunk_hash, shard_object_key) VALUES ($1, $2)";
        query(insert)
            .bind("unrelated")
            .bind("safe/shard")
            .execute(&mut *connection)
            .await
            .unwrap();
        let error = query(insert)
            .bind("pending")
            .bind(key.as_str())
            .execute(&mut *connection)
            .await
            .unwrap_err();
        assert!(PostgresMetadataStoreError::from(error).is_remote_delete_pending());
        for hash_to_publish in ["cd".repeat(32), hash.clone()] {
            let record =
                serde_json::json!({"chunk_size": 0, "chunks": [{"hash": hash_to_publish}]});
            let terms = serde_json::json!({"terms": [{"object_hash": hash_to_publish}]});
            let results = [
                query("INSERT INTO shardline_file_records(record_key,record_kind,scope_key,file_id,content_hash,record) VALUES ($1,'version','global','file','content',$2)")
                    .bind(&hash_to_publish).bind(sqlx::types::Json(record)).execute(&mut *connection).await,
                query("INSERT INTO shardline_file_reconstructions(file_id,terms) VALUES ($1,$2)")
                    .bind(&hash_to_publish).bind(sqlx::types::Json(terms)).execute(&mut *connection).await,
                query("INSERT INTO shardline_stored_objects(object_hash) VALUES ($1)")
                    .bind(&hash_to_publish).execute(&mut *connection).await,
            ];
            for result in results {
                if hash_to_publish == hash {
                    assert!(
                        PostgresMetadataStoreError::from(result.unwrap_err())
                            .is_remote_delete_pending()
                    );
                } else {
                    result.unwrap();
                }
            }
        }
        query("RESET SESSION AUTHORIZATION")
            .execute(&mut *connection)
            .await
            .unwrap();
        drop(connection);
        assert!(store.finish_remote_delete(&permit).await.unwrap());
        assert!(store.finish_remote_delete(&container_permit).await.unwrap());
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP OWNED BY {role_identifier}; DROP ROLE {role_identifier};"
        )))
        .execute(&pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn installed_guard_rejects_cross_schema_admission_and_settlement() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        for sql in [
            "SELECT shardline_remote_delete_guard_v1('public', 'remote-delete/scope')",
            "SELECT shardline_remote_delete_begin_v1('public', 'remote-delete/scope')",
            "SELECT shardline_remote_delete_finish_v1('public', 'remote-delete/scope', 1)",
        ] {
            let error = query(sql).execute(&pool).await.unwrap_err();
            assert_eq!(
                error.as_database_error().unwrap().code().as_deref(),
                Some("42501")
            );
        }
    }

    #[tokio::test]
    async fn guard_installation_safely_quotes_schema_identifiers_literals_and_template_tokens() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let name = format!(
            "shardline_test_{}_\"'\\$guard$@SCHEMA@@SCHEMA_LITERAL@",
            std::process::id()
        );
        let identifier = format!("\"{}\"", name.replace('"', "\"\""));
        query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {identifier}")))
            .execute(&pool)
            .await
            .unwrap();
        for table in [
            "shardline_retention_holds",
            "shardline_quarantine_candidates",
            "shardline_resource_fences",
            "shardline_dedupe_shards",
            "shardline_file_records",
            "shardline_file_reconstructions",
            "shardline_stored_objects",
        ] {
            query(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {identifier}.{table} (LIKE {table} INCLUDING ALL)"
            )))
            .execute(&pool)
            .await
            .unwrap();
        }
        let options = pool.options().clone().after_connect(move |connection, _| {
            let sql = format!("SET search_path TO {identifier}, public");
            Box::pin(async move {
                query(sqlx::AssertSqlSafe(sql)).execute(connection).await?;
                Ok(())
            })
        });
        let scoped = options
            .connect_with((*pool.connect_options()).clone())
            .await
            .unwrap();
        let store = PostgresIndexStore::new(scoped);
        store.initialize_remote_delete_guard().await.unwrap();
        let key = ObjectKey::parse("remote-delete/schema-escaping").unwrap();
        let permit = store.begin_remote_delete(&key).await.unwrap();
        assert!(store.finish_remote_delete(&permit).await.unwrap());
    }

    #[tokio::test]
    async fn missing_retention_trigger_disables_delete_admission_without_breaking_holds() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        store.initialize_remote_delete_guard().await.unwrap();
        query("DROP TRIGGER shardline_retention_remote_delete_v1 ON shardline_retention_holds")
            .execute(&pool)
            .await
            .unwrap();
        let key = ObjectKey::parse("remote-delete/uninitialized").unwrap();
        assert!(matches!(
            store.begin_remote_delete(&key).await.unwrap_err(),
            PostgresMetadataStoreError::Unsupported(_)
        ));
        let count: i64 = query_scalar("SELECT count(*) FROM shardline_resource_fences")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
        let hold = RetentionHold::new(key, "legacy writer".to_owned(), 1, None).unwrap();
        store.upsert_retention_hold(&hold).await.unwrap();
    }

    #[tokio::test]
    async fn retention_publication_rejects_pending_delete_without_changing_metadata_or_evidence() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool);
        let key = ObjectKey::parse("remote-delete/retention").unwrap();
        store.initialize_remote_delete_guard().await.unwrap();
        let hold = RetentionHold::new(key.clone(), "protection".to_owned(), 1, None).unwrap();
        let permit = store.begin_remote_delete(&key).await.unwrap();
        assert!(
            store
                .upsert_retention_hold(&hold)
                .await
                .unwrap_err()
                .is_remote_delete_pending()
        );
        assert!(
            store
                .delete_retention_hold(&key)
                .await
                .unwrap_err()
                .is_remote_delete_pending()
        );
        assert!(store.retention_hold(&key).await.unwrap().is_none());
        assert_eq!(
            query_scalar::<_, i64>("SELECT COUNT(*) FROM shardline_reliability_events")
                .fetch_one(&store.pool)
                .await
                .unwrap(),
            0
        );
        assert!(store.finish_remote_delete(&permit).await.unwrap());
        store.upsert_retention_hold(&hold).await.unwrap();
        assert_eq!(store.retention_hold(&key).await.unwrap(), Some(hold));
        assert!(store.delete_retention_hold(&key).await.unwrap());
    }
    use std::time::Duration;

    #[tokio::test]
    async fn pending_delete_survives_connection_replacement_and_rejects_only_its_object() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let key = ObjectKey::parse("remote-delete/pending").unwrap();
        store.initialize_remote_delete_guard().await.unwrap();
        let permit = store.begin_remote_delete(&key).await.unwrap();
        assert_eq!(permit.epoch(), 1);
        let options = pool.options().clone();
        let connection = (*pool.connect_options()).clone();
        pool.close().await;
        let replacement = options.connect_with(connection).await.unwrap();
        let mut writer = replacement.begin().await.unwrap();
        let error = PostgresIndexStore::require_remote_delete_settled(&mut writer, &key)
            .await
            .unwrap_err();
        assert!(error.is_remote_delete_pending());
        writer.rollback().await.unwrap();
        let mut unrelated_writer = replacement.begin().await.unwrap();
        let unrelated = ObjectKey::parse("remote-delete/unrelated").unwrap();
        PostgresIndexStore::require_remote_delete_settled(&mut unrelated_writer, &unrelated)
            .await
            .unwrap();
        unrelated_writer.rollback().await.unwrap();
        let replacement = PostgresIndexStore::new(replacement);
        assert!(
            replacement
                .begin_remote_delete(&key)
                .await
                .unwrap_err()
                .is_remote_delete_pending()
        );
    }

    #[tokio::test]
    async fn obsolete_completion_cannot_clear_a_newer_delete() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool);
        let key = ObjectKey::parse("remote-delete/epochs").unwrap();
        store.initialize_remote_delete_guard().await.unwrap();
        let first = store.begin_remote_delete(&key).await.unwrap();
        assert!(store.finish_remote_delete(&first).await.unwrap());
        assert!(!store.finish_remote_delete(&first).await.unwrap());
        let next = store.begin_remote_delete(&key).await.unwrap();
        assert_eq!(next.epoch(), 2);
        assert!(!store.finish_remote_delete(&first).await.unwrap());
        let mut writer = store.pool.begin().await.unwrap();
        assert!(
            PostgresIndexStore::require_remote_delete_settled(&mut writer, &key)
                .await
                .unwrap_err()
                .is_remote_delete_pending()
        );
        writer.rollback().await.unwrap();
        assert!(store.finish_remote_delete(&next).await.unwrap());
        let mut settled_writer = store.pool.begin().await.unwrap();
        PostgresIndexStore::require_remote_delete_settled(&mut settled_writer, &key)
            .await
            .unwrap();
        settled_writer.commit().await.unwrap();
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM shardline_resource_fences WHERE domain=$1",
            )
            .bind(REMOTE_DELETE_DOMAIN)
            .fetch_one(&store.pool)
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM shardline_resource_fences WHERE domain=$1",
            )
            .bind(REMOTE_DELETE_SEQUENCE_DOMAIN)
            .fetch_one(&store.pool)
            .await
            .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn publication_check_serializes_admission_even_when_no_marker_exists() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool);
        let key = ObjectKey::parse("remote-delete/absent-row").unwrap();
        store.initialize_remote_delete_guard().await.unwrap();
        let mut writer = store.pool.begin().await.unwrap();
        PostgresIndexStore::require_remote_delete_settled(&mut writer, &key)
            .await
            .unwrap();
        let task_store = store.clone();
        let task_key = key.clone();
        let mut admission =
            tokio::spawn(async move { task_store.begin_remote_delete(&task_key).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut admission)
                .await
                .is_err()
        );
        writer.commit().await.unwrap();
        let permit = tokio::time::timeout(Duration::from_secs(2), admission)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(permit.object_key(), &key);
    }

    #[tokio::test]
    async fn concurrent_admissions_allocate_unique_epochs_and_remove_completed_markers() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool);
        let mut admissions = tokio::task::JoinSet::new();
        store.initialize_remote_delete_guard().await.unwrap();
        for number in 0..16 {
            let key = ObjectKey::parse(&format!("remote-delete/concurrent/{number}")).unwrap();
            let task_store = store.clone();
            admissions.spawn(async move { task_store.begin_remote_delete(&key).await });
        }
        let mut epochs = std::collections::BTreeSet::new();
        while let Some(result) = admissions.join_next().await {
            let permit = result.unwrap().unwrap();
            assert!(
                epochs.insert(permit.epoch()),
                "concurrent admissions reused an epoch"
            );
            assert!(store.finish_remote_delete(&permit).await.unwrap());
        }
        assert_eq!(epochs.len(), 16);
        assert_eq!(epochs.first(), Some(&1));
        assert_eq!(epochs.last(), Some(&16));
        assert_eq!(
            query_scalar::<_, i64>("SELECT COUNT(*) FROM shardline_resource_fences")
                .fetch_one(&store.pool)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn exhausted_epoch_is_rejected_before_admission() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool);
        let key = ObjectKey::parse("remote-delete/overflow").unwrap();
        store.initialize_remote_delete_guard().await.unwrap();
        query("INSERT INTO shardline_resource_fences(domain,resource,epoch) VALUES($1,$2,$3)")
            .bind(REMOTE_DELETE_SEQUENCE_DOMAIN)
            .bind("global")
            .bind(i64::MAX)
            .execute(&store.pool)
            .await
            .unwrap();
        assert!(matches!(
            store.begin_remote_delete(&key).await,
            Err(PostgresMetadataStoreError::IntegerOutOfRange(_))
        ));
        let epoch = query_scalar::<_, i64>(
            "SELECT epoch FROM shardline_resource_fences WHERE domain=$1 AND resource=$2",
        )
        .bind(REMOTE_DELETE_SEQUENCE_DOMAIN)
        .bind("global")
        .fetch_one(&store.pool)
        .await
        .unwrap();
        assert_eq!(epoch, i64::MAX);
    }
}
