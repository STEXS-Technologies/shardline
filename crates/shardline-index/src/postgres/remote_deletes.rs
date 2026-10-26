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
    pub(super) object_key: ObjectKey,
    pub(super) epoch: i64,
    pub(super) schema: String,
    pub(super) receipt_key: Option<[u8; 32]>,
    pub(super) receipt_namespace: Option<String>,
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
    /// Lists pending remote deletes without changing their markers or issuing
    /// storage requests. An attempt may still be running; a row is not proof
    /// of failure. Epoch cursors are unique under the admission protocol.
    ///
    /// Returns `None` when the compatibility inspection function is absent or
    /// metadata uses row-level security whose visibility cannot be preserved,
    /// or the caller lacks existing maintenance read privileges.
    /// Pages contain at most 101 rows, including a pagination sentinel. Object
    /// keys remain raw strings so malformed metadata stays inspectable.
    ///
    /// # Errors
    ///
    /// Returns invalid page bounds, numeric overflow, or a database failure.
    pub async fn pending_remote_deletes(
        &self,
        after_epoch: u64,
        limit: usize,
    ) -> Result<Option<Vec<(String, u64)>>, PostgresMetadataStoreError> {
        if !(1..=101).contains(&limit) {
            return Err(PostgresMetadataStoreError::Unsupported(
                "pending delete page limit must be between 1 and 101".to_owned(),
            ));
        }
        let installed = query_scalar::<_, bool>(
            "SELECT to_regprocedure('shardline_pending_remote_deletes_v1(text,bigint,integer)') IS NOT NULL",
        ).fetch_one(&self.pool).await?;
        if !installed {
            return Ok(None);
        }
        let result = sqlx::query_as::<_, (String, i64)>(
            "SELECT object_key, attempt_epoch FROM shardline_pending_remote_deletes_v1(current_schema(), $1, $2)",
        ).bind(super::u64_to_i64(after_epoch)?).bind(i32::try_from(limit).map_err(|_error| PostgresMetadataStoreError::IntegerOutOfRange("pending delete page".to_owned()))?)
            .fetch_all(&self.pool).await;
        let rows = match result {
            Ok(rows) => rows,
            Err(error)
                if error
                    .as_database_error()
                    .and_then(|error| error.code())
                    .as_deref()
                    == Some("SLD03") =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let rows = rows
            .into_iter()
            .map(|(key, epoch)| Ok((key, super::i64_to_u64(epoch)?)))
            .collect::<Result<Vec<_>, PostgresMetadataStoreError>>()?;
        Ok(Some(rows))
    }

    /// Installs the compatibility guard using the existing migration-owner role.
    ///
    /// Called by `db migrate up` after the released schema is complete. This
    /// preserves the migration catalog and existing writer grants. The functions
    /// validate existing permissions and their installed schema with fixed
    /// search paths; the trigger also protects older retention writers.
    /// New file-intent recovery is guarded against older filename-only recovery.
    /// Installation commits atomically. Runtime writer roles need no DDL rights.
    ///
    /// # Errors
    ///
    /// Returns a database failure, including insufficient migration-owner rights.
    pub async fn initialize_remote_delete_guard(&self) -> Result<(), sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        // Released servers already hold the shared side for mutations. Wait
        // before taking DDL locks, so an incremental owner upgrade cannot
        // deadlock trigger installation against an in-flight publication.
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(0x5348_4152_4447_4301_i64)
            .execute(&mut *transaction)
            .await?;
        let schema = query_scalar::<_, String>("SELECT current_schema()")
            .fetch_one(&mut *transaction)
            .await?;
        let quoted_schema = format!("\"{}\"", schema.replace('"', "\"\""));
        let schema_literal = format!("E'{}'", schema.replace('\\', "\\\\").replace('\'', "\\'"));
        // Escaped identifiers and literals replace SQL header placeholders;
        // no catalog text is interpolated into dollar-quoted function bodies.
        // Expand only tokens from the original template. A schema name may
        // itself contain either token; never expand inserted catalog text.
        let template = format!(
            "{}\n{}\n{}",
            include_str!("remote_delete_guard.sql"),
            include_str!("remote_delete_receipts.sql"),
            include_str!("upload_recovery_guard.sql")
        );
        let sql = template
            .split("@SCHEMA@")
            .map(|part| part.replace("@SCHEMA_LITERAL@", &schema_literal))
            .collect::<Vec<_>>()
            .join(&quoted_schema);
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .execute(&mut *transaction)
            .await?;
        super::task_history_scope::initialize_task_history_scope(&mut transaction, &schema).await?;
        super::oci_referrers::initialize(transaction.as_mut(), &schema).await?;
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
        self.begin_remote_delete_with_key(object_key, None).await
    }

    pub(super) async fn begin_remote_delete_with_key(
        &self,
        object_key: &ObjectKey,
        receipt_key: Option<[u8; 32]>,
    ) -> Result<PostgresRemoteDeletePermit, PostgresMetadataStoreError> {
        let mut transaction = self.pool.begin().await?;
        let initialized = query_scalar::<_, bool>(
            "SELECT to_regprocedure('shardline_remote_delete_guard_v1(text,text)') IS NOT NULL
             AND to_regprocedure('shardline_remote_delete_begin_v1(text,text)') IS NOT NULL
             AND to_regprocedure('shardline_remote_delete_finish_v1(text,text,bigint)') IS NOT NULL
             AND to_regprocedure('shardline_reference_keys_remote_delete_v1(text,text[])') IS NOT NULL
             AND to_regprocedure(format('%I.shardline_remote_delete_snapshot_v1(text)', current_schema())) IS NOT NULL
             AND EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='shardline_retention_holds'::regclass
                        AND tgname='shardline_retention_remote_delete_v1' AND tgenabled='O')
             AND EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='shardline_dedupe_shards'::regclass
                        AND tgname='shardline_dedupe_remote_delete_v1' AND tgenabled='O')
             AND (SELECT count(*) FROM pg_trigger
                  WHERE tgrelid IN ('shardline_file_records'::regclass,
                                    'shardline_file_reconstructions'::regclass,
                                    'shardline_stored_objects'::regclass)
                  AND tgname='shardline_reference_remote_delete_v1' AND tgenabled='O') = 3
             AND to_regprocedure(format('%I.shardline_oci_publication_remote_delete_v1(text,text,text,text,text)', current_schema())) IS NOT NULL
             AND to_regprocedure(format('%I.shardline_oci_publication_references_remote_delete_v1(text,jsonb)', current_schema())) IS NOT NULL
             AND (SELECT count(*) FROM pg_trigger
                  WHERE tgrelid IN ('shardline_oci_tags'::regclass, 'shardline_oci_object_tombstones'::regclass)
                  AND tgname='shardline_oci_reference_remote_delete_v1' AND tgenabled='O') = 2
             AND EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='shardline_reliability_events'::regclass
                        AND tgname='shardline_oci_staged_tombstone_v1' AND tgenabled='O')",
        )
        .fetch_one(&mut *transaction)
        .await?;
        if !initialized {
            return Err(PostgresMetadataStoreError::Unsupported(
                "remote delete protection requires the existing db migrate up command".to_owned(),
            ));
        }
        let begin = if receipt_key.is_some() {
            "SELECT current_schema(), shardline_remote_delete_receipt_begin_v1(current_schema(), $1, $2), shardline_remote_delete_receipt_namespace_v1(current_schema())"
        } else {
            "SELECT current_schema(), shardline_remote_delete_begin_v1(current_schema(), $1), NULL::TEXT"
        };
        let mut begin = sqlx::query_as::<_, (String, i64, Option<String>)>(sqlx::AssertSqlSafe(
            begin.to_owned(),
        ))
        .bind(object_key.as_str());
        if let Some(receipt_key) = receipt_key {
            begin = begin.bind(hex::encode(receipt_key));
        }
        let (schema, epoch, receipt_namespace) =
            begin.fetch_one(&mut *transaction).await.map_err(|error| {
                if error
                    .as_database_error()
                    .and_then(|error| error.code())
                    .as_deref()
                    == Some("SLD02")
                {
                    PostgresMetadataStoreError::IntegerOutOfRange("remote delete epoch".to_owned())
                } else {
                    PostgresMetadataStoreError::from(error)
                }
            })?;
        transaction.commit().await?;
        Ok(PostgresRemoteDeletePermit {
            object_key: object_key.clone(),
            epoch,
            schema,
            receipt_key,
            receipt_namespace,
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
        let quoted_schema = format!("\"{}\"", permit.schema.replace('"', "\"\""));
        if permit.receipt_key.is_some() {
            let namespace = query_scalar::<_, String>(sqlx::AssertSqlSafe(format!(
                "SELECT {quoted_schema}.shardline_remote_delete_receipt_namespace_v1($1)"
            )))
            .bind(&permit.schema)
            .fetch_one(&mut *transaction)
            .await?;
            if permit.receipt_namespace.as_ref() != Some(&namespace) {
                return Ok(false);
            }
        }
        let finish = if permit.receipt_key.is_some() {
            format!(
                "SELECT {quoted_schema}.shardline_remote_delete_receipt_finish_v1($1, $2, $3, $4)"
            )
        } else {
            format!("SELECT {quoted_schema}.shardline_remote_delete_finish_v1($1, $2, $3)")
        };
        let mut finish = query_scalar::<_, bool>(sqlx::AssertSqlSafe(finish))
            .bind(&permit.schema)
            .bind(permit.object_key().as_str())
            .bind(permit.epoch());
        if let Some(key) = permit.receipt_key {
            finish = finish.bind(hex::encode(key));
        }
        let changed = finish.fetch_one(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(changed)
    }

    /// Settles an attempt after a successful response proves every issued
    /// remote request has finished. Call only after a single-attempt delete, or
    /// with equivalent proof that no earlier request can still apply later.
    ///
    /// If the original session fails, an explicitly supplied settlement pool
    /// may record this exact attempt's completion. The original error is still
    /// returned: this does not authorize continued GC work or new mutations.
    /// A missing response, elapsed time, lost owner, or absent object is not
    /// sufficient evidence to call this method.
    ///
    /// # Errors
    ///
    /// Returns the original work-session failure even if completion is recorded
    /// independently. Without independent settlement, the marker stays pending.
    pub async fn finish_acknowledged_remote_delete(
        &self,
        permit: &PostgresRemoteDeletePermit,
    ) -> Result<bool, PostgresMetadataStoreError> {
        match self.finish_remote_delete(permit).await {
            Ok(changed) => Ok(changed),
            Err(work_error) => {
                if let Some(pool) = &self.remote_delete_settlement_pool {
                    // A failed or ambiguous commit is also safe: exact-epoch
                    // settlement cannot remove a newer admitted attempt.
                    let recorder = Self::new(pool.clone());
                    let _settlement = recorder.finish_remote_delete(permit).await;
                }
                Err(work_error)
            }
        }
    }

    /// Protects a read reply's object set until its transaction finishes.
    ///
    /// The first key is mandatory; unresolved optional keys are omitted from
    /// the returned set. Returns None if owner migration has not installed the
    /// additive guard. Callers must then retain their single-object protocol.
    ///
    /// # Errors
    /// Returns a pending-delete error or a database failure.
    pub async fn try_require_remote_delete_batch_settled(
        transaction: &mut Transaction<'_, Postgres>,
        object_keys: &[ObjectKey],
    ) -> Result<Option<Vec<String>>, PostgresMetadataStoreError> {
        let initialized = query_scalar::<_, bool>(
            "SELECT to_regprocedure('shardline_remote_delete_read_batch_v1(text,text[])') IS NOT NULL",
        )
        .fetch_one(&mut **transaction)
        .await?;
        if !initialized {
            return Ok(None);
        }
        let keys: Vec<_> = object_keys.iter().map(ObjectKey::as_str).collect();
        let settled =
            query_scalar("SELECT shardline_remote_delete_read_batch_v1(current_schema(), $1)")
                .bind(keys)
                .fetch_one(&mut **transaction)
                .await?;
        Ok(Some(settled))
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

    #[tokio::test]
    async fn read_batch_guards_commute_and_block_every_advertised_delete() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let keys = [
            ObjectKey::parse("cohort/first").unwrap(),
            ObjectKey::parse("cohort/second").unwrap(),
        ];
        let mut left = pool.begin().await.unwrap();
        let mut right = pool.begin().await.unwrap();
        assert_eq!(
            PostgresIndexStore::try_require_remote_delete_batch_settled(&mut left, &keys)
                .await
                .unwrap()
                .unwrap()
                .len(),
            2
        );
        let reversed = [keys[1].clone(), keys[0].clone()];
        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                PostgresIndexStore::try_require_remote_delete_batch_settled(&mut right, &reversed)
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .len(),
            2
        );
        right.rollback().await.unwrap();
        for key in &keys {
            assert!(
                tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    store.begin_remote_delete(key)
                )
                .await
                .is_err()
            );
        }
        left.commit().await.unwrap();
        for key in &keys {
            let permit = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                store.begin_remote_delete(key),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(store.finish_remote_delete(&permit).await.unwrap());
        }
    }

    #[tokio::test]
    async fn read_batch_rejects_pending_deletes_and_preserves_pre_extension_fallback() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let key = ObjectKey::parse("cohort/pending").unwrap();
        let permit = store.begin_remote_delete(&key).await.unwrap();
        let live = ObjectKey::parse("cohort/live").unwrap();
        let mut optional_check = pool.begin().await.unwrap();
        let settled = PostgresIndexStore::try_require_remote_delete_batch_settled(
            &mut optional_check,
            &[live.clone(), key.clone()],
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(settled, vec![live.as_str().to_owned()]);
        optional_check.rollback().await.unwrap();
        let mut pending_check = pool.begin().await.unwrap();
        let error = PostgresIndexStore::try_require_remote_delete_batch_settled(
            &mut pending_check,
            std::slice::from_ref(&key),
        )
        .await
        .unwrap_err();
        assert!(error.is_remote_delete_pending());
        pending_check.rollback().await.unwrap();
        assert!(store.finish_remote_delete(&permit).await.unwrap());
        query("DROP FUNCTION shardline_remote_delete_read_batch_v1(TEXT, TEXT[])")
            .execute(&pool)
            .await
            .unwrap();
        let mut legacy_check = pool.begin().await.unwrap();
        assert!(
            PostgresIndexStore::try_require_remote_delete_batch_settled(
                &mut legacy_check,
                std::slice::from_ref(&key)
            )
            .await
            .unwrap()
            .is_none()
        );
        PostgresIndexStore::require_remote_delete_settled(&mut legacy_check, &key)
            .await
            .unwrap();
        legacy_check.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn read_batch_rejects_cross_schema_checks_and_stale_snapshots() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let schema_error = query(
            "SELECT shardline_remote_delete_read_batch_v1('public', ARRAY['cohort/foreign'])",
        )
        .execute(&pool)
        .await
        .unwrap_err();
        assert_eq!(
            schema_error.as_database_error().unwrap().code().as_deref(),
            Some("42501")
        );
        let store = PostgresIndexStore::new(pool.clone());
        let key = ObjectKey::parse("cohort/stale-snapshot").unwrap();
        let mut stale = pool.begin().await.unwrap();
        query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *stale)
            .await
            .unwrap();
        let _: i64 = query_scalar("SELECT epoch FROM shardline_resource_fences WHERE domain='maintenance-remote-delete-snapshot' AND resource='global'")
            .fetch_one(&mut *stale).await.unwrap();
        let permit = store.begin_remote_delete(&key).await.unwrap();
        assert!(store.finish_remote_delete(&permit).await.unwrap());
        let error = PostgresIndexStore::try_require_remote_delete_batch_settled(&mut stale, &[key])
            .await
            .unwrap_err();
        assert!(matches!(error, PostgresMetadataStoreError::Sqlx(ref error)
            if error.as_database_error().is_some_and(|database| database.code().as_deref() == Some("40001"))));
        stale.rollback().await.unwrap();
    }
    use crate::{AsyncIndexStore, RecordTraversal, RetentionHold};

    #[tokio::test]
    async fn canonical_reference_hash_check_preserves_legacy_regex_semantics() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        // Compare against the previous guard across byte lengths, ASCII,
        // non-ASCII, whitespace and NULL. Invalid legacy values remain ignored.
        let differences: i64 = query_scalar(
            "WITH hashes(hash) AS (
                SELECT repeat(chr(codepoint), len)
                FROM generate_series(1, 255) codepoint
                CROSS JOIN generate_series(63, 65) len
                UNION ALL SELECT NULL::TEXT
                UNION ALL SELECT repeat('a', 63) || chr(10)
                UNION ALL SELECT repeat('a', 64) || chr(10)
                UNION ALL SELECT repeat('a', 63) || '０'
                UNION ALL SELECT repeat('0123456789abcdef', 4)
             ) SELECT count(*) FROM hashes
             WHERE (hash ~ '^[0-9a-f]{64}$') IS DISTINCT FROM
                (octet_length(hash) = 64 AND translate(hash, '0123456789abcdef', '') = '')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(differences, 0);
    }

    #[tokio::test]
    async fn acknowledged_settlement_never_restores_work_ownership_or_clears_a_new_attempt() {
        let Some(observer_pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let schema: String = query_scalar("SELECT current_schema()")
            .fetch_one(&observer_pool)
            .await
            .unwrap();
        let search_path = format!(
            "SET search_path TO \"{}\", public",
            schema.replace('"', "\"\"")
        );
        let work_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .after_connect(move |connection, _metadata| {
                let search_path = search_path.clone();
                Box::pin(async move {
                    query(sqlx::AssertSqlSafe(search_path))
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect_with((*observer_pool.connect_options()).clone())
            .await
            .unwrap();
        let work = PostgresIndexStore::new(work_pool.clone());
        let observer = PostgresIndexStore::new(observer_pool.clone());
        let key = ObjectKey::parse("settlement/acknowledged").unwrap();
        let permit = work.begin_remote_delete(&key).await.unwrap();
        let alternate_pool = super::super::connect_isolated_postgres().await.unwrap();
        let alternate = PostgresIndexStore::new(alternate_pool.clone());
        let alternate_permit = alternate.begin_remote_delete(&key).await.unwrap();
        assert_eq!(alternate_permit.epoch(), permit.epoch());
        work_pool.close().await;
        // No recorder preserves the original API's conservative behavior.
        assert!(
            work.finish_acknowledged_remote_delete(&permit)
                .await
                .is_err()
        );
        assert!(
            observer
                .begin_remote_delete(&key)
                .await
                .unwrap_err()
                .is_remote_delete_pending()
        );
        // Even a different default search path must settle only the schema
        // captured at admission, never a coincident epoch in another schema.
        let acknowledged = work.with_remote_delete_settlement_pool(alternate_pool);
        assert!(
            acknowledged
                .finish_acknowledged_remote_delete(&permit)
                .await
                .is_err()
        );
        assert!(acknowledged.pool().is_closed());
        assert!(
            alternate
                .begin_remote_delete(&key)
                .await
                .unwrap_err()
                .is_remote_delete_pending()
        );
        assert!(acknowledged.begin_remote_delete(&key).await.is_err());
        let next = observer.begin_remote_delete(&key).await.unwrap();
        assert!(next.epoch() > permit.epoch());
        // A duplicated completion, including an ambiguous original commit,
        // must not clear the newer attempt or return successful work ownership.
        assert!(
            acknowledged
                .finish_acknowledged_remote_delete(&permit)
                .await
                .is_err()
        );
        assert!(
            observer
                .begin_remote_delete(&key)
                .await
                .unwrap_err()
                .is_remote_delete_pending()
        );
        let unavailable = PostgresIndexStore::new(work_pool.clone())
            .with_remote_delete_settlement_pool(work_pool);
        assert!(
            unavailable
                .finish_acknowledged_remote_delete(&next)
                .await
                .is_err()
        );
        assert!(
            observer
                .begin_remote_delete(&key)
                .await
                .unwrap_err()
                .is_remote_delete_pending()
        );
        assert!(observer.finish_remote_delete(&next).await.unwrap());
        assert!(
            alternate
                .finish_remote_delete(&alternate_permit)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn pending_delete_inspection_needs_no_fence_grant_and_respects_row_scoped_metadata() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let schema: String = query_scalar("SELECT current_schema()")
            .fetch_one(&pool)
            .await
            .unwrap();
        let identifier = format!("\"{}\"", schema.replace('"', "\"\""));
        let role_identifier = format!("\"{}_reader\"", schema.replace('"', "\"\""));
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE {role_identifier}; GRANT USAGE ON SCHEMA {identifier} TO {role_identifier};
             GRANT SELECT ON {identifier}.shardline_retention_holds, {identifier}.shardline_quarantine_candidates TO {role_identifier};"
        ))).execute(&pool).await.unwrap();
        let key = ObjectKey::parse("inspection/restricted-reader").unwrap();
        let permit = store.begin_remote_delete(&key).await.unwrap();
        let mut connection = pool.acquire().await.unwrap();
        query(sqlx::AssertSqlSafe(format!(
            "SET SESSION AUTHORIZATION {role_identifier}"
        )))
        .execute(&mut *connection)
        .await
        .unwrap();
        let rows = sqlx::query_as::<_, (String, i64)>(
            "SELECT object_key, attempt_epoch FROM shardline_pending_remote_deletes_v1(current_schema(), 0, 10)"
        ).fetch_all(&mut *connection).await.unwrap();
        assert_eq!(rows, vec![(key.as_str().to_owned(), permit.epoch())]);
        assert!(
            query("SELECT * FROM shardline_resource_fences")
                .execute(&mut *connection)
                .await
                .is_err()
        );
        query("RESET SESSION AUTHORIZATION")
            .execute(&mut *connection)
            .await
            .unwrap();
        query(sqlx::AssertSqlSafe(format!(
            "REVOKE SELECT ON {identifier}.shardline_quarantine_candidates FROM {role_identifier}"
        )))
        .execute(&mut *connection)
        .await
        .unwrap();
        query(sqlx::AssertSqlSafe(format!(
            "SET SESSION AUTHORIZATION {role_identifier}"
        )))
        .execute(&mut *connection)
        .await
        .unwrap();
        let error =
            query("SELECT * FROM shardline_pending_remote_deletes_v1(current_schema(), 0, 10)")
                .execute(&mut *connection)
                .await
                .unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("SLD03")
        );
        query("RESET SESSION AUTHORIZATION")
            .execute(&mut *connection)
            .await
            .unwrap();
        drop(connection);
        query("ALTER TABLE shardline_retention_holds ENABLE ROW LEVEL SECURITY")
            .execute(&pool)
            .await
            .unwrap();
        assert!(store.pending_remote_deletes(0, 10).await.unwrap().is_none());
        query("ALTER TABLE shardline_retention_holds DISABLE ROW LEVEL SECURITY")
            .execute(&pool)
            .await
            .unwrap();
        assert!(store.finish_remote_delete(&permit).await.unwrap());
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP OWNED BY {role_identifier}; DROP ROLE {role_identifier};"
        )))
        .execute(&pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn pending_delete_inspection_paginates_without_settling_or_reusing_attempts() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let mut permits = Vec::new();
        for name in ["inspection/third", "inspection/first", "inspection/second"] {
            permits.push(
                store
                    .begin_remote_delete(&ObjectKey::parse(name).unwrap())
                    .await
                    .unwrap(),
            );
        }
        let first = store.pending_remote_deletes(0, 2).await.unwrap().unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(first.first().unwrap().0, "inspection/third");
        let next = store
            .pending_remote_deletes(first.last().unwrap().1, 2)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next.first().unwrap().0, "inspection/second");
        let count: i64 = query_scalar("SELECT count(*) FROM shardline_resource_fences WHERE domain='maintenance-remote-delete'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(count, 3);
        for permit in &permits {
            assert!(store.finish_remote_delete(permit).await.unwrap());
        }
        assert!(
            store
                .pending_remote_deletes(0, 100)
                .await
                .unwrap()
                .unwrap()
                .is_empty()
        );
        let newest = store
            .begin_remote_delete(&ObjectKey::parse("inspection/third").unwrap())
            .await
            .unwrap();
        let resumed = store
            .pending_remote_deletes(next.last().unwrap().1, 2)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            resumed,
            vec![(
                "inspection/third".to_owned(),
                u64::try_from(newest.epoch()).unwrap()
            )]
        );
        assert!(store.finish_remote_delete(&newest).await.unwrap());
    }

    #[tokio::test]
    async fn overlapping_publications_share_reference_slots_but_exclude_delete_admission() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let hash = "ac".repeat(32);
        let key = ObjectKey::parse(&format!("ac/{hash}")).unwrap();
        let record = serde_json::json!({"chunk_size": 1, "chunks": [{"hash": hash}]});
        let mut first = pool.begin().await.unwrap();
        query("INSERT INTO shardline_file_records(record_key,record_kind,scope_key,file_id,content_hash,record) VALUES ('first','version','global','first','content',$1)")
            .bind(sqlx::types::Json(record.clone()))
            .execute(&mut *first).await.unwrap();
        let mut second = pool.begin().await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            query("INSERT INTO shardline_file_records(record_key,record_kind,scope_key,file_id,content_hash,record) VALUES ('second','version','global','second','content',$1)")
                .bind(sqlx::types::Json(record))
                .execute(&mut *second),
        ).await.expect("another publisher of immutable content must not wait for the first commit").unwrap();
        let mut admission = Box::pin(store.begin_remote_delete(&key));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut admission)
                .await
                .is_err()
        );
        first.commit().await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut admission)
                .await
                .is_err(),
            "deletion must wait for every publisher, not just the first"
        );
        second.commit().await.unwrap();
        let permit = tokio::time::timeout(std::time::Duration::from_secs(2), admission)
            .await
            .unwrap()
            .unwrap();
        assert!(store.finish_remote_delete(&permit).await.unwrap());
        pool.close().await;
    }

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
    async fn stale_strict_snapshots_cannot_publish_after_a_delete_has_settled() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let hash = "0d".repeat(32);
        let key = ObjectKey::parse(&format!("0d/{hash}")).unwrap();
        for isolation in [
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        ] {
            for file_record in [false, true] {
                let mut publication = pool.begin().await.unwrap();
                query(isolation).execute(&mut *publication).await.unwrap();
                // Establish an old snapshot with no pending marker. The marker
                // is subsequently admitted and settled in newer transactions.
                let _: i64 = query_scalar(
                    "SELECT epoch FROM shardline_resource_fences
                     WHERE domain='maintenance-remote-delete-snapshot' AND resource='global'",
                )
                .fetch_one(&mut *publication)
                .await
                .unwrap();
                let permit = store.begin_remote_delete(&key).await.unwrap();
                assert!(store.finish_remote_delete(&permit).await.unwrap());
                let statement = if file_record {
                    query("INSERT INTO shardline_file_records(record_key,record_kind,scope_key,file_id,content_hash,record) VALUES ('stale','version','global','stale','content',$1)")
                        .bind(sqlx::types::Json(serde_json::json!({"chunk_size": 1, "chunks": [{"hash": hash}]})))
                } else {
                    query("SELECT shardline_reference_keys_remote_delete_v1(current_schema(), $1)")
                        .bind(vec![key.as_str()])
                };
                let error = statement.execute(&mut *publication).await.unwrap_err();
                assert_eq!(
                    error.as_database_error().unwrap().code().as_deref(),
                    Some("40001"),
                    "{isolation}, file_record={file_record}: {error}"
                );
                publication.rollback().await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn publication_acquires_every_reference_slot_in_ascending_order() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let keys: Vec<String> = query_scalar(
            "SELECT DISTINCT ON (slot) key FROM (
                SELECT '00/' || lpad(to_hex(n), 64, '0') AS key,
                    (hashtextextended('shardline:remote-delete:00/' || lpad(to_hex(n), 64, '0'), 0) & 255)::INTEGER AS slot
                FROM generate_series(0, 10000) n
             ) candidates ORDER BY slot, key",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(keys.len(), 256);
        for (file_record, blocked_slot) in [(false, 128), (true, 128), (false, 255), (true, 255)] {
            let mut blocker = pool.begin().await.unwrap();
            query("SELECT pg_advisory_xact_lock(1397507140, $1)")
                .bind(blocked_slot)
                .execute(&mut *blocker)
                .await
                .unwrap();
            let mut publication = pool.begin().await.unwrap();
            let pid: i32 = query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *publication)
                .await
                .unwrap();
            let statement = if file_record {
                let chunks: Vec<_> = keys
                    .iter()
                    .map(|key| serde_json::json!({"hash": key.rsplit('/').next().unwrap()}))
                    .collect();
                query("INSERT INTO shardline_file_records(record_key,record_kind,scope_key,file_id,content_hash,record) VALUES ('order','version','global','order','content',$1)")
                    .bind(sqlx::types::Json(serde_json::json!({"chunk_size": 1, "chunks": chunks})))
            } else {
                query("SELECT shardline_reference_keys_remote_delete_v1(current_schema(), $1)")
                    .bind(&keys)
            };
            let mut acquire = Box::pin(statement.execute(&mut *publication));
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), &mut acquire)
                    .await
                    .is_err()
            );
            let slots: Vec<i64> = query_scalar(
                "SELECT objid::bigint FROM pg_locks WHERE pid=$1 AND locktype='advisory'
                 AND classid=1397507140 AND objsubid=2 AND mode='ShareLock' AND granted
                 ORDER BY objid",
            )
            .bind(pid)
            .fetch_all(&pool)
            .await
            .unwrap();
            assert_eq!(slots, (0..i64::from(blocked_slot)).collect::<Vec<_>>());
            blocker.rollback().await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(2), &mut acquire)
                .await
                .unwrap()
                .unwrap();
            drop(acquire);
            let count: i64 = query_scalar(
                "SELECT count(*) FROM pg_locks WHERE pid=$1 AND locktype='advisory'
                 AND classid=1397507140 AND objsubid=2 AND mode='ShareLock' AND granted",
            )
            .bind(pid)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(count, 256);
            publication.rollback().await.unwrap();
        }
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
               SELECT lpad(to_hex(ordinal), 64, '0'), 'large/shard/' || ordinal::text FROM generate_series(1, 20000) ordinal")
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
                // Missing legacy chunk_size still denotes direct chunks;
                // repeated terms and malformed hashes must not hide a key.
                for record in [
                    serde_json::json!({"chunk_size": 1, "chunks": [{"hash": hash}]}),
                    serde_json::json!({"chunks": [
                        {"hash": hash}, {"hash": hash}, {"hash": "malformed"}
                    ]}),
                ] {
                    let error = query(insert)
                        .bind(kind)
                        .bind(kind)
                        .bind(sqlx::types::Json(record))
                        .execute(&pool)
                        .await
                        .unwrap_err();
                    assert!(PostgresMetadataStoreError::from(error).is_remote_delete_pending());
                }
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
    async fn legacy_dedupe_publication_guards_chunk_identity_and_preserves_previous_mapping() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let hash = "ab".repeat(32);
        let safe_hash = "cd".repeat(32);
        let key = ObjectKey::parse(&format!("ab/{hash}")).unwrap();
        query("INSERT INTO shardline_dedupe_shards(chunk_hash, shard_object_key) VALUES ($1, 'safe/original')")
            .bind(&safe_hash).execute(&pool).await.unwrap();
        let permit = store.begin_remote_delete(&key).await.unwrap();
        let error = query("INSERT INTO shardline_dedupe_shards(chunk_hash, shard_object_key) VALUES ($1, 'safe/new')")
            .bind(&hash).execute(&pool).await.unwrap_err();
        assert!(PostgresMetadataStoreError::from(error).is_remote_delete_pending());
        let update_error =
            query("UPDATE shardline_dedupe_shards SET chunk_hash=$1 WHERE chunk_hash=$2")
                .bind(&hash)
                .bind(&safe_hash)
                .execute(&pool)
                .await
                .unwrap_err();
        assert!(PostgresMetadataStoreError::from(update_error).is_remote_delete_pending());
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT chunk_hash, shard_object_key FROM shardline_dedupe_shards")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(rows, vec![(safe_hash, "safe/original".to_owned())]);
        assert!(store.finish_remote_delete(&permit).await.unwrap());
        let mut publication = pool.begin().await.unwrap();
        query("INSERT INTO shardline_dedupe_shards(chunk_hash, shard_object_key) VALUES ($1, 'safe/new')")
            .bind(&hash).execute(&mut *publication).await.unwrap();
        let mut admission = Box::pin(store.begin_remote_delete(&key));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut admission)
                .await
                .is_err()
        );
        publication.commit().await.unwrap();
        let next = tokio::time::timeout(std::time::Duration::from_secs(5), admission)
            .await
            .unwrap()
            .unwrap();
        assert!(store.finish_remote_delete(&next).await.unwrap());
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
        // This role is created after fixture migration. Match the derived-index
        // grants that frontend installation mirrors for existing record writers;
        // do not grant access to retention or remote-delete fencing tables.
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE {role_identifier};
             GRANT USAGE ON SCHEMA {identifier} TO {role_identifier};
             GRANT SELECT, INSERT, UPDATE ON {identifier}.shardline_dedupe_shards,
                 {identifier}.shardline_file_records,
                 {identifier}.shardline_file_reconstructions,
                 {identifier}.shardline_stored_objects TO {role_identifier};
             GRANT SELECT, INSERT, DELETE ON {identifier}.shardline_record_membership
                 TO {role_identifier};"
        )))
        .execute(&pool)
        .await
        .unwrap();
        let key = ObjectKey::parse("shards/default/aa/limited.shard").unwrap();
        let permit = store.begin_remote_delete(&key).await.unwrap();
        let hash = "ab".repeat(32);
        let container = ObjectKey::parse(&format!("xorbs/default/ab/{hash}.xorb")).unwrap();
        let container_permit = store.begin_remote_delete(&container).await.unwrap();
        let chunk_hash = "ef".repeat(32);
        let chunk = ObjectKey::parse(&format!("ef/{chunk_hash}")).unwrap();
        let chunk_permit = store.begin_remote_delete(&chunk).await.unwrap();
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
        let chunk_error = query(insert)
            .bind(&chunk_hash)
            .bind("safe/chunk-shard")
            .execute(&mut *connection)
            .await
            .unwrap_err();
        assert!(PostgresMetadataStoreError::from(chunk_error).is_remote_delete_pending());
        for hash_to_publish in ["cd".repeat(32), hash.clone()] {
            let record =
                serde_json::json!({"chunk_size": 0, "chunks": [{"hash": hash_to_publish}]});
            let terms = serde_json::json!({"terms": [{"object_hash": hash_to_publish}]});
            let results = [
                query("SELECT shardline_reference_keys_remote_delete_v1(current_schema(), $1)")
                    .bind(vec![format!("xorbs/default/{}/{}.xorb", hash_to_publish.get(..2).unwrap(), hash_to_publish)])
                    .execute(&mut *connection).await,
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
        assert!(store.finish_remote_delete(&chunk_permit).await.unwrap());
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
            "SELECT shardline_reference_keys_remote_delete_v1('public', ARRAY['remote-delete/scope'])",
            "SELECT * FROM shardline_pending_remote_deletes_v1('public', 0, 10)",
            "SELECT shardline_task_history_scope_v1('public')",
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
            "shardline_oci_tags",
            "shardline_oci_object_tombstones",
            "shardline_reliability_events",
            "shardline_upload_intents",
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
        let intent = crate::UploadIntent::new(
            "file-v2-schema-escaping".to_owned(),
            "record/schema-escaping".to_owned(),
            "00".repeat(32),
            0,
        );
        crate::UploadIntentStore::create_intent(&store, &intent)
            .await
            .unwrap();
        let rows = query("UPDATE shardline_upload_intents SET state='Storing' WHERE intent_id=$1")
            .bind(intent.intent_id())
            .execute(&store.pool)
            .await
            .unwrap();
        assert_eq!(rows.rows_affected(), 0);
        assert!(
            crate::UploadIntentStore::transition_intent(
                &store,
                intent.intent_id(),
                crate::UploadIntentState::Storing
            )
            .await
            .unwrap()
        );
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
        let count: i64 = query_scalar("SELECT count(*) FROM shardline_resource_fences WHERE domain LIKE 'maintenance-remote-delete%' AND domain <> 'maintenance-remote-delete-snapshot'")
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
        let scope: String =
            query_scalar("SELECT shardline_task_history_scope_v1(current_schema())")
                .fetch_one(&store.pool)
                .await
                .unwrap();
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
            query_scalar::<_, i64>("SELECT COUNT(*) FROM shardline_resource_fences WHERE domain IN ('maintenance-remote-delete', 'maintenance-remote-delete-sequence')")
                .fetch_one(&store.pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_scalar::<_, String>("SELECT shardline_task_history_scope_v1(current_schema())")
                .fetch_one(&store.pool)
                .await
                .unwrap(),
            scope
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
