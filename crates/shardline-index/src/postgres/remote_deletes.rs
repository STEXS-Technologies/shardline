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
                        AND tgname='shardline_retention_remote_delete_v1' AND tgenabled='O')",
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
    use crate::{AsyncIndexStore, RetentionHold};

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
