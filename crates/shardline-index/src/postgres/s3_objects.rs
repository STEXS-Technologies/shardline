use serde_json::{from_value, to_value};
use sqlx::{PgConnection, Row, postgres::PgRow, query};

use super::{PostgresIndexStore, PostgresMetadataStoreError, i64_to_u64, u64_to_i64};
use crate::{S3ObjectEntry, S3ObjectIndexStore};
use shardline_reliability::{
    OperationKind, S3ObjectEvidenceLog, S3ObjectLifecycleEvent, S3ObjectSnapshot, S3ObjectState,
    SnapshotEvidence, verify_s3_object_events,
};

fn s3_object_entry_from_row(row: &PgRow) -> Result<S3ObjectEntry, PostgresMetadataStoreError> {
    Ok(S3ObjectEntry {
        scope_namespace: row.try_get("scope_namespace")?,
        object_key: row.try_get("object_key")?,
        file_id: row.try_get("file_id")?,
        size_bytes: i64_to_u64(row.try_get("size_bytes")?)?,
        content_hash: row.try_get("content_hash")?,
        etag: row.try_get("etag")?,
        user_metadata: row
            .try_get::<String, _>("user_metadata")
            .ok()
            .and_then(|json| serde_json::from_str(&json).ok())
            .unwrap_or_default(),
        updated_at_unix_seconds: row.try_get("updated_at_unix_seconds")?,
    })
}

/// Serializes S3 user metadata as JSON text for the index row.
fn user_metadata_to_json(
    user_metadata: &[(String, String)],
) -> Result<String, PostgresMetadataStoreError> {
    serde_json::to_string(user_metadata).map_err(PostgresMetadataStoreError::from)
}

fn s3_object_snapshot(
    scope_namespace: &str,
    object_key: &str,
    entry: Option<&S3ObjectEntry>,
) -> Result<S3ObjectSnapshot, PostgresMetadataStoreError> {
    Ok(S3ObjectSnapshot::new(
        scope_namespace,
        object_key,
        entry.map(|entry| S3ObjectState {
            file_id: entry.file_id.clone(),
            size_bytes: entry.size_bytes,
            content_hash: entry.content_hash.clone(),
            etag: entry.etag.clone(),
            user_metadata: entry.user_metadata.clone(),
            updated_at_unix_seconds: entry.updated_at_unix_seconds,
        }),
    )?)
}

async fn load_s3_object_evidence(
    connection: &mut PgConnection,
    scope_namespace: &str,
    object_key: &str,
) -> Result<S3ObjectEvidenceLog, PostgresMetadataStoreError> {
    let operation = s3_object_snapshot(scope_namespace, object_key, None)?.evidence_operation()?;
    let rows = query(
        "SELECT event_json FROM shardline_reliability_events
         WHERE operation_kind = $1 AND operation_id = $2 ORDER BY sequence",
    )
    .bind(OperationKind::S3Object.as_str())
    .bind(&operation.operation_id)
    .fetch_all(&mut *connection)
    .await?;
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        events.push(from_value::<S3ObjectLifecycleEvent>(
            row.try_get("event_json")?,
        )?);
    }
    Ok(S3ObjectEvidenceLog::from_events(events)?)
}

async fn current_s3_object_evidence(
    connection: &mut PgConnection,
    scope_namespace: &str,
    object_key: &str,
    entry: Option<&S3ObjectEntry>,
) -> Result<S3ObjectEvidenceLog, PostgresMetadataStoreError> {
    let snapshot = s3_object_snapshot(scope_namespace, object_key, entry)?;
    let evidence = load_s3_object_evidence(connection, scope_namespace, object_key).await?;
    if evidence.events().is_empty() {
        return Ok(S3ObjectEvidenceLog::baseline(snapshot)?);
    }
    verify_s3_object_events(evidence.events(), &snapshot)?;
    Ok(evidence)
}

async fn persist_s3_object_evidence(
    connection: &mut PgConnection,
    evidence: &S3ObjectEvidenceLog,
) -> Result<(), PostgresMetadataStoreError> {
    for event in evidence.events() {
        query(
            "INSERT INTO shardline_reliability_events
                (operation_kind, operation_id, sequence, event_json, created_at_unix_seconds)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (operation_kind, operation_id, sequence) DO NOTHING",
        )
        .bind(event.operation.kind.as_str())
        .bind(&event.operation.operation_id)
        .bind(u64_to_i64(event.sequence)?)
        .bind(to_value(event)?)
        .bind(shardline_protocol::unix_now_seconds_lossy() as i64)
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

pub(super) async fn current_s3_object_on_connection(
    connection: &mut PgConnection,
    scope_namespace: &str,
    object_key: &str,
) -> Result<Option<S3ObjectEntry>, PostgresMetadataStoreError> {
    let row = query(
        "SELECT scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
                user_metadata, updated_at_unix_seconds
         FROM shardline_s3_objects WHERE scope_namespace = $1 AND object_key = $2 LIMIT 1",
    )
    .bind(scope_namespace)
    .bind(object_key)
    .fetch_optional(&mut *connection)
    .await?;
    row.map(|row| s3_object_entry_from_row(&row)).transpose()
}

pub(super) async fn record_s3_object_transition(
    connection: &mut PgConnection,
    before: Option<&S3ObjectEntry>,
    after: Option<&S3ObjectEntry>,
) -> Result<(), PostgresMetadataStoreError> {
    let entry = after.or(before).ok_or_else(|| {
        PostgresMetadataStoreError::Unsupported("missing S3 object identity".into())
    })?;
    let mut evidence = current_s3_object_evidence(
        connection,
        &entry.scope_namespace,
        &entry.object_key,
        before,
    )
    .await?;
    evidence.record(s3_object_snapshot(
        &entry.scope_namespace,
        &entry.object_key,
        after,
    )?)?;
    persist_s3_object_evidence(connection, &evidence).await
}

#[async_trait::async_trait]
impl S3ObjectIndexStore for PostgresIndexStore {
    type Error = PostgresMetadataStoreError;

    async fn upsert_s3_object(&self, entry: &S3ObjectEntry) -> Result<(), Self::Error> {
        let user_metadata_json = user_metadata_to_json(&entry.user_metadata)?;
        let mut transaction = self.pool.begin().await?;
        let before = current_s3_object_on_connection(
            &mut transaction,
            &entry.scope_namespace,
            &entry.object_key,
        )
        .await?;
        query(
            "INSERT INTO shardline_s3_objects (
                scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
                user_metadata, updated_at_unix_seconds
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (scope_namespace, object_key)
             DO UPDATE SET
                file_id = EXCLUDED.file_id,
                size_bytes = EXCLUDED.size_bytes,
                content_hash = EXCLUDED.content_hash,
                etag = EXCLUDED.etag,
                user_metadata = EXCLUDED.user_metadata,
                updated_at_unix_seconds = EXCLUDED.updated_at_unix_seconds",
        )
        .bind(&entry.scope_namespace)
        .bind(&entry.object_key)
        .bind(&entry.file_id)
        .bind(u64_to_i64(entry.size_bytes)?)
        .bind(&entry.content_hash)
        .bind(&entry.etag)
        .bind(user_metadata_json)
        .bind(entry.updated_at_unix_seconds)
        .execute(&mut *transaction)
        .await?;
        record_s3_object_transition(&mut transaction, before.as_ref(), Some(entry)).await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn compare_and_swap_s3_object(
        &self,
        expected: Option<&S3ObjectEntry>,
        replacement: &S3ObjectEntry,
    ) -> Result<bool, Self::Error> {
        let replacement_metadata = user_metadata_to_json(&replacement.user_metadata)?;
        let mut transaction = self.pool.begin().await?;
        let result = if let Some(expected) = expected {
            let expected_metadata = user_metadata_to_json(&expected.user_metadata)?;
            query(
                "UPDATE shardline_s3_objects
                 SET file_id = $3, size_bytes = $4, content_hash = $5, etag = $6,
                     user_metadata = $7, updated_at_unix_seconds = $8
                 WHERE scope_namespace = $1 AND object_key = $2
                   AND file_id = $9 AND size_bytes = $10 AND content_hash = $11
                   AND etag = $12 AND user_metadata = $13
                   AND updated_at_unix_seconds = $14",
            )
            .bind(&replacement.scope_namespace)
            .bind(&replacement.object_key)
            .bind(&replacement.file_id)
            .bind(u64_to_i64(replacement.size_bytes)?)
            .bind(&replacement.content_hash)
            .bind(&replacement.etag)
            .bind(replacement_metadata)
            .bind(replacement.updated_at_unix_seconds)
            .bind(&expected.file_id)
            .bind(u64_to_i64(expected.size_bytes)?)
            .bind(&expected.content_hash)
            .bind(&expected.etag)
            .bind(expected_metadata)
            .bind(expected.updated_at_unix_seconds)
            .execute(&mut *transaction)
            .await?
        } else {
            query(
                "INSERT INTO shardline_s3_objects (
                    scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
                    user_metadata, updated_at_unix_seconds
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT (scope_namespace, object_key) DO NOTHING",
            )
            .bind(&replacement.scope_namespace)
            .bind(&replacement.object_key)
            .bind(&replacement.file_id)
            .bind(u64_to_i64(replacement.size_bytes)?)
            .bind(&replacement.content_hash)
            .bind(&replacement.etag)
            .bind(replacement_metadata)
            .bind(replacement.updated_at_unix_seconds)
            .execute(&mut *transaction)
            .await?
        };
        let changed = result.rows_affected() == 1;
        if changed {
            record_s3_object_transition(&mut transaction, expected, Some(replacement)).await?;
        }
        transaction.commit().await?;
        Ok(changed)
    }

    async fn delete_s3_object(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<bool, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        let before =
            current_s3_object_on_connection(&mut transaction, scope_namespace, object_key).await?;
        let result = query(
            "DELETE FROM shardline_s3_objects WHERE scope_namespace = $1 AND object_key = $2",
        )
        .bind(scope_namespace)
        .bind(object_key)
        .execute(&mut *transaction)
        .await?;
        let deleted = result.rows_affected() > 0;
        if deleted {
            record_s3_object_transition(&mut transaction, before.as_ref(), None).await?;
        }
        transaction.commit().await?;
        Ok(deleted)
    }

    async fn scan_s3_objects(
        &self,
        scope_namespace: &str,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<S3ObjectEntry>, Self::Error> {
        use std::fmt::Write as _;

        let mut sql = String::from(
            "SELECT scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
                    user_metadata, updated_at_unix_seconds
             FROM shardline_s3_objects
             WHERE scope_namespace = $1 AND substr(object_key, 1, length($2)) = $2",
        );
        let mut index = 3usize;
        if cursor.is_some() {
            write!(sql, " AND object_key > ${index}")
                .map_err(|e| PostgresMetadataStoreError::IntegerOutOfRange(e.to_string()))?;
            index = index.saturating_add(1);
        }
        let limit_i64 = u64_to_i64(u64::try_from(limit).unwrap_or(u64::MAX))?;
        sql.push_str(" ORDER BY object_key");
        write!(sql, " LIMIT ${index}")
            .map_err(|e| PostgresMetadataStoreError::IntegerOutOfRange(e.to_string()))?;

        let mut q = query(&sql).bind(scope_namespace).bind(prefix);
        if let Some(cursor) = cursor {
            q = q.bind(cursor);
        }
        q = q.bind(limit_i64);
        let mut transaction = self.pool.begin().await?;
        let rows = q.fetch_all(&mut *transaction).await?;
        let values = rows
            .iter()
            .map(s3_object_entry_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        for value in &values {
            current_s3_object_evidence(
                &mut transaction,
                &value.scope_namespace,
                &value.object_key,
                Some(value),
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(values)
    }

    async fn scan_s3_object_exact(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<Option<S3ObjectEntry>, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        let rows = query(
            "SELECT scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
                    user_metadata, updated_at_unix_seconds
             FROM shardline_s3_objects
             WHERE scope_namespace = $1 AND object_key = $2
             LIMIT 1",
        )
        .bind(scope_namespace)
        .bind(object_key)
        .fetch_all(&mut *transaction)
        .await?;
        let value = rows
            .iter()
            .map(s3_object_entry_from_row)
            .collect::<Result<Vec<_>, _>>()?
            .pop();
        current_s3_object_evidence(
            &mut transaction,
            scope_namespace,
            object_key,
            value.as_ref(),
        )
        .await?;
        transaction.commit().await?;
        Ok(value)
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
    use super::*;

    async fn connect_postgres() -> Option<sqlx::PgPool> {
        let url = std::env::var("DATABASE_URL").ok()?;
        sqlx::PgPool::connect(&url).await.ok()
    }

    fn entry(scope_namespace: &str, object_key: &str, file_id: &str) -> S3ObjectEntry {
        S3ObjectEntry {
            scope_namespace: scope_namespace.to_owned(),
            object_key: object_key.to_owned(),
            file_id: file_id.to_owned(),
            size_bytes: 123,
            content_hash: "ab".repeat(32),
            etag: "ab".repeat(32),
            user_metadata: Vec::new(),
            updated_at_unix_seconds: 1000,
        }
    }

    async fn cleanup(pool: &sqlx::PgPool, scope_namespace: &str) {
        sqlx::query("DELETE FROM shardline_s3_objects WHERE scope_namespace = $1")
            .bind(scope_namespace)
            .execute(pool)
            .await
            .expect("cleanup s3 object rows");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_compare_and_swap_has_one_cross_connection_winner() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = "pg-s3-cas";
        cleanup(&pool, scope).await;
        let first_store = PostgresIndexStore::new(pool.clone());
        let second_store = PostgresIndexStore::new(pool.clone());
        let first = entry(scope, "model.bin", "first");
        let second = entry(scope, "model.bin", "second");

        let (first_won, second_won) = tokio::join!(
            first_store.compare_and_swap_s3_object(None, &first),
            second_store.compare_and_swap_s3_object(None, &second),
        );
        let first_won = first_won.unwrap();
        let second_won = second_won.unwrap();
        assert_ne!(first_won, second_won);

        let stored = first_store
            .scan_s3_object_exact(scope, "model.bin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored, if first_won { first } else { second });

        let third = entry(scope, "model.bin", "third");
        let fourth = entry(scope, "model.bin", "fourth");
        let (third_won, fourth_won) = tokio::join!(
            first_store.compare_and_swap_s3_object(Some(&stored), &third),
            second_store.compare_and_swap_s3_object(Some(&stored), &fourth),
        );
        let third_won = third_won.unwrap();
        let fourth_won = fourth_won.unwrap();
        assert_ne!(third_won, fourth_won);

        let updated = first_store
            .scan_s3_object_exact(scope, "model.bin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated, if third_won { third } else { fourth });
        cleanup(&pool, scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_concurrent_unconditional_upserts_keep_a_verifiable_chain() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-concurrent-upsert-{}", std::process::id());
        cleanup(&pool, &scope).await;
        let first_store = PostgresIndexStore::new(pool.clone());
        let second_store = PostgresIndexStore::new(pool.clone());
        let first = entry(&scope, "model.bin", "first");
        let second = entry(&scope, "model.bin", "second");

        let (first_result, second_result) = tokio::join!(
            first_store.upsert_s3_object(&first),
            second_store.upsert_s3_object(&second),
        );
        first_result.unwrap();
        second_result.unwrap();
        let stored = first_store
            .scan_s3_object_exact(&scope, "model.bin")
            .await
            .unwrap()
            .unwrap();
        assert!(stored == first || stored == second);
        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_upsert_overwrites_existing_row() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-overwrite-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        let first = entry(&scope, "data/model.pt", "f1");
        let overwrite = entry(&scope, "data/model.pt", "f2");
        S3ObjectIndexStore::upsert_s3_object(&store, &first)
            .await
            .expect("first upsert");
        S3ObjectIndexStore::upsert_s3_object(&store, &overwrite)
            .await
            .expect("overwrite upsert");

        let rows = S3ObjectIndexStore::scan_s3_objects(&store, &scope, "", None, 100)
            .await
            .expect("scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], overwrite);

        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_delete_then_absent() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-delete-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        let e = entry(&scope, "data/delete.pt", "f3");
        S3ObjectIndexStore::upsert_s3_object(&store, &e)
            .await
            .expect("upsert");

        assert!(
            S3ObjectIndexStore::delete_s3_object(&store, &scope, "data/delete.pt")
                .await
                .expect("delete")
        );
        assert!(
            !S3ObjectIndexStore::delete_s3_object(&store, &scope, "data/delete.pt")
                .await
                .expect("second delete")
        );
        assert!(
            S3ObjectIndexStore::scan_s3_objects(&store, &scope, "", None, 100)
                .await
                .expect("scan")
                .is_empty()
        );

        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_read_rejects_tampered_evidence() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-tamper-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        let value = entry(&scope, "model.bin", "file-a");
        S3ObjectIndexStore::upsert_s3_object(&store, &value)
            .await
            .expect("upsert");

        query(
            "UPDATE shardline_reliability_events
             SET event_json = '{\"tampered\":true}'::jsonb
             WHERE operation_kind = $1 AND operation_id LIKE $2",
        )
        .bind(OperationKind::S3Object.as_str())
        .bind(format!("{}:%", scope.len()))
        .execute(&pool)
        .await
        .expect("tamper evidence");

        assert!(
            S3ObjectIndexStore::scan_s3_object_exact(&store, &scope, "model.bin")
                .await
                .is_err()
        );
        cleanup(&pool, &scope).await;
        query(
            "DELETE FROM shardline_reliability_events WHERE operation_kind = $1 AND operation_id LIKE $2",
        )
        .bind(OperationKind::S3Object.as_str())
        .bind(format!("{}:%", scope.len()))
        .execute(&pool)
        .await
        .expect("cleanup s3 object evidence");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_scan_prefix_cursor_and_limit() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-scan-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        for (key, id) in [
            ("a.txt", 1),
            ("data/1.txt", 2),
            ("data/2.txt", 3),
            ("data/sub/3.txt", 4),
            ("zz.txt", 5),
        ] {
            S3ObjectIndexStore::upsert_s3_object(&store, &entry(&scope, key, &format!("f{id}")))
                .await
                .expect("upsert");
        }

        // Empty prefix lists every key in raw-key order.
        let rows = S3ObjectIndexStore::scan_s3_objects(&store, &scope, "", None, 100)
            .await
            .expect("scan");
        let keys: Vec<&str> = rows.iter().map(|row| row.object_key.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "a.txt",
                "data/1.txt",
                "data/2.txt",
                "data/sub/3.txt",
                "zz.txt"
            ]
        );

        // Prefix filtering.
        let rows = S3ObjectIndexStore::scan_s3_objects(&store, &scope, "data", None, 100)
            .await
            .expect("scan");
        let keys: Vec<&str> = rows.iter().map(|row| row.object_key.as_str()).collect();
        assert_eq!(keys, vec!["data/1.txt", "data/2.txt", "data/sub/3.txt"]);

        // Keyset cursor resumes strictly after the given raw key.
        let rows = S3ObjectIndexStore::scan_s3_objects(&store, &scope, "", Some("data/2.txt"), 100)
            .await
            .expect("scan");
        let keys: Vec<&str> = rows.iter().map(|row| row.object_key.as_str()).collect();
        assert_eq!(keys, vec!["data/sub/3.txt", "zz.txt"]);

        // Limit truncation, then cursor resumes after the last returned key.
        let rows = S3ObjectIndexStore::scan_s3_objects(&store, &scope, "", None, 3)
            .await
            .expect("scan");
        assert_eq!(rows.len(), 3);
        let cursor = rows.last().expect("non-empty").object_key.clone();
        let more = S3ObjectIndexStore::scan_s3_objects(&store, &scope, "", Some(&cursor), 100)
            .await
            .expect("scan");
        assert_eq!(more.len(), 2);

        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_exact_requires_full_key_match() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-exact-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        for (key, id) in [("a/b", 1), ("a/b/c", 2)] {
            S3ObjectIndexStore::upsert_s3_object(&store, &entry(&scope, key, &format!("f{id}")))
                .await
                .expect("upsert");
        }

        // F-33: a prefix-sibling must never satisfy the exact lookup.
        let exact = S3ObjectIndexStore::scan_s3_object_exact(&store, &scope, "a")
            .await
            .expect("exact lookup");
        assert!(
            exact.is_none(),
            "a prefix-sibling must never satisfy an exact lookup"
        );

        let found = S3ObjectIndexStore::scan_s3_object_exact(&store, &scope, "a/b")
            .await
            .expect("exact lookup");
        assert_eq!(found.expect("exact row").object_key, "a/b");

        // The prefix scan still lists the siblings (listing unchanged).
        let rows = S3ObjectIndexStore::scan_s3_objects(&store, &scope, "a", None, 100)
            .await
            .expect("scan");
        let keys: Vec<&str> = rows.iter().map(|row| row.object_key.as_str()).collect();
        assert_eq!(keys, vec!["a/b", "a/b/c"]);

        cleanup(&pool, &scope).await;
    }

    /// Round-trips the bundled `s3_object_index` migration inside a rolled-back
    /// transaction so the shared CI database is left exactly as found.
    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_migration_up_down_roundtrip() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        // The Postgres migration set lives at the workspace root `migrations/`
        // (the `crates/shardline-index/migrations/` set is the SQLite variant).
        let up = include_str!("../../../../migrations/20260813000000_s3_object_index.up.sql");
        let down = include_str!("../../../../migrations/20260813000000_s3_object_index.down.sql");

        let mut transaction = pool.begin().await.expect("begin transaction");
        sqlx::raw_sql("DROP TABLE IF EXISTS shardline_s3_objects")
            .execute(&mut *transaction)
            .await
            .expect("drop table");
        sqlx::raw_sql(up)
            .execute(&mut *transaction)
            .await
            .expect("apply up migration");
        assert!(
            s3_object_table_exists(&mut transaction).await,
            "up migration must create shardline_s3_objects"
        );

        sqlx::raw_sql(down)
            .execute(&mut *transaction)
            .await
            .expect("apply down migration");
        assert!(
            !s3_object_table_exists(&mut transaction).await,
            "down migration must drop shardline_s3_objects"
        );

        transaction.rollback().await.expect("rollback transaction");
    }

    async fn s3_object_table_exists(connection: &mut sqlx::PgConnection) -> bool {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'shardline_s3_objects'
             )",
        )
        .fetch_one(&mut *connection)
        .await
        .expect("check table existence")
    }
}
