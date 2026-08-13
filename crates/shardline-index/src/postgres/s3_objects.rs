use sqlx::{Row, postgres::PgRow, query};

use super::{PostgresIndexStore, PostgresMetadataStoreError, i64_to_u64, u64_to_i64};
use crate::{S3ObjectEntry, S3ObjectIndexStore};

fn s3_object_entry_from_row(row: &PgRow) -> Result<S3ObjectEntry, PostgresMetadataStoreError> {
    Ok(S3ObjectEntry {
        scope_namespace: row.try_get("scope_namespace")?,
        object_key: row.try_get("object_key")?,
        file_id: row.try_get("file_id")?,
        size_bytes: i64_to_u64(row.try_get("size_bytes")?)?,
        content_hash: row.try_get("content_hash")?,
        updated_at_unix_seconds: row.try_get("updated_at_unix_seconds")?,
    })
}

#[async_trait::async_trait]
impl S3ObjectIndexStore for PostgresIndexStore {
    type Error = PostgresMetadataStoreError;

    async fn upsert_s3_object(&self, entry: &S3ObjectEntry) -> Result<(), Self::Error> {
        query(
            "INSERT INTO shardline_s3_objects (
                scope_namespace, object_key, file_id, size_bytes, content_hash,
                updated_at_unix_seconds
             )
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (scope_namespace, object_key)
             DO UPDATE SET
                file_id = EXCLUDED.file_id,
                size_bytes = EXCLUDED.size_bytes,
                content_hash = EXCLUDED.content_hash,
                updated_at_unix_seconds = EXCLUDED.updated_at_unix_seconds",
        )
        .bind(&entry.scope_namespace)
        .bind(&entry.object_key)
        .bind(&entry.file_id)
        .bind(u64_to_i64(entry.size_bytes)?)
        .bind(&entry.content_hash)
        .bind(entry.updated_at_unix_seconds)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_s3_object(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<bool, Self::Error> {
        let result = query(
            "DELETE FROM shardline_s3_objects WHERE scope_namespace = $1 AND object_key = $2",
        )
        .bind(scope_namespace)
        .bind(object_key)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
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
            "SELECT scope_namespace, object_key, file_id, size_bytes, content_hash,
                    updated_at_unix_seconds
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
        let rows = q.fetch_all(&self.pool).await?;
        rows.iter().map(s3_object_entry_from_row).collect()
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
