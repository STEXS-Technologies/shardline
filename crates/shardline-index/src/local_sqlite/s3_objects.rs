use rusqlite::{Connection, params_from_iter};
use std::fmt::Write as _;

use super::{LocalIndexStore, LocalIndexStoreError, collect_rows, helpers};
use crate::{S3ObjectEntry, S3ObjectIndexStore};

fn s3_object_entry_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<S3ObjectEntry> {
    Ok(S3ObjectEntry {
        scope_namespace: row.get("scope_namespace")?,
        object_key: row.get("object_key")?,
        file_id: row.get("file_id")?,
        size_bytes: helpers::i64_to_u64(row.get("size_bytes")?).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::new(e),
            )
        })?,
        content_hash: row.get("content_hash")?,
        updated_at_unix_seconds: row.get("updated_at_unix_seconds")?,
    })
}

fn upsert_s3_object_sql(
    connection: &Connection,
    entry: &S3ObjectEntry,
) -> Result<(), LocalIndexStoreError> {
    connection.execute(
        "INSERT INTO shardline_s3_objects (
            scope_namespace, object_key, file_id, size_bytes, content_hash,
            updated_at_unix_seconds
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT (scope_namespace, object_key)
         DO UPDATE SET
            file_id = excluded.file_id,
            size_bytes = excluded.size_bytes,
            content_hash = excluded.content_hash,
            updated_at_unix_seconds = excluded.updated_at_unix_seconds",
        rusqlite::params![
            entry.scope_namespace,
            entry.object_key,
            entry.file_id,
            helpers::u64_to_i64(entry.size_bytes)?,
            entry.content_hash,
            entry.updated_at_unix_seconds,
        ],
    )?;
    Ok(())
}

fn delete_s3_object_sql(
    connection: &Connection,
    scope_namespace: &str,
    object_key: &str,
) -> Result<bool, LocalIndexStoreError> {
    let changed = connection.execute(
        "DELETE FROM shardline_s3_objects WHERE scope_namespace = ?1 AND object_key = ?2",
        rusqlite::params![scope_namespace, object_key],
    )?;
    Ok(changed > 0)
}

fn scan_s3_objects_sql(
    connection: &Connection,
    scope_namespace: &str,
    prefix: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Vec<S3ObjectEntry>, LocalIndexStoreError> {
    use rusqlite::types::Value;

    let mut sql = String::from(
        "SELECT scope_namespace, object_key, file_id, size_bytes, content_hash,
                updated_at_unix_seconds
         FROM shardline_s3_objects
         WHERE scope_namespace = ?1 AND object_key LIKE (?2 || '%')",
    );
    let mut args: Vec<Value> = vec![
        Value::Text(scope_namespace.to_owned()),
        Value::Text(prefix.to_owned()),
    ];
    let mut index = 3usize;
    if let Some(cursor) = cursor {
        write!(sql, " AND object_key > ?{index}")
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?;
        args.push(Value::Text(cursor.to_owned()));
        index = index.saturating_add(1);
    }
    let limit_i64 =
        i64::try_from(limit).map_err(|e| LocalIndexStoreError::IntegerOutOfRange(e.to_string()))?;
    sql.push_str(" ORDER BY object_key");
    write!(sql, " LIMIT ?{index}")
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?;
    args.push(Value::Integer(limit_i64));

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(args.iter()), s3_object_entry_from_row)?;
    collect_rows(rows)
}

#[async_trait::async_trait]
impl S3ObjectIndexStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    async fn upsert_s3_object(&self, entry: &S3ObjectEntry) -> Result<(), Self::Error> {
        let store = self.clone();
        let entry = entry.clone();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            upsert_s3_object_sql(&connection, &entry)
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn delete_s3_object(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<bool, Self::Error> {
        let store = self.clone();
        let scope_namespace = scope_namespace.to_owned();
        let object_key = object_key.to_owned();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            delete_s3_object_sql(&connection, &scope_namespace, &object_key)
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn scan_s3_objects(
        &self,
        scope_namespace: &str,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<S3ObjectEntry>, Self::Error> {
        let store = self.clone();
        let scope_namespace = scope_namespace.to_owned();
        let prefix = prefix.to_owned();
        let cursor = cursor.map(ToOwned::to_owned);
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            scan_s3_objects_sql(
                &connection,
                &scope_namespace,
                &prefix,
                cursor.as_deref(),
                limit,
            )
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
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

    fn make_store() -> LocalIndexStore {
        let storage = shardline_test_support::TempStorage::new();
        LocalIndexStore::new(storage.path_buf()).expect("failed to create local index store")
    }

    fn file_id(n: u8) -> String {
        format!("{:064x}", n)
    }

    fn entry(
        scope_namespace: &str,
        object_key: &str,
        file_id: &str,
        size_bytes: u64,
        updated_at_unix_seconds: i64,
    ) -> S3ObjectEntry {
        S3ObjectEntry {
            scope_namespace: scope_namespace.to_owned(),
            object_key: object_key.to_owned(),
            file_id: file_id.to_owned(),
            size_bytes,
            content_hash: "ab".repeat(32),
            updated_at_unix_seconds,
        }
    }

    async fn scan(
        store: &LocalIndexStore,
        scope_namespace: &str,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Vec<String> {
        S3ObjectIndexStore::scan_s3_objects(store, scope_namespace, prefix, cursor, limit)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.object_key)
            .collect()
    }

    #[tokio::test]
    async fn upsert_s3_object_overwrites_existing_row() {
        let store = make_store();
        let first = entry("global", "data/model.pt", &file_id(1), 100, 1000);
        S3ObjectIndexStore::upsert_s3_object(&store, &first)
            .await
            .unwrap();
        let overwrite = entry("global", "data/model.pt", &file_id(2), 200, 2000);
        S3ObjectIndexStore::upsert_s3_object(&store, &overwrite)
            .await
            .unwrap();

        let rows = scan(&store, "global", "", None, 100).await;
        assert_eq!(rows, vec!["data/model.pt"]);
        let loaded = S3ObjectIndexStore::scan_s3_objects(&store, "global", "", None, 100)
            .await
            .unwrap();
        assert_eq!(loaded[0], overwrite);
    }

    #[tokio::test]
    async fn delete_s3_object_returns_true_then_false() {
        let store = make_store();
        let e = entry("global", "data/delete.pt", &file_id(3), 100, 1000);
        S3ObjectIndexStore::upsert_s3_object(&store, &e)
            .await
            .unwrap();

        assert!(
            S3ObjectIndexStore::delete_s3_object(&store, "global", "data/delete.pt")
                .await
                .unwrap()
        );
        assert!(
            !S3ObjectIndexStore::delete_s3_object(&store, "global", "data/delete.pt")
                .await
                .unwrap()
        );
        assert!(scan(&store, "global", "", None, 100).await.is_empty());
        // Other scope namespaces are isolated.
        assert!(
            !S3ObjectIndexStore::delete_s3_object(&store, "other-scope", "data/delete.pt")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn scan_s3_objects_prefix_and_cursor_keyset() {
        let store = make_store();
        for (key, id) in [
            ("a.txt", 1),
            ("data/1.txt", 2),
            ("data/2.txt", 3),
            ("data/sub/3.txt", 4),
            ("zz.txt", 5),
        ] {
            S3ObjectIndexStore::upsert_s3_object(&store, &entry("global", key, &file_id(id), 0, 0))
                .await
                .unwrap();
        }

        // Empty prefix lists every key in raw-key order.
        assert_eq!(
            scan(&store, "global", "", None, 100).await,
            vec![
                "a.txt",
                "data/1.txt",
                "data/2.txt",
                "data/sub/3.txt",
                "zz.txt"
            ]
        );

        // Prefix filtering.
        assert_eq!(
            scan(&store, "global", "data", None, 100).await,
            vec!["data/1.txt", "data/2.txt", "data/sub/3.txt"]
        );

        // Keyset cursor resumes strictly after the given raw key.
        assert_eq!(
            scan(&store, "global", "", Some("data/2.txt"), 100).await,
            vec!["data/sub/3.txt", "zz.txt"]
        );

        // Cursor combined with prefix filtering.
        assert_eq!(
            scan(&store, "global", "data", Some("data/2.txt"), 100).await,
            vec!["data/sub/3.txt"]
        );

        // Other scope namespaces are isolated.
        assert!(scan(&store, "other-scope", "", None, 100).await.is_empty());
    }

    #[tokio::test]
    async fn scan_s3_objects_respects_limit() {
        let store = make_store();
        for id in 1..=5 {
            S3ObjectIndexStore::upsert_s3_object(
                &store,
                &entry("global", &format!("f{id}.txt"), &file_id(id), 1, 1),
            )
            .await
            .unwrap();
        }
        let rows = S3ObjectIndexStore::scan_s3_objects(&store, "global", "", None, 3)
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        // Cursor resumes after the last returned raw key.
        let cursor = rows.last().unwrap().object_key.clone();
        assert_eq!(
            scan(&store, "global", "", Some(&cursor), 100).await,
            vec!["f4.txt", "f5.txt"]
        );
    }

    #[test]
    fn s3_object_migration_up_down_roundtrip() {
        use crate::local_sqlite::LOCAL_SQLITE_MIGRATIONS;

        let migration = LOCAL_SQLITE_MIGRATIONS
            .iter()
            .find(|m| m.version == "20260813000000")
            .expect("s3_object_index migration must be registered");
        let connection = Connection::open_in_memory().expect("open sqlite");

        connection
            .execute_batch(migration.up_sql)
            .expect("apply up migration");
        let exists = table_exists(&connection);
        assert!(exists, "up migration must create shardline_s3_objects");

        connection
            .execute_batch(migration.down_sql)
            .expect("apply down migration");
        assert!(
            !table_exists(&connection),
            "down migration must drop the table"
        );
    }

    fn table_exists(connection: &Connection) -> bool {
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'shardline_s3_objects'",
                [],
                |row| row.get(0),
            )
            .expect("query sqlite_master");
        count != 0
    }
}
