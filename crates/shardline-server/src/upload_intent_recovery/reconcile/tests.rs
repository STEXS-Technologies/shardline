use std::{num::NonZeroUsize, time::Duration};

use shardline_index::{LocalIndexStore, UploadIntent, UploadIntentStore};

use crate::{
    ServerError, admission::BoundedPool, object_store::ServerObjectStore,
    upload_intent_recovery::startup::RecoveryPass,
};

async fn recover<S: UploadIntentStore>(store: &S) -> Result<bool, ServerError>
where
    S::Error: std::fmt::Display,
    ServerError: From<S::Error>,
{
    let root = tempfile::tempdir().unwrap();
    super::reconcile(
        store,
        ServerObjectStore::local(root.path().join("objects")).unwrap(),
        BoundedPool::new(NonZeroUsize::MIN),
        Duration::ZERO,
        &RecoveryPass::Complete,
        |_, _| async { Err(ServerError::NotFound) },
    )
    .await
}

#[tokio::test]
async fn sqlite_recovery_skips_only_the_missing_intents_table() {
    for (sql, legacy) in [
        ("DROP TABLE shardline_upload_intents", true),
        (
            "ALTER TABLE shardline_upload_intents RENAME COLUMN object_hash TO broken_hash",
            false,
        ),
        ("DROP TABLE shardline_reliability_events", false),
    ] {
        let root = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::new(root.path().to_path_buf()).unwrap();
        store
            .create_intent(&UploadIntent::new(
                "query-failure".to_owned(),
                "chunks/absent".to_owned(),
                "a".repeat(64),
                1,
            ))
            .await
            .unwrap();
        rusqlite::Connection::open(root.path().join("metadata.sqlite3"))
            .unwrap()
            .execute_batch(sql)
            .unwrap();
        let result = recover(&store).await;
        if legacy {
            assert!(matches!(result, Ok(false)), "{sql}: {result:?}");
        } else {
            assert!(result.is_err(), "{sql}: query failure was swallowed");
        }
    }
}

#[test]
fn sqlite_legacy_detection_rejects_storage_failures_and_other_tables() {
    use shardline_index::LocalIndexStoreError;
    for error in [
        LocalIndexStoreError::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        LocalIndexStoreError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            Some("no such table: shardline_upload_intents".to_owned()),
        )),
        LocalIndexStoreError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
            Some("no such table: shardline_reliability_events".to_owned()),
        )),
    ] {
        assert!(!LocalIndexStore::is_upload_intents_table_missing(&error));
    }
}

#[cfg(feature = "docker")]
#[tokio::test]
async fn postgres_recovery_distinguishes_missing_table_from_database_failure() {
    use crate::error::IndexError;
    use shardline_index::{PostgresIndexStore, PostgresMetadataStoreError};
    let stack = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .start()
        .unwrap()
        .expect("PostgreSQL recovery qualification requires Docker");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&stack.postgres_url().unwrap())
        .await
        .unwrap();
    let store = PostgresIndexStore::new(pool.clone());
    assert!(matches!(recover(&store).await, Ok(false)));

    // Undefined relation alone is insufficient: another missing table is an error.
    let unrelated = sqlx::query("SELECT * FROM unrelated_recovery_table")
        .execute(&pool)
        .await
        .unwrap_err();
    assert!(!PostgresIndexStore::is_upload_intents_table_missing(
        &PostgresMetadataStoreError::from(unrelated)
    ));
    sqlx::query("CREATE TABLE shardline_upload_intents (intent_id TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    let malformed = recover(&store).await;
    assert!(matches!(
        malformed,
        Err(ServerError::Index(IndexError::PostgresMetadata(PostgresMetadataStoreError::Sqlx(error))))
            if error.as_database_error().and_then(|error| error.code()).as_deref() == Some("42703")
    ));

    sqlx::raw_sql("CREATE ROLE recovery_denied; SET ROLE recovery_denied")
        .execute(&pool)
        .await
        .unwrap();
    let denied = sqlx::query("SELECT * FROM shardline_upload_intents")
        .execute(&pool)
        .await
        .unwrap_err();
    assert!(!PostgresIndexStore::is_upload_intents_table_missing(
        &PostgresMetadataStoreError::from(denied)
    ));
    pool.close().await;
    assert!(matches!(
        recover(&store).await,
        Err(ServerError::Index(IndexError::PostgresMetadata(PostgresMetadataStoreError::Sqlx(error))))
            if matches!(error.as_ref(), sqlx::Error::PoolClosed)
    ));
}
