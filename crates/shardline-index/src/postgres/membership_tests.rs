#![allow(clippy::unwrap_used)]

use super::{PostgresMetadataStoreError, PostgresRecordStore, connect_isolated_postgres};
use crate::{FileChunkRecord, FileRecord, RecordTraversal, StorageRepresentation};
use shardline_protocol::{RepositoryProvider, RepositoryScope};
use sqlx::{Connection, query, query_scalar, raw_sql, types::Json};

async fn candidates(store: &PostgresRecordStore, scope: &RepositoryScope, hash: &str) -> usize {
    let mut count = 0_usize;
    store
        .visit_repository_hash_records(scope, hash, |stored| {
            let parsed: FileRecord = serde_json::from_slice(&stored.bytes)?;
            assert_eq!(parsed.repository_scope.as_ref(), Some(scope));
            assert!(parsed.chunks.iter().any(|member| member.hash == hash));
            count = count.saturating_add(1);
            Ok::<_, PostgresMetadataStoreError>(())
        })
        .await
        .unwrap();
    count
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_membership_backfills_and_tracks_legacy_sql_writes_atomically() {
    let Some(pool) = connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresRecordStore::new(pool.clone());
    let scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "repo", Some("main")).unwrap();
    let other =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "repo", Some("other")).unwrap();
    let mut record = FileRecord {
        file_id: "file".into(),
        content_hash: "c".repeat(64),
        total_bytes: 4,
        chunk_size: 0,
        storage_repr: StorageRepresentation::WholeFileV1,
        repository_scope: Some(scope.clone()),
        chunks: vec![FileChunkRecord {
            hash: "a".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 12,
        }],
    };
    store.commit_file_version_metadata(&record).await.unwrap();
    record.repository_scope = Some(other.clone());
    store.commit_file_version_metadata(&record).await.unwrap();
    record.repository_scope = Some(scope.clone());
    raw_sql(
        "DROP TRIGGER shardline_record_membership_refresh ON shardline_file_records;
             DROP TABLE shardline_record_membership;",
    )
    .execute(&pool)
    .await
    .unwrap();
    raw_sql(include_str!(
        "../../../../migrations/20261023010000_record_membership.up.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(candidates(&store, &scope, &"a".repeat(64)).await, 2);
    let key = store.latest_record_locator(&record).record_key().to_owned();
    let before: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT record_key, ctid::text, xmin::text FROM shardline_record_membership ORDER BY record_key",
    ).fetch_all(&pool).await.unwrap();
    query("UPDATE shardline_file_records SET record=record, updated_at=now() WHERE record_key=$1")
        .bind(&key)
        .execute(&pool)
        .await
        .unwrap();
    let after: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT record_key, ctid::text, xmin::text FROM shardline_record_membership ORDER BY record_key",
    ).fetch_all(&pool).await.unwrap();
    assert_eq!(before, after, "exact replay must not rewrite derived rows");
    record.chunks.first_mut().unwrap().hash = "b".repeat(64);
    let mut connection = pool.acquire().await.unwrap();
    let mut transaction = connection.begin().await.unwrap();
    query("UPDATE shardline_file_records SET record=$1 WHERE record_key=$2")
        .bind(Json(&record))
        .bind(&key)
        .execute(&mut *transaction)
        .await
        .unwrap();
    let changed: i64 =
        query_scalar("SELECT count(*) FROM shardline_record_membership WHERE member_hash=$1")
            .bind("b".repeat(64))
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
    assert_eq!(changed, 1);
    transaction.rollback().await.unwrap();
    drop(connection);
    assert_eq!(candidates(&store, &scope, &"b".repeat(64)).await, 0);
    query("UPDATE shardline_file_records SET record=$1 WHERE record_key=$2")
        .bind(Json(&record))
        .bind(&key)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(candidates(&store, &scope, &"a".repeat(64)).await, 1);
    assert_eq!(candidates(&store, &scope, &"b".repeat(64)).await, 1);
    assert_eq!(candidates(&store, &other, &"a".repeat(64)).await, 2);
    query("DELETE FROM shardline_file_records WHERE record_key=$1")
        .bind(&key)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(candidates(&store, &scope, &"b".repeat(64)).await, 0);
    assert_eq!(candidates(&store, &scope, &"a".repeat(64)).await, 1);
    pool.close().await;
}
