//! Exercise early proof completion and connection reuse on real PostgreSQL.

use super::{repository_covers_xorb_range, repository_xorb_coverage};
use shardline_index::{
    FileChunkRecord, FileRecord, PostgresRecordStore, RecordTraversal, StorageRepresentation,
};
use shardline_protocol::{ByteRange, RepositoryProvider, RepositoryScope};

#[tokio::test]
async fn complete_range_proof_releases_postgres_connection_and_preserves_incomplete_errors() {
    let Some(fixture) = crate::test_support::postgres_fixture::PostgresFixture::new().await else {
        assert!(
            std::env::var_os("SHARDLINE_REQUIRE_POSTGRES_TESTS").is_none(),
            "required PostgreSQL range qualification needs DATABASE_URL"
        );
        return;
    };
    fixture.migrate().await;
    let pool = fixture.index().pool().clone();
    let store = PostgresRecordStore::new(pool.clone());
    let scope = RepositoryScope::new(RepositoryProvider::Generic, "team", "range", None).unwrap();
    let hash = "a".repeat(64);
    let mut record = FileRecord {
        file_id: "aa".into(),
        content_hash: "b".repeat(64),
        total_bytes: 4,
        chunk_size: 0,
        storage_repr: StorageRepresentation::WholeFileV1,
        repository_scope: Some(scope.clone()),
        chunks: vec![FileChunkRecord {
            hash: hash.clone(),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 12,
        }],
    };
    store.commit_file_version_metadata(&record).await.unwrap();
    let first = store
        .first_repository_hash_record(&scope, &hash)
        .await
        .unwrap()
        .unwrap();
    let authoritative: FileRecord = serde_json::from_slice(&first.bytes).unwrap();
    assert_eq!(authoritative.file_id, "aa");
    assert!(
        store
            .first_repository_hash_record(&scope, &"c".repeat(64))
            .await
            .unwrap()
            .is_none()
    );
    let outsider =
        RepositoryScope::new(RepositoryProvider::Generic, "other", "range", None).unwrap();
    assert!(
        store
            .first_repository_hash_record(&outsider, &hash)
            .await
            .unwrap()
            .is_none()
    );
    record.file_id = "bb".into();
    store.commit_file_version_metadata(&record).await.unwrap();
    sqlx::query("UPDATE shardline_file_records SET record='{}' WHERE file_id='bb'")
        .execute(&pool)
        .await
        .unwrap();
    // Reintroduce a stale candidate hint; authority still comes from the record.
    sqlx::query(
        "INSERT INTO shardline_record_membership(record_key,scope_key,member_hash)
         SELECT record_key,scope_key,$1 FROM shardline_file_records WHERE file_id='bb'",
    )
    .bind(&hash)
    .execute(&pool)
    .await
    .unwrap();
    for _ in 0..32 {
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                repository_covers_xorb_range(
                    &store,
                    &hash,
                    &scope,
                    ByteRange::new(0, 11).unwrap(),
                ),
            )
            .await
            .unwrap()
            .unwrap()
        );
    }
    assert!(
        repository_covers_xorb_range(&store, &hash, &scope, ByteRange::new(0, 12).unwrap())
            .await
            .is_err()
    );
    assert!(
        repository_xorb_coverage(&store, &hash, &scope, None)
            .await
            .is_err()
    );
    fixture.finish().await;
}
