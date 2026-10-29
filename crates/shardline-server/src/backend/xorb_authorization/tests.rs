//! A file hint must prove current authoritative coverage, never broaden it.
#![allow(clippy::unwrap_used)]

use super::*;
use shardline_index::{FileChunkRecord, FileRecord, LocalRecordStore, StorageRepresentation};
use shardline_protocol::RepositoryProvider;
use std::num::NonZeroUsize;

#[tokio::test]
#[ignore = "requires an owner-migrated disposable PostgreSQL database"]
async fn file_hint_observes_postgres_authoritative_replacement_and_revocation() {
    let fixture = crate::test_support::postgres_fixture::PostgresFixture::new()
        .await
        .expect("file-hint qualification requires PostgreSQL");
    fixture.migrate().await;
    let pool = fixture.index().pool().clone();
    let store = shardline_index::PostgresRecordStore::new(pool.clone());
    let scope = RepositoryScope::new(RepositoryProvider::Generic, "team", "file", None).unwrap();
    let hash = "a".repeat(64);
    let mut record = file_record(&scope, &hash);
    let range = ByteRange::new(12, 23).unwrap();
    store.commit_file_version_metadata(&record).await.unwrap();
    assert!(
        fixture
            .backend
            .file_xorb_coverage(&record.file_id, &hash, &scope, range)
            .await
            .unwrap()
            .is_some()
    );
    record.content_hash = "d".repeat(64);
    record.chunks[0].packed_start = 24;
    record.chunks[0].packed_end = 36;
    store.commit_file_version_metadata(&record).await.unwrap();
    assert!(
        fixture
            .backend
            .file_xorb_coverage(&record.file_id, &hash, &scope, range)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !fixture
            .backend
            .repository_xorb_coverage(&hash, &scope, Some(range))
            .await
            .unwrap()
            .is_empty()
    );
    sqlx::query("DELETE FROM shardline_file_records WHERE file_id=$1")
        .bind(&record.file_id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        fixture
            .backend
            .file_xorb_coverage(&record.file_id, &hash, &scope, range)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .backend
            .repository_xorb_coverage(&hash, &scope, Some(range))
            .await
            .unwrap()
            .is_empty()
    );
    fixture.finish().await;
}

fn file_record(scope: &RepositoryScope, hash: &str) -> FileRecord {
    FileRecord {
        file_id: "hinted-file".into(),
        content_hash: "b".repeat(64),
        total_bytes: 4,
        chunk_size: 0,
        storage_repr: StorageRepresentation::WholeFileV1,
        repository_scope: Some(scope.clone()),
        chunks: vec![FileChunkRecord {
            hash: hash.to_owned(),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 12,
            packed_end: 24,
        }],
    }
}

#[tokio::test]
async fn file_hint_requires_exact_scope_hash_and_complete_range() {
    let root = tempfile::tempdir().unwrap();
    let local = crate::LocalBackend::new(
        root.path().to_owned(),
        "http://127.0.0.1:8080".into(),
        NonZeroUsize::new(4096).unwrap(),
    )
    .await
    .unwrap();
    let backend = ServerBackend::Local(local);
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    let scope = RepositoryScope::new(RepositoryProvider::Generic, "team", "file", None).unwrap();
    let other = RepositoryScope::new(RepositoryProvider::Generic, "other", "file", None).unwrap();
    let hash = "a".repeat(64);
    let mut record = file_record(&scope, &hash);
    store.commit_file_version_metadata(&record).await.unwrap();
    for (file, candidate_hash, candidate_scope, start, end, expected) in [
        ("hinted-file", hash.as_str(), &scope, 12, 23, true),
        ("missing-file", hash.as_str(), &scope, 12, 23, false),
        ("hinted-file", hash.as_str(), &other, 12, 23, false),
        (
            "hinted-file",
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            &scope,
            12,
            23,
            false,
        ),
        ("hinted-file", hash.as_str(), &scope, 0, 23, false),
        ("hinted-file", hash.as_str(), &scope, 12, 24, false),
    ] {
        let proof = backend
            .file_xorb_coverage(
                file,
                candidate_hash,
                candidate_scope,
                ByteRange::new(start, end).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(proof.is_some(), expected);
    }
    // Replacing the current file must be observed immediately. Historical
    // authorization is left to the unchanged repository fallback, not cached
    // or combined with this no-longer-valid current-file proof.
    record.content_hash = "d".repeat(64);
    record.chunks[0].packed_start = 24;
    record.chunks[0].packed_end = 36;
    store.commit_file_version_metadata(&record).await.unwrap();
    assert!(
        backend
            .file_xorb_coverage(
                "hinted-file",
                &hash,
                &scope,
                ByteRange::new(12, 23).unwrap()
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !backend
            .repository_xorb_coverage(&hash, &scope, Some(ByteRange::new(12, 23).unwrap()))
            .await
            .unwrap()
            .is_empty()
    );
}
