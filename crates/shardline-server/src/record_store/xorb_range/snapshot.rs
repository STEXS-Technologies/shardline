//! The probe and fallback must never combine authority from different snapshots.

mod store;

use shardline_index::{FileChunkRecord, FileRecord, LocalRecordStore, StorageRepresentation};
use shardline_protocol::{ByteRange, RepositoryProvider, RepositoryScope};
use store::Snapshots;

#[tokio::test]
async fn fallback_discards_terms_from_the_first_candidate_snapshot() {
    let first_root = tempfile::tempdir().unwrap();
    let fallback_root = tempfile::tempdir().unwrap();
    let first = LocalRecordStore::new(first_root.path().to_owned()).unwrap();
    let fallback = LocalRecordStore::new(fallback_root.path().to_owned()).unwrap();
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
    first.commit_file_version_metadata(&record).await.unwrap();
    record.file_id = "bb".into();
    record.chunks[0].packed_start = 12;
    record.chunks[0].packed_end = 24;
    fallback
        .commit_file_version_metadata(&record)
        .await
        .unwrap();
    let snapshots = Snapshots { first, fallback };
    let requested = ByteRange::new(0, 23).unwrap();
    assert!(
        !super::repository_covers_xorb_range(&snapshots, &hash, &scope, requested)
            .await
            .unwrap(),
        "neither snapshot owns the complete range; their union must not grant access"
    );
    record.file_id = "aa".into();
    record.chunks[0].packed_start = 0;
    record.chunks[0].packed_end = 12;
    snapshots
        .fallback
        .commit_file_version_metadata(&record)
        .await
        .unwrap();
    assert!(
        super::repository_covers_xorb_range(&snapshots, &hash, &scope, requested)
            .await
            .unwrap(),
        "a complete fresh traversal may combine its own authoritative terms"
    );
}
