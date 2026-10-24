#![allow(clippy::unwrap_used)]

use super::{XetXorbChunk, XetXorbDescriptor};
use crate::{FileChunkRecord, FileRecord, LocalRecordStore, StorageRepresentation};
use shardline_protocol::{RepositoryProvider, RepositoryScope};

mod postgres_indexed;

fn descriptor() -> XetXorbDescriptor {
    XetXorbDescriptor {
        hash: "a".repeat(64),
        total_bytes: 256,
        chunks: vec![XetXorbChunk {
            hash: "b".repeat(64),
            packed_start: 0,
            packed_end: 12,
            unpacked_start: 0,
            unpacked_end: 4,
        }],
    }
}

#[tokio::test]
async fn sqlite_indexed_descriptor_requires_every_member_row_and_replay_repairs_it() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    let mut target = descriptor();
    let mut second = target.chunks.first().unwrap().clone();
    second.hash = "c".repeat(64);
    second.packed_start = 12;
    second.packed_end = 24;
    second.unpacked_start = 4;
    second.unpacked_end = 8;
    target.chunks.push(second);
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&target))
        .await
        .unwrap();
    assert_eq!(
        store
            .xet_indexed_xorb_descriptor(&target.hash)
            .await
            .unwrap(),
        Some(target.clone())
    );
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    connection
        .execute(
            "DELETE FROM shardline_xet_chunks WHERE xorb_hash=?1 AND chunk_index=1",
            [&target.hash],
        )
        .unwrap();
    assert!(
        store
            .xet_indexed_xorb_descriptor(&target.hash)
            .await
            .unwrap()
            .is_none()
    );
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&target))
        .await
        .unwrap();
    assert_eq!(
        store
            .xet_indexed_xorb_descriptor(&target.hash)
            .await
            .unwrap(),
        Some(target.clone())
    );
    connection
        .execute(
            "UPDATE shardline_xet_chunks SET chunk_hash=?2 WHERE xorb_hash=?1 AND chunk_index=1",
            [&target.hash, &"d".repeat(64)],
        )
        .unwrap();
    assert!(
        store
            .xet_indexed_xorb_descriptor(&target.hash)
            .await
            .unwrap()
            .is_none()
    );
    connection
        .execute(
            "UPDATE shardline_xet_xorbs SET descriptor='{}' WHERE xorb_hash=?1",
            [&target.hash],
        )
        .unwrap();
    assert!(
        store
            .xet_indexed_xorb_descriptor(&target.hash)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sqlite_exact_native_location_ignores_unrelated_corruption_but_rejects_its_own() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    let target = descriptor();
    let mut unrelated = target.clone();
    unrelated.hash = "1".repeat(64);
    store
        .cache_xet_xorb_descriptors(&[target.clone(), unrelated.clone()])
        .await
        .unwrap();
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE shardline_xet_xorbs SET descriptor='{}' WHERE xorb_hash=?1",
            [&unrelated.hash],
        )
        .unwrap();
    let member = &target.chunks.first().unwrap().hash;
    assert_eq!(
        store
            .xet_chunk_location(member, &target.hash)
            .await
            .unwrap(),
        Some(target.clone())
    );
    assert!(
        store
            .xet_chunk_location(member, &unrelated.hash)
            .await
            .is_err()
    );
    connection
        .execute(
            "DELETE FROM shardline_xet_chunks WHERE xorb_hash=?1",
            [&target.hash],
        )
        .unwrap();
    assert!(
        store
            .xet_chunk_location(member, &target.hash)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn sqlite_verified_descriptor_replay_repairs_missing_and_stale_member_rows() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    let descriptor = descriptor();
    let chunk = descriptor.chunks.first().unwrap().hash.clone();
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await
        .unwrap();
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    connection
        .execute(
            "DELETE FROM shardline_xet_chunks WHERE xorb_hash=?1",
            [&descriptor.hash],
        )
        .unwrap();
    assert!(
        store
            .xet_chunk_candidates(&chunk, None)
            .await
            .unwrap()
            .is_empty()
    );
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await
        .unwrap();
    assert_eq!(
        store.xet_chunk_candidates(&chunk, None).await.unwrap(),
        vec![descriptor.clone()]
    );
    connection
        .execute(
            "UPDATE shardline_xet_chunks SET chunk_hash=?2 WHERE xorb_hash=?1",
            [&descriptor.hash, &"d".repeat(64)],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO shardline_xet_chunks(xorb_hash,chunk_index,chunk_hash) VALUES(?1,99,?2)",
            [&descriptor.hash, &"e".repeat(64)],
        )
        .unwrap();
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await
        .unwrap();
    assert_eq!(
        store.xet_chunk_candidates(&chunk, None).await.unwrap(),
        vec![descriptor.clone()]
    );
    let count: i64 = connection
        .query_row(
            "SELECT count(*) FROM shardline_xet_chunks WHERE xorb_hash=?1",
            [&descriptor.hash],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    let version: i64 = connection
        .query_row("PRAGMA data_version", [], |row| row.get(0))
        .unwrap();
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await
        .unwrap();
    let unchanged: i64 = connection
        .query_row("PRAGMA data_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        unchanged, version,
        "healthy replay must not rewrite derived rows"
    );
}

fn record(scope: &RepositoryScope, hash: &str) -> FileRecord {
    FileRecord {
        file_id: "file".into(),
        content_hash: "c".repeat(64),
        total_bytes: 4,
        chunk_size: 0,
        storage_repr: StorageRepresentation::WholeFileV1,
        repository_scope: Some(scope.clone()),
        chunks: vec![FileChunkRecord {
            hash: hash.into(),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 12,
        }],
    }
}

macro_rules! scoped_descriptor_contract {
    ($store:expr) => {{
        let descriptor = descriptor();
        let left =
            RepositoryScope::new(RepositoryProvider::GitHub, "left", "repo", Some("main")).unwrap();
        let right = RepositoryScope::new(RepositoryProvider::GitHub, "right", "repo", Some("main"))
            .unwrap();
        $store
            .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
            .await
            .unwrap();
        $store
            .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
            .await
            .unwrap();
        assert_eq!(
            $store.xet_xorb_descriptor(&descriptor.hash).await.unwrap(),
            Some(descriptor.clone())
        );
        assert_eq!(
            $store
                .xet_indexed_xorb_descriptor(&descriptor.hash)
                .await
                .unwrap(),
            Some(descriptor.clone())
        );
        assert_eq!(
            $store
                .xet_chunk_candidates(&descriptor.chunks.first().unwrap().hash, None)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            $store
                .xet_chunk_candidates(&descriptor.chunks.first().unwrap().hash, Some(&left))
                .await
                .unwrap()
                .is_empty(),
            "cached descriptors never establish publication authority"
        );
        let record = record(&left, &descriptor.hash);
        $store.commit_file_version_metadata(&record).await.unwrap();
        assert_eq!(
            $store
                .xet_chunk_candidates(&descriptor.chunks.first().unwrap().hash, Some(&left))
                .await
                .unwrap(),
            vec![descriptor.clone()]
        );
        assert!(
            $store
                .xet_chunk_candidates(&descriptor.chunks.first().unwrap().hash, Some(&right))
                .await
                .unwrap()
                .is_empty()
        );
        let mut conflicting = descriptor.clone();
        conflicting.total_bytes = 512;
        assert!(
            $store
                .cache_xet_xorb_descriptors(&[conflicting])
                .await
                .is_err()
        );
        assert_eq!(
            $store.xet_xorb_descriptor(&descriptor.hash).await.unwrap(),
            Some(descriptor.clone())
        );
        // The same chunk can occur in many independently packed containers.
        // Exhaust every page without losing or repeating a candidate, while
        // preserving scope filtering on every page rather than only the first.
        let mut descriptors = Vec::new();
        let mut records = Vec::new();
        for index in 1..=65 {
            let mut candidate = descriptor.clone();
            candidate.hash = format!("{index:064x}");
            let mut candidate_record = record.clone();
            candidate_record.file_id = format!("candidate-{index}");
            candidate_record.chunks.first_mut().unwrap().hash = candidate.hash.clone();
            descriptors.push(candidate);
            records.push(candidate_record);
        }
        $store
            .cache_xet_xorb_descriptors(&descriptors)
            .await
            .unwrap();
        for candidate_record in records {
            $store
                .commit_file_version_metadata(&candidate_record)
                .await
                .unwrap();
        }
        let mut found = Vec::new();
        let mut after = None::<String>;
        loop {
            let page = $store
                .xet_chunk_candidates_after(
                    &descriptor.chunks.first().unwrap().hash,
                    Some(&left),
                    after.as_deref(),
                )
                .await
                .unwrap();
            assert!(page.len() <= 32);
            if page.is_empty() {
                break;
            }
            for candidate in &page {
                if let Some(previous) = found.last() {
                    assert!(previous < &candidate.hash);
                }
                found.push(candidate.hash.clone());
            }
            after = page.last().map(|candidate| candidate.hash.clone());
        }
        assert_eq!(found.len(), 66);
        assert_eq!(found.last(), Some(&descriptor.hash));
        let member = &descriptor.chunks.first().unwrap().hash;
        assert_eq!(
            $store
                .xet_chunk_location(member, &descriptor.hash)
                .await
                .unwrap(),
            Some(descriptor.clone()),
            "exact lookup must find the requested source beyond the first candidate page"
        );
        assert!(
            $store
                .xet_chunk_location(&"f".repeat(64), &descriptor.hash)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            $store
                .xet_chunk_location(member, &"f".repeat(64))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            $store
                .xet_chunk_candidates_after(
                    &descriptor.chunks.first().unwrap().hash,
                    Some(&right),
                    None,
                )
                .await
                .unwrap()
                .is_empty()
        );
        (record, left)
    }};
}

#[tokio::test]
async fn sqlite_xet_descriptors_are_immutable_scoped_and_transactional() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    let (mut record, left) = scoped_descriptor_contract!(store);
    record.content_hash = "d".repeat(64);
    let mut conflicting = descriptor();
    conflicting.total_bytes = 512;
    assert!(
        store
            .commit_native_shard_metadata_with_descriptors(
                std::slice::from_ref(&record),
                &[],
                &[],
                &[conflicting]
            )
            .await
            .is_err()
    );
    let found = store.xet_xorb_descriptor(&descriptor().hash).await.unwrap();
    assert_eq!(found, Some(descriptor()));
    let identity = crate::XetFileIdentity {
        repository_scope: left.clone(),
        sha256: "e".repeat(64),
        file_id: "native".into(),
        total_bytes: 4,
    };
    store
        .commit_native_shard_metadata_with_descriptors(
            &[],
            &[],
            std::slice::from_ref(&identity),
            &[],
        )
        .await
        .unwrap();
    let mut new_descriptor = descriptor();
    new_descriptor.hash = "f".repeat(64);
    let mut conflicting_identity = identity;
    conflicting_identity.total_bytes = 5;
    assert!(
        store
            .commit_native_shard_metadata_with_descriptors(
                std::slice::from_ref(&record),
                &[],
                &[conflicting_identity],
                std::slice::from_ref(&new_descriptor)
            )
            .await
            .is_err()
    );
    assert!(
        store
            .xet_xorb_descriptor(&new_descriptor.hash)
            .await
            .unwrap()
            .is_none(),
        "a later identity conflict must roll back the earlier descriptor insertion"
    );
    let mut count = 0_usize;
    crate::RecordTraversal::visit_repository_hash_records(
        &store,
        &left,
        &record.chunks.first().unwrap().hash,
        |entry| {
            let stored: FileRecord = serde_json::from_slice(&entry.bytes)?;
            assert_ne!(
                stored.content_hash, record.content_hash,
                "a failed descriptor transaction must not publish the file"
            );
            count = count.checked_add(1).unwrap();
            Ok::<_, crate::LocalIndexStoreError>(())
        },
    )
    .await
    .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn postgres_xet_descriptors_are_immutable_scoped_and_transactional() {
    let Some(pool) = crate::postgres::connect_isolated_postgres().await else {
        return;
    };
    let store = crate::PostgresRecordStore::new(pool.clone());
    let (mut record, left) = scoped_descriptor_contract!(store);
    record.content_hash = "d".repeat(64);
    let mut conflicting = descriptor();
    conflicting.total_bytes = 512;
    let mut connection = pool.acquire().await.unwrap();
    assert!(
        store
            .commit_native_shard_metadata_on_connection_with_descriptors(
                &mut connection,
                std::slice::from_ref(&record),
                &[],
                &[],
                &[],
                &[conflicting]
            )
            .await
            .is_err()
    );
    drop(connection);
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM shardline_file_records WHERE record->>'content_hash'=$1",
    )
    .bind("d".repeat(64))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 0);
    let identity = crate::XetFileIdentity {
        repository_scope: left,
        sha256: "e".repeat(64),
        file_id: "native".into(),
        total_bytes: 4,
    };
    store
        .commit_native_shard_metadata_with_xet_identities(
            &[],
            &[],
            &[],
            std::slice::from_ref(&identity),
        )
        .await
        .unwrap();
    let mut new_descriptor = descriptor();
    new_descriptor.hash = "f".repeat(64);
    let mut conflicting_identity = identity;
    conflicting_identity.total_bytes = 5;
    let mut identity_connection = pool.acquire().await.unwrap();
    assert!(
        store
            .commit_native_shard_metadata_on_connection_with_descriptors(
                &mut identity_connection,
                &[record],
                &[],
                &[],
                &[conflicting_identity],
                std::slice::from_ref(&new_descriptor)
            )
            .await
            .is_err()
    );
    drop(identity_connection);
    assert!(
        store
            .xet_xorb_descriptor(&new_descriptor.hash)
            .await
            .unwrap()
            .is_none(),
        "a later identity conflict must roll back the earlier descriptor insertion"
    );
    pool.close().await;
}

#[test]
fn malformed_descriptor_boundaries_fail_closed() {
    let valid = descriptor();
    assert!(valid.has_valid_structure());
    for (start, end, unpacked) in [(1, 12, 4), (0, 0, 4), (0, 12, 0), (0, 256, 4)] {
        let mut invalid = valid.clone();
        invalid.chunks.first_mut().unwrap().packed_start = start;
        invalid.chunks.first_mut().unwrap().packed_end = end;
        invalid.chunks.first_mut().unwrap().unpacked_end = unpacked;
        assert!(!invalid.has_valid_structure());
    }
}

#[tokio::test]
async fn sqlite_corrupted_descriptor_identity_and_oversized_rows_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    let valid = descriptor();
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&valid))
        .await
        .unwrap();
    let connection = store.open_connection().unwrap();
    let mut forged = valid.clone();
    forged.hash = "f".repeat(64);
    for json in [
        serde_json::to_string(&forged).unwrap(),
        " ".repeat(4 * 1024 * 1024 + 1),
    ] {
        connection
            .execute(
                "UPDATE shardline_xet_xorbs SET descriptor=?1 WHERE xorb_hash=?2",
                rusqlite::params![json, valid.hash],
            )
            .unwrap();
        assert!(store.xet_xorb_descriptor(&valid.hash).await.is_err());
        assert!(
            store
                .xet_indexed_xorb_descriptor(&valid.hash)
                .await
                .is_err()
        );
        assert!(
            store
                .xet_chunk_location(&valid.chunks.first().unwrap().hash, &valid.hash)
                .await
                .is_err()
        );
        assert!(
            store
                .xet_chunk_candidates(&valid.chunks.first().unwrap().hash, None)
                .await
                .is_err()
        );
        assert!(
            store
                .cache_xet_xorb_descriptors(std::slice::from_ref(&valid))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn postgres_indexed_descriptor_requires_exact_complete_membership() {
    let Some(pool) = crate::postgres::connect_isolated_postgres().await else {
        return;
    };
    let store = crate::PostgresRecordStore::new(pool.clone());
    let target = descriptor();
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&target))
        .await
        .unwrap();
    assert_eq!(
        store
            .xet_indexed_xorb_descriptor(&target.hash)
            .await
            .unwrap(),
        Some(target.clone())
    );
    sqlx::query("DELETE FROM shardline_xet_chunks WHERE xorb_hash=$1")
        .bind(&target.hash)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        store
            .xet_indexed_xorb_descriptor(&target.hash)
            .await
            .unwrap()
            .is_none()
    );
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&target))
        .await
        .unwrap();
    sqlx::query("UPDATE shardline_xet_chunks SET chunk_hash=$2 WHERE xorb_hash=$1")
        .bind(&target.hash)
        .bind("d".repeat(64))
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        store
            .xet_indexed_xorb_descriptor(&target.hash)
            .await
            .unwrap()
            .is_none()
    );
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&target))
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO shardline_xet_chunks(xorb_hash,chunk_index,chunk_hash) VALUES($1,1,$2)",
    )
    .bind(&target.hash)
    .bind("e".repeat(64))
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        store
            .xet_indexed_xorb_descriptor(&target.hash)
            .await
            .unwrap()
            .is_none()
    );
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&target))
        .await
        .unwrap();
    assert_eq!(
        store
            .xet_indexed_xorb_descriptor(&target.hash)
            .await
            .unwrap(),
        Some(target)
    );
    pool.close().await;
}

#[tokio::test]
async fn postgres_corrupted_descriptors_fail_closed_and_candidates_follow_source_deletion() {
    let Some(pool) = crate::postgres::connect_isolated_postgres().await else {
        return;
    };
    let store = crate::PostgresRecordStore::new(pool.clone());
    let valid = descriptor();
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&valid))
        .await
        .unwrap();
    let mut forged = valid.clone();
    forged.hash = "f".repeat(64);
    for json in [
        serde_json::to_value(&forged).unwrap(),
        serde_json::json!({"padding": "x".repeat(4 * 1024 * 1024 + 1)}),
    ] {
        sqlx::query("UPDATE shardline_xet_xorbs SET descriptor=$1 WHERE xorb_hash=$2")
            .bind(json)
            .bind(&valid.hash)
            .execute(&pool)
            .await
            .unwrap();
        assert!(store.xet_xorb_descriptor(&valid.hash).await.is_err());
        assert!(
            store
                .xet_indexed_xorb_descriptor(&valid.hash)
                .await
                .is_err()
        );
        assert!(
            store
                .xet_chunk_location(&valid.chunks.first().unwrap().hash, &valid.hash)
                .await
                .is_err()
        );
        assert!(
            store
                .xet_chunk_candidates(&valid.chunks.first().unwrap().hash, None)
                .await
                .is_err()
        );
        assert!(
            store
                .cache_xet_xorb_descriptors(std::slice::from_ref(&valid))
                .await
                .is_err()
        );
    }
    sqlx::query("DELETE FROM shardline_xet_xorbs WHERE xorb_hash=$1")
        .bind(&valid.hash)
        .execute(&pool)
        .await
        .unwrap();
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM shardline_xet_chunks WHERE xorb_hash=$1")
            .bind(&valid.hash)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        count, 0,
        "derived chunk candidates must cascade with their descriptor"
    );
    pool.close().await;
}
