//! Physical reclamation with missing native copies and derived index loss.
#![allow(clippy::unwrap_used)]

use crate::{LocalGcOptions, ServerFrontend, run_gc_with_stores};

fn generic_record(content: &[u8]) -> FileRecord {
    let member = compute_data_hash(content).hex();
    FileRecord {
        file_id: "historical-file".to_owned(),
        content_hash: member.clone(),
        total_bytes: content.len() as u64,
        chunk_size: content.len() as u64,
        storage_repr: StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash: member,
            offset: 0,
            length: content.len() as u64,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 0,
        }],
    }
}

#[tokio::test]
async fn missing_copy_recovery_stops_before_sweep_on_corrupt_source() {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let records = LocalRecordStore::new(root.path().join("records")).unwrap();
    let content = b"source requiring authenticated recovery";
    let hash = source(&objects, content).await;
    let member = compute_data_hash(content).hex();
    ObjectStore::delete_if_present(&objects, &chunk_object_key(&member).unwrap()).unwrap();
    records
        .write_version_record(&generic_record(content))
        .await
        .unwrap();
    let key = shardline_xet_adapter::xorb_object_key(&hash).unwrap();
    let length = ObjectStore::metadata(&objects, &key)
        .unwrap()
        .unwrap()
        .length();
    let mut corrupt = objects.read_full_object(&key, length).unwrap();
    *corrupt.get_mut(8).unwrap() ^= 1;
    ObjectStore::delete_if_present(&objects, &key).unwrap();
    ObjectStore::put_if_absent(
        &objects,
        &key,
        shardline_storage::ObjectBody::from_slice(&corrupt),
        &shardline_storage::ObjectIntegrity::new(
            shardline_server_core::chunk_hash(&corrupt),
            corrupt.len() as u64,
        ),
    )
    .unwrap();
    let result = run_gc_with_stores(
        &records,
        &MemoryIndexStore::new(),
        &objects,
        &[ServerFrontend::Xet],
        LocalGcOptions::mark_and_sweep(0),
    )
    .await;
    assert!(
        result.is_err(),
        "damaged source must stop GC, not become an orphan"
    );
    assert!(ObjectStore::metadata(&objects, &key).unwrap().is_some());
}
use shardline_index::{
    FileChunkRecord, FileRecord, LocalRecordStore, MemoryIndexStore, RecordMutation,
    RecordTraversal, StorageRepresentation,
};
use shardline_server_core::{ServerObjectStore, chunk_object_key};
use shardline_storage::ObjectStore;
use shardline_xet_core::{
    merklehash::{compute_data_hash, xorb_hash},
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

async fn source(objects: &ServerObjectStore, content: &[u8]) -> String {
    let leaf = compute_data_hash(content);
    let length = content.len() as u64;
    let tail = b"a second native container member";
    let tail_hash = compute_data_hash(tail);
    let hash = xorb_hash(&[(leaf, length), (tail_hash, tail.len() as u64)]);
    let bytes = serialized_xorb_object_from_components(
        &hash,
        [content, tail.as_slice()].concat(),
        vec![
            (leaf, length),
            (tail_hash, length.checked_add(tail.len() as u64).unwrap()),
        ],
        CompressionScheme::None,
    )
    .unwrap()
    .serialized_data;
    shardline_xet_adapter::store_uploaded_xorb_bytes(objects, &hash.hex(), &bytes)
        .await
        .unwrap();
    hash.hex()
}

#[tokio::test]
async fn physical_sweep_recovers_reachability_without_member_index_or_copy() {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let records = LocalRecordStore::new(root.path().join("records")).unwrap();
    let index = MemoryIndexStore::new();
    let content = b"retained native bytes";
    let live = source(&objects, content).await;
    let orphan = source(&objects, b"unreferenced native bytes").await;
    let member = compute_data_hash(content).hex();
    ObjectStore::delete_if_present(&objects, &chunk_object_key(&member).unwrap()).unwrap();
    // No descriptor was cached: the member index is completely absent, as after
    // derived metadata loss. Only this immutable generic version protects bytes.
    records
        .write_version_record(&FileRecord {
            file_id: "historical-file".to_owned(),
            content_hash: member.clone(),
            total_bytes: content.len() as u64,
            chunk_size: content.len() as u64,
            storage_repr: StorageRepresentation::FixedChunkV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: member.clone(),
                offset: 0,
                length: content.len() as u64,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 0,
            }],
        })
        .await
        .unwrap();
    let live_key = shardline_xet_adapter::xorb_object_key(&live).unwrap();
    let orphan_key = shardline_xet_adapter::xorb_object_key(&orphan).unwrap();
    run_gc_with_stores(
        &records,
        &index,
        &objects,
        &[ServerFrontend::Xet],
        LocalGcOptions::mark_and_sweep(0),
    )
    .await
    .unwrap();
    assert!(
        ObjectStore::metadata(&objects, &live_key)
            .unwrap()
            .is_some()
    );
    assert!(
        ObjectStore::metadata(&objects, &orphan_key)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        shardline_xet_adapter::read_verified_xorb_chunk(&objects, &live, &member)
            .unwrap()
            .unwrap(),
        content
    );
    for locator in records.list_version_record_locators().await.unwrap() {
        records.delete_record_locator(&locator).await.unwrap();
    }
    run_gc_with_stores(
        &records,
        &index,
        &objects,
        &[ServerFrontend::Xet],
        LocalGcOptions::mark_and_sweep(0),
    )
    .await
    .unwrap();
    assert!(
        ObjectStore::metadata(&objects, &live_key)
            .unwrap()
            .is_none()
    );
}
