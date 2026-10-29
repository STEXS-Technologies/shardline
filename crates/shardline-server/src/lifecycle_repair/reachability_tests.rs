//! Provider-backed request counts and fresh corruption detection across repairs.
use shardline_index::{
    FileChunkRecord, FileRecord, MemoryIndexStore, MemoryRecordStore, RecordMutation,
    StorageRepresentation,
};
use shardline_storage::{AsyncObjectStore, ObjectBody, ObjectIntegrity, S3ObjectStoreConfig};
use shardline_test_support::{DockerLocalStack, S3FaultProxy, S3ProxyFault};

use super::{reachability::collect_referenced_object_keys, types::RepairReachability};
use crate::{ServerFrontend, ServerObjectStore};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Docker MinIO"]
async fn repair_shared_xorb_reads_once_per_scan_and_rechecks_corruption() {
    qualify_shared_xorb_repair(&[ServerFrontend::Xet]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Docker MinIO"]
async fn hub_only_repair_reuses_verified_sources_and_rechecks_corruption() {
    qualify_shared_xorb_repair(&[ServerFrontend::Hub]).await;
}

async fn qualify_shared_xorb_repair(frontends: &[ServerFrontend]) {
    let stack = tokio::task::spawn_blocking(|| {
        DockerLocalStack::builder()
            .with_minio()
            .start()
            .unwrap()
            .expect("repair qualification requires MinIO")
    })
    .await
    .unwrap();
    let raw = stack.s3_raw_config(Some("repair-shared-source")).unwrap();
    let proxy = S3FaultProxy::start_disarmed(
        raw.endpoint.as_ref().unwrap(),
        S3ProxyFault::ServiceUnavailableOnce,
    )
    .unwrap();
    let store = ServerObjectStore::s3(
        S3ObjectStoreConfig::new(raw.bucket, raw.region)
            .with_endpoint(Some(proxy.endpoint().to_owned()))
            .with_allow_http(true)
            .with_key_prefix(raw.key_prefix.as_deref())
            .with_credentials(
                raw.access_key.map(shardline_protocol::SecretString::new),
                raw.secret_key.map(shardline_protocol::SecretString::new),
                None,
            ),
    )
    .unwrap();
    let (bytes, hash) = crate::test_fixtures::single_chunk_xorb(&vec![b'a'; 128 * 1024]);
    let key = crate::xet_adapter::xorb_object_key(&hash).unwrap();
    let integrity = ObjectIntegrity::new(
        crate::local_backend::chunk_hash(&bytes),
        u64::try_from(bytes.len()).unwrap(),
    );
    AsyncObjectStore::put_if_absent(
        &store,
        &key,
        ObjectBody::from_bytes(bytes.clone()),
        &integrity,
    )
    .await
    .unwrap();
    let descriptor = crate::xet_adapter::validate_serialized_xorb(
        &mut std::io::Cursor::new(&bytes),
        shardline_index::parse_xet_hash_hex(&hash).unwrap(),
    )
    .unwrap();
    let records = MemoryRecordStore::new();
    for index in 0..4 {
        let record = FileRecord {
            file_id: format!("shared-source-{index}"),
            content_hash: hash.clone(),
            total_bytes: 128 * 1024,
            chunk_size: 64 * 1024,
            storage_repr: StorageRepresentation::XorbCdcV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: hash.clone(),
                offset: 0,
                length: 128 * 1024,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: descriptor.packed_content_length(),
            }],
        };
        records.write_latest_record(&record).await.unwrap();
        records.write_version_record(&record).await.unwrap();
    }
    let index = MemoryIndexStore::new();
    for _ in 0..2 {
        let before = proxy.xorb_get_requests();
        let mut reachability = RepairReachability::default();
        collect_referenced_object_keys(&records, &index, &store, frontends, &mut reachability)
            .await
            .unwrap();
        assert_eq!(reachability.scanned_records, 8);
        assert_eq!(proxy.xorb_get_requests().checked_sub(before).unwrap(), 1);
        assert!(reachability.referenced_object_keys.contains(key.as_str()));
        let member = shardline_index::xet_hash_hex_string(crate::local_backend::chunk_hash(&vec![
                b'a';
                128 * 1024
            ]));
        assert!(reachability.live_dedupe_chunk_hashes.contains(&member));
    }
    AsyncObjectStore::delete_if_present(&store, &key)
        .await
        .unwrap();
    let mut corrupt = bytes.to_vec();
    corrupt[16] ^= 1;
    let integrity = ObjectIntegrity::new(
        crate::local_backend::chunk_hash(&corrupt),
        u64::try_from(corrupt.len()).unwrap(),
    );
    AsyncObjectStore::put_if_absent(
        &store,
        &key,
        ObjectBody::from_bytes(corrupt.into()),
        &integrity,
    )
    .await
    .unwrap();
    let before = proxy.xorb_get_requests();
    assert!(
        collect_referenced_object_keys(
            &records,
            &index,
            &store,
            frontends,
            &mut RepairReachability::default()
        )
        .await
        .is_err()
    );
    assert_eq!(proxy.xorb_get_requests().checked_sub(before).unwrap(), 1);
    // Legacy raw records can repeat a chunk whose optional Xorb is absent.
    // Preserve one materialization probe per record, rather than per member.
    AsyncObjectStore::delete_if_present(&store, &key)
        .await
        .unwrap();
    let legacy = MemoryRecordStore::new();
    let mut record = FileRecord {
        file_id: "repeated-legacy-raw".to_owned(),
        content_hash: hash.clone(),
        total_bytes: 256 * 1024,
        chunk_size: 64 * 1024,
        storage_repr: StorageRepresentation::XorbCdcV1,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash,
            offset: 0,
            length: 128 * 1024,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: descriptor.packed_content_length(),
        }],
    };
    let mut repeated = record.chunks[0].clone();
    repeated.offset = 128 * 1024;
    record.chunks.push(repeated);
    legacy.write_latest_record(&record).await.unwrap();
    legacy.write_version_record(&record).await.unwrap();
    let before = proxy.head_requests();
    let before_gets = proxy.xorb_get_requests();
    let mut reachability = RepairReachability::default();
    collect_referenced_object_keys(&legacy, &index, &store, frontends, &mut reachability)
        .await
        .unwrap();
    assert_eq!(reachability.scanned_records, 2);
    assert_eq!(proxy.head_requests().checked_sub(before).unwrap(), 4);
    assert_eq!(
        proxy.xorb_get_requests().checked_sub(before_gets).unwrap(),
        2
    );
}
