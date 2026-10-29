//! Native decoder admission and response backpressure under competing readers.
use std::{num::NonZeroUsize, time::Duration};

use futures_util::StreamExt;
use shardline_index::{FileChunkRecord, FileRecord, StorageRepresentation};
use shardline_storage::{ObjectBody, ObjectIntegrity};

use super::*;

async fn wait_for_capacity(pool: &crate::admission::BoundedPool, expected: u32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.available_permits() != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("native worker admission must settle");
}

#[tokio::test]
async fn slow_native_readers_bound_workers_and_cancelled_waiters_do_not_starve_retry() {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path()).unwrap();
    qualify_read_pressure(objects).await;
}

#[cfg(feature = "docker")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker MinIO; counts real provider reads"]
async fn adjacent_native_terms_reuse_one_source_and_eviction_rechecks_it() {
    let stack = tokio::task::spawn_blocking(|| {
        shardline_test_support::DockerLocalStack::builder()
            .with_minio()
            .start()
            .unwrap()
            .expect("native source qualification requires Docker")
    })
    .await
    .unwrap();
    let raw = stack.s3_raw_config(Some("adjacent-native-terms")).unwrap();
    let proxy = shardline_test_support::S3FaultProxy::start_disarmed(
        raw.endpoint.as_deref().unwrap(),
        shardline_test_support::S3ProxyFault::ServiceUnavailableOnce,
    )
    .unwrap();
    let config = shardline_storage::S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(Some(proxy.endpoint().to_owned()))
        .with_allow_http(true)
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_credentials(
            raw.access_key.map(shardline_protocol::SecretString::new),
            raw.secret_key.map(shardline_protocol::SecretString::new),
            None,
        );
    let objects = ServerObjectStore::s3(config).unwrap();
    let mut hashes = Vec::new();
    for byte in [42, 91] {
        let packed =
            crate::upload_ingest::xorb_packer::pack_chunks_into_xorb(&[(vec![byte; 65536], 0)])
                .unwrap();
        objects
            .put_overwrite(
                &xorb_object_key(&packed.xorb_hash_hex).unwrap(),
                ObjectBody::Borrowed(&packed.serialized),
                &ObjectIntegrity::new(
                    crate::local_backend::chunk_hash(&packed.serialized),
                    packed.serialized.len() as u64,
                ),
            )
            .unwrap();
        hashes.push((packed.xorb_hash_hex, packed.serialized));
    }
    let record = FileRecord {
        file_id: "a".repeat(64),
        content_hash: "c".repeat(64),
        total_bytes: 4 * 65536,
        chunk_size: 0,
        storage_repr: StorageRepresentation::WholeFileV1,
        repository_scope: None,
        chunks: [0, 0, 1, 0]
            .into_iter()
            .enumerate()
            .map(|(index, source)| FileChunkRecord {
                hash: hashes[source].0.clone(),
                offset: index as u64 * 65536,
                length: 65536,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: hashes[source].1.len() as u64,
            })
            .collect(),
    };
    let pool = crate::admission::BoundedPool::new(NonZeroUsize::new(1).unwrap());
    let before = proxy.get_requests();
    let heads = proxy.head_requests();
    let mut response =
        referenced_file_record_byte_stream(objects.clone(), record.clone(), pool.clone()).unwrap();
    let mut observed = Vec::new();
    while let Some(frame) = response.next().await {
        observed.extend_from_slice(&frame.unwrap());
    }
    assert_eq!(
        observed,
        [vec![42; 2 * 65536], vec![91; 65536], vec![42; 65536]].concat()
    );
    assert_eq!(
        proxy.get_requests() - before,
        3,
        "adjacent reuse, then a fresh fetch after eviction"
    );
    assert_eq!(proxy.head_requests(), heads);
    // No verified source crosses response boundaries. A later corruption must
    // fail before even the first member of a fresh response is exposed.
    let mut damaged = hashes[0].1.clone();
    damaged[0] ^= 1;
    objects
        .put_overwrite(
            &xorb_object_key(&hashes[0].0).unwrap(),
            ObjectBody::Borrowed(&damaged),
            &ObjectIntegrity::new(
                crate::local_backend::chunk_hash(&damaged),
                damaged.len() as u64,
            ),
        )
        .unwrap();
    let before = proxy.get_requests();
    let mut response = referenced_file_record_byte_stream(objects, record, pool.clone()).unwrap();
    assert!(response.next().await.unwrap().is_err());
    drop(response);
    wait_for_capacity(&pool, pool.capacity()).await;
    assert_eq!(proxy.get_requests() - before, 1);
}

#[cfg(feature = "docker")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker MinIO"]
async fn minio_slow_native_readers_release_capacity_after_cancellation() {
    let stack = tokio::task::spawn_blocking(|| {
        shardline_test_support::DockerLocalStack::builder()
            .with_minio()
            .start()
            .unwrap()
            .expect("native read pressure qualification requires Docker")
    })
    .await
    .unwrap();
    let raw = stack.s3_raw_config(Some("native-read-pressure")).unwrap();
    let config = shardline_storage::S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(raw.endpoint)
        .with_allow_http(raw.allow_http)
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_credentials(
            raw.access_key.map(shardline_protocol::SecretString::new),
            raw.secret_key.map(shardline_protocol::SecretString::new),
            raw.session_token.map(shardline_protocol::SecretString::new),
        );
    qualify_read_pressure(ServerObjectStore::s3(config).unwrap()).await;
}

async fn qualify_read_pressure(objects: ServerObjectStore) {
    let member = vec![42; 65536];
    let packed =
        crate::upload_ingest::xorb_packer::pack_chunks_into_xorb(&[(member.clone(), 0)]).unwrap();
    objects
        .put_overwrite(
            &xorb_object_key(&packed.xorb_hash_hex).unwrap(),
            ObjectBody::Borrowed(&packed.serialized),
            &ObjectIntegrity::new(
                crate::local_backend::chunk_hash(&packed.serialized),
                u64::try_from(packed.serialized.len()).unwrap(),
            ),
        )
        .unwrap();
    let record = FileRecord {
        file_id: "a".repeat(64),
        content_hash: "c".repeat(64),
        total_bytes: 262144,
        chunk_size: 0,
        storage_repr: StorageRepresentation::WholeFileV1,
        repository_scope: None,
        chunks: (0_u64..4)
            .map(|index| FileChunkRecord {
                hash: packed.xorb_hash_hex.clone(),
                offset: index.checked_mul(65536).unwrap(),
                length: 65536,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: u64::try_from(packed.serialized.len()).unwrap(),
            })
            .collect(),
    };
    let pool = crate::admission::BoundedPool::new(NonZeroUsize::new(2).unwrap());
    let mut first =
        referenced_file_record_byte_stream(objects.clone(), record.clone(), pool.clone()).unwrap();
    wait_for_capacity(&pool, 1).await;
    let second =
        referenced_file_record_byte_stream(objects.clone(), record.clone(), pool.clone()).unwrap();
    wait_for_capacity(&pool, 0).await;
    let queued: Vec<_> = (0..30)
        .map(|_| {
            referenced_file_record_byte_stream(objects.clone(), record.clone(), pool.clone())
                .unwrap()
        })
        .collect();
    tokio::task::yield_now().await;
    drop(queued);
    // The two unread responses still own their workers; cancelling queued
    // responses must not release capacity belonging to running decoders.
    assert_eq!(pool.available_permits(), 0);
    let expected = member.repeat(4);
    let mut actual = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(frame) = first.next().await {
            actual.extend_from_slice(&frame.unwrap());
        }
    })
    .await
    .expect("a consuming reader must progress despite cancelled competitors");
    assert_eq!(actual, expected);
    wait_for_capacity(&pool, 1).await;
    drop(second);
    wait_for_capacity(&pool, 2).await;
    let mut retry = referenced_file_record_byte_stream(objects, record, pool.clone()).unwrap();
    let mut actual = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(frame) = retry.next().await {
            actual.extend_from_slice(&frame.unwrap());
        }
    })
    .await
    .expect("verified retry must not be starved by cancelled waiters");
    assert_eq!(actual, expected);
    wait_for_capacity(&pool, 2).await;
}
