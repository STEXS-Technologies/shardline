#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Count provider requests while retaining complete native source verification.

use std::{io::Cursor, sync::Arc};

mod native_head;

use sha2::{Digest, Sha256};
use shardline_protocol::{RepositoryProvider, RepositoryScope, SecretString};
use shardline_server_core::{DEFAULT_SHARD_METADATA_LIMITS, ServerObjectStore};
use shardline_storage::{ObjectPrefix, ObjectStore, S3ObjectStoreConfig};
use shardline_test_support::{DockerLocalStack, S3FaultProxy, S3ProxyFault};
use shardline_xet_adapter::{prepare_uploaded_shard, store_uploaded_xorb};
use shardline_xet_core::{
    merklehash::{MerkleHash, compute_data_hash, file_hash, xorb_hash},
    metadata_shard::{
        file_structs::{
            FileDataSequenceEntry, FileDataSequenceHeader, FileMetadataExt, MDBFileInfo,
        },
        shard_in_memory::MDBInMemoryShard,
        xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
    },
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

fn container(data: &[u8]) -> (MerkleHash, Vec<u8>, MDBXorbInfo) {
    let member = compute_data_hash(data);
    let length = u64::try_from(data.len()).unwrap();
    let hash = xorb_hash(&[(member, length)]);
    let serialized = serialized_xorb_object_from_components(
        &hash,
        data.to_vec(),
        vec![(member, length)],
        CompressionScheme::None,
    )
    .unwrap();
    (
        hash,
        serialized.serialized_data,
        MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(hash, 1, length),
            chunks: vec![XorbChunkSequenceEntry::new(member, length, 0)],
        },
    )
}

#[test]
#[ignore = "requires disposable Docker MinIO; counts real provider GETs"]
fn native_shard_preparation_reuses_each_verified_provider_source() {
    let services = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
        .expect("Docker is required");
    let raw = services.s3_raw_config(None).unwrap();
    let upstream = raw.endpoint.as_deref().unwrap();
    let proxy =
        S3FaultProxy::start_disarmed(upstream, S3ProxyFault::ServiceUnavailableOnce).unwrap();
    let make_config = |endpoint: &str| {
        S3ObjectStoreConfig::new(raw.bucket.clone(), raw.region.clone())
            .with_endpoint(Some(endpoint.to_owned()))
            .with_key_prefix(Some(&services.unique_s3_key_prefix("shard-source-order")))
            .with_credentials(
                raw.access_key.clone().map(SecretString::new),
                raw.secret_key.clone().map(SecretString::new),
                None,
            )
            .with_allow_http(true)
    };
    // Keep the exact prefix for the healthy and corrupting proxy clients.
    let config = make_config(proxy.endpoint());
    let objects = ServerObjectStore::s3(config.clone()).unwrap();
    let left = vec![b'L'; 32 * 1024];
    let right = vec![b'R'; 32 * 1024];
    let (left_hash, left_bytes, left_info) = container(&left);
    let (right_hash, right_bytes, right_info) = container(&right);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(store_uploaded_xorb(&objects, &left_hash.hex(), &left_bytes))
        .unwrap();
    runtime
        .block_on(store_uploaded_xorb(
            &objects,
            &right_hash.hex(),
            &right_bytes,
        ))
        .unwrap();
    let left_leaf = compute_data_hash(&left);
    let right_leaf = compute_data_hash(&right);
    let data = [left, right].concat();
    let sha256 = format!("{:x}", Sha256::digest(&data));
    let mut shard = MDBInMemoryShard::default();
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(
                file_hash(&[(left_leaf, 32 * 1024), (right_leaf, 32 * 1024)]),
                2,
                false,
                true,
            ),
            segments: vec![
                FileDataSequenceEntry::new(left_hash, 32 * 1024, 0, 1),
                FileDataSequenceEntry::new(right_hash, 32 * 1024, 0, 1),
            ],
            verification: Vec::new(),
            metadata_ext: Some(FileMetadataExt::new(MerkleHash::from_hex(&sha256).unwrap())),
        })
        .unwrap();
    shard.add_xorb_block(left_info).unwrap();
    shard.add_xorb_block(right_info).unwrap();
    let bytes = shard.to_bytes().unwrap();
    let scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "source-order", None).unwrap();
    let before = proxy.get_requests();
    let before_heads = proxy.head_requests();
    let prepared = prepare_uploaded_shard(
        &mut Cursor::new(&bytes),
        &objects,
        Some(&scope),
        DEFAULT_SHARD_METADATA_LIMITS,
    )
    .unwrap();
    assert_eq!(
        proxy.get_requests().checked_sub(before),
        Some(2),
        "two Xorbs need exactly two provider GETs"
    );
    let response = runtime
        .block_on(
            prepared.register(|records, _mappings, _references, identities, descriptors| {
                assert_eq!(records.len(), 1);
                assert_eq!(records.first().unwrap().total_bytes, 64 * 1024);
                assert_eq!(identities.first().unwrap().sha256, sha256);
                assert_eq!(descriptors.len(), 2);
                async { Ok(()) }
            }),
        )
        .unwrap();
    assert_eq!(
        response.result, 1,
        "first shard publication inserts content"
    );
    assert_eq!(
        proxy.head_requests(),
        before_heads,
        "conditional shard creation needs no preceding HEAD"
    );
    assert_eq!(
        proxy.get_requests().checked_sub(before),
        Some(2),
        "new shard publication retains exactly the two source GETs"
    );
    let before_replay = proxy.get_requests();
    let replay = prepare_uploaded_shard(
        &mut Cursor::new(&bytes),
        &objects,
        Some(&scope),
        DEFAULT_SHARD_METADATA_LIMITS,
    )
    .unwrap();
    let replay_response = runtime
        .block_on(replay.register(|_, _, _, _, _| async { Ok(()) }))
        .unwrap();
    assert_eq!(
        replay_response.result, 0,
        "verified duplicate retains its wire result"
    );
    assert_eq!(
        proxy.head_requests(),
        before_heads,
        "duplicate shard verification needs no preceding HEAD"
    );
    assert_eq!(
        proxy.get_requests().checked_sub(before_replay),
        Some(3),
        "replay still verifies both source Xorbs and existing shard bytes"
    );
    assert!(!proxy.fault_was_injected());

    // A persisted shard is not accepted on metadata alone: corrupt duplicate
    // bytes must still reject registration before the publication callback.
    let shards = objects
        .list_prefix(&ObjectPrefix::parse("shards/").unwrap())
        .unwrap();
    assert_eq!(shards.len(), 1);
    let length = usize::try_from(shards.first().unwrap().length()).unwrap();
    let duplicate_fault = S3FaultProxy::start_disarmed(
        upstream,
        S3ProxyFault::ReplaceSuccessfulGetBody(Arc::from(vec![0_u8; length])),
    )
    .unwrap();
    let damaged_shard = ServerObjectStore::s3(
        config
            .clone()
            .with_endpoint(Some(duplicate_fault.endpoint().to_owned())),
    )
    .unwrap();
    let corrupt_duplicate = prepare_uploaded_shard(
        &mut Cursor::new(&bytes),
        &damaged_shard,
        Some(&scope),
        DEFAULT_SHARD_METADATA_LIMITS,
    )
    .unwrap();
    duplicate_fault.arm();
    let published = std::sync::atomic::AtomicBool::new(false);
    let result = runtime.block_on(corrupt_duplicate.register(|_, _, _, _, _| {
        published.store(true, std::sync::atomic::Ordering::Release);
        async { Ok(()) }
    }));
    assert!(
        result.is_err(),
        "corrupt existing shard must reject registration"
    );
    assert!(duplicate_fault.fault_was_injected());
    assert!(!published.load(std::sync::atomic::Ordering::Acquire));

    let mut corrupt = left_bytes;
    let last = corrupt.last_mut().unwrap();
    *last ^= 0x80;
    let fault = S3FaultProxy::start(
        upstream,
        S3ProxyFault::ReplaceSuccessfulGetBody(Arc::from(corrupt)),
    )
    .unwrap();
    let damaged =
        ServerObjectStore::s3(config.with_endpoint(Some(fault.endpoint().to_owned()))).unwrap();
    assert!(
        prepare_uploaded_shard(
            &mut Cursor::new(&bytes),
            &damaged,
            Some(&scope),
            DEFAULT_SHARD_METADATA_LIMITS
        )
        .is_err()
    );
    assert!(
        fault.fault_was_injected(),
        "the source corruption must be exercised"
    );
}
