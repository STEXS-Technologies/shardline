#![allow(clippy::unwrap_used, clippy::indexing_slicing)]
//! Legacy shard mappings are hints; missing sources must not prevent reuse.

use super::*;
use shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS;
use shardline_xet_core::{
    merklehash::xorb_hash,
    metadata_shard::{shard_format::MDBShardInfo, xorb_structs::XorbChunkSequenceEntry},
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

fn source(suffix: u8) -> (MerkleHash, Vec<u8>, MDBXorbInfo) {
    let shared = compute_data_hash(b"shared");
    let other = compute_data_hash(&[suffix]);
    let hash = xorb_hash(&[(shared, 6), (other, 1)]);
    let mut body = b"shared".to_vec();
    body.push(suffix);
    let encoded = serialized_xorb_object_from_components(
        &hash,
        body,
        vec![(shared, 6), (other, 7)],
        CompressionScheme::None,
    )
    .unwrap();
    (
        hash,
        encoded.serialized_data,
        MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(hash, 2, 7),
            chunks: vec![
                XorbChunkSequenceEntry::new(shared, 6, 0),
                XorbChunkSequenceEntry::new(other, 1, 6),
            ],
        },
    )
}

fn shard(sources: impl IntoIterator<Item = MDBXorbInfo>) -> Vec<u8> {
    let mut shard = MDBInMemoryShard::default();
    for source in sources {
        shard.add_xorb_block(source).unwrap();
    }
    let mut bytes = Vec::new();
    MDBShardInfo::serialize_from(&mut bytes, &shard, None).unwrap();
    bytes
}

#[test]
fn missing_legacy_dedup_sources_are_a_miss() {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let (_, _, info) = source(b'a');
    let recovered = recover_dedup_xorb_descriptor(
        &objects,
        &shard([info]),
        &compute_data_hash(b"shared").hex(),
        DEFAULT_SHARD_METADATA_LIMITS,
    );
    assert!(matches!(recovered, Ok(None)), "{recovered:?}");
}

#[test]
fn missing_legacy_dedup_source_does_not_hide_a_later_live_source() {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let mut sources = [source(b'a'), source(b'b')];
    sources.sort_by_key(|source| source.0);
    let live_hash = sources[1].0.hex();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(crate::store_uploaded_xorb(
            &objects,
            &live_hash,
            &sources[1].1,
        ))
        .unwrap();
    let recovered = recover_dedup_xorb_descriptor(
        &objects,
        &shard(sources.into_iter().map(|source| source.2)),
        &compute_data_hash(b"shared").hex(),
        DEFAULT_SHARD_METADATA_LIMITS,
    )
    .unwrap()
    .unwrap();
    assert_eq!(recovered.hash, live_hash);
}

#[test]
fn corrupt_legacy_dedup_source_is_not_silently_treated_as_missing() {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let (hash, _, info) = source(b'a');
    let bytes = b"corrupt";
    objects
        .put_overwrite(
            &xorb_object_key(&hash.hex()).unwrap(),
            ObjectBody::from_slice(bytes),
            &ObjectIntegrity::new(chunk_hash(bytes), bytes.len() as u64),
        )
        .unwrap();
    assert!(
        recover_dedup_xorb_descriptor(
            &objects,
            &shard([info]),
            &compute_data_hash(b"shared").hex(),
            DEFAULT_SHARD_METADATA_LIMITS,
        )
        .is_err()
    );
}
