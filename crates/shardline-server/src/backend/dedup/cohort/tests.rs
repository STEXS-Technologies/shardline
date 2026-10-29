#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
use super::*;
use crate::{LocalBackend, ShardMetadataLimits, upload_ingest::RequestBodyReader};
use shardline_protocol::RepositoryProvider;
use std::num::NonZeroUsize;
use xet_core_structures::{
    merklehash::MerkleHash as WireHash, metadata_shard::shard_format::MDBShardInfo as WireShardInfo,
};

fn matches_member(bytes: &[u8], member: &[u8]) -> bool {
    let mut reader = std::io::Cursor::new(bytes);
    let shard = WireShardInfo::load_from_reader(&mut reader).unwrap();
    let hash = shardline_xet_core::merklehash::compute_data_hash(member);
    shard
        .chunk_hash_dedup_query(&mut reader, &[WireHash::from(<[u8; 32]>::from(hash))])
        .unwrap()
        .is_some()
}

#[tokio::test]
async fn related_container_hides_members_outside_authorized_file_terms() {
    use shardline_xet_core::{
        merklehash::{compute_data_hash, file_hash, xorb_hash},
        metadata_shard::{
            file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
            shard_format::MDBShardInfo,
            shard_in_memory::MDBInMemoryShard,
        },
        xorb_object::{
            CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
        },
    };
    let root = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(
        root.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(128).unwrap(),
    )
    .await
    .unwrap();
    let scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "alice", "partial", Some("main")).unwrap();
    let primary = b"primary authorized member";
    let public = b"authorized sibling member";
    let private = b"other tenant secret in shared container";
    let (primary_object, primary_hex) = crate::test_fixtures::single_chunk_xorb(primary);
    backend
        .upload_xorb(&primary_hex, primary_object)
        .await
        .unwrap();
    let primary_hash = shardline_xet_core::merklehash::MerkleHash::from_hex(&primary_hex).unwrap();
    let public_len = u64::try_from(public.len()).unwrap();
    let private_len = u64::try_from(private.len()).unwrap();
    let primary_len = u64::try_from(primary.len()).unwrap();
    let members = [
        (compute_data_hash(public), public_len),
        (compute_data_hash(private), private_len),
    ];
    let sibling = xorb_hash(&members);
    let object = serialized_xorb_object_from_components(
        &sibling,
        [public.as_slice(), private.as_slice()].concat(),
        vec![
            (members[0].0, public_len),
            (members[1].0, public_len + private_len),
        ],
        CompressionScheme::None,
    )
    .unwrap();
    backend
        .upload_xorb(&sibling.hex(), object.serialized_data.into())
        .await
        .unwrap();
    let mut shard = MDBInMemoryShard::default();
    shard
        .add_xorb_block(MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(primary_hash, 1, primary_len),
            chunks: vec![XorbChunkSequenceEntry::new(
                compute_data_hash(primary),
                primary_len,
                0,
            )],
        })
        .unwrap();
    shard
        .add_xorb_block(MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(sibling, 2, public_len + private_len),
            chunks: vec![
                XorbChunkSequenceEntry::new(members[0].0, public_len, 0),
                XorbChunkSequenceEntry::new(members[1].0, private_len, public_len),
            ],
        })
        .unwrap();
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(
                file_hash(&[(compute_data_hash(primary), primary_len), members[0]]),
                2,
                false,
                false,
            ),
            segments: vec![
                FileDataSequenceEntry::new(primary_hash, primary_len, 0, 1),
                FileDataSequenceEntry::new(sibling, public_len, 0, 1),
            ],
            verification: Vec::new(),
            metadata_ext: None,
        })
        .unwrap();
    let mut serialized = Vec::new();
    MDBShardInfo::serialize_from(&mut serialized, &shard, None).unwrap();
    backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(serialized.into()),
            Some(&scope),
            ShardMetadataLimits::default(),
        )
        .await
        .unwrap();
    let backend = ServerBackend::Local(backend);
    let reply = backend
        .scoped_dedup_shard(
            &compute_data_hash(primary).hex(),
            Some(&scope),
            ShardMetadataLimits::default(),
        )
        .await
        .unwrap();
    assert!(matches_member(&reply, primary));
    assert!(matches_member(&reply, public));
    assert!(!matches_member(&reply, private));
    assert!(matches!(
        backend
            .scoped_dedup_shard(
                &compute_data_hash(private).hex(),
                Some(&scope),
                ShardMetadataLimits::default(),
            )
            .await,
        Err(ServerError::NotFound)
    ));
}

#[tokio::test]
async fn related_reply_is_scoped_bounded_and_omits_missing_optional_sources() {
    let root = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(
        root.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(128).unwrap(),
    )
    .await
    .unwrap();
    let scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "alice", "model", Some("main")).unwrap();
    let denied =
        RepositoryScope::new(RepositoryProvider::GitHub, "bob", "model", Some("main")).unwrap();
    let mut hashes = Vec::new();
    for bytes in [
        b"first member".as_slice(),
        b"second member",
        b"unrelated private member",
    ] {
        let (object, hash) = crate::test_fixtures::single_chunk_xorb(bytes);
        backend.upload_xorb(&hash, object).await.unwrap();
        hashes.push(hash);
    }
    let (shard, _) = crate::test_fixtures::single_file_shard(&[
        (b"first member", &hashes[0]),
        (b"second member", &hashes[1]),
    ]);
    backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(shard),
            Some(&scope),
            ShardMetadataLimits::default(),
        )
        .await
        .unwrap();
    let (private, _) =
        crate::test_fixtures::single_file_shard(&[(b"unrelated private member", &hashes[2])]);
    backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(private),
            Some(&denied),
            ShardMetadataLimits::default(),
        )
        .await
        .unwrap();
    let primary = backend
        .record_store()
        .xet_xorb_descriptor(&hashes[0])
        .await
        .unwrap()
        .unwrap();
    let objects = backend.object_store();
    let backend = ServerBackend::Local(backend);
    let related = descriptors(
        &backend,
        &primary,
        Some(&scope),
        ShardMetadataLimits::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        related.iter().map(|d| &d.hash).collect::<Vec<_>>(),
        vec![&hashes[0], &hashes[1]]
    );
    let one = NonZeroUsize::new(1).unwrap();
    assert_eq!(
        descriptors(
            &backend,
            &primary,
            Some(&scope),
            ShardMetadataLimits::new(one, one, one, one)
        )
        .await
        .unwrap()
        .len(),
        1
    );
    assert_eq!(
        descriptors(
            &backend,
            &primary,
            Some(&denied),
            ShardMetadataLimits::default()
        )
        .await
        .unwrap()
        .len(),
        1
    );
    let member = shardline_xet_core::merklehash::compute_data_hash(b"first member").hex();
    let complete = backend
        .scoped_dedup_shard(&member, Some(&scope), ShardMetadataLimits::default())
        .await
        .unwrap();
    assert!(matches_member(&complete, b"first member"));
    assert!(matches_member(&complete, b"second member"));
    assert!(!matches_member(&complete, b"unrelated private member"));
    assert!(matches!(
        backend
            .scoped_dedup_shard(&member, Some(&denied), ShardMetadataLimits::default())
            .await,
        Err(ServerError::NotFound)
    ));
    AsyncObjectStore::delete_if_present(
        &objects,
        &crate::xet_adapter::xorb_object_key(&hashes[1]).unwrap(),
    )
    .await
    .unwrap();
    let reduced = backend
        .scoped_dedup_shard(&member, Some(&scope), ShardMetadataLimits::default())
        .await
        .unwrap();
    assert!(matches_member(&reduced, b"first member"));
    assert!(!matches_member(&reduced, b"second member"));
    assert!(
        reduced.len() < complete.len(),
        "missing sibling must not be advertised or hide the live primary"
    );
}
