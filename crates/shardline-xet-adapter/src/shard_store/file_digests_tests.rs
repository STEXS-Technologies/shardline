#![allow(clippy::unwrap_used)]

use super::*;
use sha2::{Digest, Sha256};
use shardline_protocol::RepositoryProvider;
use shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS;
use shardline_xet_core::{
    merklehash::{file_hash, xorb_hash},
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

fn store_members(
    objects: &ServerObjectStore,
    members: &[&[u8]],
) -> (MerkleHash, Vec<(MerkleHash, u64)>) {
    let leaves: Vec<_> = members
        .iter()
        .map(|data| (compute_data_hash(data), data.len() as u64))
        .collect();
    let hash = xorb_hash(&leaves);
    let mut end = 0_u64;
    let boundaries = leaves
        .iter()
        .map(|(hash, length)| {
            end = end.checked_add(*length).unwrap();
            (*hash, end)
        })
        .collect();
    let serialized = serialized_xorb_object_from_components(
        &hash,
        members.concat(),
        boundaries,
        CompressionScheme::LZ4,
    )
    .unwrap();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(crate::store_uploaded_xorb(
            objects,
            &hash.hex(),
            &serialized.serialized_data,
        ))
        .unwrap();
    (hash, leaves)
}

#[test]
fn cancelled_digest_stops_between_verified_members_and_can_retry() {
    use std::cell::Cell;
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let members = [b"first".as_slice(), b"second".as_slice()];
    let (hash, leaves) = store_members(&objects, &members);
    let file = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(file_hash(&leaves), 1, false, true),
        segments: vec![FileDataSequenceEntry::new(hash, 11, 0, 2)],
        verification: Vec::new(),
        metadata_ext: None,
    };
    let mut verified = HashMap::new();
    verified.insert(
        hash.hex(),
        load_xorb_range_info(&objects, &hash.hex(), None).unwrap(),
    );
    let digest = format!("{:x}", Sha256::digest(members.concat()));
    let checks = Cell::new(0_usize);
    let mut verifier = file_digests::FileDigestVerifier::default();
    let error = verifier
        .verify(&objects, &file, Some(&digest), false, &verified, &|| {
            let seen = checks.get();
            checks.set(seen.checked_add(1).unwrap());
            if seen == 2 {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "cancelled",
                ))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert!(matches!(error, XetAdapterError::Io(ref error)
        if error.kind() == std::io::ErrorKind::Interrupted));
    assert_eq!(checks.get(), 3);
    verifier
        .verify(&objects, &file, Some(&digest), false, &verified, &|| Ok(()))
        .unwrap();
}

#[test]
fn cancelled_source_validation_stops_inside_container_and_can_retry() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let (hash, _) = store_members(&objects, &[b"first", b"second"]);
    let checks = AtomicUsize::new(0);
    let result = load_xorb_range_info_with_checkpoint(&objects, &hash.hex(), None, &|| {
        let count = checks.fetch_add(1, Ordering::SeqCst);
        if count >= 5 {
            Err(std::io::Error::from(std::io::ErrorKind::Interrupted))
        } else {
            Ok(())
        }
    });
    assert!(matches!(result, Err(XetAdapterError::Io(ref error))
        if error.kind() == std::io::ErrorKind::Interrupted));
    assert!(
        checks.load(Ordering::SeqCst) > 5,
        "validation must observe cancellation inside I/O"
    );
    let (verified, _) =
        load_xorb_range_info_with_checkpoint(&objects, &hash.hex(), None, &|| Ok(())).unwrap();
    assert_eq!(verified.verified_chunks.len(), 2);
}

#[test]
fn nonempty_extended_sha256_must_match_selected_members_in_file_order() {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let repository =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main")).unwrap();
    let members = [b"excluded".as_slice(), b"first", b"second"];
    let (hash, leaves) = store_members(&objects, &members);

    // Reversed, repeated, and discontiguous terms must hash logical file order,
    // not container order, the entire Xorb, or merely one selected term.
    let selected = [2_usize, 1, 2];
    let data: Vec<_> = selected
        .iter()
        .flat_map(|index| members[*index])
        .copied()
        .collect();
    let digest = format!("{:x}", Sha256::digest(&data));
    for supplied in [
        Some(digest.as_str()),
        Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        None,
    ] {
        let mut shard = MDBInMemoryShard::default();
        shard
            .add_file_reconstruction_info(MDBFileInfo {
                metadata: FileDataSequenceHeader::new(
                    file_hash(&selected.map(|index| leaves[index])),
                    3,
                    false,
                    supplied.is_some(),
                ),
                segments: selected
                    .map(|index| {
                        FileDataSequenceEntry::new(
                            hash,
                            members[index].len() as u64,
                            index as u64,
                            index as u64 + 1,
                        )
                    })
                    .to_vec(),
                verification: Vec::new(),
                metadata_ext: supplied
                    .map(|value| FileMetadataExt::new(MerkleHash::from_hex(value).unwrap())),
            })
            .unwrap();
        let bytes = shard.to_bytes().unwrap();
        for scope in [None, Some(&repository)] {
            let parsed =
                parse_uploaded_shard(&objects, &bytes, scope, DEFAULT_SHARD_METADATA_LIMITS);
            if supplied.is_none() || supplied == Some(digest.as_str()) {
                let parsed = parsed.unwrap();
                assert_eq!(parsed.records[0].total_bytes, data.len() as u64);
                assert_eq!(parsed.file_identities.len(), usize::from(scope.is_some()));
                if let Some(identity) = parsed.file_identities.first() {
                    assert_eq!(identity.sha256, digest);
                    assert_eq!(identity.total_bytes, data.len() as u64);
                    assert_eq!(identity.repository_scope, repository);
                }
            } else {
                assert!(
                    matches!(
                        parsed,
                        Err(XetAdapterError::InvalidSerializedShard(
                            InvalidSerializedShardError::ParserRejectedMetadata
                        ))
                    ),
                    "forged SHA-256 must not publish an identity for valid Merkle content"
                );
            }
        }
    }
}

#[test]
fn digest_verification_revisits_evicted_sources_in_logical_file_order() {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let (left, left_members) = store_members(&objects, &[b"left", b"right"]);
    let (middle, middle_members) = store_members(&objects, &[b"middle"]);
    let leaves = [left_members[1], middle_members[0], left_members[0]];
    let expected_file = file_hash(&leaves);
    let digest = format!("{:x}", Sha256::digest(b"rightmiddleleft"));
    let mut shard = MDBInMemoryShard::default();
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(expected_file, 3, false, true),
            segments: vec![
                FileDataSequenceEntry::new(left, 5, 1, 2),
                FileDataSequenceEntry::new(middle, 6, 0, 1),
                FileDataSequenceEntry::new(left, 4, 0, 1),
            ],
            verification: Vec::new(),
            metadata_ext: Some(FileMetadataExt::new(MerkleHash::from_hex(&digest).unwrap())),
        })
        .unwrap();
    let repository =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None).unwrap();
    let parsed = parse_uploaded_shard(
        &objects,
        &shard.to_bytes().unwrap(),
        Some(&repository),
        DEFAULT_SHARD_METADATA_LIMITS,
    )
    .unwrap();
    assert_eq!(parsed.records[0].file_id, expected_file.hex());
    assert_eq!(parsed.file_identities[0].sha256, digest);
    assert_eq!(parsed.file_identities[0].total_bytes, 15);
}

#[test]
fn source_validation_and_file_digest_share_materializations() {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let (left, left_members) = store_members(&objects, &[b"left"]);
    let (right, right_members) = store_members(&objects, &[b"right"]);
    let repository =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None).unwrap();
    for (terms, leaves, bytes, expected_reads) in [
        (
            vec![
                FileDataSequenceEntry::new(left, 4, 0, 1),
                FileDataSequenceEntry::new(right, 5, 0, 1),
            ],
            vec![left_members[0], right_members[0]],
            b"leftright".as_slice(),
            2,
        ),
        (
            vec![
                FileDataSequenceEntry::new(left, 4, 0, 1),
                FileDataSequenceEntry::new(right, 5, 0, 1),
                FileDataSequenceEntry::new(left, 4, 0, 1),
            ],
            vec![left_members[0], right_members[0], left_members[0]],
            b"leftrightleft".as_slice(),
            3,
        ),
    ] {
        let file = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(
                file_hash(&leaves),
                terms.len() as u64,
                false,
                true,
            ),
            segments: terms,
            verification: Vec::new(),
            metadata_ext: Some(FileMetadataExt::new(
                MerkleHash::from_hex(&format!("{:x}", Sha256::digest(bytes))).unwrap(),
            )),
        };
        let mut verifier = file_digests::FileDigestVerifier::default();
        let (records, identities) = build_file_records_with_cache(
            vec![file],
            &objects,
            Some(&repository),
            None,
            &mut HashMap::new(),
            &mut verifier,
            &|| Ok(()),
        )
        .unwrap();
        assert_eq!(verifier.source_reads, expected_reads);
        assert_eq!(records[0].total_bytes, bytes.len() as u64);
        assert_eq!(identities[0].sha256, format!("{:x}", Sha256::digest(bytes)));
    }
}

#[test]
fn unreferenced_cas_blocks_still_require_verified_membership() {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let (selected, selected_members) = store_members(&objects, &[b"selected"]);
    let (extra, extra_members) = store_members(&objects, &[b"unreferenced"]);
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None).unwrap();
    for forged in [false, true] {
        let mut shard = MDBInMemoryShard::default();
        shard
            .add_file_reconstruction_info(MDBFileInfo {
                metadata: FileDataSequenceHeader::new(
                    file_hash(&selected_members),
                    1,
                    false,
                    false,
                ),
                segments: vec![FileDataSequenceEntry::new(selected, 8, 0, 1)],
                verification: Vec::new(),
                metadata_ext: None,
            })
            .unwrap();
        shard
            .add_xorb_block(MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(extra, 1, 12),
                chunks: vec![XorbChunkSequenceEntry::new(
                    if forged {
                        compute_data_hash(b"forged")
                    } else {
                        extra_members[0].0
                    },
                    12,
                    0,
                )],
            })
            .unwrap();
        let result = prepare_uploaded_shard(
            &mut Cursor::new(shard.to_bytes().unwrap()),
            &objects,
            Some(&scope),
            DEFAULT_SHARD_METADATA_LIMITS,
        );
        if forged {
            assert!(matches!(
                result,
                Err(XetAdapterError::InvalidSerializedShard(
                    InvalidSerializedShardError::ParserRejectedMetadata
                ))
            ));
        } else {
            assert_eq!(result.unwrap().descriptors.len(), 2);
        }
    }
}
