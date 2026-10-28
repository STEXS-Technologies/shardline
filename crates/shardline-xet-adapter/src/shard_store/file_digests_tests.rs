#![allow(clippy::unwrap_used)]

use super::*;
mod ordered_terms;
use sha2::{Digest, Sha256};
use shardline_protocol::RepositoryProvider;
use shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS;
use shardline_xet_core::{
    merklehash::{file_hash, xorb_hash},
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

pub(super) fn store_members(
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
    use std::sync::atomic::{AtomicUsize, Ordering};
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
    let checks = AtomicUsize::new(0);
    let mut verifier = file_digests::FileDigestVerifier::default();
    let error = verifier
        .verify(&objects, &file, Some(&digest), false, &verified, &|| {
            if checks.fetch_add(1, Ordering::SeqCst) == 4 {
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
    assert_eq!(checks.load(Ordering::SeqCst), 5);
    verifier
        .verify(&objects, &file, Some(&digest), false, &verified, &|| Ok(()))
        .unwrap();
}

#[test]
fn cancelled_digest_reload_stops_s3_streaming_and_can_retry() {
    use shardline_protocol::SecretString;
    use shardline_storage::S3ObjectStoreConfig;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let data = vec![b'x'; 2 * 1024 * 1024];
    let leaf = compute_data_hash(&data);
    let hash = xorb_hash(&[(leaf, data.len() as u64)]);
    let serialized = serialized_xorb_object_from_components(
        &hash,
        data.clone(),
        vec![(leaf, data.len() as u64)],
        CompressionScheme::None,
    )
    .unwrap();
    let validated = validate_serialized_xorb(
        &mut std::io::Cursor::new(&serialized.serialized_data),
        parse_xet_hash_hex(&hash.hex()).unwrap(),
    )
    .unwrap();
    let info = XorbRangeInfo {
        total_length: validated.total_length(),
        verified_chunks: validated.chunks().to_vec(),
    };
    let file = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(
            file_hash(&[(leaf, data.len() as u64)]),
            1,
            false,
            true,
        ),
        segments: vec![FileDataSequenceEntry::new(hash, data.len() as u64, 0, 1)],
        verification: Vec::new(),
        metadata_ext: None,
    };
    let expected = format!("{:x}", Sha256::digest(&data));
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let body = serialized.serialized_data;
            let app = axum::Router::new().fallback(move |headers: axum::http::HeaderMap| {
                let body = body.clone();
                async move {
                    let range = headers.get("range").unwrap().to_str().unwrap();
                    let (start, end) = range
                        .strip_prefix("bytes=")
                        .unwrap()
                        .split_once('-')
                        .unwrap();
                    let start: usize = start.parse().unwrap();
                    let end: usize = end.parse().unwrap();
                    axum::http::Response::builder()
                        .status(206)
                        .header(
                            "content-range",
                            format!("bytes {start}-{end}/{}", body.len()),
                        )
                        .body(axum::body::Body::from(body[start..=end].to_vec()))
                        .unwrap()
                }
            });
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let objects = ServerObjectStore::s3(
                S3ObjectStoreConfig::new("fixture".to_owned(), "us-east-1".to_owned())
                    .with_endpoint(Some(endpoint))
                    .with_allow_http(true)
                    .with_credentials(
                        Some(SecretString::new("fixture".to_owned())),
                        Some(SecretString::new("fixture-secret".to_owned())),
                        None,
                    ),
            )
            .unwrap();
            tokio::task::spawn_blocking(move || {
                let mut verified = HashMap::new();
                verified.insert(hash.hex(), info);
                let checks = AtomicUsize::new(0);
                let mut verifier = file_digests::FileDigestVerifier::default();
                // Model an evicted source whose authenticated metadata remains cached.
                let error = verifier
                    .verify(&objects, &file, Some(&expected), false, &verified, &|| {
                        if checks.fetch_add(1, Ordering::SeqCst) >= 3 {
                            Err(std::io::ErrorKind::Interrupted.into())
                        } else {
                            Ok(())
                        }
                    })
                    .unwrap_err();
                assert!(matches!(error, XetAdapterError::Io(ref error)
                if error.kind() == std::io::ErrorKind::Interrupted));
                assert_eq!(
                    verifier.source_reads, 0,
                    "partial source must not enter the verified cache"
                );
                verifier
                    .verify(&objects, &file, Some(&expected), false, &verified, &|| {
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(verifier.source_reads, 1);
            })
            .await
            .unwrap();
            server.abort();
        });
}

#[test]
fn nonempty_preparation_cancellation_at_each_checkpoint_never_publishes() {
    use shardline_storage::ObjectPrefix;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let (hash, leaves) = store_members(&objects, &[b"first", b"second"]);
    let repeated: Vec<_> = leaves.iter().copied().cycle().take(128).collect();
    let mut shard = MDBInMemoryShard::default();
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash(&repeated), 64, false, false),
            segments: vec![FileDataSequenceEntry::new(hash, 11, 0, 2); 64],
            verification: Vec::new(),
            metadata_ext: None,
        })
        .unwrap();
    let bytes = shard.to_bytes().unwrap();
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None).unwrap();
    let checks = AtomicUsize::new(0);
    drop(
        prepare_uploaded_shard_with_checkpoint(
            &mut Cursor::new(&bytes),
            &objects,
            Some(&scope),
            DEFAULT_SHARD_METADATA_LIMITS,
            &|| {
                checks.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .unwrap(),
    );
    let total = checks.load(Ordering::SeqCst);
    assert!(
        total > 128,
        "fixture must exercise repeated member processing"
    );
    for cut in 0..total {
        checks.store(0, Ordering::SeqCst);
        assert!(
            prepare_uploaded_shard_with_checkpoint(
                &mut Cursor::new(&bytes),
                &objects,
                Some(&scope),
                DEFAULT_SHARD_METADATA_LIMITS,
                &|| {
                    if checks.fetch_add(1, Ordering::SeqCst) >= cut {
                        Err(std::io::ErrorKind::Interrupted.into())
                    } else {
                        Ok(())
                    }
                },
            )
            .is_err(),
            "cancelled preparation at checkpoint {cut} returned publishable state"
        );
    }
    let mut stored = 0;
    objects
        .visit_prefix(&ObjectPrefix::parse("shards/").unwrap(), |_| {
            stored += 1;
            Ok::<(), XetAdapterError>(())
        })
        .unwrap();
    assert_eq!(stored, 0);
    let prepared = prepare_uploaded_shard(
        &mut Cursor::new(&bytes),
        &objects,
        Some(&scope),
        DEFAULT_SHARD_METADATA_LIMITS,
    )
    .unwrap();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(
            prepared.register(|records, _, _, identities, _| async move {
                assert_eq!(records.len(), 1);
                assert_eq!(identities.len(), 1);
                assert_eq!(records[0].total_bytes, 704);
                Ok(())
            }),
        )
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
        // Fresh sources feed their checked bytes directly into the digest;
        // returning to an evicted source still reauthenticates its members.
        assert_eq!(verifier.member_rechecks, expected_reads - 2);
        assert_eq!(records[0].total_bytes, bytes.len() as u64);
        assert_eq!(identities[0].sha256, format!("{:x}", Sha256::digest(bytes)));
    }
}

#[test]
fn provisional_digest_cannot_accept_a_later_corrupt_member() {
    let members = [b"selected".as_slice(), b"unselected"];
    let leaves: Vec<_> = members
        .iter()
        .map(|data| (compute_data_hash(data), data.len() as u64))
        .collect();
    let hash = xorb_hash(&leaves);
    let mut end = 0;
    let boundaries = leaves
        .iter()
        .map(|(hash, length)| {
            end += length;
            (*hash, end)
        })
        .collect();
    let mut bytes = serialized_xorb_object_from_components(
        &hash,
        members.concat(),
        boundaries,
        CompressionScheme::None,
    )
    .unwrap()
    .serialized_data;
    let verified = validate_serialized_xorb(
        &mut Cursor::new(&bytes),
        parse_xet_hash_hex(&hash.hex()).unwrap(),
    )
    .unwrap();
    let corrupt = usize::try_from(verified.chunks()[1].packed_end() - 1).unwrap();
    bytes[corrupt] ^= 1;
    let mut visited = Vec::new();
    let result = crate::xorb::validate_serialized_xorb_with_data_and_visitor(
        &mut Cursor::new(&bytes),
        parse_xet_hash_hex(&hash.hex()).unwrap(),
        0,
        Vec::new(),
        &mut |member, data| {
            visited.push((member, data.to_vec()));
            Ok(())
        },
    );
    assert!(
        result.is_err(),
        "a provisional digest cannot authorize corrupt source bytes"
    );
    assert_eq!(visited, vec![(0, members[0].to_vec())]);
}

#[test]
fn cached_source_metadata_does_not_hide_later_payload_corruption() {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let (hash, leaves) = store_members(&objects, &[b"first", b"second"]);
    let (info, source) = load_xorb_range_info_with_source(&objects, &hash.hex(), None).unwrap();
    let mut cache = HashMap::from([(hash.hex(), info)]);
    let mut verifier = file_digests::FileDigestVerifier::default();
    verifier.retain_verified_source(&hash.hex(), source);
    let key = xorb_object_key(&hash.hex()).unwrap();
    let path = objects.local_root().unwrap().join(key.as_str());
    assert!(path.is_file());
    std::fs::write(path, b"corrupt source after metadata validation").unwrap();
    let file = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(file_hash(&leaves), 1, false, true),
        segments: vec![FileDataSequenceEntry::new(hash, 11, 0, 2)],
        verification: Vec::new(),
        metadata_ext: Some(FileMetadataExt::new(
            MerkleHash::from_hex(&format!("{:x}", Sha256::digest(b"firstsecond"))).unwrap(),
        )),
    };
    assert!(
        build_file_records_with_cache(
            vec![file],
            &objects,
            None,
            None,
            &mut cache,
            &mut verifier,
            &|| Ok(()),
        )
        .is_err(),
        "cached metadata must not replace current member verification"
    );
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
