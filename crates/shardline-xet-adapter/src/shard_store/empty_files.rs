#![allow(clippy::unwrap_used)]

use super::*;
use shardline_protocol::RepositoryProvider;
use shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS;

#[test]
fn empty_shard_file_requires_the_actual_empty_sha256_when_extended_metadata_is_present() {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let repository =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main")).unwrap();
    for (sha256, valid) in [
        (
            Some("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            true,
        ),
        (
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            false,
        ),
        (None, true),
    ] {
        let mut shard = MDBInMemoryShard::default();
        let file_hash = shardline_xet_core::merklehash::file_hash(&[]);
        shard
            .add_file_reconstruction_info(MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, 0, false, sha256.is_some()),
                segments: Vec::new(),
                verification: Vec::new(),
                metadata_ext: sha256
                    .map(|digest| FileMetadataExt::new(MerkleHash::from_hex(digest).unwrap())),
            })
            .unwrap();
        let bytes = shard.to_bytes().unwrap();
        for scope in [None, Some(&repository)] {
            let parsed =
                parse_uploaded_shard(&objects, &bytes, scope, DEFAULT_SHARD_METADATA_LIMITS);
            if valid {
                let parsed = parsed.unwrap();
                assert_eq!(parsed.records.len(), 1);
                assert_eq!(parsed.records[0].total_bytes, 0);
                assert!(parsed.records[0].chunks.is_empty());
                assert_eq!(parsed.file_identities.len(), usize::from(scope.is_some()));
            } else {
                assert!(
                    matches!(
                        parsed,
                        Err(XetAdapterError::InvalidSerializedShard(
                            InvalidSerializedShardError::ParserRejectedMetadata
                        ))
                    ),
                    "a non-empty SHA-256 must not become an empty native identity"
                );
            }
        }
    }
}
