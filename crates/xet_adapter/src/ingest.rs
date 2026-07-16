use std::future::Future;

use shardline_index::{DedupeShardMapping, FileRecord};
use shardline_protocol::RepositoryScope;
use shardline_server_core::{ServerObjectStore, ShardMetadataLimits};

use crate::{
    error::XetAdapterError,
    model::{ShardUploadResponse, XorbUploadResponse},
};

use super::{dedupe_shard_mapping, parse_uploaded_shard, store_uploaded_xorb};

/// # Errors
///
/// Returns an error when the xorb upload fails validation or storage.
pub fn store_uploaded_xorb_bytes(
    object_store: &ServerObjectStore,
    expected_hash: &str,
    uploaded_body: &[u8],
) -> Result<XorbUploadResponse, XetAdapterError> {
    let stored = store_uploaded_xorb(object_store, expected_hash, uploaded_body)?;

    Ok(XorbUploadResponse {
        was_inserted: stored.was_inserted,
    })
}

/// # Errors
///
/// Returns an error when shard parsing or commit fails.
pub async fn register_uploaded_shard_bytes<Commit, CommitFuture>(
    object_store: &ServerObjectStore,
    uploaded_body: &[u8],
    repository_scope: Option<&RepositoryScope>,
    shard_metadata_limits: ShardMetadataLimits,
    commit_metadata: Commit,
) -> Result<ShardUploadResponse, XetAdapterError>
where
    Commit: FnOnce(Vec<FileRecord>, Vec<DedupeShardMapping>) -> CommitFuture,
    CommitFuture: Future<Output = Result<(), XetAdapterError>>,
{
    let parsed = parse_uploaded_shard(
        object_store,
        uploaded_body,
        repository_scope,
        shard_metadata_limits,
    )?;
    let mappings = parsed
        .dedupe_chunk_hashes
        .iter()
        .map(|chunk_hash_hex| dedupe_shard_mapping(chunk_hash_hex, &parsed.shard_key))
        .collect::<Result<Vec<_>, _>>()?;
    commit_metadata(parsed.records, mappings).await?;

    Ok(ShardUploadResponse {
        result: parsed.result,
    })
}

#[cfg(test)]
mod tests {
    use shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS;
    use shardline_xet_core::{
        merklehash::{compute_data_hash, file_hash, xorb_hash},
        metadata_shard::{
            file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
            shard_format::MDBShardInfo,
            shard_in_memory::MDBInMemoryShard,
            xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
        },
        xorb_object::{
            CompressionScheme, SerializedXorbObject,
            xorb_format_test_utils::{ChunkSize, build_raw_xorb},
        },
    };

    use super::*;
    use crate::error::XetAdapterError;

    // ---- helpers ----

    fn serialize_test_shard(file_infos: Vec<MDBFileInfo>, xorb_infos: Vec<MDBXorbInfo>) -> Vec<u8> {
        let mut shard = MDBInMemoryShard::default();
        for file_info in file_infos {
            assert!(shard.add_file_reconstruction_info(file_info).is_ok());
        }
        for xorb_info in xorb_infos {
            assert!(shard.add_xorb_block(xorb_info).is_ok());
        }
        let mut serialized = Vec::new();
        assert!(MDBShardInfo::serialize_from(&mut serialized, &shard, None).is_ok());
        serialized
    }

    // ---- store_uploaded_xorb_bytes ----

    #[test]
    fn store_uploaded_xorb_bytes_stores_and_returns_response() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(2, ChunkSize::Fixed(512));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        let result = store_uploaded_xorb_bytes(&object_store, &hash, &serialized.serialized_data);
        assert!(
            result.is_ok(),
            "store_uploaded_xorb_bytes failed: {result:?}"
        );
        let response = result.unwrap();
        assert!(response.was_inserted);
    }

    #[test]
    fn store_uploaded_xorb_bytes_rejects_wrong_hash() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let wrong_hash = "00".repeat(32);

        let result =
            store_uploaded_xorb_bytes(&object_store, &wrong_hash, &serialized.serialized_data);
        assert!(result.is_err(), "expected error for wrong hash");
    }

    #[test]
    fn store_uploaded_xorb_bytes_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        let first = store_uploaded_xorb_bytes(&object_store, &hash, &serialized.serialized_data);
        assert!(first.is_ok());
        assert!(first.unwrap().was_inserted);

        let second = store_uploaded_xorb_bytes(&object_store, &hash, &serialized.serialized_data);
        assert!(second.is_ok());
        assert!(
            !second.unwrap().was_inserted,
            "second insert should report was_inserted=false"
        );
    }

    #[test]
    fn store_uploaded_xorb_bytes_rejects_invalid_hash_format() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let result = store_uploaded_xorb_bytes(&object_store, "not-a-hash", b"data");
        assert!(result.is_err(), "expected error for invalid hash format");
    }

    #[test]
    fn store_uploaded_xorb_bytes_rejects_empty_body_with_valid_hash() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let hash = "ab".repeat(32);

        let result = store_uploaded_xorb_bytes(&object_store, &hash, b"");
        assert!(
            result.is_err(),
            "expected error for empty body with valid hash"
        );
    }

    // ---- register_uploaded_shard_bytes ----

    #[test]
    fn register_uploaded_shard_bytes_success_path() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        // 1. Create and store a xorb
        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let xorb_hash = serialized.hash;
        store_uploaded_xorb(&object_store, &xorb_hash.hex(), &serialized.serialized_data).unwrap();

        // 2. Build a shard referencing that xorb
        let chunk_hash = compute_data_hash(b"x");
        let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
        let shard = serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, 1_usize, false, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u32, 0_u32, 1_u32)],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u32, 1_u32),
                chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u32, 0_u32)],
            }],
        );

        // 3. Register with a commit closure
        let mut committed_records = None;
        let mut committed_mappings = None;

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(register_uploaded_shard_bytes(
            &object_store,
            &shard,
            None,
            DEFAULT_SHARD_METADATA_LIMITS,
            |records, mappings| {
                committed_records = Some(records);
                committed_mappings = Some(mappings);
                async { Ok(()) }
            },
        ));

        assert!(
            result.is_ok(),
            "register_uploaded_shard_bytes failed: {result:?}"
        );
        let response = result.unwrap();
        assert_eq!(response.result, 1, "expected newly inserted shard");

        // Verify commit_metadata was called
        assert!(
            committed_records.is_some(),
            "commit closure should have been called with records"
        );
        assert!(
            committed_mappings.is_some(),
            "commit closure should have been called with mappings"
        );
        let records = committed_records.unwrap();
        assert_eq!(records.len(), 1, "expected one file record");
        assert_eq!(records[0].chunks.len(), 1, "expected one chunk");
        assert!(!records[0].file_id.is_empty());
    }

    #[test]
    fn register_uploaded_shard_bytes_rejects_missing_xorb() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        // Build a shard referencing a xorb that doesn't exist
        let chunk_hash = compute_data_hash(b"x");
        let xorb_hash = xorb_hash(&[(chunk_hash, 1_u64)]);
        let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
        let shard = serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, 1_usize, false, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u32, 0_u32, 1_u32)],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u32, 1_u32),
                chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u32, 0_u32)],
            }],
        );

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(register_uploaded_shard_bytes(
            &object_store,
            &shard,
            None,
            DEFAULT_SHARD_METADATA_LIMITS,
            |_records, _mappings| async { Ok(()) },
        ));

        assert!(
            matches!(result, Err(XetAdapterError::MissingReferencedXorb)),
            "expected MissingReferencedXorb, got {result:?}"
        );
    }

    #[test]
    fn register_uploaded_shard_bytes_propagates_commit_error() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        // 1. Create and store a xorb
        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let xorb_hash = serialized.hash;
        store_uploaded_xorb(&object_store, &xorb_hash.hex(), &serialized.serialized_data).unwrap();

        // 2. Build a shard referencing that xorb
        let chunk_hash = compute_data_hash(b"x");
        let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
        let shard = serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, 1_usize, false, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u32, 0_u32, 1_u32)],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u32, 1_u32),
                chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u32, 0_u32)],
            }],
        );

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(register_uploaded_shard_bytes(
            &object_store,
            &shard,
            None,
            DEFAULT_SHARD_METADATA_LIMITS,
            |_records, _mappings| async { Err(XetAdapterError::Overflow) },
        ));

        assert!(
            matches!(result, Err(XetAdapterError::Overflow)),
            "expected Overflow from commit, got {result:?}"
        );
    }

    #[test]
    fn register_uploaded_shard_bytes_rejects_empty_shard() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(register_uploaded_shard_bytes(
            &object_store,
            b"",
            None,
            DEFAULT_SHARD_METADATA_LIMITS,
            |_records, _mappings| async { Ok(()) },
        ));

        assert!(result.is_err(), "expected error for empty shard");
    }

    #[test]
    fn register_uploaded_shard_bytes_with_repository_scope() {
        use shardline_protocol::{RepositoryProvider, RepositoryScope};

        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let xorb_hash = serialized.hash;
        store_uploaded_xorb(&object_store, &xorb_hash.hex(), &serialized.serialized_data).unwrap();

        let chunk_hash = compute_data_hash(b"x");
        let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
        let shard = serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, 1_usize, false, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u32, 0_u32, 1_u32)],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u32, 1_u32),
                chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u32, 0_u32)],
            }],
        );

        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "test-owner", "test-repo", None);
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };

        let mut committed = false;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(register_uploaded_shard_bytes(
            &object_store,
            &shard,
            Some(&scope),
            DEFAULT_SHARD_METADATA_LIMITS,
            |records, _mappings| {
                committed = true;
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].repository_scope, Some(scope.clone()));
                async { Ok(()) }
            },
        ));

        assert!(result.is_ok(), "register with scope failed: {result:?}");
        assert!(committed, "commit closure should have been called");
    }
}
