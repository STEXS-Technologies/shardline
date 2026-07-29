use std::collections::{HashMap, HashSet};

use shardline_index::{
    AsyncIndexStore, FileRecordStorageLayout, RecordStore, RecordTraversal, StoredRecord,
    xet_hash_hex_string,
};
use shardline_storage::{ObjectKey, ObjectPrefix, ObjectStore};

use crate::{
    GcError, ServerFrontend,
    dispatch::{
        managed_protocol_object_identity, optional_chunk_container_keys,
        referenced_term_object_key, visit_protocol_object_member_chunks,
    },
};
use shardline_server_core::{
    ServerObjectStore, checked_increment, chunk_hash_from_chunk_object_key_if_present,
    chunk_object_key, parse_stored_file_record_bytes,
};

#[derive(Debug, Clone)]
pub(super) struct OrphanObject {
    pub(super) hash: String,
    pub(super) object_key: ObjectKey,
    pub(super) bytes: u64,
}

#[derive(Debug, Default)]
pub(super) struct ReachabilityAccumulator {
    pub(super) referenced_object_keys: HashSet<String>,
    live_dedupe_chunk_hashes: HashSet<String>,
    missing_optional_object_keys: HashSet<String>,
    inspected_protocol_objects: HashSet<String>,
    pub(super) scanned_records: u64,
}

pub(super) async fn collect_referenced_object_keys<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    reachability: &mut ReachabilityAccumulator,
) -> Result<(), GcError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<GcError>,
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<GcError>,
{
    RecordTraversal::visit_latest_records(record_store, |entry| {
        collect_record_object_references(object_store, frontends, &entry, reachability)
    })
    .await?;

    RecordTraversal::visit_version_records(record_store, |entry| {
        collect_record_object_references(object_store, frontends, &entry, reachability)
    })
    .await?;

    index_store
        .visit_dedupe_shard_mappings(|mapping| {
            let chunk_hash_hex = xet_hash_hex_string(mapping.chunk_hash());
            if reachability
                .live_dedupe_chunk_hashes
                .contains(&chunk_hash_hex)
            {
                reachability
                    .referenced_object_keys
                    .insert(mapping.shard_object_key().as_str().to_owned());
            }
            Ok::<(), GcError>(())
        })
        .await?;

    Ok(())
}

fn collect_record_object_references<Locator>(
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    entry: &StoredRecord<Locator>,
    reachability: &mut ReachabilityAccumulator,
) -> Result<(), GcError> {
    let record = parse_stored_file_record_bytes(&entry.bytes)?;
    let storage_layout = record.storage_layout();
    reachability.scanned_records = checked_increment(reachability.scanned_records)?;
    for chunk in &record.chunks {
        match storage_layout {
            FileRecordStorageLayout::ReferencedObjectTerms => {
                let object_key = referenced_term_object_key(frontends, &chunk.hash)?;
                reachability
                    .referenced_object_keys
                    .insert(object_key.as_str().to_owned());
                collect_live_chunk_references_from_protocol_object(
                    object_store,
                    frontends,
                    &object_key,
                    reachability,
                )?;
            }
            FileRecordStorageLayout::StoredChunks => {
                let chunk_object_key = chunk_object_key(&chunk.hash)?;
                reachability
                    .referenced_object_keys
                    .insert(chunk_object_key.as_str().to_owned());
                reachability
                    .live_dedupe_chunk_hashes
                    .insert(chunk.hash.clone());

                for object_key in optional_chunk_container_keys(frontends, &chunk.hash)? {
                    mark_optional_object_reference(
                        object_store,
                        &object_key,
                        &mut reachability.referenced_object_keys,
                        &mut reachability.missing_optional_object_keys,
                    )?;
                }

                // For XorbCdcV1 records, also resolve the xorb container
                // to its constituent chunk hashes so that the individual
                // chunks inside the xorb are protected from GC even if
                // the xorb container itself is missing or corrupted.
                if record.storage_repr == shardline_index::StorageRepresentation::XorbCdcV1 {
                    if let Some(object_key) = optional_chunk_container_keys(frontends, &chunk.hash)?.into_iter().next() {
                        collect_live_chunk_references_from_protocol_object(
                            object_store,
                            frontends,
                            &object_key,
                            reachability,
                        )?;
                    }
                }
            }
        }
    }

    Ok(())
}

fn mark_optional_object_reference<ObjectAdapter>(
    object_store: &ObjectAdapter,
    object_key: &ObjectKey,
    referenced_object_keys: &mut HashSet<String>,
    missing_optional_object_keys: &mut HashSet<String>,
) -> Result<(), GcError>
where
    ObjectAdapter: ObjectStore,
    ObjectAdapter::Error: Into<GcError>,
{
    let object_key_string = object_key.as_str().to_owned();
    if referenced_object_keys.contains(&object_key_string)
        || missing_optional_object_keys.contains(&object_key_string)
    {
        return Ok(());
    }

    if object_store
        .metadata(object_key)
        .map_err(Into::into)?
        .is_some()
    {
        referenced_object_keys.insert(object_key_string);
    } else {
        missing_optional_object_keys.insert(object_key_string);
    }

    Ok(())
}

fn collect_live_chunk_references_from_protocol_object(
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    object_key: &ObjectKey,
    reachability: &mut ReachabilityAccumulator,
) -> Result<(), GcError> {
    let object_key_string = object_key.as_str().to_owned();
    if reachability
        .inspected_protocol_objects
        .contains(&object_key_string)
    {
        return Ok(());
    }
    reachability
        .inspected_protocol_objects
        .insert(object_key_string);

    visit_protocol_object_member_chunks(frontends, object_store, object_key, |chunk_hash_hex| {
        let chunk_object_key = chunk_object_key(&chunk_hash_hex)?;
        reachability
            .referenced_object_keys
            .insert(chunk_object_key.as_str().to_owned());
        reachability.live_dedupe_chunk_hashes.insert(chunk_hash_hex);
        Ok::<(), GcError>(())
    })
}

pub(super) fn scan_orphan_objects(
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    referenced_object_keys: &HashSet<String>,
) -> Result<HashMap<String, OrphanObject>, GcError> {
    let mut orphans = HashMap::new();
    let prefix = ObjectPrefix::parse("").map_err(|_error| GcError::InvalidContentHash)?;
    object_store.visit_prefix(&prefix, |metadata| {
        let Some(hash) = managed_object_hash(metadata.key(), frontends)? else {
            return Ok(());
        };
        if referenced_object_keys.contains(metadata.key().as_str()) {
            return Ok(());
        }

        orphans.insert(
            metadata.key().as_str().to_owned(),
            OrphanObject {
                hash,
                object_key: metadata.key().clone(),
                bytes: metadata.length(),
            },
        );
        Ok::<(), GcError>(())
    })?;

    Ok(orphans)
}

fn managed_object_hash(
    key: &ObjectKey,
    frontends: &[ServerFrontend],
) -> Result<Option<String>, GcError> {
    if let Some(hash) = chunk_hash_from_chunk_object_key_if_present(key)? {
        return Ok(Some(hash.to_owned()));
    }

    managed_protocol_object_identity(frontends, key)
}

pub(super) fn managed_object_hash_or_object_key(
    key: &ObjectKey,
    frontends: &[ServerFrontend],
) -> String {
    match managed_object_hash(key, frontends) {
        Ok(Some(hash)) => hash,
        Ok(None) | Err(_) => key.as_str().to_owned(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use shardline_index::{MemoryIndexStore, MemoryRecordStore};
    use shardline_storage::{
        DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectMetadata, PutOutcome,
    };

    /// A mock object store that returns metadata for a set of known keys.
    struct MockObjectStore {
        existing_keys: HashSet<String>,
    }

    impl MockObjectStore {
        fn new(existing: &[&str]) -> Self {
            Self {
                existing_keys: existing.iter().map(|s| (*s).to_owned()).collect(),
            }
        }
    }

    #[allow(clippy::unreachable)]
    impl ObjectStore for MockObjectStore {
        type Error = std::io::Error;

        fn put_if_absent(
            &self,
            _key: &ObjectKey,
            _body: ObjectBody<'_>,
            _integrity: &ObjectIntegrity,
        ) -> Result<PutOutcome, Self::Error> {
            unreachable!("not used in tests")
        }

        fn read_range(
            &self,
            _key: &ObjectKey,
            _range: shardline_protocol::ByteRange,
        ) -> Result<Vec<u8>, Self::Error> {
            unreachable!("not used in tests")
        }

        fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
            Ok(self.existing_keys.contains(key.as_str()))
        }

        fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
            if self.existing_keys.contains(key.as_str()) {
                Ok(Some(ObjectMetadata::new(key.clone(), 1024, None)))
            } else {
                Ok(None)
            }
        }

        fn list_prefix(&self, _prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error> {
            unreachable!("not used in tests")
        }

        fn delete_if_present(&self, _key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
            unreachable!("not used in tests")
        }
    }

    #[test]
    fn mark_optional_existing_object_is_referenced() {
        let store = MockObjectStore::new(&["chunks/abc123"]);
        let key = ObjectKey::parse("chunks/abc123").unwrap();
        let mut referenced = HashSet::new();
        let mut missing = HashSet::new();

        mark_optional_object_reference(&store, &key, &mut referenced, &mut missing).unwrap();

        assert!(
            referenced.contains("chunks/abc123"),
            "existing object should be in referenced set"
        );
        assert!(
            missing.is_empty(),
            "existing object should not be in missing set"
        );
    }

    #[test]
    fn mark_optional_missing_object_is_marked_missing() {
        let store = MockObjectStore::new(&[]);
        let key = ObjectKey::parse("chunks/xyz789").unwrap();
        let mut referenced = HashSet::new();
        let mut missing = HashSet::new();

        mark_optional_object_reference(&store, &key, &mut referenced, &mut missing).unwrap();

        assert!(
            referenced.is_empty(),
            "missing object should not be in referenced set"
        );
        assert!(
            missing.contains("chunks/xyz789"),
            "missing object should be in missing set"
        );
    }

    #[test]
    fn mark_optional_already_referenced_is_noop() {
        let store = MockObjectStore::new(&["chunks/abc"]);
        let key = ObjectKey::parse("chunks/abc").unwrap();
        let mut referenced = HashSet::new();
        referenced.insert("chunks/abc".to_owned());
        let mut missing = HashSet::new();

        mark_optional_object_reference(&store, &key, &mut referenced, &mut missing).unwrap();

        // Should not error, and the sets should remain unchanged.
        assert_eq!(referenced.len(), 1);
        assert!(missing.is_empty());
    }

    #[test]
    fn mark_optional_already_missing_is_noop() {
        let store = MockObjectStore::new(&[]);
        let key = ObjectKey::parse("chunks/def").unwrap();
        let mut referenced = HashSet::new();
        let mut missing = HashSet::new();
        missing.insert("chunks/def".to_owned());

        mark_optional_object_reference(&store, &key, &mut referenced, &mut missing).unwrap();

        // Should not error, and the sets should remain unchanged.
        assert!(referenced.is_empty());
        assert_eq!(missing.len(), 1);
    }

    #[test]
    fn managed_object_hash_or_object_key_with_chunk_key_returns_hash() {
        let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let key_str = format!("ab/{hash}");
        let key = ObjectKey::parse(key_str.as_str()).unwrap();
        let result = managed_object_hash_or_object_key(&key, &[]);
        assert_eq!(result, hash);
    }

    #[test]
    fn managed_object_hash_or_object_key_with_unknown_key_returns_key_as_is() {
        let key = ObjectKey::parse("unknown/somevalue").unwrap();
        let result = managed_object_hash_or_object_key(&key, &[]);
        assert_eq!(result, "unknown/somevalue");
    }

    // --- GC safety guarantee tests ---

    const TEST_HASH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn managed_object_hash_or_object_key_chunk_format_returns_hash() {
        // Chunk keys use the format <2-char-prefix>/<64-char-hash> where the
        // prefix is the first two characters of the hash.
        let prefix = &TEST_HASH[..2];
        let key = ObjectKey::parse(&format!("{prefix}/{TEST_HASH}")).unwrap();
        let result = managed_object_hash_or_object_key(&key, &[ServerFrontend::Xet]);
        assert_eq!(result, TEST_HASH);
    }

    #[test]
    fn managed_object_hash_or_object_key_xorb_format_returns_hash() {
        // Xorb keys follow the format xorbs/default/<prefix>/<hash>.xorb
        let prefix = &TEST_HASH[..2];
        let key = ObjectKey::parse(&format!("xorbs/default/{prefix}/{TEST_HASH}.xorb")).unwrap();
        let result = managed_object_hash_or_object_key(&key, &[ServerFrontend::Xet]);
        assert_eq!(result, TEST_HASH);
    }

    #[test]
    fn managed_object_hash_or_object_key_shard_format_returns_hash() {
        // Shard keys follow the format shards/<prefix>/<hash>.shard
        let prefix = &TEST_HASH[..2];
        let key = ObjectKey::parse(&format!("shards/{prefix}/{TEST_HASH}.shard")).unwrap();
        let result = managed_object_hash_or_object_key(&key, &[ServerFrontend::Xet]);
        assert_eq!(result, TEST_HASH);
    }

    #[test]
    fn managed_object_hash_or_object_key_oci_format_returns_key_as_is() {
        // OCI keys are not a managed format; the key should be returned as-is.
        let key = ObjectKey::parse(&format!("oci/{TEST_HASH}")).unwrap();
        let result = managed_object_hash_or_object_key(&key, &[ServerFrontend::Xet]);
        assert_eq!(result, format!("oci/{TEST_HASH}"));
    }

    // ── scan_orphan_objects tests ────────────────────────────────────────

    fn make_temp_chunks_dir() -> (tempfile::TempDir, ServerObjectStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();
        (dir, store)
    }

    fn put_test_object(store: &ServerObjectStore, key: &ObjectKey, data: &[u8]) {
        let hash = shardline_server_core::chunk_hash(data);
        let integrity = ObjectIntegrity::new(hash, u64::try_from(data.len()).unwrap_or(0));
        store
            .put_if_absent(key, ObjectBody::Borrowed(data), &integrity)
            .unwrap();
    }

    #[test]
    fn scan_orphan_objects_discovers_chunk_objects() {
        let (_dir, store) = make_temp_chunks_dir();
        let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let prefix = &hash[..2];
        let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
        put_test_object(&store, &key, b"test data");

        let referenced = HashSet::new();
        let orphans = scan_orphan_objects(&store, &[ServerFrontend::Xet], &referenced).unwrap();

        assert_eq!(orphans.len(), 1);
        assert!(orphans.contains_key(key.as_str()));
        assert_eq!(orphans[key.as_str()].hash, hash);
        assert_eq!(orphans[key.as_str()].bytes, 9);
    }

    #[test]
    fn scan_orphan_objects_skips_referenced_objects() {
        let (_dir, store) = make_temp_chunks_dir();
        let hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let prefix = &hash[..2];
        let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
        put_test_object(&store, &key, b"data");

        let mut referenced = HashSet::new();
        referenced.insert(key.as_str().to_owned());
        let orphans = scan_orphan_objects(&store, &[ServerFrontend::Xet], &referenced).unwrap();

        assert!(
            orphans.is_empty(),
            "referenced object should not appear as orphan"
        );
    }

    #[test]
    fn scan_orphan_objects_skips_non_chunk_keys() {
        let (_dir, store) = make_temp_chunks_dir();
        let key = ObjectKey::parse("some/random/key").unwrap();
        put_test_object(&store, &key, b"data");

        let referenced = HashSet::new();
        let orphans = scan_orphan_objects(&store, &[], &referenced).unwrap();

        assert!(
            orphans.is_empty(),
            "non-chunk key should be skipped when no frontend can identify it"
        );
    }

    #[test]
    fn scan_orphan_objects_empty_store_yields_empty_orphans() {
        let (_dir, store) = make_temp_chunks_dir();

        let referenced = HashSet::new();
        let orphans = scan_orphan_objects(&store, &[ServerFrontend::Xet], &referenced).unwrap();
        assert!(orphans.is_empty());
    }

    // ── collect_referenced_object_keys tests ────────────────────────────

    #[test]
    fn collect_referenced_object_keys_with_empty_stores() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let record_store = MemoryRecordStore::new();
            let index_store = MemoryIndexStore::new();
            let object_store = ServerObjectStore::blackhole();

            let mut reachability = ReachabilityAccumulator::default();
            collect_referenced_object_keys(
                &record_store,
                &index_store,
                &object_store,
                &[ServerFrontend::Xet],
                &mut reachability,
            )
            .await
            .unwrap();

            assert_eq!(reachability.scanned_records, 0);
            assert!(reachability.referenced_object_keys.is_empty());
        });
    }

    #[test]
    fn collect_referenced_object_keys_with_stored_chunk_record() {
        use shardline_index::{FileChunkRecord, FileRecord, MemoryRecordStore, RecordMutation};

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let record_store = MemoryRecordStore::new();
            let index_store = MemoryIndexStore::new();
            let object_store = ServerObjectStore::blackhole();

            let hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
            let record = FileRecord {
                file_id: "test-file".to_owned(),
                content_hash: hash.to_owned(),
                total_bytes: 100,
                chunk_size: 100,
                storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
                repository_scope: None,
                chunks: vec![FileChunkRecord {
                    hash: hash.to_owned(),
                    offset: 0,
                    length: 100,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 0,
                }],
            };
            record_store.write_latest_record(&record).await.unwrap();

            let mut reachability = ReachabilityAccumulator::default();
            collect_referenced_object_keys(
                &record_store,
                &index_store,
                &object_store,
                &[ServerFrontend::Xet],
                &mut reachability,
            )
            .await
            .unwrap();

            assert_eq!(reachability.scanned_records, 1);
            let chunk_key = format!("{prefix}/{hash}", prefix = &hash[..2]);
            assert!(
                reachability.referenced_object_keys.contains(&chunk_key),
                "chunk key must be marked as referenced"
            );
        });
    }

    // ── managed_object_hash tests ───────────────────────────────────────

    #[test]
    fn managed_object_hash_returns_hash_for_chunk_key() {
        let prefix = &TEST_HASH[..2];
        let key = ObjectKey::parse(&format!("{prefix}/{TEST_HASH}")).unwrap();
        let result = managed_object_hash(&key, &[]).unwrap();
        assert_eq!(result, Some(TEST_HASH.to_owned()));
    }

    #[test]
    fn managed_object_hash_returns_none_for_unrecognized_key() {
        let key = ObjectKey::parse("unknown/key").unwrap();
        let result = managed_object_hash(&key, &[]).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn managed_object_hash_returns_hash_for_xorb_key_with_xet_frontend() {
        let xorb_key = shardline_xet_adapter::xorb_object_key(TEST_HASH).unwrap();
        let result = managed_object_hash(&xorb_key, &[ServerFrontend::Xet]).unwrap();
        assert_eq!(result, Some(TEST_HASH.to_owned()));
    }

    #[test]
    fn managed_object_hash_returns_none_for_xorb_key_without_xet() {
        let xorb_key = shardline_xet_adapter::xorb_object_key(TEST_HASH).unwrap();
        let result = managed_object_hash(&xorb_key, &[]).unwrap();
        assert_eq!(result, None);
    }

    // ── collect_referenced_object_keys with version records ──────────────

    #[test]
    fn collect_referenced_object_keys_with_version_records() {
        use shardline_index::{FileChunkRecord, FileRecord, MemoryRecordStore, RecordMutation};

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let record_store = MemoryRecordStore::new();
            let index_store = MemoryIndexStore::new();
            let object_store = ServerObjectStore::blackhole();

            let hash = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
            let record = FileRecord {
                file_id: "version-file".to_owned(),
                content_hash: hash.to_owned(),
                total_bytes: 200,
                chunk_size: 200,
                storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
                repository_scope: None,
                chunks: vec![FileChunkRecord {
                    hash: hash.to_owned(),
                    offset: 0,
                    length: 200,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 0,
                }],
            };
            record_store.write_version_record(&record).await.unwrap();

            let mut reachability = ReachabilityAccumulator::default();
            collect_referenced_object_keys(
                &record_store,
                &index_store,
                &object_store,
                &[ServerFrontend::Xet],
                &mut reachability,
            )
            .await
            .unwrap();

            assert_eq!(reachability.scanned_records, 1);
            let chunk_key = format!("{prefix}/{hash}", prefix = &hash[..2]);
            assert!(
                reachability.referenced_object_keys.contains(&chunk_key),
                "version record chunk should be referenced"
            );
        });
    }

    // ── collect_referenced_object_keys with dedupe shard mapping ─────────

    #[test]
    fn collect_referenced_object_keys_with_dedupe_shard_mapping() {
        use shardline_index::{
            DedupeShardMapping, FileChunkRecord, FileRecord, MemoryRecordStore, RecordMutation,
        };
        use shardline_protocol::ShardlineHash;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let record_store = MemoryRecordStore::new();
            let index_store = MemoryIndexStore::new();
            let object_store = ServerObjectStore::blackhole();

            // Create a record with a stored chunk so its hash goes into
            // live_dedupe_chunk_hashes.
            let chunk_hash_hex = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
            let record = FileRecord {
                file_id: "dedupe-file".to_owned(),
                content_hash: chunk_hash_hex.to_owned(),
                total_bytes: 100,
                chunk_size: 100,
                storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
                repository_scope: None,
                chunks: vec![FileChunkRecord {
                    hash: chunk_hash_hex.to_owned(),
                    offset: 0,
                    length: 100,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 0,
                }],
            };
            record_store.write_latest_record(&record).await.unwrap();

            // Add a dedupe shard mapping that references the same chunk hash.
            let chunk_hash = ShardlineHash::from_bytes([0xee; 32]);
            let shard_key = ObjectKey::parse(
                "shards/ee/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee.shard",
            )
            .unwrap();
            let mapping = DedupeShardMapping::new(chunk_hash, shard_key.clone());
            index_store.upsert_dedupe_shard_mapping(&mapping).unwrap();

            let mut reachability = ReachabilityAccumulator::default();
            collect_referenced_object_keys(
                &record_store,
                &index_store,
                &object_store,
                &[ServerFrontend::Xet],
                &mut reachability,
            )
            .await
            .unwrap();

            // The shard key should be referenced because the chunk hash
            // is in live_dedupe_chunk_hashes.
            assert!(
                reachability
                    .referenced_object_keys
                    .contains(shard_key.as_str()),
                "shard key from dedupe mapping should be referenced"
            );
            // The chunk key should also be referenced.
            let chunk_key = format!("ee/{chunk_hash_hex}");
            assert!(
                reachability.referenced_object_keys.contains(&chunk_key),
                "chunk key should be referenced"
            );
        });
    }

    // ── collect_referenced_object_keys with referenced-object-terms layout ─

    #[test]
    fn collect_referenced_object_keys_with_referenced_object_terms() {
        use shardline_index::{FileChunkRecord, FileRecord, MemoryRecordStore, RecordMutation};

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let record_store = MemoryRecordStore::new();
            let index_store = MemoryIndexStore::new();
            let object_store = ServerObjectStore::blackhole();

            // chunk_size = 0 → ReferencedObjectTerms storage layout.
            let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
            let record = FileRecord {
                file_id: "terms-file".to_owned(),
                content_hash: hash.to_owned(),
                total_bytes: 100,
                chunk_size: 0,
                storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
                repository_scope: None,
                chunks: vec![FileChunkRecord {
                    hash: hash.to_owned(),
                    offset: 0,
                    length: 100,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 0,
                }],
            };
            record_store.write_latest_record(&record).await.unwrap();

            let mut reachability = ReachabilityAccumulator::default();
            collect_referenced_object_keys(
                &record_store,
                &index_store,
                &object_store,
                &[ServerFrontend::Xet],
                &mut reachability,
            )
            .await
            .unwrap();

            assert_eq!(reachability.scanned_records, 1);
            // With terms layout, the referenced key is a xorb key.
            let xorb_key = format!("xorbs/default/aa/{hash}.xorb");
            assert!(
                reachability.referenced_object_keys.contains(&xorb_key),
                "xorb term key must be referenced"
            );
        });
    }

    // ── collect_referenced_object_keys with both latest and version records ─

    #[test]
    fn collect_referenced_object_keys_counts_both_record_types() {
        use shardline_index::{FileChunkRecord, FileRecord, MemoryRecordStore, RecordMutation};

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let record_store = MemoryRecordStore::new();
            let index_store = MemoryIndexStore::new();
            let object_store = ServerObjectStore::blackhole();

            let hash1 = "1111111111111111111111111111111111111111111111111111111111111111";
            let hash2 = "2222222222222222222222222222222222222222222222222222222222222222";

            // Latest record with hash1
            let latest = FileRecord {
                file_id: "latest-file".to_owned(),
                content_hash: hash1.to_owned(),
                total_bytes: 50,
                chunk_size: 50,
                storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
                repository_scope: None,
                chunks: vec![FileChunkRecord {
                    hash: hash1.to_owned(),
                    offset: 0,
                    length: 50,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 0,
                }],
            };
            record_store.write_latest_record(&latest).await.unwrap();

            // Version record with hash2
            let version = FileRecord {
                file_id: "version-file".to_owned(),
                content_hash: hash2.to_owned(),
                total_bytes: 75,
                chunk_size: 75,
                storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
                repository_scope: None,
                chunks: vec![FileChunkRecord {
                    hash: hash2.to_owned(),
                    offset: 0,
                    length: 75,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 0,
                }],
            };
            record_store.write_version_record(&version).await.unwrap();

            let mut reachability = ReachabilityAccumulator::default();
            collect_referenced_object_keys(
                &record_store,
                &index_store,
                &object_store,
                &[ServerFrontend::Xet],
                &mut reachability,
            )
            .await
            .unwrap();

            assert_eq!(reachability.scanned_records, 2);
            assert!(
                reachability.referenced_object_keys.contains(
                    "11/1111111111111111111111111111111111111111111111111111111111111111"
                )
            );
            assert!(
                reachability.referenced_object_keys.contains(
                    "22/2222222222222222222222222222222222222222222222222222222222222222"
                )
            );
        });
    }
}
