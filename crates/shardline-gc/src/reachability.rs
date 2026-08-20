use std::collections::{HashMap, HashSet};

#[cfg(test)]
use std::cell::Cell;

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
    quarantine::LAST_GC_CLOCK_ANCHOR_KEY,
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

// Test-only invocation counter for `collect_referenced_object_keys`.
//
// A full reachability mark is an expensive operation (every record, every
// dedupe shard mapping, and — for XorbCdcV1 records — a read + parse of the
// stored xorb container). The sweep path must collect the referenced set
// exactly once regardless of how many quarantine candidates it sweeps, so the
// regression test asserts this count stays at 1. The counter is thread-local:
// each `#[test]` runs on its own thread, so concurrent tests do not perturb
// one another's counts.
#[cfg(test)]
thread_local! {
    static COLLECT_CALL_COUNT: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_collect_call_count() {
    COLLECT_CALL_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn collect_call_count() -> u64 {
    COLLECT_CALL_COUNT.with(Cell::get)
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
    #[cfg(test)]
    COLLECT_CALL_COUNT.with(|count| count.set(count.get().saturating_add(1)));

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
                if record.storage_repr == shardline_index::StorageRepresentation::XorbCdcV1
                    && let Some(object_key) = optional_chunk_container_keys(frontends, &chunk.hash)?
                        .into_iter()
                        .next()
                {
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

/// Upper bound (seconds) for a managed-object temp artifact before GC reaps it.
///
/// Temp-then-hardlink object writes (chunks, xorb containers, and shards)
/// complete in seconds-to-minutes, so a `.tmp-*` artifact older than an hour is
/// a stranded remnant of a killed/crashed writer — never an in-flight write.
///
/// The age bound is applied to the GC-OBSERVED last-modified time of the
/// artifact (with a fallback to the writer-embedded unix-nanos in the key when
/// the backend exposes no mtime). Because the observed mtime is backend truth,
/// divergence between the writer's wall clock and the GC host's wall clock
/// (NTP step, VM pause/resume, a slow write exceeding an hour) cannot make a
/// live in-flight write look stale and get it reaped mid-write.
pub(super) const STALE_TEMP_ARTIFACT_AGE_SECONDS: u64 = 60 * 60;

/// Returns the embedded unix-nanoseconds creation timestamp of a temporary
/// artifact key: `<managed-base>.tmp-<unix_nanos>-<counter>` where
/// `<managed-base>` is a key this object store writes and `.tmp-<unix_nanos>-<counter>`
/// is the temp-then-hardlink suffix produced by
/// `write_anchored_temporary_file_shared` for EVERY local object write
/// (chunks, xorb containers, shards, xorb chunk-cache sidecars, and the
/// last-GC-clock anchor alike). Returns `None` when the key is not such a temp
/// artifact.
///
/// The base is validated with [`is_gc_reaper_managed_base`], so the accepted
/// key space mirrors exactly the keys the store writes through the
/// temp-then-hardlink path: chunk `<2hex>/<64hex>`,
/// `xorbs/default/<2hex>/<hash>.xorb`, `shards/<2hex>/<hash>.shard` (F-67),
/// the xorb chunk-cache sidecar namespace `_xorb_chunks/<2hex>/<64hex>`, and
/// the reserved GC clock anchor `gc/last-gc-clock-anchor` (F-99). This is
/// important for two reasons:
///
/// * A matching key can never be a live object: the `.tmp-` suffix is only
///   ever present on temp-then-hardlink write artifacts, and the final object
///   is hard-linked/renamed to the suffix-free key. A crash between the temp
///   write and the hardlink strands `<managed-base>.tmp-...`, which is exactly
///   the artifact this predicate exists to reap.
/// * It cannot match a user-controlled key: the namespaces above are
///   server-internal, while user keys (for example S3 frontend objects under
///   `protocols/s3/{scope}/{key}`) never parse as a store-written base key, so
///   a user object that merely ends in `.tmp-<digits>-<digits>` is never
///   reaped.
fn temporary_artifact_unix_nanos(key: &ObjectKey, frontends: &[ServerFrontend]) -> Option<u128> {
    // `<managed-base>.tmp-<unix_nanos>-<counter>`
    let (base, temp_suffix) = key.as_str().rsplit_once(".tmp-")?;
    let (nanos_str, counter_str) = temp_suffix.rsplit_once('-')?;
    if nanos_str.is_empty()
        || counter_str.is_empty()
        || !nanos_str.bytes().all(|byte| byte.is_ascii_digit())
        || !counter_str.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    // The base must be exactly a key this store writes via the temp-then-
    // hardlink local write path: a managed object key (chunk, xorb, or shard),
    // the xorb chunk-cache sidecar namespace, or the reserved GC clock anchor.
    // Mirroring the store-written key space keeps the reaper's accepted keys
    // identical to the namespace the rest of GC operates on, so a matching key
    // can never be a live object or a user-controlled key (F-67, F-99).
    let Ok(base_key) = ObjectKey::parse(base) else {
        return None;
    };
    if !is_gc_reaper_managed_base(&base_key, frontends) {
        return None;
    }
    nanos_str.parse::<u128>().ok()
}

/// Returns true when `base_key` names a key this object store writes through
/// the temp-then-hardlink local write path (`write_anchored_temporary_file_shared`
/// followed by a hardlink or rename), so a stranded
/// `<base>.tmp-<unix_nanos>-<counter>` sibling is a reaping candidate:
///
/// * managed object keys — chunk `<2hex>/<64hex>`, xorb
///   `xorbs/default/<2hex>/<hash>.xorb`, shard `shards/<2hex>/<hash>.shard`
///   (mirroring [`managed_object_hash`], F-67);
/// * the xorb chunk-cache sidecar namespace `_xorb_chunks/<2hex>/<64hex>`
///   (written by `xorb_store::visit_stored_xorb_chunk_hashes`);
/// * the reserved last-GC-clock anchor `gc/last-gc-clock-anchor` (written by
///   `quarantine::write_last_gc_clock_anchor`).
///
/// Every final key above is written via the temp-then-hardlink path, so a
/// crash between the temp write and the hardlink/rename strands
/// `<base>.tmp-<unix_nanos>-<counter>`, which is exactly the artifact this
/// reaper exists to collect (F-99).
///
/// The key space stays tight: a matching base can never be a live object (the
/// `.tmp-` suffix is only ever present on temp-then-hardlink write artifacts),
/// and user keys — for example S3 frontend objects under
/// `protocols/s3/{scope}/{key}`, or any other `gc/`-shaped key that is not the
/// exact anchor — never match, so a user object that merely ends in
/// `.tmp-<digits>-<digits>` is never reaped.
fn is_gc_reaper_managed_base(base_key: &ObjectKey, frontends: &[ServerFrontend]) -> bool {
    if managed_object_hash(base_key, frontends)
        .ok()
        .is_some_and(|hash| hash.is_some())
    {
        return true;
    }
    if shardline_xet_adapter::xorb_chunks_cache_hash_from_key_if_present(base_key)
        .ok()
        .is_some_and(|hash| hash.is_some())
    {
        return true;
    }
    base_key.as_str() == LAST_GC_CLOCK_ANCHOR_KEY
}

/// Result of scanning the object store for stranded managed-object temp
/// artifacts.
pub(super) struct StaleTempScan {
    /// `(key, length)` pairs older than [`STALE_TEMP_ARTIFACT_AGE_SECONDS`].
    pub(super) stale: Vec<(ObjectKey, u64)>,
    /// The newest writer-embedded creation timestamp observed across ALL temp
    /// artifacts (fresh and stale alike), used by the runner's backward-clock
    /// guard. `None` when no temp artifacts were seen.
    pub(super) max_embedded_temp_nanos: Option<u128>,
}

/// Scans the object store for stranded temp artifacts older than
/// [`STALE_TEMP_ARTIFACT_AGE_SECONDS`].
///
/// The temp shape `<base>.tmp-<unix_nanos>-<counter>` is produced by
/// `write_anchored_temporary_file_shared` for EVERY local object write — chunk
/// `<2hex>/<64hex>`, xorb `xorbs/default/<2hex>/<hash>.xorb`, shard
/// `shards/<2hex>/<hash>.shard` (F-67), the xorb chunk-cache sidecar namespace
/// `_xorb_chunks/<2hex>/<64hex>`, and the last-GC-clock anchor
/// `gc/last-gc-clock-anchor` (F-99) — so a crash between the temp write and
/// the hardlink strands `<base>.tmp-...` that no other GC path can see:
/// `scan_orphan_objects` skips temp keys entirely and `managed_object_hash`
/// rejects them. This reaper closes that gap.
///
/// The age bound is applied to the GC-OBSERVED last-modified time of each
/// artifact ([`ObjectMetadata::modified_unix_nanos`]) when the backend exposes
/// one, falling back to the writer-embedded unix-nanos in the key. The
/// observed mtime is backend truth, so a live in-flight write (whose mtime is
/// fresh) is never reaped even when the writer-embedded timestamp looks
/// ancient due to wall-clock divergence between the writer and the GC host.
///
/// # Errors
///
/// Returns [`GcError`] when the object store cannot be enumerated.
pub(super) fn scan_stale_temporary_artifacts<Store>(
    object_store: &Store,
    frontends: &[ServerFrontend],
    now_unix_seconds: u64,
) -> Result<StaleTempScan, GcError>
where
    Store: ObjectStore,
    Store::Error: Into<GcError>,
{
    let mut stale = Vec::new();
    let mut max_embedded_temp_nanos: Option<u128> = None;
    let cutoff_unix_nanos =
        u128::from(now_unix_seconds.saturating_sub(STALE_TEMP_ARTIFACT_AGE_SECONDS))
            * 1_000_000_000;
    let prefix = ObjectPrefix::parse("").map_err(|_error| GcError::InvalidContentHash)?;
    object_store.visit_prefix(&prefix, |metadata| {
        let key = metadata.key();
        let Some(embedded_unix_nanos) = temporary_artifact_unix_nanos(key, frontends) else {
            return Ok(());
        };
        max_embedded_temp_nanos = Some(
            max_embedded_temp_nanos.map_or(embedded_unix_nanos, |max| max.max(embedded_unix_nanos)),
        );
        // Prefer the GC-observed mtime (backend truth, immune to writer/GC
        // clock divergence); fall back to the writer-embedded nanos.
        let effective_created_nanos = metadata
            .modified_unix_nanos()
            .map_or(embedded_unix_nanos, u128::from);
        if effective_created_nanos >= cutoff_unix_nanos {
            return Ok(());
        }
        stale.push((key.clone(), metadata.length()));
        Ok::<(), GcError>(())
    })?;
    Ok(StaleTempScan {
        stale,
        max_embedded_temp_nanos,
    })
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

    // ── scan_stale_temporary_artifacts tests ────────────────────────────

    const TEMP_TEST_HASH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    /// A mock object store exposing controllable metadata (including
    /// backend-observed mtimes) for a fixed set of keys.
    struct MockTempStore {
        objects: Vec<(String, u64, Option<u64>)>,
    }

    impl MockTempStore {
        fn new(objects: Vec<(&str, u64, Option<u64>)>) -> Self {
            Self {
                objects: objects
                    .into_iter()
                    .map(|(key, length, modified)| (key.to_owned(), length, modified))
                    .collect(),
            }
        }
    }

    #[allow(clippy::unreachable)]
    impl ObjectStore for MockTempStore {
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
            Ok(self.objects.iter().any(|(k, _, _)| k == key.as_str()))
        }

        fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
            Ok(self.objects.iter().find(|(k, _, _)| k == key.as_str()).map(
                |(k, length, modified)| {
                    let mut metadata =
                        ObjectMetadata::new(ObjectKey::parse(k).unwrap(), *length, None);
                    if let Some(modified) = modified {
                        metadata = metadata.with_modified(*modified);
                    }
                    metadata
                },
            ))
        }

        fn list_prefix(&self, _prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error> {
            Ok(self
                .objects
                .iter()
                .map(|(k, length, modified)| {
                    let mut metadata =
                        ObjectMetadata::new(ObjectKey::parse(k).unwrap(), *length, None);
                    if let Some(modified) = modified {
                        metadata = metadata.with_modified(*modified);
                    }
                    metadata
                })
                .collect())
        }

        fn delete_if_present(&self, _key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
            unreachable!("not used in tests")
        }
    }

    fn temp_key(nanos: u128, counter: u64) -> String {
        let prefix = &TEMP_TEST_HASH[..2];
        format!("{prefix}/{TEMP_TEST_HASH}.tmp-{nanos}-{counter}")
    }

    #[test]
    fn stale_temp_scan_skips_live_temp_with_fresh_observed_mtime() {
        // Clock-divergence case: the writer-embedded nanos are ancient (the
        // writer's clock is behind the GC clock by >1h) but the GC-observed
        // mtime is fresh because the write is still in flight. The temp must
        // NOT be reaped.
        let now_secs = 2_000_000_000_u64;
        let old_embedded = u128::from(now_secs - 2 * 3600) * 1_000_000_000;
        let fresh_mtime = u64::try_from(u128::from(now_secs - 10) * 1_000_000_000).unwrap();
        let key = temp_key(old_embedded, 0);
        let store = MockTempStore::new(vec![(key.as_str(), 42, Some(fresh_mtime))]);

        let scan =
            scan_stale_temporary_artifacts(&store, &[ServerFrontend::Xet], now_secs).unwrap();

        assert!(
            scan.stale.is_empty(),
            "live in-flight temp with a fresh observed mtime must not be reaped"
        );
        assert_eq!(scan.max_embedded_temp_nanos, Some(old_embedded));
    }

    #[test]
    fn stale_temp_scan_reaps_temp_with_old_embedded_and_old_observed_mtime() {
        // A stranded temp: old embedded nanos AND an old observed mtime. It is
        // an orphaned remnant of a killed/crashed writer and must be reaped.
        let now_secs = 2_000_000_000_u64;
        let old_nanos = u128::from(now_secs - 2 * 3600) * 1_000_000_000;
        let old_mtime = u64::try_from(old_nanos).unwrap();
        let key = temp_key(old_nanos, 0);
        let store = MockTempStore::new(vec![(key.as_str(), 42, Some(old_mtime))]);

        let scan =
            scan_stale_temporary_artifacts(&store, &[ServerFrontend::Xet], now_secs).unwrap();

        assert_eq!(scan.stale.len(), 1);
        assert_eq!(scan.stale[0].0.as_str(), key);
        assert_eq!(scan.stale[0].1, 42);
    }

    #[test]
    fn stale_temp_scan_falls_back_to_embedded_nanos_without_mtime() {
        // Backends without an observed mtime (None) fall back to the
        // writer-embedded nanos exactly as before: old embedded → stale, fresh
        // embedded → kept.
        let now_secs = 2_000_000_000_u64;
        let old_embedded = u128::from(now_secs - 2 * 3600) * 1_000_000_000;
        let fresh_embedded = u128::from(now_secs) * 1_000_000_000;
        let stale_key = temp_key(old_embedded, 0);
        let fresh_key = temp_key(fresh_embedded, 1);
        let store = MockTempStore::new(vec![
            (stale_key.as_str(), 42, None),
            (fresh_key.as_str(), 7, None),
        ]);

        let scan =
            scan_stale_temporary_artifacts(&store, &[ServerFrontend::Xet], now_secs).unwrap();

        assert_eq!(scan.stale.len(), 1, "only the old temp should be stale");
        assert_eq!(scan.stale[0].0.as_str(), stale_key);
        assert_eq!(scan.stale[0].1, 42);
        assert_eq!(scan.max_embedded_temp_nanos, Some(fresh_embedded));
    }

    // ── scan_stale_temporary_artifacts: managed xorb/shard temps (F-67) ──

    const XORB_TEMP_TEST_HASH: &str =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn xorb_temp_key(nanos: u128, counter: u64) -> String {
        let prefix = &XORB_TEMP_TEST_HASH[..2];
        format!("xorbs/default/{prefix}/{XORB_TEMP_TEST_HASH}.xorb.tmp-{nanos}-{counter}")
    }

    fn shard_temp_key(nanos: u128, counter: u64) -> String {
        let prefix = &XORB_TEMP_TEST_HASH[..2];
        format!("shards/{prefix}/{XORB_TEMP_TEST_HASH}.shard.tmp-{nanos}-{counter}")
    }

    #[test]
    fn stale_temp_scan_reaps_stranded_xorb_and_shard_temps() {
        // F-67 regression: ALL local object writes (chunks, xorb containers,
        // shards) go through temp-then-hardlink and get the `.tmp-<nanos>-
        // <counter>` suffix. A crash between the temp write and the hardlink
        // strands xorbs/...xorb.tmp-... and shards/...shard.tmp-... which the
        // old chunk-only grammar never matched (and which managed_object_hash
        // rejects, so no other GC path reaps them). The extended reaper must
        // reap them after the age bound.
        let now_secs = 2_000_000_000_u64;
        let old_nanos = u128::from(now_secs - 2 * 3600) * 1_000_000_000;
        let old_mtime = u64::try_from(old_nanos).unwrap();
        let xorb_key = xorb_temp_key(old_nanos, 0);
        let shard_key = shard_temp_key(old_nanos, 1);
        let store = MockTempStore::new(vec![
            (xorb_key.as_str(), 512, Some(old_mtime)),
            (shard_key.as_str(), 64, Some(old_mtime)),
        ]);

        let scan =
            scan_stale_temporary_artifacts(&store, &[ServerFrontend::Xet], now_secs).unwrap();

        let mut reaped = scan
            .stale
            .iter()
            .map(|(key, _bytes)| key.as_str())
            .collect::<Vec<_>>();
        reaped.sort_unstable();
        assert_eq!(reaped, vec![shard_key.as_str(), xorb_key.as_str()]);
        assert_eq!(
            scan.max_embedded_temp_nanos,
            Some(old_nanos),
            "the backward-clock guard must observe the xorb/shard temps too"
        );
    }

    #[test]
    fn stale_temp_scan_leaves_live_xorb_and_shard_objects_untouched() {
        // F-67 safety: live xorb and shard objects (no `.tmp-` suffix) share
        // the managed namespace with the stranded temps and must never be
        // classified as reaping candidates.
        let now_secs = 2_000_000_000_u64;
        let old_embedded = u128::from(now_secs - 2 * 3600) * 1_000_000_000;
        let old_mtime = u64::try_from(old_embedded).unwrap();
        let xorb_live = format!(
            "xorbs/default/{}/{}.xorb",
            &XORB_TEMP_TEST_HASH[..2],
            XORB_TEMP_TEST_HASH
        );
        let shard_live = format!(
            "shards/{}/{}.shard",
            &XORB_TEMP_TEST_HASH[..2],
            XORB_TEMP_TEST_HASH
        );
        let chunk_live = format!("{}/{}", &XORB_TEMP_TEST_HASH[..2], XORB_TEMP_TEST_HASH);
        let store = MockTempStore::new(vec![
            (xorb_live.as_str(), 512, Some(old_mtime)),
            (shard_live.as_str(), 64, Some(old_mtime)),
            (chunk_live.as_str(), 32, Some(old_mtime)),
        ]);

        let scan =
            scan_stale_temporary_artifacts(&store, &[ServerFrontend::Xet], now_secs).unwrap();

        assert!(
            scan.stale.is_empty(),
            "live managed objects must never be reaped"
        );
        assert!(
            scan.max_embedded_temp_nanos.is_none(),
            "live managed objects must not feed the backward-clock guard"
        );
    }

    #[test]
    fn stale_temp_scan_ignores_non_managed_temp_like_keys() {
        // A user-controlled key that merely ends in `.tmp-<digits>-<digits>`
        // must never be reaped: its base is not a managed object key (these are
        // server-internal namespaces; user keys live under e.g.
        // `protocols/s3/{scope}/{key}`). Also a managed-shaped base with a
        // malformed hash must not match.
        let now_secs = 2_000_000_000_u64;
        let old_embedded = u128::from(now_secs - 2 * 3600) * 1_000_000_000;
        let old_mtime = u64::try_from(old_embedded).unwrap();
        let user_key = format!("protocols/s3/abc123/user-file.tmp-{old_embedded}-0");
        let non_managed_key = format!("lfs/repo/objects/somefile.tmp-{old_embedded}-1");
        let short_hash_xorb = format!("xorbs/default/aa/abcd.xorb.tmp-{old_embedded}-2");
        let upper_xorb = format!(
            "xorbs/default/aa/{}.xorb.tmp-{old_embedded}-3",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        );
        let store = MockTempStore::new(vec![
            (user_key.as_str(), 10, Some(old_mtime)),
            (non_managed_key.as_str(), 11, Some(old_mtime)),
            (short_hash_xorb.as_str(), 12, Some(old_mtime)),
            (upper_xorb.as_str(), 13, Some(old_mtime)),
        ]);

        let scan =
            scan_stale_temporary_artifacts(&store, &[ServerFrontend::Xet], now_secs).unwrap();

        assert!(
            scan.stale.is_empty(),
            "non-managed temp-like keys must never be reaped"
        );
        assert!(
            scan.max_embedded_temp_nanos.is_none(),
            "non-managed temp-like keys must not feed the backward-clock guard either"
        );
    }

    // ── scan_stale_temporary_artifacts: xorb chunk-cache sidecar + GC anchor temps (F-99) ──

    const SIDECAR_TEMP_TEST_HASH: &str =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn xorb_chunks_temp_key(nanos: u128, counter: u64) -> String {
        let prefix = &SIDECAR_TEMP_TEST_HASH[..2];
        format!("_xorb_chunks/{prefix}/{SIDECAR_TEMP_TEST_HASH}.tmp-{nanos}-{counter}")
    }

    fn anchor_temp_key(nanos: u128, counter: u64) -> String {
        format!("gc/last-gc-clock-anchor.tmp-{nanos}-{counter}")
    }

    #[test]
    fn stale_temp_scan_reaps_stranded_xorb_chunks_and_anchor_temps() {
        // F-99 regression: the xorb chunk-cache sidecar
        // (`_xorb_chunks/{prefix}/{hash}`, written by
        // `xorb_store::visit_stored_xorb_chunk_hashes`) and the last-GC-clock
        // anchor (`gc/last-gc-clock-anchor`, written by
        // `write_last_gc_clock_anchor`) are ALSO written via
        // temp-then-hardlink, so a crash between the temp write and the
        // hardlink strands `_xorb_chunks/{prefix}/{hash}.tmp-...` and
        // `gc/last-gc-clock-anchor.tmp-...` which the old managed-object-only
        // base validation never matched (managed_object_hash rejects both
        // namespaces, so no other GC path reaps them either — F-67 residual).
        // The extended reaper must reap them after the age bound.
        let now_secs = 2_000_000_000_u64;
        let old_nanos = u128::from(now_secs - 2 * 3600) * 1_000_000_000;
        let old_mtime = u64::try_from(old_nanos).unwrap();
        let sidecar_key = xorb_chunks_temp_key(old_nanos, 0);
        let anchor_key = anchor_temp_key(old_nanos, 1);
        let store = MockTempStore::new(vec![
            (sidecar_key.as_str(), 42, Some(old_mtime)),
            (anchor_key.as_str(), 7, Some(old_mtime)),
        ]);

        let scan =
            scan_stale_temporary_artifacts(&store, &[ServerFrontend::Xet], now_secs).unwrap();

        let mut reaped = scan
            .stale
            .iter()
            .map(|(key, _bytes)| key.as_str())
            .collect::<Vec<_>>();
        reaped.sort_unstable();
        // `_xorb_chunks/...` sorts before `gc/...` (ASCII '_' < 'g').
        assert_eq!(reaped, vec![sidecar_key.as_str(), anchor_key.as_str()]);
        assert_eq!(scan.stale[0].1, 42, "sidecar temp byte count");
        assert_eq!(scan.stale[1].1, 7, "anchor temp byte count");
        assert_eq!(
            scan.max_embedded_temp_nanos,
            Some(old_nanos),
            "the backward-clock guard must observe the sidecar and anchor temps too"
        );
    }

    #[test]
    fn stale_temp_scan_leaves_live_xorb_chunks_and_anchor_untouched() {
        // F-99 safety: the LIVE xorb chunk-cache sidecar and the LIVE
        // last-GC-clock anchor (no `.tmp-` suffix) share the reaped namespaces
        // with the stranded temps and must never be classified as reaping
        // candidates, however old their mtime is.
        let now_secs = 2_000_000_000_u64;
        let old_embedded = u128::from(now_secs - 2 * 3600) * 1_000_000_000;
        let old_mtime = u64::try_from(old_embedded).unwrap();
        let sidecar_live = format!(
            "_xorb_chunks/{}/{}",
            &SIDECAR_TEMP_TEST_HASH[..2],
            SIDECAR_TEMP_TEST_HASH
        );
        let anchor_live = "gc/last-gc-clock-anchor";
        let chunk_live = format!(
            "{}/{}",
            &SIDECAR_TEMP_TEST_HASH[..2],
            SIDECAR_TEMP_TEST_HASH
        );
        let store = MockTempStore::new(vec![
            (sidecar_live.as_str(), 512, Some(old_mtime)),
            (anchor_live, 19, Some(old_mtime)),
            (chunk_live.as_str(), 32, Some(old_mtime)),
        ]);

        let scan =
            scan_stale_temporary_artifacts(&store, &[ServerFrontend::Xet], now_secs).unwrap();

        assert!(
            scan.stale.is_empty(),
            "live sidecar, anchor, and chunk must never be reaped"
        );
        assert!(
            scan.max_embedded_temp_nanos.is_none(),
            "live keys must not feed the backward-clock guard"
        );
    }

    #[test]
    fn stale_temp_scan_ignores_gc_and_xorb_chunks_near_miss_temp_keys() {
        // F-99 safety: user keys and near-miss keys in the reaped namespaces
        // must never match. A user/protocol key ending in
        // `.tmp-<digits>-<digits>` (e.g. under `protocols/s3/`), any `gc/`
        // key that is not the exact anchor base, and any `_xorb_chunks/` key
        // whose hash is malformed (short, non-hex, or prefix-mismatched) are
        // all invisible to the reaper.
        let now_secs = 2_000_000_000_u64;
        let old_embedded = u128::from(now_secs - 2 * 3600) * 1_000_000_000;
        let old_mtime = u64::try_from(old_embedded).unwrap();
        let user_key = format!("protocols/s3/acme/repo/user-file.tmp-{old_embedded}-0");
        let other_gc_key = format!("gc/quarantine/aa/abc123.json.tmp-{old_embedded}-1");
        let anchor_like_key = format!("gc/last-gc-clock-anchor-v2.tmp-{old_embedded}-2");
        let short_hash_sidecar = format!("_xorb_chunks/aa/abcd.tmp-{old_embedded}-3");
        let non_hex_prefix_sidecar =
            format!("_xorb_chunks/zz/{}.tmp-{old_embedded}-4", "z".repeat(64));
        let prefix_mismatch_hash = format!("bb{}", "0".repeat(62));
        let prefix_mismatch_sidecar =
            format!("_xorb_chunks/aa/{prefix_mismatch_hash}.tmp-{old_embedded}-5");
        let store = MockTempStore::new(vec![
            (user_key.as_str(), 10, Some(old_mtime)),
            (other_gc_key.as_str(), 11, Some(old_mtime)),
            (anchor_like_key.as_str(), 12, Some(old_mtime)),
            (short_hash_sidecar.as_str(), 13, Some(old_mtime)),
            (non_hex_prefix_sidecar.as_str(), 14, Some(old_mtime)),
            (prefix_mismatch_sidecar.as_str(), 15, Some(old_mtime)),
        ]);

        let scan =
            scan_stale_temporary_artifacts(&store, &[ServerFrontend::Xet], now_secs).unwrap();

        assert!(
            scan.stale.is_empty(),
            "user and near-miss keys must never be reaped"
        );
        assert!(
            scan.max_embedded_temp_nanos.is_none(),
            "user and near-miss keys must not feed the backward-clock guard either"
        );
    }
}
