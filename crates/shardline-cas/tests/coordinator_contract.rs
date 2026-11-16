#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use std::{num::NonZeroU64, time::Duration};

use async_trait::async_trait;
use shardline_cas::paths::xorb_key;
use shardline_cas::{CasCoordinator, CasError, CasLimits};
use shardline_index::{
    DedupeShardMapping, DedupeStore, FileId, FileReconstruction, LifecycleStore, LocalIndexStore,
    MemoryIndexStore, ProviderRepositoryState, QuarantineCandidate, ReconstructionTerm,
    RetentionHold, StoredObjectId, UploadIntent, UploadIntentState, UploadIntentStore,
    WebhookDelivery, xet_hash_hex_string,
};
use shardline_protocol::{ByteRange, ChunkRange, RepositoryProvider, ShardlineHash};
use shardline_storage::{
    DeleteOutcome, LocalObjectStore, LocalObjectStoreError, ObjectBody, ObjectIntegrity, ObjectKey,
    ObjectPrefix, ObjectStore, PutOutcome, SyncObjectStoreBridge,
};

// ── Mock upload-intent store for error-injection tests ─────────────

#[derive(Debug, Clone)]
struct UploadIntentError(String);

impl std::fmt::Display for UploadIntentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for UploadIntentError {}

#[derive(Debug)]
struct FailingIntentStore;

#[async_trait]
impl UploadIntentStore for FailingIntentStore {
    type Error = UploadIntentError;

    async fn create_intent(&self, _intent: &UploadIntent) -> Result<(), Self::Error> {
        Err(UploadIntentError("create_intent failed".to_owned()))
    }

    async fn transition_intent(
        &self,
        _intent_id: &str,
        _new_state: UploadIntentState,
    ) -> Result<bool, Self::Error> {
        Ok(false)
    }

    async fn intent_by_id(&self, _intent_id: &str) -> Result<Option<UploadIntent>, Self::Error> {
        Ok(None)
    }

    async fn intents_by_state(
        &self,
        _state: UploadIntentState,
    ) -> Result<Vec<UploadIntent>, Self::Error> {
        Ok(vec![])
    }

    async fn stale_intents(
        &self,
        _state: UploadIntentState,
        _older_than: Duration,
    ) -> Result<Vec<UploadIntent>, Self::Error> {
        Ok(vec![])
    }
}

// ── Contract tests ─────────────────────────────────────────────────

#[tokio::test]
async fn coordinator_adapters_support_lifecycle_and_reconstruction_contracts() {
    let index = MemoryIndexStore::default();
    let storage = tempfile::tempdir().unwrap();
    let object_store = LocalObjectStore::new(storage.path().join("objects")).unwrap();
    let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MAX, NonZeroU64::MIN);
    let coordinator = CasCoordinator::new(index, object_store, (), limits);

    assert_lifecycle_and_reconstruction_contract(&coordinator, 5, [10, 20, 30, 40], (10, 20)).await;
}

#[tokio::test]
async fn coordinator_local_filesystem_adapters_support_lifecycle_and_reconstruction_contracts() {
    let storage = tempfile::tempdir().unwrap();
    let object_store = LocalObjectStore::new(storage.path().join("objects")).unwrap();
    let index = LocalIndexStore::new(storage.path().join("index")).unwrap();

    let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MAX, NonZeroU64::MIN);
    let coordinator = CasCoordinator::new(index, object_store, (), limits);

    assert_lifecycle_and_reconstruction_contract(&coordinator, 8, [1, 2, 3, 4], (11, 22)).await;
}

async fn assert_lifecycle_and_reconstruction_contract<I>(
    coordinator: &CasCoordinator<I, LocalObjectStore, ()>,
    hash_byte: u8,
    body: [u8; 4],
    timeline: (u64, u64),
) where
    I: shardline_index::AsyncIndexStore,
    <I as shardline_index::AsyncIndexStore>::Error: std::fmt::Debug,
{
    let index = coordinator.index();
    let objects = coordinator.object_store();
    let hash = ShardlineHash::from_bytes([hash_byte; 32]);
    let key = xorb_key(&format!("{hash_byte:02x}"), "hash");
    let integrity = ObjectIntegrity::new(blake3_hash(&body), 4);
    let first = objects.put_if_absent(&key, ObjectBody::from_slice(&body), &integrity);
    let second = objects.put_if_absent(&key, ObjectBody::from_slice(&body), &integrity);

    assert_eq!(first.unwrap(), PutOutcome::Inserted);
    assert_eq!(second.unwrap(), PutOutcome::AlreadyExists);
    assert!(objects.contains(&key).unwrap());
    assert_eq!(
        objects
            .metadata(&key)
            .unwrap()
            .map(|metadata| metadata.length()),
        Some(4)
    );

    let prefix = ObjectPrefix::parse("xorbs/default/").unwrap();
    let listed = objects.list_prefix(&prefix).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].key(), &key);

    let range = ByteRange::new(1, 2).unwrap();
    assert_eq!(objects.read_range(&key, range).unwrap(), [body[1], body[2]]);

    let file_id = FileId::new(hash);
    let object_id = StoredObjectId::new(hash);
    let chunk_range = ChunkRange::new(0, 1).unwrap();
    let term = ReconstructionTerm::new(object_id, chunk_range, 4);
    let reconstruction = FileReconstruction::new(vec![term]);

    index
        .insert_reconstruction(&file_id, &reconstruction)
        .await
        .unwrap();
    index.insert_object(&object_id).await.unwrap();

    let candidate = QuarantineCandidate::new(key.clone(), 4, timeline.0, timeline.1).unwrap();
    index.upsert_quarantine_candidate(&candidate).await.unwrap();

    assert!(index.contains_object(&object_id).await.unwrap());
    assert_eq!(
        index.reconstruction(&file_id).await.unwrap(),
        Some(reconstruction)
    );
    assert_eq!(
        index.quarantine_candidate(&key).await.unwrap(),
        Some(candidate.clone())
    );
    assert_eq!(
        index.list_quarantine_candidates().await.unwrap(),
        vec![candidate]
    );
    assert_eq!(
        objects.delete_if_present(&key).unwrap(),
        DeleteOutcome::Deleted
    );
    assert_eq!(
        objects.delete_if_present(&key).unwrap(),
        DeleteOutcome::NotFound
    );
    assert!(index.delete_quarantine_candidate(&key).await.unwrap());
    assert!(!index.delete_quarantine_candidate(&key).await.unwrap());

    let hold = RetentionHold::new(
        key.clone(),
        "provider deletion grace".to_owned(),
        30,
        Some(90),
    )
    .unwrap();
    index.upsert_retention_hold(&hold).await.unwrap();
    assert_eq!(
        index.retention_hold(&key).await.unwrap(),
        Some(hold.clone())
    );
    assert_eq!(index.list_retention_holds().await.unwrap(), vec![hold]);
    assert!(index.delete_retention_hold(&key).await.unwrap());
    assert!(!index.delete_retention_hold(&key).await.unwrap());
}

// ── LocalObjectStore error paths ─────────────────────────────────────

#[test]
fn local_object_store_put_if_absent_rejects_wrong_body_length() {
    let storage = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(storage.path().join("objects")).unwrap();
    let key = ObjectKey::parse("xorbs/default/05/hash.xorb").unwrap();
    let integrity = ObjectIntegrity::new(ShardlineHash::from_bytes([1; 32]), 4);

    // Body has 5 bytes but integrity declares 4.
    let result = store.put_if_absent(
        &key,
        ObjectBody::from_slice(&[10, 20, 30, 40, 50]),
        &integrity,
    );
    assert!(matches!(
        result,
        Err(LocalObjectStoreError::IntegrityLengthMismatch)
    ));
}

#[test]
fn local_object_store_put_if_absent_rejects_existing_different_content() {
    let storage = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(storage.path().join("objects")).unwrap();
    let key = ObjectKey::parse("xorbs/default/05/hash.xorb").unwrap();
    let hash = blake3_hash(&[10, 20, 30, 40]);

    let first = store.put_if_absent(
        &key,
        ObjectBody::from_slice(&[10, 20, 30, 40]),
        &ObjectIntegrity::new(hash, 4),
    );
    assert_eq!(first.unwrap(), PutOutcome::Inserted);

    // Both bodies have valid integrity, but the immutable key already has different bytes.
    let second = store.put_if_absent(
        &key,
        ObjectBody::from_slice(&[50, 60, 70, 80]),
        &ObjectIntegrity::new(blake3_hash(&[50, 60, 70, 80]), 4),
    );
    assert!(matches!(
        second,
        Err(LocalObjectStoreError::IntegrityHashMismatch)
    ));
    assert_eq!(
        store
            .read_range(&key, ByteRange::new(0, 3).unwrap())
            .unwrap(),
        vec![10, 20, 30, 40]
    );
}

#[test]
fn local_object_store_read_range_returns_missing_for_unknown_key() {
    let storage = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(storage.path().join("objects")).unwrap();
    let key = ObjectKey::parse("xorbs/default/05/hash.xorb").unwrap();
    let range = ByteRange::new(0, 0).unwrap();

    assert!(
        matches!(store.read_range(&key, range), Err(LocalObjectStoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound)
    );
}

#[test]
fn local_object_store_read_range_rejects_unrepresentable_length() {
    let storage = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(storage.path().join("objects")).unwrap();
    let key = ObjectKey::parse("xorbs/default/05/hash.xorb").unwrap();
    store
        .put_if_absent(
            &key,
            ObjectBody::from_slice(&[10, 20, 30, 40]),
            &ObjectIntegrity::new(blake3_hash(&[10, 20, 30, 40]), 4),
        )
        .unwrap();

    // ByteRange(0, u64::MAX) has len() = None because u64::MAX + 1 overflows
    let range = ByteRange::new(0, u64::MAX).unwrap();
    assert_eq!(range.len(), None);

    assert!(matches!(
        store.read_range(&key, range),
        Err(LocalObjectStoreError::RangeOutOfBounds)
    ));
}

#[test]
fn local_object_store_metadata_returns_none_for_missing_key() {
    let storage = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(storage.path().join("objects")).unwrap();
    let key = ObjectKey::parse("xorbs/default/05/hash.xorb").unwrap();

    assert_eq!(store.metadata(&key).unwrap(), None);
}

// ── Index adapter record lifecycles ──────────────────────────────────

#[test]
fn dedupe_shard_mappings_preserve_lookup_listing_and_deletion() {
    let index = MemoryIndexStore::default();
    assert!(index.list_dedupe_shard_mappings().unwrap().is_empty());
    for byte in [5, 7] {
        let missing = ShardlineHash::from_bytes([byte; 32]);
        assert_eq!(index.dedupe_shard_mapping(&missing).unwrap(), None);
        assert!(!index.delete_dedupe_shard_mapping(&missing).unwrap());
    }

    let mut mappings = Vec::new();
    for (byte, key) in [
        (4, "shards/aa/example.shard"),
        (1, "shards/aa/a.shard"),
        (2, "shards/bb/b.shard"),
        (6, "shards/aa/del.shard"),
    ] {
        let hash = ShardlineHash::from_bytes([byte; 32]);
        let mapping = DedupeShardMapping::new(hash, ObjectKey::parse(key).unwrap());
        index.upsert_dedupe_shard_mapping(&mapping).unwrap();
        assert_eq!(
            index.dedupe_shard_mapping(&hash).unwrap(),
            Some(mapping.clone())
        );
        mappings.push(mapping);
    }
    mappings.sort_by_key(|mapping| xet_hash_hex_string(mapping.chunk_hash()));
    while !mappings.is_empty() {
        let mut listed = index.list_dedupe_shard_mappings().unwrap();
        listed.sort_by_key(|mapping| xet_hash_hex_string(mapping.chunk_hash()));
        assert_eq!(listed, mappings);
        let deleted = mappings.remove(0);
        assert!(
            index
                .delete_dedupe_shard_mapping(&deleted.chunk_hash())
                .unwrap()
        );
        assert_eq!(
            index.dedupe_shard_mapping(&deleted.chunk_hash()).unwrap(),
            None
        );
        assert!(
            !index
                .delete_dedupe_shard_mapping(&deleted.chunk_hash())
                .unwrap()
        );
    }
    assert!(index.list_dedupe_shard_mappings().unwrap().is_empty());
}

#[test]
fn webhook_deliveries_preserve_idempotency_ordering_and_deletion() {
    let index = MemoryIndexStore::default();
    assert!(index.list_webhook_deliveries().unwrap().is_empty());
    let mut deliveries = Vec::new();
    // Insert in reverse provider order to exercise the store's ordering contract.
    for (provider, id, timestamp) in [
        (RepositoryProvider::GitLab, "delivery-2", 2000),
        (RepositoryProvider::GitHub, "delivery-1", 1000),
    ] {
        let delivery = WebhookDelivery::new(
            provider,
            "owner".to_owned(),
            "repo".to_owned(),
            id.to_owned(),
            timestamp,
        )
        .unwrap();
        assert!(!index.delete_webhook_delivery(&delivery).unwrap());
        assert!(index.record_webhook_delivery(&delivery).unwrap());
        assert!(!index.record_webhook_delivery(&delivery).unwrap());
        deliveries.insert(0, delivery);
    }
    while !deliveries.is_empty() {
        assert_eq!(index.list_webhook_deliveries().unwrap(), deliveries);
        let deleted = deliveries.remove(0);
        assert!(index.delete_webhook_delivery(&deleted).unwrap());
        assert!(!index.delete_webhook_delivery(&deleted).unwrap());
    }
    assert!(index.list_webhook_deliveries().unwrap().is_empty());
}

#[test]
fn provider_repository_states_preserve_upserts_ordering_and_deletion() {
    let index = MemoryIndexStore::default();
    assert!(index.list_provider_repository_states().unwrap().is_empty());
    assert_eq!(
        index
            .provider_repository_state(RepositoryProvider::GitHub, "missing", "repo")
            .unwrap(),
        None
    );
    assert!(
        !index
            .delete_provider_repository_state(RepositoryProvider::GitHub, "missing", "repo")
            .unwrap()
    );

    let gitlab = ProviderRepositoryState::new(
        RepositoryProvider::GitLab,
        "owner-b".to_owned(),
        "repo-b".to_owned(),
        Some(300),
        Some(400),
        Some("rev".to_owned()),
    );
    let github = ProviderRepositoryState::new(
        RepositoryProvider::GitHub,
        "owner-a".to_owned(),
        "repo-a".to_owned(),
        Some(100),
        Some(200),
        None,
    );
    let current = ProviderRepositoryState::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        Some(1000),
        Some(2000),
        Some("rev".to_owned()),
    );
    for state in [&gitlab, &github, &current] {
        index.upsert_provider_repository_state(state).unwrap();
    }
    assert_eq!(
        index
            .provider_repository_state(RepositoryProvider::GitHub, "owner", "repo")
            .unwrap(),
        Some(current.clone())
    );
    let stale = ProviderRepositoryState::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        Some(100),
        Some(200),
        None,
    );
    // Older observations must not roll back the stored provider state.
    index.upsert_provider_repository_state(&stale).unwrap();
    let mut states = vec![current, github, gitlab];
    while !states.is_empty() {
        assert_eq!(index.list_provider_repository_states().unwrap(), states);
        let deleted = states.remove(0);
        assert!(
            index
                .delete_provider_repository_state(
                    deleted.provider(),
                    deleted.owner(),
                    deleted.repo()
                )
                .unwrap()
        );
        assert_eq!(
            index
                .provider_repository_state(deleted.provider(), deleted.owner(), deleted.repo())
                .unwrap(),
            None
        );
        assert!(
            !index
                .delete_provider_repository_state(
                    deleted.provider(),
                    deleted.owner(),
                    deleted.repo()
                )
                .unwrap()
        );
    }
    assert!(index.list_provider_repository_states().unwrap().is_empty());
}

// ── CasLimits constructors and accessors ──────────────────────────────

#[test]
fn cas_limits_new_constructs_with_provided_bounds() {
    let limits = CasLimits::new(
        NonZeroU64::MIN,
        NonZeroU64::MAX,
        NonZeroU64::new(3).unwrap(),
    );
    assert_eq!(limits.max_xorb_bytes(), NonZeroU64::MIN);
    assert_eq!(limits.max_shard_bytes(), NonZeroU64::MAX);
    assert_eq!(limits.max_object_bytes().get(), 3);
}

#[test]
fn cas_limits_accessors_return_configured_values() {
    let xorb = NonZeroU64::new(4096).unwrap();
    let shard = NonZeroU64::new(8192).unwrap();
    let max_object = NonZeroU64::new(16384).unwrap();
    let limits = CasLimits::new(xorb, shard, max_object);
    assert_eq!(limits.max_xorb_bytes(), xorb);
    assert_eq!(limits.max_shard_bytes(), shard);
    assert_eq!(limits.max_object_bytes(), max_object);
}

#[test]
fn cas_limits_default_bounds_use_provided_constants() {
    // NonZeroU64::MIN is the smallest positive value (1)
    let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MIN, NonZeroU64::MIN);
    assert_eq!(limits.max_xorb_bytes().get(), 1);
    assert_eq!(limits.max_shard_bytes().get(), 1);
    assert_eq!(limits.max_object_bytes().get(), 1);
}

// ── CasCoordinator constructors and accessors ─────────────────────────

#[test]
fn cas_coordinator_constructs_and_exposes_adapters_and_limits() {
    let index = MemoryIndexStore::default();
    let storage = tempfile::tempdir().unwrap();
    let object_store = LocalObjectStore::new(storage.path().join("objects")).unwrap();
    let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MAX, NonZeroU64::MIN);
    let coordinator = CasCoordinator::new(index, object_store, (), limits);

    // index() returns the stored index adapter
    let _index_ref: &MemoryIndexStore = coordinator.index();
    // object_store() returns the stored object store adapter
    let _store_ref: &LocalObjectStore = coordinator.object_store();
    // limits() returns the configured limits
    assert_eq!(coordinator.limits(), limits);
}

// ── Coordinator store_content_addressed_blob methods ─────────────────

#[tokio::test]
async fn coordinator_store_content_addressed_blob_enforces_size_limits() {
    for (name, max, body) in [
        ("oversized", 5, vec![0_u8; 10]),
        ("valid", 100, b"hello world".to_vec()),
        ("at-boundary", 5, b"hello".to_vec()),
        ("below-boundary", 5, b"abc".to_vec()),
    ] {
        let storage = tempfile::tempdir().unwrap();
        let objects = LocalObjectStore::new(storage.path().join("objects")).unwrap();
        let limits = CasLimits::new(
            NonZeroU64::new(100).unwrap(),
            NonZeroU64::new(100).unwrap(),
            NonZeroU64::new(max).unwrap(),
        );
        let coordinator = CasCoordinator::new((), objects.clone(), (), limits);
        let key = xorb_key("ab", name);
        let length = u64::try_from(body.len()).unwrap();
        let integrity = ObjectIntegrity::new(blake3_hash(&body), length);
        let result = coordinator
            .store_content_addressed_blob(&key, &integrity, body.clone())
            .await;

        if length > max {
            assert_eq!(
                result,
                Err(CasError::BodyTooLarge {
                    actual: length,
                    max
                }),
                "{name}"
            );
            assert!(!objects.contains(&key).unwrap(), "{name}");
        } else {
            assert_eq!(result, Ok(PutOutcome::Inserted), "{name}");
            assert_eq!(
                objects
                    .read_range(&key, ByteRange::new(0, length - 1).unwrap())
                    .unwrap(),
                body,
                "{name}"
            );
        }
    }
}

#[test]
fn with_upload_intent_create_intent_error_returns_record_error() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = tempfile::tempdir().unwrap();
    let object_store =
        SyncObjectStoreBridge::new(LocalObjectStore::new(storage.path().join("objects")).unwrap());
    let limits = CasLimits::new(
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(100).unwrap(),
    );
    let failing_store = FailingIntentStore;
    let coordinator = CasCoordinator::new(failing_store, object_store, (), limits);
    let intent = UploadIntent::new(
        "failing-intent".to_owned(),
        "objects/test".to_owned(),
        "abcdef".to_owned(),
        42,
    );

    let result: Result<i32, CasError> =
        rt.block_on(coordinator.with_upload_intent(&intent, || async { Ok(42) }));

    assert!(
        matches!(&result, Err(CasError::Record(msg)) if msg.contains("create_intent failed")),
        "expected Record error with 'create_intent failed', got: {result:?}"
    );
}

fn blake3_hash(bytes: &[u8]) -> ShardlineHash {
    let digest = blake3::hash(bytes);
    ShardlineHash::from_bytes(*digest.as_bytes())
}
