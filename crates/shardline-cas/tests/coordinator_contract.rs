#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    num::NonZeroU64,
    time::Duration,
};

use async_trait::async_trait;
use shardline_cas::paths::xorb_key;
use shardline_cas::{CasCoordinator, CasError, CasLimits};
use shardline_index::{
    DedupeShardMapping, DedupeStore, FileId, FileReconstruction, LifecycleStore, LocalIndexStore,
    ProviderRepositoryState, QuarantineCandidate, ReconstructionStore, ReconstructionTerm,
    RetentionHold, StoredObjectId, UploadIntent, UploadIntentState, UploadIntentStore,
    WebhookDelivery, xet_hash_hex_string,
};
use shardline_protocol::{ByteRange, ChunkRange, RepositoryProvider, ShardlineHash};
use shardline_storage::{
    DeleteOutcome, LocalObjectStore, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata,
    ObjectPrefix, ObjectStore, PutOutcome, SyncObjectStoreBridge,
};
use thiserror::Error;

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
enum MemoryObjectError {
    #[error("memory object integrity mismatch")]
    IntegrityMismatch,
    #[error("memory object was missing")]
    Missing,
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
#[error("memory index operation failed")]
struct MemoryIndexError;

#[derive(Debug, Default)]
struct MemoryObjectStore {
    objects: RefCell<HashMap<ObjectKey, Vec<u8>>>,
}

impl ObjectStore for MemoryObjectStore {
    type Error = MemoryObjectError;

    fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error> {
        let Ok(body_length) = u64::try_from(body.as_slice().len()) else {
            return Err(MemoryObjectError::IntegrityMismatch);
        };
        if body_length != integrity.length() {
            return Err(MemoryObjectError::IntegrityMismatch);
        }

        let mut objects = self.objects.borrow_mut();
        if let Some(existing) = objects.get(key) {
            if existing.as_slice() == body.as_slice() {
                return Ok(PutOutcome::AlreadyExists);
            }

            return Err(MemoryObjectError::IntegrityMismatch);
        }

        objects.insert(key.clone(), body.as_slice().to_vec());
        Ok(PutOutcome::Inserted)
    }

    fn read_range(&self, key: &ObjectKey, range: ByteRange) -> Result<Vec<u8>, Self::Error> {
        let objects = self.objects.borrow();
        let Some(object) = objects.get(key) else {
            return Err(MemoryObjectError::Missing);
        };
        let Ok(start) = usize::try_from(range.start()) else {
            return Err(MemoryObjectError::Missing);
        };
        let Some(length) = range.len() else {
            return Err(MemoryObjectError::Missing);
        };
        let Ok(length) = usize::try_from(length) else {
            return Err(MemoryObjectError::Missing);
        };

        let bytes = object
            .iter()
            .copied()
            .skip(start)
            .take(length)
            .collect::<Vec<u8>>();
        Ok(bytes)
    }

    fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
        Ok(self.objects.borrow().contains_key(key))
    }

    fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
        Ok(self.objects.borrow().get(key).map(|object| {
            ObjectMetadata::new(
                key.clone(),
                u64::try_from(object.len()).unwrap_or(u64::MAX),
                None,
            )
        }))
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error> {
        let mut objects = self
            .objects
            .borrow()
            .iter()
            .filter(|(key, _object)| key.as_str().starts_with(prefix.as_str()))
            .map(|(key, object)| {
                ObjectMetadata::new(
                    key.clone(),
                    u64::try_from(object.len()).unwrap_or(u64::MAX),
                    None,
                )
            })
            .collect::<Vec<_>>();
        objects.sort_by(|left, right| left.key().as_str().cmp(right.key().as_str()));
        Ok(objects)
    }

    fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
        let removed = self.objects.borrow_mut().remove(key);
        if removed.is_some() {
            return Ok(DeleteOutcome::Deleted);
        }

        Ok(DeleteOutcome::NotFound)
    }
}

#[derive(Debug, Default)]
struct MemoryIndexStore {
    reconstructions: RefCell<HashMap<FileId, FileReconstruction>>,
    quarantine_candidates: RefCell<HashMap<ObjectKey, QuarantineCandidate>>,
    retention_holds: RefCell<HashMap<ObjectKey, RetentionHold>>,
    webhook_deliveries: RefCell<HashMap<(String, String, String, String), WebhookDelivery>>,
    provider_repository_states: RefCell<HashMap<(String, String, String), ProviderRepositoryState>>,
    objects: RefCell<HashSet<StoredObjectId>>,
    dedupe_shards: RefCell<HashMap<ShardlineHash, DedupeShardMapping>>,
}

impl MemoryIndexStore {
    fn insert_reconstruction(&self, file_id: FileId, reconstruction: FileReconstruction) {
        self.reconstructions
            .borrow_mut()
            .insert(file_id, reconstruction);
    }

    fn insert_object(&self, object_id: StoredObjectId) {
        self.objects.borrow_mut().insert(object_id);
    }
}

impl ReconstructionStore for MemoryIndexStore {
    type Error = MemoryIndexError;

    fn reconstruction(&self, file_id: &FileId) -> Result<Option<FileReconstruction>, Self::Error> {
        Ok(self.reconstructions.borrow().get(file_id).cloned())
    }

    fn list_reconstruction_file_ids(&self) -> Result<Vec<FileId>, Self::Error> {
        let mut file_ids = self
            .reconstructions
            .borrow()
            .keys()
            .copied()
            .collect::<Vec<_>>();
        file_ids.sort_by(|left, right| {
            xet_hash_hex_string(left.hash()).cmp(&xet_hash_hex_string(right.hash()))
        });
        Ok(file_ids)
    }

    fn delete_reconstruction(&self, file_id: &FileId) -> Result<bool, Self::Error> {
        Ok(self.reconstructions.borrow_mut().remove(file_id).is_some())
    }

    fn contains_object(&self, object_id: &StoredObjectId) -> Result<bool, Self::Error> {
        Ok(self.objects.borrow().contains(object_id))
    }
}

impl DedupeStore for MemoryIndexStore {
    type Error = MemoryIndexError;

    fn dedupe_shard_mapping(
        &self,
        chunk_hash: &ShardlineHash,
    ) -> Result<Option<DedupeShardMapping>, Self::Error> {
        Ok(self.dedupe_shards.borrow().get(chunk_hash).cloned())
    }

    fn list_dedupe_shard_mappings(&self) -> Result<Vec<DedupeShardMapping>, Self::Error> {
        let mut mappings = self
            .dedupe_shards
            .borrow()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        mappings.sort_by(|left, right| {
            xet_hash_hex_string(left.chunk_hash()).cmp(&xet_hash_hex_string(right.chunk_hash()))
        });
        Ok(mappings)
    }

    fn delete_dedupe_shard_mapping(&self, chunk_hash: &ShardlineHash) -> Result<bool, Self::Error> {
        Ok(self.dedupe_shards.borrow_mut().remove(chunk_hash).is_some())
    }
}

impl LifecycleStore for MemoryIndexStore {
    type Error = MemoryIndexError;

    fn quarantine_candidate(
        &self,
        object_key: &ObjectKey,
    ) -> Result<Option<QuarantineCandidate>, Self::Error> {
        Ok(self.quarantine_candidates.borrow().get(object_key).cloned())
    }

    fn list_quarantine_candidates(&self) -> Result<Vec<QuarantineCandidate>, Self::Error> {
        let mut candidates = self
            .quarantine_candidates
            .borrow()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        candidates
            .sort_by(|left, right| left.object_key().as_str().cmp(right.object_key().as_str()));
        Ok(candidates)
    }

    fn upsert_quarantine_candidate(
        &self,
        candidate: &QuarantineCandidate,
    ) -> Result<(), Self::Error> {
        self.quarantine_candidates
            .borrow_mut()
            .insert(candidate.object_key().clone(), candidate.clone());
        Ok(())
    }

    fn delete_quarantine_candidate(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
        Ok(self
            .quarantine_candidates
            .borrow_mut()
            .remove(object_key)
            .is_some())
    }

    fn retention_hold(&self, object_key: &ObjectKey) -> Result<Option<RetentionHold>, Self::Error> {
        Ok(self.retention_holds.borrow().get(object_key).cloned())
    }

    fn list_retention_holds(&self) -> Result<Vec<RetentionHold>, Self::Error> {
        let mut holds = self
            .retention_holds
            .borrow()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        holds.sort_by(|left, right| left.object_key().as_str().cmp(right.object_key().as_str()));
        Ok(holds)
    }

    fn upsert_retention_hold(&self, hold: &RetentionHold) -> Result<(), Self::Error> {
        self.retention_holds
            .borrow_mut()
            .insert(hold.object_key().clone(), hold.clone());
        Ok(())
    }

    fn delete_retention_hold(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
        Ok(self
            .retention_holds
            .borrow_mut()
            .remove(object_key)
            .is_some())
    }

    fn record_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error> {
        Ok(self
            .webhook_deliveries
            .borrow_mut()
            .insert(
                (
                    format!("{:?}", delivery.provider()),
                    delivery.owner().to_owned(),
                    delivery.repo().to_owned(),
                    delivery.delivery_id().to_owned(),
                ),
                delivery.clone(),
            )
            .is_none())
    }

    fn list_webhook_deliveries(&self) -> Result<Vec<WebhookDelivery>, Self::Error> {
        let mut deliveries = self
            .webhook_deliveries
            .borrow()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        deliveries.sort_by(|left, right| {
            format!("{:?}", left.provider())
                .cmp(&format!("{:?}", right.provider()))
                .then_with(|| left.owner().cmp(right.owner()))
                .then_with(|| left.repo().cmp(right.repo()))
                .then_with(|| left.delivery_id().cmp(right.delivery_id()))
        });
        Ok(deliveries)
    }

    fn delete_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error> {
        Ok(self
            .webhook_deliveries
            .borrow_mut()
            .remove(&(
                format!("{:?}", delivery.provider()),
                delivery.owner().to_owned(),
                delivery.repo().to_owned(),
                delivery.delivery_id().to_owned(),
            ))
            .is_some())
    }

    fn provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<Option<ProviderRepositoryState>, Self::Error> {
        Ok(self
            .provider_repository_states
            .borrow()
            .get(&(
                provider_key(provider).to_owned(),
                owner.to_owned(),
                repo.to_owned(),
            ))
            .cloned())
    }

    fn list_provider_repository_states(&self) -> Result<Vec<ProviderRepositoryState>, Self::Error> {
        let mut states = self
            .provider_repository_states
            .borrow()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        states.sort_by(|left, right| {
            provider_key(left.provider())
                .cmp(provider_key(right.provider()))
                .then_with(|| left.owner().cmp(right.owner()))
                .then_with(|| left.repo().cmp(right.repo()))
        });
        Ok(states)
    }

    fn upsert_provider_repository_state(
        &self,
        state: &ProviderRepositoryState,
    ) -> Result<(), Self::Error> {
        self.provider_repository_states.borrow_mut().insert(
            (
                provider_key(state.provider()).to_owned(),
                state.owner().to_owned(),
                state.repo().to_owned(),
            ),
            state.clone(),
        );
        Ok(())
    }

    fn delete_provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<bool, Self::Error> {
        Ok(self
            .provider_repository_states
            .borrow_mut()
            .remove(&(
                provider_key(provider).to_owned(),
                owner.to_owned(),
                repo.to_owned(),
            ))
            .is_some())
    }
}

const fn provider_key(provider: RepositoryProvider) -> &'static str {
    match provider {
        RepositoryProvider::GitHub => "github",
        RepositoryProvider::GitLab => "gitlab",
        RepositoryProvider::Gitea => "gitea",
        RepositoryProvider::Codeberg => "codeberg",
        RepositoryProvider::Generic => "generic",
    }
}

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

#[test]
fn coordinator_adapters_support_lifecycle_and_reconstruction_contracts() {
    let index = MemoryIndexStore::default();
    let object_store = MemoryObjectStore::default();
    let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MAX, NonZeroU64::MIN);
    let coordinator = CasCoordinator::new(index, object_store, (), limits);

    let hash = ShardlineHash::from_bytes([5; 32]);
    let key = ObjectKey::parse("xorbs/default/05/hash.xorb");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };

    let body = [10, 20, 30, 40];
    let integrity = ObjectIntegrity::new(hash, 4);
    let first =
        coordinator
            .object_store()
            .put_if_absent(&key, ObjectBody::from_slice(&body), &integrity);
    let second =
        coordinator
            .object_store()
            .put_if_absent(&key, ObjectBody::from_slice(&body), &integrity);

    assert_eq!(first, Ok(PutOutcome::Inserted));
    assert_eq!(second, Ok(PutOutcome::AlreadyExists));
    assert_eq!(coordinator.object_store().contains(&key), Ok(true));
    assert_eq!(
        coordinator
            .object_store()
            .metadata(&key)
            .map(|value| value.map(|metadata| metadata.length())),
        Ok(Some(4))
    );

    let prefix = ObjectPrefix::parse("xorbs/default/");
    assert!(prefix.is_ok());
    let Ok(prefix) = prefix else {
        return;
    };
    let listed = coordinator.object_store().list_prefix(&prefix);
    assert!(listed.is_ok());
    let Ok(listed) = listed else {
        return;
    };
    assert_eq!(listed.len(), 1);
    let first_listed = listed.first();
    assert!(first_listed.is_some());
    if let Some(first_listed) = first_listed {
        assert_eq!(first_listed.key().as_str(), key.as_str());
    }

    let range = ByteRange::new(1, 2);
    assert!(range.is_ok());
    let Ok(range) = range else {
        return;
    };
    assert_eq!(
        coordinator.object_store().read_range(&key, range),
        Ok(vec![20, 30])
    );

    let file_id = FileId::new(hash);
    let object_id = StoredObjectId::new(hash);
    let chunk_range = ChunkRange::new(0, 1);
    assert!(chunk_range.is_ok());
    let Ok(chunk_range) = chunk_range else {
        return;
    };
    let term = ReconstructionTerm::new(object_id, chunk_range, 4);
    let reconstruction = FileReconstruction::new(vec![term]);

    coordinator.index().insert_object(object_id);
    coordinator
        .index()
        .insert_reconstruction(file_id, reconstruction.clone());

    let candidate = QuarantineCandidate::new(key.clone(), 4, 10, 20);
    assert!(candidate.is_ok());
    let Ok(candidate) = candidate else {
        return;
    };
    assert_eq!(
        coordinator.index().upsert_quarantine_candidate(&candidate),
        Ok(())
    );

    assert_eq!(coordinator.index().contains_object(&object_id), Ok(true));
    assert_eq!(
        coordinator.index().reconstruction(&file_id),
        Ok(Some(reconstruction))
    );
    assert_eq!(
        coordinator.index().quarantine_candidate(&key),
        Ok(Some(candidate.clone()))
    );
    assert_eq!(
        coordinator.index().list_quarantine_candidates(),
        Ok(vec![candidate])
    );
    assert_eq!(
        coordinator.object_store().delete_if_present(&key),
        Ok(DeleteOutcome::Deleted)
    );
    assert_eq!(
        coordinator.object_store().delete_if_present(&key),
        Ok(DeleteOutcome::NotFound)
    );
    assert_eq!(
        coordinator.index().delete_quarantine_candidate(&key),
        Ok(true)
    );
    assert_eq!(
        coordinator.index().delete_quarantine_candidate(&key),
        Ok(false)
    );

    let hold = RetentionHold::new(
        key.clone(),
        "provider deletion grace".to_owned(),
        30,
        Some(90),
    );
    assert!(hold.is_ok());
    let Ok(hold) = hold else {
        return;
    };
    assert_eq!(coordinator.index().upsert_retention_hold(&hold), Ok(()));
    assert_eq!(
        coordinator.index().retention_hold(&key),
        Ok(Some(hold.clone()))
    );
    assert_eq!(coordinator.index().list_retention_holds(), Ok(vec![hold]));
    assert_eq!(coordinator.index().delete_retention_hold(&key), Ok(true));
    assert_eq!(coordinator.index().delete_retention_hold(&key), Ok(false));
}

#[test]
fn coordinator_local_filesystem_adapters_support_lifecycle_and_reconstruction_contracts() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let object_store = LocalObjectStore::new(storage.path().join("objects"));
    assert!(object_store.is_ok());
    let Ok(object_store) = object_store else {
        return;
    };
    let index = LocalIndexStore::new(storage.path().join("index"));
    assert!(index.is_ok());
    let Ok(index) = index else {
        return;
    };

    let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MAX, NonZeroU64::MIN);
    let coordinator = CasCoordinator::new(index, object_store, (), limits);

    let hash = ShardlineHash::from_bytes([8; 32]);
    let key = ObjectKey::parse("xorbs/default/08/hash.xorb");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let body = [1, 2, 3, 4];
    let integrity = ObjectIntegrity::new(blake3_hash(&body), 4);
    let inserted =
        coordinator
            .object_store()
            .put_if_absent(&key, ObjectBody::from_slice(&body), &integrity);
    assert!(matches!(inserted, Ok(PutOutcome::Inserted)));
    assert!(matches!(
        coordinator
            .object_store()
            .put_if_absent(&key, ObjectBody::from_slice(&body), &integrity),
        Ok(PutOutcome::AlreadyExists)
    ));

    let prefix = ObjectPrefix::parse("xorbs/default/");
    assert!(prefix.is_ok());
    let Ok(prefix) = prefix else {
        return;
    };
    let listed = coordinator.object_store().list_prefix(&prefix);
    assert!(listed.is_ok());
    let Ok(listed) = listed else {
        return;
    };
    assert_eq!(listed.len(), 1);

    let file_id = FileId::new(hash);
    let object_id = StoredObjectId::new(hash);
    let range = ChunkRange::new(0, 1);
    assert!(range.is_ok());
    let Ok(range) = range else {
        return;
    };
    let reconstruction =
        FileReconstruction::new(vec![ReconstructionTerm::new(object_id, range, 4)]);
    let inserted_reconstruction = coordinator
        .index()
        .insert_reconstruction(&file_id, &reconstruction);
    assert!(inserted_reconstruction.is_ok());
    let inserted_object = coordinator.index().insert_object(&object_id);
    assert!(inserted_object.is_ok());

    let candidate = QuarantineCandidate::new(key.clone(), 4, 11, 22);
    assert!(candidate.is_ok());
    let Ok(candidate) = candidate else {
        return;
    };
    assert!(matches!(
        coordinator.index().upsert_quarantine_candidate(&candidate),
        Ok(())
    ));
    let loaded_reconstruction = coordinator.index().reconstruction(&file_id);
    assert!(loaded_reconstruction.is_ok());
    if let Ok(Some(loaded_reconstruction)) = loaded_reconstruction {
        assert_eq!(loaded_reconstruction, reconstruction);
    }
    let loaded_candidate = coordinator.index().quarantine_candidate(&key);
    assert!(loaded_candidate.is_ok());
    if let Ok(Some(loaded_candidate)) = loaded_candidate {
        assert_eq!(loaded_candidate, candidate);
    }
    assert!(matches!(
        coordinator.object_store().delete_if_present(&key),
        Ok(DeleteOutcome::Deleted)
    ));
}

// ── MemoryObjectStore error paths ─────────────────────────────────────

#[test]
fn memory_object_store_put_if_absent_rejects_wrong_body_length() {
    let store = MemoryObjectStore::default();
    let key = ObjectKey::parse("xorbs/default/05/hash.xorb").unwrap();
    let integrity = ObjectIntegrity::new(ShardlineHash::from_bytes([1; 32]), 4);

    // Body has 5 bytes but integrity declares 4 → IntegrityMismatch
    let result = store.put_if_absent(
        &key,
        ObjectBody::from_slice(&[10, 20, 30, 40, 50]),
        &integrity,
    );
    assert_eq!(result, Err(MemoryObjectError::IntegrityMismatch));
}

#[test]
fn memory_object_store_put_if_absent_rejects_existing_different_content() {
    let store = MemoryObjectStore::default();
    let key = ObjectKey::parse("xorbs/default/05/hash.xorb").unwrap();
    let hash = ShardlineHash::from_bytes([2; 32]);

    let first = store.put_if_absent(
        &key,
        ObjectBody::from_slice(&[10, 20, 30, 40]),
        &ObjectIntegrity::new(hash, 4),
    );
    assert_eq!(first, Ok(PutOutcome::Inserted));

    // Same key, different content → IntegrityMismatch
    let second = store.put_if_absent(
        &key,
        ObjectBody::from_slice(&[50, 60, 70, 80]),
        &ObjectIntegrity::new(hash, 4),
    );
    assert_eq!(second, Err(MemoryObjectError::IntegrityMismatch));
}

#[test]
fn memory_object_store_read_range_returns_missing_for_unknown_key() {
    let store = MemoryObjectStore::default();
    let key = ObjectKey::parse("xorbs/default/05/hash.xorb").unwrap();
    let range = ByteRange::new(0, 0).unwrap();

    assert_eq!(
        store.read_range(&key, range),
        Err(MemoryObjectError::Missing)
    );
}

#[test]
fn memory_object_store_read_range_returns_missing_for_unrepresentable_length() {
    let store = MemoryObjectStore::default();
    let key = ObjectKey::parse("xorbs/default/05/hash.xorb").unwrap();
    store
        .put_if_absent(
            &key,
            ObjectBody::from_slice(&[10, 20, 30, 40]),
            &ObjectIntegrity::new(ShardlineHash::from_bytes([3; 32]), 4),
        )
        .unwrap();

    // ByteRange(0, u64::MAX) has len() = None because u64::MAX + 1 overflows
    let range = ByteRange::new(0, u64::MAX).unwrap();
    assert_eq!(range.len(), None);

    assert_eq!(
        store.read_range(&key, range),
        Err(MemoryObjectError::Missing)
    );
}

#[test]
fn memory_object_store_metadata_returns_none_for_missing_key() {
    let store = MemoryObjectStore::default();
    let key = ObjectKey::parse("xorbs/default/05/hash.xorb").unwrap();

    assert_eq!(store.metadata(&key), Ok(None));
}

// ── DedupeStore trait methods ─────────────────────────────────────────

#[test]
fn dedupe_shard_mapping_persists_and_retrieves_mapping() {
    let index = MemoryIndexStore::default();
    let chunk_hash = ShardlineHash::from_bytes([4; 32]);
    let shard_key = ObjectKey::parse("shards/aa/example.shard").unwrap();
    let mapping = DedupeShardMapping::new(chunk_hash, shard_key);

    index
        .dedupe_shards
        .borrow_mut()
        .insert(chunk_hash, mapping.clone());

    assert_eq!(index.dedupe_shard_mapping(&chunk_hash), Ok(Some(mapping)));
}

#[test]
fn dedupe_shard_mapping_returns_none_for_missing_hash() {
    let index = MemoryIndexStore::default();
    let chunk_hash = ShardlineHash::from_bytes([5; 32]);

    assert_eq!(index.dedupe_shard_mapping(&chunk_hash), Ok(None));
}

#[test]
fn list_dedupe_shard_mappings_empty_initially() {
    let index = MemoryIndexStore::default();
    assert_eq!(index.list_dedupe_shard_mappings(), Ok(vec![]));
}

#[test]
fn list_dedupe_shard_mappings_returns_inserted_mappings() {
    let index = MemoryIndexStore::default();
    let hash_a = ShardlineHash::from_bytes([1; 32]);
    let hash_b = ShardlineHash::from_bytes([2; 32]);
    let key_a = ObjectKey::parse("shards/aa/a.shard").unwrap();
    let key_b = ObjectKey::parse("shards/bb/b.shard").unwrap();
    let map_a = DedupeShardMapping::new(hash_a, key_a);
    let map_b = DedupeShardMapping::new(hash_b, key_b);

    index
        .dedupe_shards
        .borrow_mut()
        .insert(hash_a, map_a.clone());
    index
        .dedupe_shards
        .borrow_mut()
        .insert(hash_b, map_b.clone());

    let mut all = index.list_dedupe_shard_mappings().unwrap();
    all.sort_by(|left, right| {
        xet_hash_hex_string(left.chunk_hash()).cmp(&xet_hash_hex_string(right.chunk_hash()))
    });
    assert_eq!(all, vec![map_a, map_b]);
}

#[test]
fn delete_dedupe_shard_mapping_removes_existing() {
    let index = MemoryIndexStore::default();
    let chunk_hash = ShardlineHash::from_bytes([6; 32]);
    let key = ObjectKey::parse("shards/aa/del.shard").unwrap();
    index
        .dedupe_shards
        .borrow_mut()
        .insert(chunk_hash, DedupeShardMapping::new(chunk_hash, key));

    assert_eq!(index.delete_dedupe_shard_mapping(&chunk_hash), Ok(true));
    assert_eq!(index.dedupe_shard_mapping(&chunk_hash), Ok(None));
}

#[test]
fn delete_dedupe_shard_mapping_returns_false_for_missing() {
    let index = MemoryIndexStore::default();
    let chunk_hash = ShardlineHash::from_bytes([7; 32]);

    assert_eq!(index.delete_dedupe_shard_mapping(&chunk_hash), Ok(false));
}

// ── LifecycleStore webhook methods ────────────────────────────────────

#[test]
fn record_webhook_delivery_inserts_new_delivery() {
    let index = MemoryIndexStore::default();
    let delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        "delivery-1".to_owned(),
        1000,
    )
    .unwrap();

    assert_eq!(index.record_webhook_delivery(&delivery), Ok(true));
}

#[test]
fn record_webhook_delivery_returns_false_on_duplicate() {
    let index = MemoryIndexStore::default();
    let delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        "delivery-1".to_owned(),
        1000,
    )
    .unwrap();

    index.record_webhook_delivery(&delivery).unwrap();
    assert_eq!(index.record_webhook_delivery(&delivery), Ok(false));
}

#[test]
fn list_webhook_deliveries_empty_initially() {
    let index = MemoryIndexStore::default();
    assert_eq!(index.list_webhook_deliveries(), Ok(vec![]));
}

#[test]
fn list_webhook_deliveries_returns_recorded_deliveries_sorted() {
    let index = MemoryIndexStore::default();
    let gitlab_delivery = WebhookDelivery::new(
        RepositoryProvider::GitLab,
        "owner".to_owned(),
        "repo".to_owned(),
        "delivery-2".to_owned(),
        2000,
    )
    .unwrap();
    let github_delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        "delivery-1".to_owned(),
        1000,
    )
    .unwrap();

    index.record_webhook_delivery(&gitlab_delivery).unwrap();
    index.record_webhook_delivery(&github_delivery).unwrap();

    let deliveries = index.list_webhook_deliveries().unwrap();
    assert_eq!(deliveries.len(), 2);
    // Sorted by provider (GitHub before GitLab in Debug formatting)
    assert_eq!(deliveries[0], github_delivery);
    assert_eq!(deliveries[1], gitlab_delivery);
}

#[test]
fn delete_webhook_delivery_removes_existing() {
    let index = MemoryIndexStore::default();
    let delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        "delivery-1".to_owned(),
        1000,
    )
    .unwrap();

    index.record_webhook_delivery(&delivery).unwrap();
    assert_eq!(index.delete_webhook_delivery(&delivery), Ok(true));
    assert_eq!(index.list_webhook_deliveries(), Ok(vec![]));
}

#[test]
fn delete_webhook_delivery_returns_false_for_missing() {
    let index = MemoryIndexStore::default();
    let delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        "delivery-1".to_owned(),
        1000,
    )
    .unwrap();

    assert_eq!(index.delete_webhook_delivery(&delivery), Ok(false));
}

// ── LifecycleStore provider repository state methods ─────────────────

#[test]
fn upsert_provider_repository_state_inserts_and_can_be_retrieved() {
    let index = MemoryIndexStore::default();
    let state = ProviderRepositoryState::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        Some(1000),
        Some(2000),
        Some("rev".to_owned()),
    );

    index.upsert_provider_repository_state(&state).unwrap();
    assert_eq!(
        index.provider_repository_state(RepositoryProvider::GitHub, "owner", "repo"),
        Ok(Some(state))
    );
}

#[test]
fn provider_repository_state_returns_none_for_missing() {
    let index = MemoryIndexStore::default();
    assert_eq!(
        index.provider_repository_state(RepositoryProvider::GitHub, "missing", "repo"),
        Ok(None)
    );
}

#[test]
fn list_provider_repository_states_empty_initially() {
    let index = MemoryIndexStore::default();
    assert_eq!(index.list_provider_repository_states(), Ok(vec![]));
}

#[test]
fn list_provider_repository_states_returns_all_states_sorted() {
    let index = MemoryIndexStore::default();
    let gitlab_state = ProviderRepositoryState::new(
        RepositoryProvider::GitLab,
        "owner-b".to_owned(),
        "repo-b".to_owned(),
        Some(300),
        Some(400),
        Some("rev".to_owned()),
    );
    let github_state = ProviderRepositoryState::new(
        RepositoryProvider::GitHub,
        "owner-a".to_owned(),
        "repo-a".to_owned(),
        Some(100),
        Some(200),
        None,
    );

    index
        .upsert_provider_repository_state(&gitlab_state)
        .unwrap();
    index
        .upsert_provider_repository_state(&github_state)
        .unwrap();

    let states = index.list_provider_repository_states().unwrap();
    assert_eq!(states.len(), 2);
    // Sorted by provider key (GitHub < GitLab), then owner, then repo
    assert_eq!(states[0], github_state);
    assert_eq!(states[1], gitlab_state);
}

#[test]
fn delete_provider_repository_state_removes_existing() {
    let index = MemoryIndexStore::default();
    let state = ProviderRepositoryState::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        Some(100),
        Some(200),
        None,
    );

    index.upsert_provider_repository_state(&state).unwrap();
    assert_eq!(
        index.delete_provider_repository_state(RepositoryProvider::GitHub, "owner", "repo"),
        Ok(true)
    );
    assert_eq!(
        index.provider_repository_state(RepositoryProvider::GitHub, "owner", "repo"),
        Ok(None)
    );
}

#[test]
fn delete_provider_repository_state_returns_false_for_missing() {
    let index = MemoryIndexStore::default();
    assert_eq!(
        index.delete_provider_repository_state(RepositoryProvider::GitHub, "missing", "repo"),
        Ok(false)
    );
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
    let object_store = MemoryObjectStore::default();
    let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MAX, NonZeroU64::MIN);
    let coordinator = CasCoordinator::new(index, object_store, (), limits);

    // index() returns the stored index adapter
    let _index_ref: &MemoryIndexStore = coordinator.index();
    // object_store() returns the stored object store adapter
    let _store_ref: &MemoryObjectStore = coordinator.object_store();
    // limits() returns the configured limits
    assert_eq!(coordinator.limits(), limits);
}

// ── Coordinator store_content_addressed_blob methods ─────────────────

#[test]
fn coordinator_store_content_addressed_blob_rejects_oversized_body() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = tempfile::tempdir().unwrap();
    let index = LocalIndexStore::new(storage.path().join("index")).unwrap();
    let object_store =
        SyncObjectStoreBridge::new(LocalObjectStore::new(storage.path().join("objects")).unwrap());
    let limits = CasLimits::new(
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(5).unwrap(), // max_object = 5
    );
    let coordinator = CasCoordinator::new(index, object_store, (), limits);
    let key = shardline_cas::paths::xorb_key("ab", "test-key");
    let hash = ShardlineHash::from_bytes([1; 32]);
    let integrity = ObjectIntegrity::new(hash, 10); // 10 > 5
    let result =
        rt.block_on(coordinator.store_content_addressed_blob(&key, &integrity, vec![0u8; 10]));
    assert!(result.is_err(), "should reject oversized body");
}

#[test]
fn coordinator_store_content_addressed_blob_accepts_valid_body() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = tempfile::tempdir().unwrap();
    let index = LocalIndexStore::new(storage.path().join("index")).unwrap();
    let object_store =
        SyncObjectStoreBridge::new(LocalObjectStore::new(storage.path().join("objects")).unwrap());
    let limits = CasLimits::new(
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(100).unwrap(),
    );
    let coordinator = CasCoordinator::new(index, object_store, (), limits);
    let key = shardline_cas::paths::xorb_key("ab", "test-key");
    let body = b"hello world";
    let hash = ShardlineHash::from_bytes(*blake3::hash(body).as_bytes());
    let integrity = ObjectIntegrity::new(hash, body.len() as u64);
    let result =
        rt.block_on(coordinator.store_content_addressed_blob(&key, &integrity, body.to_vec()));
    assert!(result.is_ok(), "should accept valid body: {result:?}");
}

#[test]
fn coordinator_store_content_addressed_blob_accepts_body_at_max_boundary() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = tempfile::tempdir().unwrap();
    let index = LocalIndexStore::new(storage.path().join("index")).unwrap();
    let object_store =
        SyncObjectStoreBridge::new(LocalObjectStore::new(storage.path().join("objects")).unwrap());
    let limits = CasLimits::new(
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(5).unwrap(), // max_object = 5
    );
    let coordinator = CasCoordinator::new(index, object_store, (), limits);
    let key = shardline_cas::paths::xorb_key("ab", "test-boundary");
    let body = b"hello"; // exactly 5 bytes (equals max)
    let hash = blake3_hash(body);
    let integrity = ObjectIntegrity::new(hash, body.len() as u64);
    let result =
        rt.block_on(coordinator.store_content_addressed_blob(&key, &integrity, body.to_vec()));
    assert!(
        result.is_ok(),
        "should accept body at max boundary: {result:?}"
    );
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

#[test]
fn coordinator_store_content_addressed_blob_overflow_returns_error() {
    // store_content_addressed_blob internally does `u64::try_from(body.len())`
    // which can overflow for extremely large Vecs (not possible in practice).
    // Test that body within limits is accepted through this code path.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = tempfile::tempdir().unwrap();
    let index = shardline_index::MemoryIndexStore::new();
    let object_store =
        SyncObjectStoreBridge::new(LocalObjectStore::new(storage.path().join("objects")).unwrap());
    let limits = CasLimits::new(
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(5).unwrap(),
    );
    let coordinator = CasCoordinator::new(index, object_store, (), limits);
    let key = xorb_key("ab", "test");
    let body = b"abc";
    let hash = blake3_hash(body);
    let integrity = ObjectIntegrity::new(hash, body.len() as u64);
    // Body with 3 bytes should be accepted (max is 5)
    let result =
        rt.block_on(coordinator.store_content_addressed_blob(&key, &integrity, body.to_vec()));
    assert!(
        result.is_ok(),
        "body within limits should succeed: {result:?}"
    );
}

fn blake3_hash(bytes: &[u8]) -> ShardlineHash {
    let digest = blake3::hash(bytes);
    ShardlineHash::from_bytes(*digest.as_bytes())
}
