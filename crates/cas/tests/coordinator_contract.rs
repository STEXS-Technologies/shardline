use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    num::NonZeroU64,
};

use shardline_cas::{CasCoordinator, CasLimits};
use shardline_index::{
    DedupeShardMapping, FileId, FileReconstruction, IndexStore, LocalIndexStore,
    ProviderRepositoryState, QuarantineCandidate, ReconstructionTerm, RetentionHold,
    WebhookDelivery, XorbId,
};
use shardline_protocol::{ByteRange, ChunkRange, RepositoryProvider, ShardlineHash};
use shardline_storage::{
    DeleteOutcome, LocalObjectStore, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata,
    ObjectPrefix, ObjectStore, PutOutcome,
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
    xorbs: RefCell<HashSet<XorbId>>,
    dedupe_shards: RefCell<HashMap<ShardlineHash, DedupeShardMapping>>,
}

impl MemoryIndexStore {
    fn insert_reconstruction(&self, file_id: FileId, reconstruction: FileReconstruction) {
        self.reconstructions
            .borrow_mut()
            .insert(file_id, reconstruction);
    }

    fn insert_xorb(&self, xorb_id: XorbId) {
        self.xorbs.borrow_mut().insert(xorb_id);
    }
}

impl IndexStore for MemoryIndexStore {
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
            left.hash()
                .api_hex_string()
                .cmp(&right.hash().api_hex_string())
        });
        Ok(file_ids)
    }

    fn delete_reconstruction(&self, file_id: &FileId) -> Result<bool, Self::Error> {
        Ok(self.reconstructions.borrow_mut().remove(file_id).is_some())
    }

    fn contains_xorb(&self, xorb_id: &XorbId) -> Result<bool, Self::Error> {
        Ok(self.xorbs.borrow().contains(xorb_id))
    }

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
            left.chunk_hash()
                .api_hex_string()
                .cmp(&right.chunk_hash().api_hex_string())
        });
        Ok(mappings)
    }

    fn delete_dedupe_shard_mapping(&self, chunk_hash: &ShardlineHash) -> Result<bool, Self::Error> {
        Ok(self.dedupe_shards.borrow_mut().remove(chunk_hash).is_some())
    }

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
        RepositoryProvider::Generic => "generic",
    }
}

#[test]
fn coordinator_adapters_support_lifecycle_and_reconstruction_contracts() {
    let index = MemoryIndexStore::default();
    let object_store = MemoryObjectStore::default();
    let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MAX);
    let coordinator = CasCoordinator::new(index, object_store, limits);

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
    let xorb_id = XorbId::new(hash);
    let chunk_range = ChunkRange::new(0, 1);
    assert!(chunk_range.is_ok());
    let Ok(chunk_range) = chunk_range else {
        return;
    };
    let term = ReconstructionTerm::new(xorb_id, chunk_range, 4);
    let reconstruction = FileReconstruction::new(vec![term]);

    coordinator.index().insert_xorb(xorb_id);
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

    assert_eq!(coordinator.index().contains_xorb(&xorb_id), Ok(true));
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

    let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MAX);
    let coordinator = CasCoordinator::new(index, object_store, limits);

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
    let xorb_id = XorbId::new(hash);
    let range = ChunkRange::new(0, 1);
    assert!(range.is_ok());
    let Ok(range) = range else {
        return;
    };
    let reconstruction = FileReconstruction::new(vec![ReconstructionTerm::new(xorb_id, range, 4)]);
    let inserted_reconstruction = coordinator
        .index()
        .insert_reconstruction(&file_id, &reconstruction);
    assert!(inserted_reconstruction.is_ok());
    let inserted_xorb = coordinator.index().insert_xorb(&xorb_id);
    assert!(inserted_xorb.is_ok());

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

fn blake3_hash(bytes: &[u8]) -> ShardlineHash {
    let digest = blake3::hash(bytes);
    ShardlineHash::from_bytes(*digest.as_bytes())
}
