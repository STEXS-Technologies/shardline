use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use serde_json::{Error as SerdeJsonError, to_vec};
use shardline_protocol::{RepositoryProvider, RepositoryScope, ShardlineHash};
use shardline_storage::ObjectKey;
use thiserror::Error;

use crate::{
    AsyncIndexStore, DedupeShardMapping, DedupeStore, FileId, FileReconstruction, FileRecord,
    IndexStoreFuture, LifecycleStore, ProviderRepositoryState, QuarantineCandidate,
    ReconstructionStore, RecordMutation, RecordStoreFuture, RecordTraversal, RepositoryRecordScope,
    RetentionHold, StoredObjectId, StoredRecord, WebhookDelivery, XorbId, xet_hash_hex_string,
};

/// In-memory implementation of [`IndexStore`].
#[derive(Debug, Clone, Default)]
pub struct MemoryIndexStore {
    state: Arc<Mutex<MemoryIndexState>>,
}

impl MemoryIndexStore {
    /// Creates an empty memory index store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Persists a file reconstruction in memory.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryIndexStoreError`] when the in-memory state lock is poisoned.
    pub fn insert_reconstruction(
        &self,
        file_id: &FileId,
        reconstruction: &FileReconstruction,
    ) -> Result<(), MemoryIndexStoreError> {
        self.lock_state()?
            .reconstructions
            .insert(*file_id, reconstruction.clone());
        Ok(())
    }

    /// Persists stored-object presence metadata in memory.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryIndexStoreError`] when the in-memory state lock is poisoned.
    pub fn insert_object(&self, object_id: &StoredObjectId) -> Result<(), MemoryIndexStoreError> {
        self.lock_state()?.xorbs.insert(*object_id);
        Ok(())
    }

    /// Persists Xet xorb presence metadata in memory.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryIndexStoreError`] when the in-memory state lock is poisoned.
    pub fn insert_xorb(&self, xorb_id: &XorbId) -> Result<(), MemoryIndexStoreError> {
        self.insert_object(xorb_id)
    }

    /// Persists a chunk-hash to retained-shard mapping in memory.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryIndexStoreError`] when the in-memory state lock is poisoned.
    pub fn upsert_dedupe_shard_mapping(
        &self,
        mapping: &DedupeShardMapping,
    ) -> Result<(), MemoryIndexStoreError> {
        self.lock_state()?
            .dedupe_shards
            .insert(mapping.chunk_hash(), mapping.clone());
        Ok(())
    }

    /// Records a processed provider webhook delivery if it has not been seen before.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryIndexStoreError`] when the in-memory state lock is poisoned.
    pub fn record_webhook_delivery(
        &self,
        delivery: &WebhookDelivery,
    ) -> Result<bool, MemoryIndexStoreError> {
        let key = MemoryWebhookDeliveryKey::from_domain(delivery);
        Ok(self
            .lock_state()?
            .webhook_deliveries
            .insert(key, delivery.clone())
            .is_none())
    }

    /// Persists provider-derived repository lifecycle state in memory.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryIndexStoreError`] when the in-memory state lock is poisoned.
    pub fn upsert_provider_repository_state(
        &self,
        state: &ProviderRepositoryState,
    ) -> Result<(), MemoryIndexStoreError> {
        let key = MemoryProviderRepositoryStateKey::from_domain(state);
        self.lock_state()?
            .provider_repository_states
            .insert(key, state.clone());
        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, MemoryIndexState>, MemoryIndexStoreError> {
        self.state
            .lock()
            .map_err(|_error| MemoryIndexStoreError::LockPoisoned)
    }
}

impl ReconstructionStore for MemoryIndexStore {
    type Error = MemoryIndexStoreError;

    fn reconstruction(&self, file_id: &FileId) -> Result<Option<FileReconstruction>, Self::Error> {
        Ok(self.lock_state()?.reconstructions.get(file_id).cloned())
    }

    fn list_reconstruction_file_ids(&self) -> Result<Vec<FileId>, Self::Error> {
        let mut file_ids = self
            .lock_state()?
            .reconstructions
            .keys()
            .copied()
            .collect::<Vec<_>>();
        file_ids.sort_by(|left, right| {
            xet_hash_hex_string(left.hash()).cmp(&xet_hash_hex_string(right.hash()))
        });
        Ok(file_ids)
    }

    fn delete_reconstruction(&self, file_id: &FileId) -> Result<bool, Self::Error> {
        Ok(self.lock_state()?.reconstructions.remove(file_id).is_some())
    }

    fn contains_object(&self, object_id: &StoredObjectId) -> Result<bool, Self::Error> {
        Ok(self.lock_state()?.xorbs.contains(object_id))
    }
}

impl DedupeStore for MemoryIndexStore {
    type Error = MemoryIndexStoreError;

    fn dedupe_shard_mapping(
        &self,
        chunk_hash: &ShardlineHash,
    ) -> Result<Option<DedupeShardMapping>, Self::Error> {
        Ok(self.lock_state()?.dedupe_shards.get(chunk_hash).cloned())
    }

    fn list_dedupe_shard_mappings(&self) -> Result<Vec<DedupeShardMapping>, Self::Error> {
        let mut mappings = self
            .lock_state()?
            .dedupe_shards
            .values()
            .cloned()
            .collect::<Vec<_>>();
        mappings.sort_by(|left, right| {
            xet_hash_hex_string(left.chunk_hash()).cmp(&xet_hash_hex_string(right.chunk_hash()))
        });
        Ok(mappings)
    }

    fn visit_dedupe_shard_mappings<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(DedupeShardMapping) -> Result<(), VisitorError>,
    {
        for mapping in DedupeStore::list_dedupe_shard_mappings(self).map_err(Into::into)? {
            visitor(mapping)?;
        }

        Ok(())
    }

    fn delete_dedupe_shard_mapping(&self, chunk_hash: &ShardlineHash) -> Result<bool, Self::Error> {
        Ok(self
            .lock_state()?
            .dedupe_shards
            .remove(chunk_hash)
            .is_some())
    }
}

impl LifecycleStore for MemoryIndexStore {
    type Error = MemoryIndexStoreError;

    fn quarantine_candidate(
        &self,
        object_key: &ObjectKey,
    ) -> Result<Option<QuarantineCandidate>, Self::Error> {
        Ok(self.lock_state()?.quarantine.get(object_key).cloned())
    }

    fn list_quarantine_candidates(&self) -> Result<Vec<QuarantineCandidate>, Self::Error> {
        let mut candidates = self
            .lock_state()?
            .quarantine
            .values()
            .cloned()
            .collect::<Vec<_>>();
        candidates
            .sort_by(|left, right| left.object_key().as_str().cmp(right.object_key().as_str()));
        Ok(candidates)
    }

    fn visit_quarantine_candidates<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(QuarantineCandidate) -> Result<(), VisitorError>,
    {
        for candidate in LifecycleStore::list_quarantine_candidates(self).map_err(Into::into)? {
            visitor(candidate)?;
        }

        Ok(())
    }

    fn upsert_quarantine_candidate(
        &self,
        candidate: &QuarantineCandidate,
    ) -> Result<(), Self::Error> {
        self.lock_state()?
            .quarantine
            .insert(candidate.object_key().clone(), candidate.clone());
        Ok(())
    }

    fn delete_quarantine_candidate(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
        Ok(self.lock_state()?.quarantine.remove(object_key).is_some())
    }

    fn retention_hold(&self, object_key: &ObjectKey) -> Result<Option<RetentionHold>, Self::Error> {
        Ok(self.lock_state()?.retention_holds.get(object_key).cloned())
    }

    fn list_retention_holds(&self) -> Result<Vec<RetentionHold>, Self::Error> {
        let mut holds = self
            .lock_state()?
            .retention_holds
            .values()
            .cloned()
            .collect::<Vec<_>>();
        holds.sort_by(|left, right| left.object_key().as_str().cmp(right.object_key().as_str()));
        Ok(holds)
    }

    fn visit_retention_holds<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(RetentionHold) -> Result<(), VisitorError>,
    {
        for hold in LifecycleStore::list_retention_holds(self).map_err(Into::into)? {
            visitor(hold)?;
        }

        Ok(())
    }

    fn upsert_retention_hold(&self, hold: &RetentionHold) -> Result<(), Self::Error> {
        self.lock_state()?
            .retention_holds
            .insert(hold.object_key().clone(), hold.clone());
        Ok(())
    }

    fn delete_retention_hold(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
        Ok(self
            .lock_state()?
            .retention_holds
            .remove(object_key)
            .is_some())
    }

    fn record_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error> {
        self.record_webhook_delivery(delivery)
    }

    fn list_webhook_deliveries(&self) -> Result<Vec<WebhookDelivery>, Self::Error> {
        let mut deliveries = self
            .lock_state()?
            .webhook_deliveries
            .values()
            .cloned()
            .collect::<Vec<_>>();
        deliveries.sort_by(|left, right| {
            provider_sort_key(left.provider())
                .cmp(provider_sort_key(right.provider()))
                .then_with(|| left.owner().cmp(right.owner()))
                .then_with(|| left.repo().cmp(right.repo()))
                .then_with(|| left.delivery_id().cmp(right.delivery_id()))
        });
        Ok(deliveries)
    }

    fn delete_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error> {
        let key = MemoryWebhookDeliveryKey::from_domain(delivery);
        Ok(self.lock_state()?.webhook_deliveries.remove(&key).is_some())
    }

    fn provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<Option<ProviderRepositoryState>, Self::Error> {
        let key = MemoryProviderRepositoryStateKey::new(provider, owner, repo);
        Ok(self
            .lock_state()?
            .provider_repository_states
            .get(&key)
            .cloned())
    }

    fn list_provider_repository_states(&self) -> Result<Vec<ProviderRepositoryState>, Self::Error> {
        let mut states = self
            .lock_state()?
            .provider_repository_states
            .values()
            .cloned()
            .collect::<Vec<_>>();
        states.sort_by(|left, right| {
            provider_sort_key(left.provider())
                .cmp(provider_sort_key(right.provider()))
                .then_with(|| left.owner().cmp(right.owner()))
                .then_with(|| left.repo().cmp(right.repo()))
        });
        Ok(states)
    }

    fn upsert_provider_repository_state(
        &self,
        state: &ProviderRepositoryState,
    ) -> Result<(), Self::Error> {
        MemoryIndexStore::upsert_provider_repository_state(self, state)
    }

    fn delete_provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<bool, Self::Error> {
        let key = MemoryProviderRepositoryStateKey::new(provider, owner, repo);
        Ok(self
            .lock_state()?
            .provider_repository_states
            .remove(&key)
            .is_some())
    }
}

impl AsyncIndexStore for MemoryIndexStore {
    type Error = MemoryIndexStoreError;

    fn reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, Option<FileReconstruction>, Self::Error> {
        Box::pin(async move { ReconstructionStore::reconstruction(self, file_id) })
    }

    fn insert_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
        reconstruction: &'operation FileReconstruction,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { self.insert_reconstruction(file_id, reconstruction) })
    }

    fn list_reconstruction_file_ids(&self) -> IndexStoreFuture<'_, Vec<FileId>, Self::Error> {
        Box::pin(async move { ReconstructionStore::list_reconstruction_file_ids(self) })
    }

    fn delete_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { ReconstructionStore::delete_reconstruction(self, file_id) })
    }

    fn contains_object<'operation>(
        &'operation self,
        object_id: &'operation StoredObjectId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { ReconstructionStore::contains_object(self, object_id) })
    }

    fn insert_object<'operation>(
        &'operation self,
        object_id: &'operation StoredObjectId,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { self.insert_object(object_id) })
    }

    fn dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, Option<DedupeShardMapping>, Self::Error> {
        Box::pin(async move { DedupeStore::dedupe_shard_mapping(self, chunk_hash) })
    }

    fn list_dedupe_shard_mappings(
        &self,
    ) -> IndexStoreFuture<'_, Vec<DedupeShardMapping>, Self::Error> {
        Box::pin(async move { DedupeStore::list_dedupe_shard_mappings(self) })
    }

    fn visit_dedupe_shard_mappings<'operation, Visitor, VisitorError>(
        &'operation self,
        visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(DedupeShardMapping) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move { DedupeStore::visit_dedupe_shard_mappings(self, visitor) })
    }

    fn upsert_dedupe_shard_mapping<'operation>(
        &'operation self,
        mapping: &'operation DedupeShardMapping,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { self.upsert_dedupe_shard_mapping(mapping) })
    }

    fn delete_dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { DedupeStore::delete_dedupe_shard_mapping(self, chunk_hash) })
    }

    impl_async_lifecycle_delegation!(MemoryIndexStore);
}

const fn provider_sort_key(provider: RepositoryProvider) -> &'static str {
    match provider {
        RepositoryProvider::GitHub => "github",
        RepositoryProvider::Gitea => "gitea",
        RepositoryProvider::GitLab => "gitlab",
        RepositoryProvider::Codeberg => "codeberg",
        RepositoryProvider::Generic => "generic",
    }
}

/// Memory index-store failure.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum MemoryIndexStoreError {
    /// The in-memory state lock was poisoned.
    #[error("memory index store lock was poisoned")]
    LockPoisoned,
}

#[derive(Debug, Default)]
struct MemoryIndexState {
    reconstructions: HashMap<FileId, FileReconstruction>,
    xorbs: HashSet<XorbId>,
    dedupe_shards: HashMap<ShardlineHash, DedupeShardMapping>,
    quarantine: HashMap<ObjectKey, QuarantineCandidate>,
    retention_holds: HashMap<ObjectKey, RetentionHold>,
    webhook_deliveries: HashMap<MemoryWebhookDeliveryKey, WebhookDelivery>,
    provider_repository_states: HashMap<MemoryProviderRepositoryStateKey, ProviderRepositoryState>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryWebhookDeliveryKey {
    provider: MemoryRepositoryProvider,
    owner: String,
    repo: String,
    delivery_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryProviderRepositoryStateKey {
    provider: MemoryRepositoryProvider,
    owner: String,
    repo: String,
}

impl MemoryProviderRepositoryStateKey {
    fn new(provider: RepositoryProvider, owner: &str, repo: &str) -> Self {
        Self {
            provider: MemoryRepositoryProvider::from_protocol(provider),
            owner: owner.to_owned(),
            repo: repo.to_owned(),
        }
    }

    fn from_domain(state: &ProviderRepositoryState) -> Self {
        Self::new(state.provider(), state.owner(), state.repo())
    }
}

impl MemoryWebhookDeliveryKey {
    fn from_domain(delivery: &WebhookDelivery) -> Self {
        Self {
            provider: MemoryRepositoryProvider::from_protocol(delivery.provider()),
            owner: delivery.owner().to_owned(),
            repo: delivery.repo().to_owned(),
            delivery_id: delivery.delivery_id().to_owned(),
        }
    }
}

/// Opaque in-memory file-record locator.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemoryRecordLocator {
    kind: MemoryRecordKind,
    repository_scope: Option<MemoryRepositoryScope>,
    file_id: String,
    content_hash: Option<String>,
}

/// In-memory implementation of [`RecordStore`].
#[derive(Debug, Clone, Default)]
pub struct MemoryRecordStore {
    state: Arc<Mutex<MemoryRecordState>>,
}

impl MemoryRecordStore {
    /// Creates an empty memory record store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts or replaces an immutable version record.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryRecordStoreError`] when serialization fails or the state lock is poisoned.
    pub fn insert_version_record(&self, record: &FileRecord) -> Result<(), MemoryRecordStoreError> {
        let locator = self.version_record_locator(record);
        let bytes = to_vec(record)?;
        self.insert_record(locator, bytes)
    }

    fn insert_record(
        &self,
        locator: MemoryRecordLocator,
        bytes: Vec<u8>,
    ) -> Result<(), MemoryRecordStoreError> {
        let mut state = self.lock_state()?;
        state.modified_clock = state.modified_clock.saturating_add(1);
        let entry = MemoryRecordEntry {
            bytes,
            modified_since_epoch: Duration::from_secs(state.modified_clock),
        };
        match locator.kind {
            MemoryRecordKind::Latest => {
                state.latest_records.insert(locator, entry);
            }
            MemoryRecordKind::Version => {
                state.version_records.insert(locator, entry);
            }
        }
        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, MemoryRecordState>, MemoryRecordStoreError> {
        self.state
            .lock()
            .map_err(|_error| MemoryRecordStoreError::LockPoisoned)
    }
}

impl RecordTraversal for MemoryRecordStore {
    type Error = MemoryRecordStoreError;
    type Locator = MemoryRecordLocator;

    fn list_latest_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            Ok(self
                .lock_state()?
                .latest_records
                .keys()
                .cloned()
                .collect::<Vec<_>>())
        })
    }

    fn visit_latest_record_locators<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(Self::Locator) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            let locators = self
                .lock_state()
                .map_err(Into::into)?
                .latest_records
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            for locator in locators {
                visitor(locator)?;
            }

            Ok(())
        })
    }

    fn visit_latest_records<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            let records = self
                .lock_state()
                .map_err(Into::into)?
                .latest_records
                .iter()
                .map(|(locator, entry)| StoredRecord {
                    locator: locator.clone(),
                    bytes: entry.bytes.clone(),
                    modified_since_epoch: entry.modified_since_epoch,
                })
                .collect::<Vec<_>>();
            for record in records {
                visitor(record)?;
            }

            Ok(())
        })
    }

    fn list_version_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            Ok(self
                .lock_state()?
                .version_records
                .keys()
                .cloned()
                .collect::<Vec<_>>())
        })
    }

    fn list_repository_latest_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            Ok(self
                .lock_state()?
                .latest_records
                .keys()
                .filter(|locator| locator.matches_repository(repository))
                .cloned()
                .collect::<Vec<_>>())
        })
    }

    fn visit_repository_latest_records<'operation, Visitor, VisitorError>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            let records = self
                .lock_state()
                .map_err(Into::into)?
                .latest_records
                .iter()
                .filter(|(locator, _entry)| locator.matches_repository(repository))
                .map(|(locator, entry)| StoredRecord {
                    locator: locator.clone(),
                    bytes: entry.bytes.clone(),
                    modified_since_epoch: entry.modified_since_epoch,
                })
                .collect::<Vec<_>>();
            for record in records {
                visitor(record)?;
            }

            Ok(())
        })
    }

    fn list_repository_version_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            Ok(self
                .lock_state()?
                .version_records
                .keys()
                .filter(|locator| locator.matches_repository(repository))
                .cloned()
                .collect::<Vec<_>>())
        })
    }

    fn visit_version_record_locators<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(Self::Locator) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            let locators = self
                .lock_state()
                .map_err(Into::into)?
                .version_records
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            for locator in locators {
                visitor(locator)?;
            }

            Ok(())
        })
    }

    fn visit_version_records<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            let records = self
                .lock_state()
                .map_err(Into::into)?
                .version_records
                .iter()
                .map(|(locator, entry)| StoredRecord {
                    locator: locator.clone(),
                    bytes: entry.bytes.clone(),
                    modified_since_epoch: entry.modified_since_epoch,
                })
                .collect::<Vec<_>>();
            for record in records {
                visitor(record)?;
            }

            Ok(())
        })
    }

    fn visit_repository_version_records<'operation, Visitor, VisitorError>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            let records = self
                .lock_state()
                .map_err(Into::into)?
                .version_records
                .iter()
                .filter(|(locator, _entry)| locator.matches_repository(repository))
                .map(|(locator, entry)| StoredRecord {
                    locator: locator.clone(),
                    bytes: entry.bytes.clone(),
                    modified_since_epoch: entry.modified_since_epoch,
                })
                .collect::<Vec<_>>();
            for record in records {
                visitor(record)?;
            }

            Ok(())
        })
    }

    fn read_record_bytes<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Vec<u8>, Self::Error> {
        Box::pin(async move {
            self.record_entry(locator)?
                .map(|entry| entry.bytes)
                .ok_or(MemoryRecordStoreError::RecordNotFound)
        })
    }

    fn read_latest_record_bytes<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, Option<Vec<u8>>, Self::Error> {
        Box::pin(async move {
            let locator = self.latest_record_locator(record);
            Ok(self.record_entry(&locator)?.map(|entry| entry.bytes))
        })
    }

    fn record_locator_exists<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { Ok(self.record_entry(locator)?.is_some()) })
    }

    fn modified_since_epoch<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Duration, Self::Error> {
        Box::pin(async move {
            self.record_entry(locator)?
                .map(|entry| entry.modified_since_epoch)
                .ok_or(MemoryRecordStoreError::RecordNotFound)
        })
    }

    fn latest_record_locator(&self, record: &FileRecord) -> Self::Locator {
        MemoryRecordLocator {
            kind: MemoryRecordKind::Latest,
            repository_scope: record
                .repository_scope
                .as_ref()
                .map(MemoryRepositoryScope::from_protocol),
            file_id: record.file_id.clone(),
            content_hash: None,
        }
    }

    fn version_record_locator(&self, record: &FileRecord) -> Self::Locator {
        MemoryRecordLocator {
            kind: MemoryRecordKind::Version,
            repository_scope: record
                .repository_scope
                .as_ref()
                .map(MemoryRepositoryScope::from_protocol),
            file_id: record.file_id.clone(),
            content_hash: Some(record.content_hash.clone()),
        }
    }
}

impl RecordMutation for MemoryRecordStore {
    fn write_version_record<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { self.insert_version_record(record) })
    }

    fn write_latest_record<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let locator = self.latest_record_locator(record);
            let bytes = to_vec(record)?;
            self.insert_record(locator, bytes)
        })
    }

    fn delete_record_locator<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let mut state = self.lock_state()?;
            let removed = match locator.kind {
                MemoryRecordKind::Latest => state.latest_records.remove(locator),
                MemoryRecordKind::Version => state.version_records.remove(locator),
            };
            if removed.is_some() {
                return Ok(());
            }

            Err(MemoryRecordStoreError::RecordNotFound)
        })
    }

    fn prune_empty_latest_records(&self) -> RecordStoreFuture<'_, (), Self::Error> {
        Box::pin(async move { Ok(()) })
    }
}

impl MemoryRecordStore {
    fn record_entry(
        &self,
        locator: &MemoryRecordLocator,
    ) -> Result<Option<MemoryRecordEntry>, MemoryRecordStoreError> {
        let state = self.lock_state()?;
        let entry = match locator.kind {
            MemoryRecordKind::Latest => state.latest_records.get(locator).cloned(),
            MemoryRecordKind::Version => state.version_records.get(locator).cloned(),
        };
        Ok(entry)
    }
}

impl MemoryRecordLocator {
    fn matches_repository(&self, repository: &RepositoryRecordScope) -> bool {
        matches!(
            self.repository_scope.as_ref(),
            Some(scope) if scope.matches_repository(repository)
        )
    }
}

/// Memory record-store failure.
#[derive(Debug, Error)]
pub enum MemoryRecordStoreError {
    /// The in-memory state lock was poisoned.
    #[error("memory record store lock was poisoned")]
    LockPoisoned,
    /// The requested record locator does not exist.
    #[error("memory record locator was not found")]
    RecordNotFound,
    /// Record serialization failed.
    #[error("memory record serialization failed")]
    Json(#[from] SerdeJsonError),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum MemoryRecordKind {
    Latest,
    Version,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryRepositoryScope {
    provider: MemoryRepositoryProvider,
    owner: String,
    name: String,
    revision: Option<String>,
}

impl MemoryRepositoryScope {
    fn from_protocol(scope: &RepositoryScope) -> Self {
        Self {
            provider: MemoryRepositoryProvider::from_protocol(scope.provider()),
            owner: scope.owner().to_owned(),
            name: scope.name().to_owned(),
            revision: scope.revision().map(ToOwned::to_owned),
        }
    }

    fn matches_repository(&self, repository: &RepositoryRecordScope) -> bool {
        self.provider == MemoryRepositoryProvider::from_protocol(repository.provider())
            && self.owner == repository.owner()
            && self.name == repository.name()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum MemoryRepositoryProvider {
    GitHub,
    Gitea,
    GitLab,
    Codeberg,
    Generic,
}

impl MemoryRepositoryProvider {
    const fn from_protocol(provider: RepositoryProvider) -> Self {
        match provider {
            RepositoryProvider::GitHub => Self::GitHub,
            RepositoryProvider::Gitea => Self::Gitea,
            RepositoryProvider::GitLab => Self::GitLab,
            RepositoryProvider::Codeberg => Self::Codeberg,
            RepositoryProvider::Generic => Self::Generic,
        }
    }
}

#[derive(Debug, Default)]
struct MemoryRecordState {
    latest_records: BTreeMap<MemoryRecordLocator, MemoryRecordEntry>,
    version_records: BTreeMap<MemoryRecordLocator, MemoryRecordEntry>,
    modified_clock: u64,
}

#[derive(Debug, Clone)]
struct MemoryRecordEntry {
    bytes: Vec<u8>,
    modified_since_epoch: Duration,
}

#[cfg(test)]
mod tests {
    use std::{fmt::Debug, time::Duration};

    use serde_json::from_slice;
    use shardline_protocol::{ChunkRange, RepositoryProvider, RepositoryScope, ShardlineHash};
    use shardline_storage::ObjectKey;

    use super::{MemoryIndexStore, MemoryRecordStore};
    use crate::{
        DedupeShardMapping, DedupeStore, FileChunkRecord, FileId, FileReconstruction, FileRecord,
        IndexStore, LifecycleStore, LocalIndexStore, MemoryIndexStoreError, MemoryRecordStoreError,
        ProviderRepositoryState, QuarantineCandidate, ReconstructionStore, ReconstructionTerm,
        RecordMutation, RecordTraversal, RepositoryRecordScope, RetentionHold, StoredObjectId,
        WebhookDelivery, XorbId,
    };

    #[test]
    fn memory_index_store_satisfies_index_store_lifecycle_contract() {
        let store = MemoryIndexStore::new();

        assert_index_store_lifecycle_contract(&store, |store, file_id, reconstruction, xorb_id| {
            store.insert_reconstruction(file_id, reconstruction)?;
            store.insert_xorb(xorb_id)
        });
    }

    #[test]
    fn local_index_store_satisfies_index_store_lifecycle_contract() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf());
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };

        assert_index_store_lifecycle_contract(&store, |store, file_id, reconstruction, xorb_id| {
            store.insert_reconstruction(file_id, reconstruction)?;
            store.insert_xorb(xorb_id)
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_satisfies_record_store_lifecycle_contract() {
        let store = MemoryRecordStore::new();
        let record = scoped_file_record();
        assert!(record.is_some());
        let Some(record) = record else {
            return;
        };

        let inserted_version = store.insert_version_record(&record);
        assert!(inserted_version.is_ok());
        let written_latest = store.write_latest_record(&record).await;
        assert!(written_latest.is_ok());

        let latest_locator = store.latest_record_locator(&record);
        let version_locator = store.version_record_locator(&record);

        let latest_locators = store.list_latest_record_locators().await;
        assert!(latest_locators.is_ok());
        if let Ok(latest_locators) = latest_locators {
            assert_eq!(latest_locators, vec![latest_locator.clone()]);
        }

        let version_locators = store.list_version_record_locators().await;
        assert!(version_locators.is_ok());
        if let Ok(version_locators) = version_locators {
            assert_eq!(version_locators, vec![version_locator.clone()]);
        }

        let latest_exists = store.record_locator_exists(&latest_locator).await;
        assert!(matches!(latest_exists, Ok(true)));
        let version_exists = store.record_locator_exists(&version_locator).await;
        assert!(matches!(version_exists, Ok(true)));

        let latest_bytes = store.read_latest_record_bytes(&record).await;
        assert!(latest_bytes.is_ok());
        if let Ok(Some(latest_bytes)) = latest_bytes {
            let decoded = from_slice::<FileRecord>(&latest_bytes);
            assert!(matches!(decoded, Ok(ref decoded) if decoded == &record));
        }

        let version_bytes = store.read_record_bytes(&version_locator).await;
        assert!(version_bytes.is_ok());
        if let Ok(version_bytes) = version_bytes {
            let decoded = from_slice::<FileRecord>(&version_bytes);
            assert!(matches!(decoded, Ok(ref decoded) if decoded == &record));
        }

        let version_modified = store.modified_since_epoch(&version_locator).await;
        assert!(matches!(version_modified, Ok(duration) if duration > Duration::ZERO));

        let deleted_latest = store.delete_record_locator(&latest_locator).await;
        assert!(deleted_latest.is_ok());
        let latest_exists_after_delete = store.record_locator_exists(&latest_locator).await;
        assert!(matches!(latest_exists_after_delete, Ok(false)));
        let pruned = store.prune_empty_latest_records().await;
        assert!(pruned.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_lists_repository_versions_across_revisions_only() {
        let store = MemoryRecordStore::new();
        let main_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        let release_scope = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "team",
            "assets",
            Some("release"),
        );
        let other_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "other", Some("main"));
        assert!(main_scope.is_ok());
        assert!(release_scope.is_ok());
        assert!(other_scope.is_ok());
        let (Ok(main_scope), Ok(release_scope), Ok(other_scope)) =
            (main_scope, release_scope, other_scope)
        else {
            return;
        };

        let main_record = file_record_with_scope(main_scope, "a");
        let release_record = file_record_with_scope(release_scope, "b");
        let other_record = file_record_with_scope(other_scope, "c");
        assert!(store.insert_version_record(&main_record).is_ok());
        assert!(store.insert_version_record(&release_record).is_ok());
        assert!(store.insert_version_record(&other_record).is_ok());

        let repository = RepositoryRecordScope::new(RepositoryProvider::GitHub, "team", "assets");
        let locators = store
            .list_repository_version_record_locators(&repository)
            .await;
        assert!(locators.is_ok());
        if let Ok(locators) = locators {
            assert_eq!(
                locators,
                vec![
                    store.version_record_locator(&main_record),
                    store.version_record_locator(&release_record),
                ]
            );
        }
    }

    #[test]
    fn memory_index_store_list_reconstruction_file_ids() {
        let store = MemoryIndexStore::new();
        let ids = store.list_reconstruction_file_ids().unwrap();
        assert!(ids.is_empty());

        let hash1 = ShardlineHash::from_bytes([10; 32]);
        let hash2 = ShardlineHash::from_bytes([11; 32]);
        let file_id1 = FileId::new(hash1);
        let file_id2 = FileId::new(hash2);
        let range = ChunkRange::new(0, 1);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };
        let reconstruction1 =
            FileReconstruction::new(vec![ReconstructionTerm::new(XorbId::new(hash1), range, 64)]);
        let reconstruction2 =
            FileReconstruction::new(vec![ReconstructionTerm::new(XorbId::new(hash2), range, 128)]);

        store.insert_reconstruction(&file_id1, &reconstruction1).unwrap();
        store.insert_reconstruction(&file_id2, &reconstruction2).unwrap();

        let ids = store.list_reconstruction_file_ids().unwrap();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&file_id1));
        assert!(ids.contains(&file_id2));
    }

    #[test]
    fn memory_index_store_delete_reconstruction() {
        let store = MemoryIndexStore::new();
        let hash = ShardlineHash::from_bytes([12; 32]);
        let file_id = FileId::new(hash);
        let range = ChunkRange::new(0, 1);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };
        let reconstruction =
            FileReconstruction::new(vec![ReconstructionTerm::new(XorbId::new(hash), range, 64)]);

        assert!(!store.delete_reconstruction(&file_id).unwrap());

        store.insert_reconstruction(&file_id, &reconstruction).unwrap();
        assert!(store.delete_reconstruction(&file_id).unwrap());

        assert!(!store.delete_reconstruction(&file_id).unwrap());
    }

    #[test]
    fn memory_index_store_contains_object() {
        let store = MemoryIndexStore::new();
        let hash = ShardlineHash::from_bytes([13; 32]);
        let object_id = StoredObjectId::new(hash);

        assert!(!store.contains_object(&object_id).unwrap());

        store.insert_object(&object_id).unwrap();
        assert!(store.contains_object(&object_id).unwrap());

        let xorb_id = XorbId::new(hash);
        assert!(store.contains_xorb(&xorb_id).unwrap());
    }

    #[test]
    fn memory_index_store_dedupe_shard_mapping_roundtrip() {
        let store = MemoryIndexStore::new();
        let hash = ShardlineHash::from_bytes([14; 32]);
        let key = ObjectKey::parse("shards/aa/example.shard");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let mapping = DedupeShardMapping::new(hash, key);

        assert!(store.dedupe_shard_mapping(&hash).unwrap().is_none());

        store.upsert_dedupe_shard_mapping(&mapping).unwrap();
        let loaded = store.dedupe_shard_mapping(&hash).unwrap();
        assert_eq!(loaded, Some(mapping.clone()));

        let mappings = store.list_dedupe_shard_mappings().unwrap();
        assert_eq!(mappings, vec![mapping.clone()]);

        let mut visited = Vec::new();
        store
            .visit_dedupe_shard_mappings(|m| {
                visited.push(m);
                Ok::<(), MemoryIndexStoreError>(())
            })
            .unwrap();
        assert_eq!(visited, vec![mapping]);

        assert!(store.delete_dedupe_shard_mapping(&hash).unwrap());
        assert!(store.dedupe_shard_mapping(&hash).unwrap().is_none());
        assert!(!store.delete_dedupe_shard_mapping(&hash).unwrap());
    }

    #[test]
    fn memory_index_store_provider_repository_state_lifecycle() {
        let store = MemoryIndexStore::new();
        let provider = RepositoryProvider::GitHub;

        let loaded = store
            .provider_repository_state(provider, "team", "assets")
            .unwrap();
        assert!(loaded.is_none());

        let state = ProviderRepositoryState::new(
            provider,
            "team".to_owned(),
            "assets".to_owned(),
            Some(100),
            Some(200),
            Some("refs/heads/main".to_owned()),
        );

        // Call through LifecycleStore trait to exercise the delegation impl.
        LifecycleStore::upsert_provider_repository_state(&store, &state).unwrap();

        let loaded =
            LifecycleStore::provider_repository_state(&store, provider, "team", "assets").unwrap();
        assert_eq!(loaded, Some(state.clone()));

        let states = LifecycleStore::list_provider_repository_states(&store).unwrap();
        assert_eq!(states, vec![state]);

        assert!(LifecycleStore::delete_provider_repository_state(
            &store,
            provider,
            "team",
            "assets"
        )
        .unwrap());
        let loaded =
            LifecycleStore::provider_repository_state(&store, provider, "team", "assets").unwrap();
        assert!(loaded.is_none());
        assert!(!LifecycleStore::delete_provider_repository_state(
            &store,
            provider,
            "team",
            "assets"
        )
        .unwrap());
    }

    #[test]
    fn memory_index_store_visit_webhook_deliveries_default_impl() {
        let store = MemoryIndexStore::new();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets".to_owned(),
            "visit-delivery".to_owned(),
            42,
        )
        .unwrap();
        store.record_webhook_delivery(&delivery).unwrap();

        // Call through LifecycleStore to exercise the default visit_items! impl.
        let mut visited = Vec::new();
        LifecycleStore::visit_webhook_deliveries(&store, |d| {
            visited.push(d);
            Ok::<(), MemoryIndexStoreError>(())
        })
        .unwrap();
        assert_eq!(visited, vec![delivery]);
    }

    #[test]
    fn memory_index_store_visit_provider_repository_states_default_impl() {
        let store = MemoryIndexStore::new();
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitLab,
            "group".to_owned(),
            "project".to_owned(),
            Some(1),
            Some(2),
            None,
        );
        store.upsert_provider_repository_state(&state).unwrap();

        // Call through LifecycleStore to exercise the default visit_items! impl.
        let mut visited = Vec::new();
        LifecycleStore::visit_provider_repository_states(&store, |s| {
            visited.push(s);
            Ok::<(), MemoryIndexStoreError>(())
        })
        .unwrap();
        assert_eq!(visited, vec![state]);
    }

    #[test]
    fn memory_index_store_multi_provider_enum_branches() {
        use shardline_protocol::RepositoryProvider as Rp;

        let store = MemoryIndexStore::new();

        // Exercise every MemoryRepositoryProvider match arm via different providers.
        let providers = [
            Rp::GitHub,
            Rp::Gitea,
            Rp::GitLab,
            Rp::Codeberg,
            Rp::Generic,
        ];
        for (i, &p) in providers.iter().enumerate() {
            let state = ProviderRepositoryState::new(
                p,
                format!("owner_{i}"),
                format!("repo_{i}"),
                None,
                None,
                None,
            );
            store.upsert_provider_repository_state(&state).unwrap();
        }

        let states = store.list_provider_repository_states().unwrap();
        assert_eq!(states.len(), 5);

        for (i, &p) in providers.iter().enumerate() {
            let loaded = store
                .provider_repository_state(p, &format!("owner_{i}"), &format!("repo_{i}"))
                .unwrap();
            assert!(loaded.is_some());
        }
    }

    #[test]
    fn memory_index_store_list_webhook_deliveries() {
        let store = MemoryIndexStore::new();
        let delivery1 = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets".to_owned(),
            "delivery-1".to_owned(),
            100,
        )
        .unwrap();
        let delivery2 = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "other".to_owned(),
            "delivery-2".to_owned(),
            200,
        )
        .unwrap();

        assert!(store.record_webhook_delivery(&delivery1).unwrap());
        assert!(store.record_webhook_delivery(&delivery2).unwrap());

        let deliveries = store.list_webhook_deliveries().unwrap();
        assert_eq!(deliveries.len(), 2);
        assert!(deliveries.contains(&delivery1));
        assert!(deliveries.contains(&delivery2));
    }

    #[test]
    fn memory_index_store_delete_webhook_delivery() {
        let store = MemoryIndexStore::new();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets".to_owned(),
            "delivery-1".to_owned(),
            100,
        )
        .unwrap();

        assert!(store.record_webhook_delivery(&delivery).unwrap());
        assert!(store.delete_webhook_delivery(&delivery).unwrap());
        let deliveries = store.list_webhook_deliveries().unwrap();
        assert!(deliveries.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_index_store_async_interface() {
        use crate::AsyncIndexStore;
        let store = MemoryIndexStore::new();
        let hash = ShardlineHash::from_bytes([15; 32]);
        let file_id = FileId::new(hash);
        let range = ChunkRange::new(0, 1).unwrap();
        let reconstruction = FileReconstruction::new(vec![ReconstructionTerm::new(
            XorbId::new(hash),
            range,
            64,
        )]);

        AsyncIndexStore::insert_reconstruction(&store, &file_id, &reconstruction)
            .await
            .unwrap();
        let loaded = AsyncIndexStore::reconstruction(&store, &file_id)
            .await
            .unwrap();
        assert_eq!(loaded, Some(reconstruction));

        let ids = AsyncIndexStore::list_reconstruction_file_ids(&store)
            .await
            .unwrap();
        assert_eq!(ids, vec![file_id]);

        AsyncIndexStore::insert_object(&store, &StoredObjectId::new(hash))
            .await
            .unwrap();
        assert!(AsyncIndexStore::contains_object(&store, &StoredObjectId::new(hash))
            .await
            .unwrap());

        assert!(AsyncIndexStore::delete_reconstruction(&store, &file_id)
            .await
            .unwrap());
        let ids = AsyncIndexStore::list_reconstruction_file_ids(&store)
            .await
            .unwrap();
        assert!(ids.is_empty());

        // Async dedupe operations.
        let dedupe_hash = ShardlineHash::from_bytes([16; 32]);
        let dedupe_key = ObjectKey::parse("shards/aa/dedupe.shard").unwrap();
        let mapping = DedupeShardMapping::new(dedupe_hash, dedupe_key);
        AsyncIndexStore::upsert_dedupe_shard_mapping(&store, &mapping)
            .await
            .unwrap();
        let loaded_mapping =
            AsyncIndexStore::dedupe_shard_mapping(&store, &dedupe_hash)
                .await
                .unwrap();
        assert_eq!(loaded_mapping, Some(mapping.clone()));
        let all_mappings = AsyncIndexStore::list_dedupe_shard_mappings(&store)
            .await
            .unwrap();
        assert_eq!(all_mappings, vec![mapping.clone()]);
        let mut visited_m = Vec::new();
        AsyncIndexStore::visit_dedupe_shard_mappings(&store, |m| {
            visited_m.push(m);
            Ok::<(), MemoryIndexStoreError>(())
        })
        .await
        .unwrap();
        assert_eq!(visited_m, vec![mapping]);
        assert!(
            AsyncIndexStore::delete_dedupe_shard_mapping(&store, &dedupe_hash)
                .await
                .unwrap()
        );

        // Also exercise async lifecycle delegations (impl_async_lifecycle_delegation).
        let object_key = ObjectKey::parse("xorbs/default/aa/bb/hash.xorb").unwrap();
        let candidate = QuarantineCandidate::new(object_key.clone(), 128, 10, 20).unwrap();
        AsyncIndexStore::upsert_quarantine_candidate(&store, &candidate)
            .await
            .unwrap();
        let loaded = AsyncIndexStore::quarantine_candidate(&store, &object_key)
            .await
            .unwrap();
        assert_eq!(loaded, Some(candidate));

        // Async visit quarantine candidates.
        let mut visited_q = Vec::new();
        AsyncIndexStore::visit_quarantine_candidates(&store, |c| {
            visited_q.push(c);
            Ok::<(), MemoryIndexStoreError>(())
        })
        .await
        .unwrap();
        assert_eq!(visited_q.len(), 1);

        assert!(
            AsyncIndexStore::delete_quarantine_candidate(&store, &object_key)
                .await
                .unwrap()
        );
        assert!(
            !AsyncIndexStore::delete_quarantine_candidate(&store, &object_key)
                .await
                .unwrap()
        );

        let hold = RetentionHold::new(
            object_key.clone(),
            "test hold".to_owned(),
            10,
            Some(20),
        )
        .unwrap();
        AsyncIndexStore::upsert_retention_hold(&store, &hold)
            .await
            .unwrap();
        let loaded_hold = AsyncIndexStore::retention_hold(&store, &object_key)
            .await
            .unwrap();
        assert_eq!(loaded_hold, Some(hold));

        // Async visit retention holds.
        let mut visited_h = Vec::new();
        AsyncIndexStore::visit_retention_holds(&store, |h| {
            visited_h.push(h);
            Ok::<(), MemoryIndexStoreError>(())
        })
        .await
        .unwrap();
        assert_eq!(visited_h.len(), 1);

        assert!(
            AsyncIndexStore::delete_retention_hold(&store, &object_key)
                .await
                .unwrap()
        );

        // Async webhook operations.
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets".to_owned(),
            "async-delivery".to_owned(),
            100,
        )
        .unwrap();
        assert!(
            AsyncIndexStore::record_webhook_delivery(&store, &delivery)
                .await
                .unwrap()
        );
        let deliveries = AsyncIndexStore::list_webhook_deliveries(&store)
            .await
            .unwrap();
        assert_eq!(deliveries.len(), 1);
        assert!(
            AsyncIndexStore::delete_webhook_delivery(&store, &delivery)
                .await
                .unwrap()
        );

        // Async provider repository state operations.
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets".to_owned(),
            Some(1),
            Some(2),
            Some("refs/heads/main".to_owned()),
        );
        AsyncIndexStore::upsert_provider_repository_state(&store, &state)
            .await
            .unwrap();
        let loaded_state =
            AsyncIndexStore::provider_repository_state(&store, RepositoryProvider::GitHub, "team", "assets")
                .await
                .unwrap();
        assert_eq!(loaded_state, Some(state));

        let all_states = AsyncIndexStore::list_provider_repository_states(&store)
            .await
            .unwrap();
        assert_eq!(all_states.len(), 1);

        assert!(
            AsyncIndexStore::delete_provider_repository_state(
                &store,
                RepositoryProvider::GitHub,
                "team",
                "assets"
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_visit_latest_records() {
        let store = MemoryRecordStore::new();
        let record = scoped_file_record();
        assert!(record.is_some());
        let Some(record) = record else {
            return;
        };
        store.write_latest_record(&record).await.unwrap();

        let mut visited = Vec::new();
        store
            .visit_latest_records(|stored| {
                visited.push(stored);
                Ok::<(), MemoryRecordStoreError>(())
            })
            .await
            .unwrap();
        assert_eq!(visited.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_visit_version_records() {
        let store = MemoryRecordStore::new();
        let record = scoped_file_record();
        assert!(record.is_some());
        let Some(record) = record else {
            return;
        };
        store.insert_version_record(&record).unwrap();

        let mut visited = Vec::new();
        store
            .visit_version_records(|stored| {
                visited.push(stored);
                Ok::<(), MemoryRecordStoreError>(())
            })
            .await
            .unwrap();
        assert_eq!(visited.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_visit_latest_record_locators() {
        let store = MemoryRecordStore::new();
        let record = scoped_file_record();
        assert!(record.is_some());
        let Some(record) = record else {
            return;
        };
        store.write_latest_record(&record).await.unwrap();

        let mut visited = Vec::new();
        store
            .visit_latest_record_locators(|loc| {
                visited.push(loc);
                Ok::<(), MemoryRecordStoreError>(())
            })
            .await
            .unwrap();
        assert_eq!(visited.len(), 1);
        assert_eq!(visited[0], store.latest_record_locator(&record));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_visit_version_record_locators() {
        let store = MemoryRecordStore::new();
        let record = scoped_file_record();
        assert!(record.is_some());
        let Some(record) = record else {
            return;
        };
        store.insert_version_record(&record).unwrap();

        let mut visited = Vec::new();
        store
            .visit_version_record_locators(|loc| {
                visited.push(loc);
                Ok::<(), MemoryRecordStoreError>(())
            })
            .await
            .unwrap();
        assert_eq!(visited.len(), 1);
        assert_eq!(visited[0], store.version_record_locator(&record));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_visit_repository_latest_records() {
        let store = MemoryRecordStore::new();
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let record = file_record_with_scope(scope, "a");
        store.write_latest_record(&record).await.unwrap();

        let repository = RepositoryRecordScope::new(RepositoryProvider::GitHub, "team", "assets");

        // list_repository_latest_record_locators
        let locators = store
            .list_repository_latest_record_locators(&repository)
            .await
            .unwrap();
        assert_eq!(locators.len(), 1);

        // visit_repository_latest_records
        let mut visited = Vec::new();
        store
            .visit_repository_latest_records(&repository, |stored| {
                visited.push(stored);
                Ok::<(), MemoryRecordStoreError>(())
            })
            .await
            .unwrap();
        assert_eq!(visited.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_visit_repository_version_records() {
        let store = MemoryRecordStore::new();
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let record = file_record_with_scope(scope, "a");
        store.insert_version_record(&record).unwrap();

        let repository = RepositoryRecordScope::new(RepositoryProvider::GitHub, "team", "assets");

        let mut visited = Vec::new();
        store
            .visit_repository_version_records(&repository, |stored| {
                visited.push(stored);
                Ok::<(), MemoryRecordStoreError>(())
            })
            .await
            .unwrap();
        assert_eq!(visited.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_record_locator_exists_false() {
        let store = MemoryRecordStore::new();
        let record = scoped_file_record();
        assert!(record.is_some());
        let Some(record) = record else {
            return;
        };
        let locator = store.version_record_locator(&record);

        let exists = store.record_locator_exists(&locator).await.unwrap();
        assert!(!exists);
    }

    fn assert_index_store_lifecycle_contract<Store, Seed>(store: &Store, seed_index: Seed)
    where
        Store: ReconstructionStore + DedupeStore + LifecycleStore,
        <Store as ReconstructionStore>::Error: Debug + Into<<Store as LifecycleStore>::Error>,
        <Store as DedupeStore>::Error: Debug + Into<<Store as LifecycleStore>::Error>,
        <Store as LifecycleStore>::Error: Debug,
        Seed: Fn(
            &Store,
            &FileId,
            &FileReconstruction,
            &XorbId,
        ) -> Result<(), <Store as ReconstructionStore>::Error>,
    {
        let hash = ShardlineHash::from_bytes([9; 32]);
        let file_id = FileId::new(hash);
        let xorb_id = XorbId::new(hash);
        let range = ChunkRange::new(2, 5);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };
        let reconstruction =
            FileReconstruction::new(vec![ReconstructionTerm::new(xorb_id, range, 128)]);

        let missing_reconstruction = store.reconstruction(&file_id);
        assert!(matches!(missing_reconstruction, Ok(None)));
        let missing_xorb = store.contains_xorb(&xorb_id);
        assert!(matches!(missing_xorb, Ok(false)));

        let seeded = seed_index(store, &file_id, &reconstruction, &xorb_id);
        assert!(seeded.is_ok());

        let loaded_reconstruction = store.reconstruction(&file_id);
        assert!(matches!(loaded_reconstruction, Ok(Some(ref loaded)) if loaded == &reconstruction));
        let contains_xorb = store.contains_xorb(&xorb_id);
        assert!(matches!(contains_xorb, Ok(true)));

        let object_key = ObjectKey::parse("xorbs/default/aa/bb/hash.xorb");
        assert!(object_key.is_ok());
        let Ok(object_key) = object_key else {
            return;
        };
        let candidate = QuarantineCandidate::new(object_key.clone(), 128, 10, 20);
        assert!(candidate.is_ok());
        let Ok(candidate) = candidate else {
            return;
        };

        let missing_candidate = store.quarantine_candidate(&object_key);
        assert!(matches!(missing_candidate, Ok(None)));
        let upserted = store.upsert_quarantine_candidate(&candidate);
        assert!(upserted.is_ok());

        let loaded_candidate = store.quarantine_candidate(&object_key);
        assert!(matches!(loaded_candidate, Ok(Some(ref loaded)) if loaded == &candidate));
        let listed_candidates = store.list_quarantine_candidates();
        assert!(listed_candidates.is_ok());
        if let Ok(listed_candidates) = listed_candidates {
            assert_eq!(listed_candidates, vec![candidate.clone()]);
        }
        let mut visited_candidates = Vec::new();
        let visited_candidates_result = store.visit_quarantine_candidates(|entry| {
            visited_candidates.push(entry);
            Ok::<(), <Store as LifecycleStore>::Error>(())
        });
        assert!(visited_candidates_result.is_ok());
        assert_eq!(visited_candidates, vec![candidate]);

        let deleted = store.delete_quarantine_candidate(&object_key);
        assert!(matches!(deleted, Ok(true)));
        let deleted_again = store.delete_quarantine_candidate(&object_key);
        assert!(matches!(deleted_again, Ok(false)));
        let missing_after_delete = store.quarantine_candidate(&object_key);
        assert!(matches!(missing_after_delete, Ok(None)));

        let hold = RetentionHold::new(
            object_key.clone(),
            "provider deletion grace".to_owned(),
            30,
            Some(90),
        );
        assert!(hold.is_ok());
        let Ok(hold) = hold else {
            return;
        };

        let missing_hold = store.retention_hold(&object_key);
        assert!(matches!(missing_hold, Ok(None)));
        let upserted_hold = store.upsert_retention_hold(&hold);
        assert!(upserted_hold.is_ok());

        let loaded_hold = store.retention_hold(&object_key);
        assert!(matches!(loaded_hold, Ok(Some(ref loaded)) if loaded == &hold));
        let listed_holds = store.list_retention_holds();
        assert!(listed_holds.is_ok());
        if let Ok(listed_holds) = listed_holds {
            assert_eq!(listed_holds, vec![hold.clone()]);
        }
        let mut visited_holds = Vec::new();
        let visited_holds_result = store.visit_retention_holds(|entry| {
            visited_holds.push(entry);
            Ok::<(), <Store as LifecycleStore>::Error>(())
        });
        assert!(visited_holds_result.is_ok());
        assert_eq!(visited_holds, vec![hold]);

        let deleted_hold = store.delete_retention_hold(&object_key);
        assert!(matches!(deleted_hold, Ok(true)));
        let deleted_hold_again = store.delete_retention_hold(&object_key);
        assert!(matches!(deleted_hold_again, Ok(false)));
        let missing_hold_after_delete = store.retention_hold(&object_key);
        assert!(matches!(missing_hold_after_delete, Ok(None)));

        let webhook_delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets".to_owned(),
            "delivery-1".to_owned(),
            100,
        );
        assert!(webhook_delivery.is_ok());
        let Ok(webhook_delivery) = webhook_delivery else {
            return;
        };
        let recorded_delivery = store.record_webhook_delivery(&webhook_delivery);
        assert!(matches!(recorded_delivery, Ok(true)));
        let duplicate_delivery = store.record_webhook_delivery(&webhook_delivery);
        assert!(matches!(duplicate_delivery, Ok(false)));
    }

    fn scoped_file_record() -> Option<FileRecord> {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return None;
        };

        Some(FileRecord {
            file_id: "asset.bin".to_owned(),
            content_hash: "a".repeat(64),
            total_bytes: 8,
            chunk_size: 4,
            repository_scope: Some(scope),
            chunks: vec![
                FileChunkRecord {
                    hash: "b".repeat(64),
                    offset: 0,
                    length: 4,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 4,
                },
                FileChunkRecord {
                    hash: "c".repeat(64),
                    offset: 4,
                    length: 4,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 4,
                },
            ],
        })
    }

    fn file_record_with_scope(scope: RepositoryScope, content_seed: &str) -> FileRecord {
        FileRecord {
            file_id: "asset.bin".to_owned(),
            content_hash: content_seed.repeat(64),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: Some(scope),
            chunks: Vec::new(),
        }
    }

    // ── Edge-case tests ────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_read_record_bytes_missing_locator() {
        let store = MemoryRecordStore::new();
        let record = scoped_file_record().unwrap();
        let locator = store.version_record_locator(&record);

        let result = store.read_record_bytes(&locator).await;
        assert!(matches!(
            result,
            Err(MemoryRecordStoreError::RecordNotFound)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_read_latest_record_bytes_no_latest() {
        let store = MemoryRecordStore::new();
        let record = scoped_file_record().unwrap();

        // No latest record written yet => should return None (not an error).
        let result = store.read_latest_record_bytes(&record).await;
        assert!(matches!(result, Ok(None)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_modified_since_epoch_missing_locator() {
        let store = MemoryRecordStore::new();
        let record = scoped_file_record().unwrap();
        let locator = store.version_record_locator(&record);

        let result = store.modified_since_epoch(&locator).await;
        assert!(matches!(
            result,
            Err(MemoryRecordStoreError::RecordNotFound)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_delete_non_existent_locator() {
        let store = MemoryRecordStore::new();
        let record = scoped_file_record().unwrap();
        let locator = store.version_record_locator(&record);

        let result = store.delete_record_locator(&locator).await;
        assert!(matches!(
            result,
            Err(MemoryRecordStoreError::RecordNotFound)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_list_repository_latest_locators_no_match() {
        let store = MemoryRecordStore::new();
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "other", Some("main")).unwrap();
        let record = file_record_with_scope(scope, "x");
        store.write_latest_record(&record).await.unwrap();

        // Different repository scope => no match.
        let repository =
            RepositoryRecordScope::new(RepositoryProvider::GitHub, "team", "assets");
        let locators = store
            .list_repository_latest_record_locators(&repository)
            .await
            .unwrap();
        assert!(locators.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_list_repository_version_locators_no_match() {
        let store = MemoryRecordStore::new();
        let scope =
            RepositoryScope::new(RepositoryProvider::GitLab, "group", "project", None).unwrap();
        let record = file_record_with_scope(scope, "y");
        store.insert_version_record(&record).unwrap();

        let repository =
            RepositoryRecordScope::new(RepositoryProvider::GitLab, "other", "project");
        let locators = store
            .list_repository_version_record_locators(&repository)
            .await
            .unwrap();
        assert!(locators.is_empty());
    }

    #[test]
    fn memory_index_store_contains_object_different_object() {
        let store = MemoryIndexStore::new();
        let hash1 = ShardlineHash::from_bytes([20; 32]);
        let hash2 = ShardlineHash::from_bytes([21; 32]);
        let object1 = StoredObjectId::new(hash1);
        let object2 = StoredObjectId::new(hash2);

        store.insert_object(&object1).unwrap();
        assert!(store.contains_object(&object1).unwrap());
        assert!(!store.contains_object(&object2).unwrap());
    }

    #[test]
    fn memory_index_store_insert_object_duplicate() {
        let store = MemoryIndexStore::new();
        let hash = ShardlineHash::from_bytes([22; 32]);
        let object = StoredObjectId::new(hash);

        store.insert_object(&object).unwrap();
        // Duplicate insert should succeed (idempotent).
        store.insert_object(&object).unwrap();
        assert!(store.contains_object(&object).unwrap());
    }

    #[test]
    fn memory_index_store_delete_dedupe_shard_mapping_not_found() {
        let store = MemoryIndexStore::new();
        let hash = ShardlineHash::from_bytes([23; 32]);
        assert!(!store.delete_dedupe_shard_mapping(&hash).unwrap());
    }

    #[test]
    fn memory_index_store_delete_quarantine_candidate_not_found() {
        let store = MemoryIndexStore::new();
        let key = ObjectKey::parse("xorbs/absent/key").unwrap();
        assert!(!store.delete_quarantine_candidate(&key).unwrap());
    }

    #[test]
    fn memory_index_store_delete_retention_hold_not_found() {
        let store = MemoryIndexStore::new();
        let key = ObjectKey::parse("xorbs/absent/hold").unwrap();
        assert!(!store.delete_retention_hold(&key).unwrap());
    }

    #[test]
    fn memory_index_store_delete_provider_repository_state_not_found() {
        let store = MemoryIndexStore::new();
        assert!(!store
            .delete_provider_repository_state(RepositoryProvider::GitHub, "nonexistent", "repo")
            .unwrap());
    }

    // ── visit methods with no records ──────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_visit_latest_records_empty() {
        let store = MemoryRecordStore::new();
        let mut visited = Vec::new();
        store
            .visit_latest_records(|stored| {
                visited.push(stored);
                Ok::<(), MemoryRecordStoreError>(())
            })
            .await
            .unwrap();
        assert!(visited.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_visit_version_records_empty() {
        let store = MemoryRecordStore::new();
        let mut visited = Vec::new();
        store
            .visit_version_records(|stored| {
                visited.push(stored);
                Ok::<(), MemoryRecordStoreError>(())
            })
            .await
            .unwrap();
        assert!(visited.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_visit_latest_record_locators_empty() {
        let store = MemoryRecordStore::new();
        let mut visited = Vec::new();
        store
            .visit_latest_record_locators(|loc| {
                visited.push(loc);
                Ok::<(), MemoryRecordStoreError>(())
            })
            .await
            .unwrap();
        assert!(visited.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_visit_version_record_locators_empty() {
        let store = MemoryRecordStore::new();
        let mut visited = Vec::new();
        store
            .visit_version_record_locators(|loc| {
                visited.push(loc);
                Ok::<(), MemoryRecordStoreError>(())
            })
            .await
            .unwrap();
        assert!(visited.is_empty());
    }

    // ── Empty lifecycle list methods ───────────────────────────────────────

    #[test]
    fn memory_index_store_list_webhook_deliveries_empty() {
        let store = MemoryIndexStore::new();
        let deliveries = store.list_webhook_deliveries().unwrap();
        assert!(deliveries.is_empty());
    }

    #[test]
    fn memory_index_store_list_retention_holds_empty() {
        let store = MemoryIndexStore::new();
        let holds = store.list_retention_holds().unwrap();
        assert!(holds.is_empty());
    }

    #[test]
    fn memory_index_store_list_quarantine_candidates_empty() {
        let store = MemoryIndexStore::new();
        let candidates = store.list_quarantine_candidates().unwrap();
        assert!(candidates.is_empty());
    }

    #[test]
    fn memory_index_store_list_provider_repository_states_empty() {
        let store = MemoryIndexStore::new();
        let states = store.list_provider_repository_states().unwrap();
        assert!(states.is_empty());
    }

    // ── Repository-scoped listing with populated stores ───────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_list_repository_latest_records_populated() {
        let store = MemoryRecordStore::new();
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main")).unwrap();
        let record = file_record_with_scope(scope, "a");
        store.write_latest_record(&record).await.unwrap();

        let repository = RepositoryRecordScope::new(RepositoryProvider::GitHub, "team", "assets");
        let locators = store.list_repository_latest_record_locators(&repository).await.unwrap();
        assert!(!locators.is_empty());

        let mut visited = Vec::new();
        store.visit_repository_latest_records(&repository, |stored| {
            visited.push(stored);
            Ok::<(), MemoryRecordStoreError>(())
        }).await.unwrap();
        assert!(!visited.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_list_repository_version_records_populated() {
        let store = MemoryRecordStore::new();
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main")).unwrap();
        let record = file_record_with_scope(scope, "a");
        store.insert_version_record(&record).unwrap();

        let repository = RepositoryRecordScope::new(RepositoryProvider::GitHub, "team", "assets");
        let locators = store.list_repository_version_record_locators(&repository).await.unwrap();
        assert!(!locators.is_empty());

        let mut visited = Vec::new();
        store.visit_repository_version_records(&repository, |stored| {
            visited.push(stored);
            Ok::<(), MemoryRecordStoreError>(())
        }).await.unwrap();
        assert!(!visited.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_list_repository_latest_locators_no_match_different_provider() {
        let store = MemoryRecordStore::new();
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main")).unwrap();
        let record = file_record_with_scope(scope, "a");
        store.write_latest_record(&record).await.unwrap();

        // Different provider => no match.
        let repository =
            RepositoryRecordScope::new(RepositoryProvider::GitLab, "team", "assets");
        let locators = store.list_repository_latest_record_locators(&repository).await.unwrap();
        assert!(locators.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_record_store_list_repository_version_locators_no_match_different_owner() {
        let store = MemoryRecordStore::new();
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None).unwrap();
        let record = file_record_with_scope(scope, "a");
        store.insert_version_record(&record).unwrap();

        // Different owner => no match.
        let repository =
            RepositoryRecordScope::new(RepositoryProvider::GitHub, "other", "assets");
        let locators = store.list_repository_version_record_locators(&repository).await.unwrap();
        assert!(locators.is_empty());
    }

    // ── Empty visit lifecycle methods ──────────────────────────────────────

    #[test]
    fn memory_index_store_visit_webhook_deliveries_empty() {
        let store = MemoryIndexStore::new();
        let mut visited = Vec::new();
        LifecycleStore::visit_webhook_deliveries(&store, |d| {
            visited.push(d);
            Ok::<(), MemoryIndexStoreError>(())
        })
        .unwrap();
        assert!(visited.is_empty());
    }

    #[test]
    fn memory_index_store_visit_retention_holds_empty() {
        let store = MemoryIndexStore::new();
        let mut visited = Vec::new();
        LifecycleStore::visit_retention_holds(&store, |h| {
            visited.push(h);
            Ok::<(), MemoryIndexStoreError>(())
        })
        .unwrap();
        assert!(visited.is_empty());
    }

    #[test]
    fn memory_index_store_visit_quarantine_candidates_empty() {
        let store = MemoryIndexStore::new();
        let mut visited = Vec::new();
        LifecycleStore::visit_quarantine_candidates(&store, |c| {
            visited.push(c);
            Ok::<(), MemoryIndexStoreError>(())
        })
        .unwrap();
        assert!(visited.is_empty());
    }

    #[test]
    fn memory_index_store_visit_provider_repository_states_empty() {
        let store = MemoryIndexStore::new();
        let mut visited = Vec::new();
        LifecycleStore::visit_provider_repository_states(&store, |s| {
            visited.push(s);
            Ok::<(), MemoryIndexStoreError>(())
        })
        .unwrap();
        assert!(visited.is_empty());
    }

    // ── MemoryIndexStoreError Display / Debug / Clone / Copy ───────────────

    #[test]
    fn memory_index_store_error_display() {
        let err = MemoryIndexStoreError::LockPoisoned;
        assert_eq!(format!("{err}"), "memory index store lock was poisoned");
    }

    #[test]
    fn memory_index_store_error_debug() {
        let err = MemoryIndexStoreError::LockPoisoned;
        let debug = format!("{err:?}");
        assert!(debug.contains("LockPoisoned"));
    }

    #[test]
    fn memory_index_store_error_clone_copy() {
        let err = MemoryIndexStoreError::LockPoisoned;
        let cloned = err;
        assert_eq!(err, cloned);
    }

    #[test]
    fn memory_record_store_error_display_lock_poisoned() {
        let err = MemoryRecordStoreError::LockPoisoned;
        assert_eq!(format!("{err}"), "memory record store lock was poisoned");
    }

    #[test]
    fn memory_record_store_error_display_record_not_found() {
        let err = MemoryRecordStoreError::RecordNotFound;
        assert_eq!(
            format!("{err}"),
            "memory record locator was not found"
        );
    }

    #[test]
    fn memory_record_store_error_display_json_error() {
        let json_err = serde_json::from_slice::<()>(b"invalid json").unwrap_err();
        let err = MemoryRecordStoreError::Json(json_err);
        let msg = format!("{err}");
        assert!(msg.contains("memory record serialization failed"));
    }

    #[test]
    fn memory_record_store_error_debug() {
        let err = MemoryRecordStoreError::RecordNotFound;
        let debug = format!("{err:?}");
        assert!(debug.contains("RecordNotFound"));
    }
}
