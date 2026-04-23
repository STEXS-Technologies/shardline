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
    AsyncIndexStore, DedupeShardMapping, FileId, FileReconstruction, FileRecord, IndexStore,
    IndexStoreFuture, ProviderRepositoryState, QuarantineCandidate, RecordStore, RecordStoreFuture,
    RepositoryRecordScope, RetentionHold, StoredRecord, WebhookDelivery, XorbId,
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

    /// Persists xorb presence metadata in memory.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryIndexStoreError`] when the in-memory state lock is poisoned.
    pub fn insert_xorb(&self, xorb_id: &XorbId) -> Result<(), MemoryIndexStoreError> {
        self.lock_state()?.xorbs.insert(*xorb_id);
        Ok(())
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

impl IndexStore for MemoryIndexStore {
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
            left.hash()
                .api_hex_string()
                .cmp(&right.hash().api_hex_string())
        });
        Ok(file_ids)
    }

    fn delete_reconstruction(&self, file_id: &FileId) -> Result<bool, Self::Error> {
        Ok(self.lock_state()?.reconstructions.remove(file_id).is_some())
    }

    fn contains_xorb(&self, xorb_id: &XorbId) -> Result<bool, Self::Error> {
        Ok(self.lock_state()?.xorbs.contains(xorb_id))
    }

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
            left.chunk_hash()
                .api_hex_string()
                .cmp(&right.chunk_hash().api_hex_string())
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
        for mapping in IndexStore::list_dedupe_shard_mappings(self).map_err(Into::into)? {
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
        for candidate in IndexStore::list_quarantine_candidates(self).map_err(Into::into)? {
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
        for hold in IndexStore::list_retention_holds(self).map_err(Into::into)? {
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
        Box::pin(async move { IndexStore::reconstruction(self, file_id) })
    }

    fn insert_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
        reconstruction: &'operation FileReconstruction,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { self.insert_reconstruction(file_id, reconstruction) })
    }

    fn list_reconstruction_file_ids(&self) -> IndexStoreFuture<'_, Vec<FileId>, Self::Error> {
        Box::pin(async move { IndexStore::list_reconstruction_file_ids(self) })
    }

    fn delete_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { IndexStore::delete_reconstruction(self, file_id) })
    }

    fn contains_xorb<'operation>(
        &'operation self,
        xorb_id: &'operation XorbId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { IndexStore::contains_xorb(self, xorb_id) })
    }

    fn insert_xorb<'operation>(
        &'operation self,
        xorb_id: &'operation XorbId,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { self.insert_xorb(xorb_id) })
    }

    fn dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, Option<DedupeShardMapping>, Self::Error> {
        Box::pin(async move { IndexStore::dedupe_shard_mapping(self, chunk_hash) })
    }

    fn list_dedupe_shard_mappings(
        &self,
    ) -> IndexStoreFuture<'_, Vec<DedupeShardMapping>, Self::Error> {
        Box::pin(async move { IndexStore::list_dedupe_shard_mappings(self) })
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
        Box::pin(async move { IndexStore::visit_dedupe_shard_mappings(self, visitor) })
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
        Box::pin(async move { IndexStore::delete_dedupe_shard_mapping(self, chunk_hash) })
    }

    fn quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<QuarantineCandidate>, Self::Error> {
        Box::pin(async move { IndexStore::quarantine_candidate(self, object_key) })
    }

    fn list_quarantine_candidates(
        &self,
    ) -> IndexStoreFuture<'_, Vec<QuarantineCandidate>, Self::Error> {
        Box::pin(async move { IndexStore::list_quarantine_candidates(self) })
    }

    fn visit_quarantine_candidates<'operation, Visitor, VisitorError>(
        &'operation self,
        visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(QuarantineCandidate) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move { IndexStore::visit_quarantine_candidates(self, visitor) })
    }

    fn upsert_quarantine_candidate<'operation>(
        &'operation self,
        candidate: &'operation QuarantineCandidate,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { IndexStore::upsert_quarantine_candidate(self, candidate) })
    }

    fn delete_quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { IndexStore::delete_quarantine_candidate(self, object_key) })
    }

    fn retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<RetentionHold>, Self::Error> {
        Box::pin(async move { IndexStore::retention_hold(self, object_key) })
    }

    fn list_retention_holds(&self) -> IndexStoreFuture<'_, Vec<RetentionHold>, Self::Error> {
        Box::pin(async move { IndexStore::list_retention_holds(self) })
    }

    fn visit_retention_holds<'operation, Visitor, VisitorError>(
        &'operation self,
        visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(RetentionHold) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move { IndexStore::visit_retention_holds(self, visitor) })
    }

    fn upsert_retention_hold<'operation>(
        &'operation self,
        hold: &'operation RetentionHold,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { IndexStore::upsert_retention_hold(self, hold) })
    }

    fn delete_retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { IndexStore::delete_retention_hold(self, object_key) })
    }

    fn record_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { IndexStore::record_webhook_delivery(self, delivery) })
    }

    fn list_webhook_deliveries(&self) -> IndexStoreFuture<'_, Vec<WebhookDelivery>, Self::Error> {
        Box::pin(async move { IndexStore::list_webhook_deliveries(self) })
    }

    fn delete_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { IndexStore::delete_webhook_delivery(self, delivery) })
    }

    fn provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, Option<ProviderRepositoryState>, Self::Error> {
        Box::pin(async move { IndexStore::provider_repository_state(self, provider, owner, repo) })
    }

    fn list_provider_repository_states(
        &self,
    ) -> IndexStoreFuture<'_, Vec<ProviderRepositoryState>, Self::Error> {
        Box::pin(async move { IndexStore::list_provider_repository_states(self) })
    }

    fn upsert_provider_repository_state<'operation>(
        &'operation self,
        state: &'operation ProviderRepositoryState,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { IndexStore::upsert_provider_repository_state(self, state) })
    }

    fn delete_provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            IndexStore::delete_provider_repository_state(self, provider, owner, repo)
        })
    }
}

const fn provider_sort_key(provider: RepositoryProvider) -> &'static str {
    match provider {
        RepositoryProvider::GitHub => "github",
        RepositoryProvider::Gitea => "gitea",
        RepositoryProvider::GitLab => "gitlab",
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

impl RecordStore for MemoryRecordStore {
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

    fn record_locator_exists<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { Ok(self.record_entry(locator)?.is_some()) })
    }

    fn prune_empty_latest_records(&self) -> RecordStoreFuture<'_, (), Self::Error> {
        Box::pin(async move { Ok(()) })
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
    Generic,
}

impl MemoryRepositoryProvider {
    const fn from_protocol(provider: RepositoryProvider) -> Self {
        match provider {
            RepositoryProvider::GitHub => Self::GitHub,
            RepositoryProvider::Gitea => Self::Gitea,
            RepositoryProvider::GitLab => Self::GitLab,
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
        FileChunkRecord, FileId, FileReconstruction, FileRecord, IndexStore, LocalIndexStore,
        QuarantineCandidate, ReconstructionTerm, RecordStore, RepositoryRecordScope, RetentionHold,
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
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let store = LocalIndexStore::new(storage.path().to_path_buf());
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };

        assert_index_store_lifecycle_contract(&store, |store, file_id, reconstruction, xorb_id| {
            store.insert_reconstruction(file_id, reconstruction)?;
            store.insert_xorb(xorb_id)
        });
    }

    #[tokio::test]
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

    #[tokio::test]
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

    fn assert_index_store_lifecycle_contract<Store, Seed>(store: &Store, seed_index: Seed)
    where
        Store: IndexStore,
        Store::Error: Debug,
        Seed: Fn(&Store, &FileId, &FileReconstruction, &XorbId) -> Result<(), Store::Error>,
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
            Ok::<(), Store::Error>(())
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
            Ok::<(), Store::Error>(())
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
}
