use shardline_protocol::{RepositoryProvider, ShardlineHash};
use shardline_storage::ObjectKey;

use super::{LocalIndexStore, LocalIndexStoreError};
use crate::{
    AsyncIndexStore, DedupeShardMapping, DedupeStore, FileId, FileReconstruction, IndexStoreFuture,
    LifecycleStore, ProviderRepositoryState, QuarantineCandidate, ReconstructionStore,
    RetentionHold, StoredObjectId, WebhookDelivery,
};

impl AsyncIndexStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

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
        Box::pin(
            async move { LocalIndexStore::insert_reconstruction(self, file_id, reconstruction) },
        )
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
        Box::pin(async move { LocalIndexStore::insert_object(self, object_id) })
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
        Box::pin(async move { LocalIndexStore::upsert_dedupe_shard_mapping(self, mapping) })
    }

    fn delete_dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { DedupeStore::delete_dedupe_shard_mapping(self, chunk_hash) })
    }

    fn quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<QuarantineCandidate>, Self::Error> {
        Box::pin(async move { LifecycleStore::quarantine_candidate(self, object_key) })
    }

    fn list_quarantine_candidates(
        &self,
    ) -> IndexStoreFuture<'_, Vec<QuarantineCandidate>, Self::Error> {
        Box::pin(async move { LifecycleStore::list_quarantine_candidates(self) })
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
        Box::pin(async move { LifecycleStore::visit_quarantine_candidates(self, visitor) })
    }

    fn upsert_quarantine_candidate<'operation>(
        &'operation self,
        candidate: &'operation QuarantineCandidate,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { LifecycleStore::upsert_quarantine_candidate(self, candidate) })
    }

    fn delete_quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { LifecycleStore::delete_quarantine_candidate(self, object_key) })
    }

    fn retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<RetentionHold>, Self::Error> {
        Box::pin(async move { LifecycleStore::retention_hold(self, object_key) })
    }

    fn list_retention_holds(&self) -> IndexStoreFuture<'_, Vec<RetentionHold>, Self::Error> {
        Box::pin(async move { LifecycleStore::list_retention_holds(self) })
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
        Box::pin(async move { LifecycleStore::visit_retention_holds(self, visitor) })
    }

    fn upsert_retention_hold<'operation>(
        &'operation self,
        hold: &'operation RetentionHold,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { LifecycleStore::upsert_retention_hold(self, hold) })
    }

    fn delete_retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { LifecycleStore::delete_retention_hold(self, object_key) })
    }

    fn record_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { LifecycleStore::record_webhook_delivery(self, delivery) })
    }

    fn list_webhook_deliveries(&self) -> IndexStoreFuture<'_, Vec<WebhookDelivery>, Self::Error> {
        Box::pin(async move { LifecycleStore::list_webhook_deliveries(self) })
    }

    fn visit_webhook_deliveries<'operation, Visitor, VisitorError>(
        &'operation self,
        visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(WebhookDelivery) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move { LifecycleStore::visit_webhook_deliveries(self, visitor) })
    }

    fn delete_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { LifecycleStore::delete_webhook_delivery(self, delivery) })
    }

    fn provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, Option<ProviderRepositoryState>, Self::Error> {
        Box::pin(async move {
            LifecycleStore::provider_repository_state(self, provider, owner, repo)
        })
    }

    fn list_provider_repository_states(
        &self,
    ) -> IndexStoreFuture<'_, Vec<ProviderRepositoryState>, Self::Error> {
        Box::pin(async move { LifecycleStore::list_provider_repository_states(self) })
    }

    fn visit_provider_repository_states<'operation, Visitor, VisitorError>(
        &'operation self,
        visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(ProviderRepositoryState) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            LifecycleStore::visit_provider_repository_states(self, visitor)
        })
    }

    fn upsert_provider_repository_state<'operation>(
        &'operation self,
        state: &'operation ProviderRepositoryState,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { LifecycleStore::upsert_provider_repository_state(self, state) })
    }

    fn delete_provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            LifecycleStore::delete_provider_repository_state(self, provider, owner, repo)
        })
    }
}
