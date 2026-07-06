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
        let store = self.clone();
        let file_id = *file_id;
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                ReconstructionStore::reconstruction(&store, &file_id)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn insert_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
        reconstruction: &'operation FileReconstruction,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        let store = self.clone();
        let file_id = *file_id;
        let reconstruction = reconstruction.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LocalIndexStore::insert_reconstruction(&store, &file_id, &reconstruction)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn list_reconstruction_file_ids(&self) -> IndexStoreFuture<'_, Vec<FileId>, Self::Error> {
        let store = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                ReconstructionStore::list_reconstruction_file_ids(&store)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn delete_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        let store = self.clone();
        let file_id = *file_id;
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                ReconstructionStore::delete_reconstruction(&store, &file_id)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn contains_object<'operation>(
        &'operation self,
        object_id: &'operation StoredObjectId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        let store = self.clone();
        let object_id = *object_id;
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                ReconstructionStore::contains_object(&store, &object_id)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn insert_object<'operation>(
        &'operation self,
        object_id: &'operation StoredObjectId,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        let store = self.clone();
        let object_id = *object_id;
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LocalIndexStore::insert_object(&store, &object_id)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, Option<DedupeShardMapping>, Self::Error> {
        let store = self.clone();
        let chunk_hash = *chunk_hash;
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                DedupeStore::dedupe_shard_mapping(&store, &chunk_hash)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn list_dedupe_shard_mappings(
        &self,
    ) -> IndexStoreFuture<'_, Vec<DedupeShardMapping>, Self::Error> {
        let store = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                DedupeStore::list_dedupe_shard_mappings(&store)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
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
        let store = self.clone();
        let mut visitor = visitor;
        Box::pin(async move {
            tokio::task::block_in_place(move || {
                DedupeStore::visit_dedupe_shard_mappings(&store, &mut visitor)
            })
        })
    }

    fn upsert_dedupe_shard_mapping<'operation>(
        &'operation self,
        mapping: &'operation DedupeShardMapping,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        let store = self.clone();
        let mapping = mapping.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LocalIndexStore::upsert_dedupe_shard_mapping(&store, &mapping)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn delete_dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        let store = self.clone();
        let chunk_hash = *chunk_hash;
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                DedupeStore::delete_dedupe_shard_mapping(&store, &chunk_hash)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<QuarantineCandidate>, Self::Error> {
        let store = self.clone();
        let object_key = object_key.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LifecycleStore::quarantine_candidate(&store, &object_key)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn list_quarantine_candidates(
        &self,
    ) -> IndexStoreFuture<'_, Vec<QuarantineCandidate>, Self::Error> {
        let store = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LifecycleStore::list_quarantine_candidates(&store)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
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
        let store = self.clone();
        let mut visitor = visitor;
        Box::pin(async move {
            tokio::task::block_in_place(move || {
                LifecycleStore::visit_quarantine_candidates(&store, &mut visitor)
            })
        })
    }

    fn upsert_quarantine_candidate<'operation>(
        &'operation self,
        candidate: &'operation QuarantineCandidate,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        let store = self.clone();
        let candidate = candidate.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LifecycleStore::upsert_quarantine_candidate(&store, &candidate)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn delete_quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        let store = self.clone();
        let object_key = object_key.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LifecycleStore::delete_quarantine_candidate(&store, &object_key)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<RetentionHold>, Self::Error> {
        let store = self.clone();
        let object_key = object_key.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LifecycleStore::retention_hold(&store, &object_key)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn list_retention_holds(&self) -> IndexStoreFuture<'_, Vec<RetentionHold>, Self::Error> {
        let store = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LifecycleStore::list_retention_holds(&store)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
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
        let store = self.clone();
        let mut visitor = visitor;
        Box::pin(async move {
            tokio::task::block_in_place(move || {
                LifecycleStore::visit_retention_holds(&store, &mut visitor)
            })
        })
    }

    fn upsert_retention_hold<'operation>(
        &'operation self,
        hold: &'operation RetentionHold,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        let store = self.clone();
        let hold = hold.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LifecycleStore::upsert_retention_hold(&store, &hold)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn delete_retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        let store = self.clone();
        let object_key = object_key.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LifecycleStore::delete_retention_hold(&store, &object_key)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn record_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        let store = self.clone();
        let delivery = delivery.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LifecycleStore::record_webhook_delivery(&store, &delivery)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn list_webhook_deliveries(&self) -> IndexStoreFuture<'_, Vec<WebhookDelivery>, Self::Error> {
        let store = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LifecycleStore::list_webhook_deliveries(&store)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
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
        let store = self.clone();
        let mut visitor = visitor;
        Box::pin(async move {
            tokio::task::block_in_place(move || {
                LifecycleStore::visit_webhook_deliveries(&store, &mut visitor)
            })
        })
    }

    fn delete_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        let store = self.clone();
        let delivery = delivery.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LifecycleStore::delete_webhook_delivery(&store, &delivery)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, Option<ProviderRepositoryState>, Self::Error> {
        let store = self.clone();
        let owner = owner.to_owned();
        let repo = repo.to_owned();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LifecycleStore::provider_repository_state(&store, provider, &owner, &repo)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn list_provider_repository_states(
        &self,
    ) -> IndexStoreFuture<'_, Vec<ProviderRepositoryState>, Self::Error> {
        let store = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LifecycleStore::list_provider_repository_states(&store)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
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
        let store = self.clone();
        let mut visitor = visitor;
        Box::pin(async move {
            tokio::task::block_in_place(move || {
                LifecycleStore::visit_provider_repository_states(&store, &mut visitor)
            })
        })
    }

    fn upsert_provider_repository_state<'operation>(
        &'operation self,
        state: &'operation ProviderRepositoryState,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        let store = self.clone();
        let state = state.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LifecycleStore::upsert_provider_repository_state(&store, &state)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn delete_provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        let store = self.clone();
        let owner = owner.to_owned();
        let repo = repo.to_owned();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LifecycleStore::delete_provider_repository_state(&store, provider, &owner, &repo)
            })
            .await
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }
}
