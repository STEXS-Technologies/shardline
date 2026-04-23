use std::{future::Future, pin::Pin};

use shardline_protocol::{RepositoryProvider, ShardlineHash};
use shardline_storage::ObjectKey;

use crate::{
    DedupeShardMapping, FileId, FileReconstruction, ProviderRepositoryState, QuarantineCandidate,
    RetentionHold, StoredObjectId, WebhookDelivery, XorbId,
};

/// Boxed asynchronous index-store operation.
pub type IndexStoreFuture<'operation, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'operation>>;

/// Metadata index adapter contract.
pub trait IndexStore {
    /// Adapter-specific error type.
    type Error;

    /// Loads a file reconstruction by file ID.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the index lookup fails.
    fn reconstruction(&self, file_id: &FileId) -> Result<Option<FileReconstruction>, Self::Error>;

    /// Lists every persisted file-reconstruction identifier.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the inventory lookup fails.
    fn list_reconstruction_file_ids(&self) -> Result<Vec<FileId>, Self::Error>;

    /// Deletes one persisted file reconstruction.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when persistence fails.
    fn delete_reconstruction(&self, file_id: &FileId) -> Result<bool, Self::Error>;

    /// Returns whether a stored object is registered.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the index lookup fails.
    fn contains_object(&self, object_id: &StoredObjectId) -> Result<bool, Self::Error>;

    /// Backward-compatible Xet alias for [`Self::contains_object`].
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the index lookup fails.
    fn contains_xorb(&self, xorb_id: &XorbId) -> Result<bool, Self::Error> {
        self.contains_object(xorb_id)
    }

    /// Loads the retained shard mapping for one chunk hash.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the index lookup fails.
    fn dedupe_shard_mapping(
        &self,
        chunk_hash: &ShardlineHash,
    ) -> Result<Option<DedupeShardMapping>, Self::Error>;

    /// Lists every retained dedupe-shard mapping.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the inventory lookup fails.
    fn list_dedupe_shard_mappings(&self) -> Result<Vec<DedupeShardMapping>, Self::Error>;

    /// Visits every retained dedupe-shard mapping.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the inventory lookup fails or when the visitor
    /// rejects an entry.
    fn visit_dedupe_shard_mappings<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(DedupeShardMapping) -> Result<(), VisitorError>,
    {
        for mapping in self.list_dedupe_shard_mappings().map_err(Into::into)? {
            visitor(mapping)?;
        }

        Ok(())
    }

    /// Deletes one retained dedupe-shard mapping.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when persistence fails.
    fn delete_dedupe_shard_mapping(&self, chunk_hash: &ShardlineHash) -> Result<bool, Self::Error>;

    /// Loads durable quarantine state for one object key.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the index lookup fails.
    fn quarantine_candidate(
        &self,
        object_key: &ObjectKey,
    ) -> Result<Option<QuarantineCandidate>, Self::Error>;

    /// Lists every durable quarantine candidate.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the inventory lookup fails.
    fn list_quarantine_candidates(&self) -> Result<Vec<QuarantineCandidate>, Self::Error>;

    /// Visits every durable quarantine candidate.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the inventory lookup fails or when the visitor
    /// rejects an entry.
    fn visit_quarantine_candidates<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(QuarantineCandidate) -> Result<(), VisitorError>,
    {
        for candidate in self.list_quarantine_candidates().map_err(Into::into)? {
            visitor(candidate)?;
        }

        Ok(())
    }

    /// Inserts or replaces durable quarantine state for one object key.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when persistence fails.
    fn upsert_quarantine_candidate(
        &self,
        candidate: &QuarantineCandidate,
    ) -> Result<(), Self::Error>;

    /// Deletes durable quarantine state for one object key.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when persistence fails.
    fn delete_quarantine_candidate(&self, object_key: &ObjectKey) -> Result<bool, Self::Error>;

    /// Loads durable retention-hold state for one object key.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the index lookup fails.
    fn retention_hold(&self, object_key: &ObjectKey) -> Result<Option<RetentionHold>, Self::Error>;

    /// Lists every durable retention hold.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the inventory lookup fails.
    fn list_retention_holds(&self) -> Result<Vec<RetentionHold>, Self::Error>;

    /// Visits every durable retention hold.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the inventory lookup fails or when the visitor
    /// rejects an entry.
    fn visit_retention_holds<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(RetentionHold) -> Result<(), VisitorError>,
    {
        for hold in self.list_retention_holds().map_err(Into::into)? {
            visitor(hold)?;
        }

        Ok(())
    }

    /// Inserts or replaces durable retention-hold state for one object key.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when persistence fails.
    fn upsert_retention_hold(&self, hold: &RetentionHold) -> Result<(), Self::Error>;

    /// Deletes durable retention-hold state for one object key.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when persistence fails.
    fn delete_retention_hold(&self, object_key: &ObjectKey) -> Result<bool, Self::Error>;

    /// Records a processed provider webhook delivery once.
    ///
    /// Returns `true` when the delivery was newly recorded and `false` when the same
    /// provider delivery identifier had already been processed before.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when persistence fails.
    fn record_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error>;

    /// Lists every processed provider webhook delivery claim.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the inventory lookup fails.
    fn list_webhook_deliveries(&self) -> Result<Vec<WebhookDelivery>, Self::Error>;

    /// Visits every processed provider webhook delivery claim.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the inventory lookup fails or when the visitor
    /// rejects an entry.
    fn visit_webhook_deliveries<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(WebhookDelivery) -> Result<(), VisitorError>,
    {
        for delivery in self.list_webhook_deliveries().map_err(Into::into)? {
            visitor(delivery)?;
        }

        Ok(())
    }

    /// Deletes a recorded provider webhook delivery claim.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when persistence fails.
    fn delete_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error>;

    /// Loads durable provider-derived lifecycle state for one repository.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the lookup fails.
    fn provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<Option<ProviderRepositoryState>, Self::Error>;

    /// Lists every durable provider-derived repository lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the inventory lookup fails.
    fn list_provider_repository_states(&self) -> Result<Vec<ProviderRepositoryState>, Self::Error>;

    /// Visits every durable provider-derived repository lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the inventory lookup fails or the visitor rejects an entry.
    fn visit_provider_repository_states<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(ProviderRepositoryState) -> Result<(), VisitorError>,
    {
        for state in self.list_provider_repository_states().map_err(Into::into)? {
            visitor(state)?;
        }

        Ok(())
    }

    /// Inserts or replaces durable provider-derived lifecycle state for one repository.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when persistence fails.
    fn upsert_provider_repository_state(
        &self,
        state: &ProviderRepositoryState,
    ) -> Result<(), Self::Error>;

    /// Deletes durable provider-derived lifecycle state for one repository.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when persistence fails.
    fn delete_provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<bool, Self::Error>;
}

/// Asynchronous metadata index adapter contract.
pub trait AsyncIndexStore {
    /// Adapter-specific error type.
    type Error;

    /// Loads a file reconstruction by file ID.
    fn reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, Option<FileReconstruction>, Self::Error>;

    /// Inserts or replaces a file reconstruction by file ID.
    fn insert_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
        reconstruction: &'operation FileReconstruction,
    ) -> IndexStoreFuture<'operation, (), Self::Error>;

    /// Lists every persisted file-reconstruction identifier.
    fn list_reconstruction_file_ids(&self) -> IndexStoreFuture<'_, Vec<FileId>, Self::Error>;

    /// Deletes one persisted file reconstruction.
    fn delete_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error>;

    /// Returns whether a stored object is registered.
    fn contains_object<'operation>(
        &'operation self,
        object_id: &'operation StoredObjectId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error>;

    /// Backward-compatible Xet alias for [`Self::contains_object`].
    fn contains_xorb<'operation>(
        &'operation self,
        xorb_id: &'operation XorbId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        self.contains_object(xorb_id)
    }

    /// Inserts stored-object presence metadata.
    fn insert_object<'operation>(
        &'operation self,
        object_id: &'operation StoredObjectId,
    ) -> IndexStoreFuture<'operation, (), Self::Error>;

    /// Backward-compatible Xet alias for [`Self::insert_object`].
    fn insert_xorb<'operation>(
        &'operation self,
        xorb_id: &'operation XorbId,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        self.insert_object(xorb_id)
    }

    /// Loads the retained shard mapping for one chunk hash.
    fn dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, Option<DedupeShardMapping>, Self::Error>;

    /// Lists every retained dedupe-shard mapping.
    fn list_dedupe_shard_mappings(
        &self,
    ) -> IndexStoreFuture<'_, Vec<DedupeShardMapping>, Self::Error>;

    /// Visits every retained dedupe-shard mapping.
    fn visit_dedupe_shard_mappings<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(DedupeShardMapping) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            for mapping in self
                .list_dedupe_shard_mappings()
                .await
                .map_err(Into::into)?
            {
                visitor(mapping)?;
            }

            Ok(())
        })
    }

    /// Inserts or replaces one chunk-hash to retained-shard mapping.
    fn upsert_dedupe_shard_mapping<'operation>(
        &'operation self,
        mapping: &'operation DedupeShardMapping,
    ) -> IndexStoreFuture<'operation, (), Self::Error>;

    /// Deletes one retained dedupe-shard mapping.
    fn delete_dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, bool, Self::Error>;

    /// Loads durable quarantine state for one object key.
    fn quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<QuarantineCandidate>, Self::Error>;

    /// Lists every durable quarantine candidate.
    fn list_quarantine_candidates(
        &self,
    ) -> IndexStoreFuture<'_, Vec<QuarantineCandidate>, Self::Error>;

    /// Visits every durable quarantine candidate.
    fn visit_quarantine_candidates<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(QuarantineCandidate) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            for candidate in self
                .list_quarantine_candidates()
                .await
                .map_err(Into::into)?
            {
                visitor(candidate)?;
            }

            Ok(())
        })
    }

    /// Inserts or replaces durable quarantine state for one object key.
    fn upsert_quarantine_candidate<'operation>(
        &'operation self,
        candidate: &'operation QuarantineCandidate,
    ) -> IndexStoreFuture<'operation, (), Self::Error>;

    /// Deletes durable quarantine state for one object key.
    fn delete_quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error>;

    /// Loads durable retention-hold state for one object key.
    fn retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<RetentionHold>, Self::Error>;

    /// Lists every durable retention hold.
    fn list_retention_holds(&self) -> IndexStoreFuture<'_, Vec<RetentionHold>, Self::Error>;

    /// Visits every durable retention hold.
    fn visit_retention_holds<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(RetentionHold) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            for hold in self.list_retention_holds().await.map_err(Into::into)? {
                visitor(hold)?;
            }

            Ok(())
        })
    }

    /// Inserts or replaces durable retention-hold state for one object key.
    fn upsert_retention_hold<'operation>(
        &'operation self,
        hold: &'operation RetentionHold,
    ) -> IndexStoreFuture<'operation, (), Self::Error>;

    /// Deletes durable retention-hold state for one object key.
    fn delete_retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error>;

    /// Records a processed provider webhook delivery once.
    ///
    /// Returns `true` when the delivery was newly recorded and `false` when it had
    /// already been processed before.
    fn record_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error>;

    /// Lists every processed provider webhook delivery claim.
    fn list_webhook_deliveries(&self) -> IndexStoreFuture<'_, Vec<WebhookDelivery>, Self::Error>;

    /// Visits every processed provider webhook delivery claim.
    fn visit_webhook_deliveries<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(WebhookDelivery) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            for delivery in self.list_webhook_deliveries().await.map_err(Into::into)? {
                visitor(delivery)?;
            }

            Ok(())
        })
    }

    /// Deletes a recorded provider webhook delivery claim.
    fn delete_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error>;

    /// Loads durable provider-derived lifecycle state for one repository.
    fn provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, Option<ProviderRepositoryState>, Self::Error>;

    /// Lists every durable provider-derived repository lifecycle state.
    fn list_provider_repository_states(
        &self,
    ) -> IndexStoreFuture<'_, Vec<ProviderRepositoryState>, Self::Error>;

    /// Visits every durable provider-derived repository lifecycle state.
    fn visit_provider_repository_states<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(ProviderRepositoryState) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            for state in self
                .list_provider_repository_states()
                .await
                .map_err(Into::into)?
            {
                visitor(state)?;
            }

            Ok(())
        })
    }

    /// Inserts or replaces durable provider-derived lifecycle state for one repository.
    fn upsert_provider_repository_state<'operation>(
        &'operation self,
        state: &'operation ProviderRepositoryState,
    ) -> IndexStoreFuture<'operation, (), Self::Error>;

    /// Deletes durable provider-derived lifecycle state for one repository.
    fn delete_provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, bool, Self::Error>;
}
