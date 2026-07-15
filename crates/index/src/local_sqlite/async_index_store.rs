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
            tokio::task::spawn_blocking(move || LocalIndexStore::insert_object(&store, &object_id))
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
            tokio::task::spawn_blocking(move || DedupeStore::list_dedupe_shard_mappings(&store))
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
            tokio::task::spawn_blocking(move || LifecycleStore::list_quarantine_candidates(&store))
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
            tokio::task::spawn_blocking(move || LifecycleStore::retention_hold(&store, &object_key))
                .await
                .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
        })
    }

    fn list_retention_holds(&self) -> IndexStoreFuture<'_, Vec<RetentionHold>, Self::Error> {
        let store = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || LifecycleStore::list_retention_holds(&store))
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
            tokio::task::spawn_blocking(move || LifecycleStore::list_webhook_deliveries(&store))
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

#[cfg(test)]
mod tests {
    use shardline_protocol::{ChunkRange, RepositoryProvider};

    use super::*;
    use crate::{
        AsyncIndexStore, ProviderRepositoryState, QuarantineCandidate, ReconstructionTerm,
        RetentionHold, WebhookDelivery,
    };

    fn make_store() -> super::super::LocalIndexStore {
        let storage = shardline_test_support::TempStorage::new();
        super::super::LocalIndexStore::new(storage.path_buf())
            .expect("failed to create local index store")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn visit_quarantine_candidates_on_empty_store_does_not_call_visitor() {
        let store = make_store();
        let mut call_count = 0u32;
        AsyncIndexStore::visit_quarantine_candidates(&store, |_| {
            call_count += 1;
            Ok::<(), LocalIndexStoreError>(())
        })
        .await
        .expect("visit should succeed");
        assert_eq!(call_count, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn visit_retention_holds_on_empty_store_does_not_call_visitor() {
        let store = make_store();
        let mut call_count = 0u32;
        AsyncIndexStore::visit_retention_holds(&store, |_| {
            call_count += 1;
            Ok::<(), LocalIndexStoreError>(())
        })
        .await
        .expect("visit should succeed");
        assert_eq!(call_count, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn visit_webhook_deliveries_on_empty_store_does_not_call_visitor() {
        let store = make_store();
        let mut call_count = 0u32;
        AsyncIndexStore::visit_webhook_deliveries(&store, |_| {
            call_count += 1;
            Ok::<(), LocalIndexStoreError>(())
        })
        .await
        .expect("visit should succeed");
        assert_eq!(call_count, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn visit_provider_repository_states_on_empty_store_does_not_call_visitor() {
        let store = make_store();
        let mut call_count = 0u32;
        AsyncIndexStore::visit_provider_repository_states(&store, |_| {
            call_count += 1;
            Ok::<(), LocalIndexStoreError>(())
        })
        .await
        .expect("visit should succeed");
        assert_eq!(call_count, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn visit_dedupe_shard_mappings_on_empty_store_does_not_call_visitor() {
        let store = make_store();
        let mut call_count = 0u32;
        AsyncIndexStore::visit_dedupe_shard_mappings(&store, |_| {
            call_count += 1;
            Ok::<(), LocalIndexStoreError>(())
        })
        .await
        .expect("visit should succeed");
        assert_eq!(call_count, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_insert_and_get_reconstruction_roundtrip() {
        let store = make_store();
        let file_id = FileId::new(ShardlineHash::from_bytes([1; 32]));
        let object_id = StoredObjectId::new(ShardlineHash::from_bytes([2; 32]));
        let range = ChunkRange::new(0, 1).unwrap();
        let reconstruction =
            FileReconstruction::new(vec![ReconstructionTerm::new(object_id, range, 100)]);

        AsyncIndexStore::insert_reconstruction(&store, &file_id, &reconstruction)
            .await
            .expect("insert should succeed");
        let loaded = AsyncIndexStore::reconstruction(&store, &file_id)
            .await
            .expect("lookup should succeed");
        assert_eq!(loaded, Some(reconstruction));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_delete_reconstruction_returns_true_then_false() {
        let store = make_store();
        let file_id = FileId::new(ShardlineHash::from_bytes([3; 32]));
        let reconstruction = FileReconstruction::new(vec![]);

        AsyncIndexStore::insert_reconstruction(&store, &file_id, &reconstruction)
            .await
            .expect("insert should succeed");
        let deleted = AsyncIndexStore::delete_reconstruction(&store, &file_id)
            .await
            .expect("delete should succeed");
        assert!(deleted);
        let deleted_again = AsyncIndexStore::delete_reconstruction(&store, &file_id)
            .await
            .expect("second delete should succeed");
        assert!(!deleted_again);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_insert_object_and_contains_object() {
        let store = make_store();
        let object_id = StoredObjectId::new(ShardlineHash::from_bytes([5; 32]));

        assert!(
            !AsyncIndexStore::contains_object(&store, &object_id)
                .await
                .unwrap()
        );
        AsyncIndexStore::insert_object(&store, &object_id)
            .await
            .expect("insert should succeed");
        assert!(
            AsyncIndexStore::contains_object(&store, &object_id)
                .await
                .unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_dedupe_shard_mapping_roundtrip() {
        let store = make_store();
        let chunk_hash = ShardlineHash::from_bytes([7; 32]);
        let object_key = shardline_storage::ObjectKey::parse("shards/aa/test.shard").unwrap();
        let mapping = DedupeShardMapping::new(chunk_hash, object_key);

        AsyncIndexStore::upsert_dedupe_shard_mapping(&store, &mapping)
            .await
            .expect("upsert should succeed");
        let loaded = AsyncIndexStore::dedupe_shard_mapping(&store, &chunk_hash)
            .await
            .expect("lookup should succeed");
        assert_eq!(loaded, Some(mapping));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_list_reconstruction_file_ids_empty_initially() {
        let store = make_store();
        let ids = AsyncIndexStore::list_reconstruction_file_ids(&store)
            .await
            .expect("list should succeed");
        assert!(ids.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_list_reconstruction_file_ids_after_insert() {
        let store = make_store();
        let file_id = FileId::new(ShardlineHash::from_bytes([10; 32]));
        let reconstruction = FileReconstruction::new(vec![]);

        AsyncIndexStore::insert_reconstruction(&store, &file_id, &reconstruction)
            .await
            .expect("insert should succeed");
        let ids = AsyncIndexStore::list_reconstruction_file_ids(&store)
            .await
            .expect("list should succeed");
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], file_id);
    }

    // ── Async LifecycleStore operations ───────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_quarantine_candidate_upsert_and_read() {
        let store = make_store();
        let key = shardline_storage::ObjectKey::parse("chunks/aa/async-cand").unwrap();
        let candidate = QuarantineCandidate::new(key.clone(), 500, 5000, 6000).unwrap();

        AsyncIndexStore::upsert_quarantine_candidate(&store, &candidate)
            .await
            .expect("upsert should succeed");
        let loaded = AsyncIndexStore::quarantine_candidate(&store, &key)
            .await
            .expect("lookup should succeed");
        assert_eq!(loaded, Some(candidate));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_quarantine_candidate_delete() {
        let store = make_store();
        let key = shardline_storage::ObjectKey::parse("chunks/bb/async-del").unwrap();
        let candidate = QuarantineCandidate::new(key.clone(), 600, 6000, 7000).unwrap();

        AsyncIndexStore::upsert_quarantine_candidate(&store, &candidate)
            .await
            .unwrap();
        assert!(
            AsyncIndexStore::delete_quarantine_candidate(&store, &key)
                .await
                .unwrap()
        );
        assert!(
            !AsyncIndexStore::delete_quarantine_candidate(&store, &key)
                .await
                .unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_retention_hold_upsert_and_read() {
        let store = make_store();
        let key = shardline_storage::ObjectKey::parse("chunks/cc/async-hold").unwrap();
        let hold = RetentionHold::new(key.clone(), "async reason".into(), 700, None).unwrap();

        AsyncIndexStore::upsert_retention_hold(&store, &hold)
            .await
            .expect("upsert should succeed");
        let loaded = AsyncIndexStore::retention_hold(&store, &key)
            .await
            .expect("lookup should succeed");
        assert_eq!(loaded, Some(hold));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_retention_hold_delete() {
        let store = make_store();
        let key = shardline_storage::ObjectKey::parse("chunks/dd/async-del-hold").unwrap();
        let hold = RetentionHold::new(key.clone(), "del".into(), 800, None).unwrap();

        AsyncIndexStore::upsert_retention_hold(&store, &hold)
            .await
            .unwrap();
        assert!(
            AsyncIndexStore::delete_retention_hold(&store, &key)
                .await
                .unwrap()
        );
        assert!(
            !AsyncIndexStore::delete_retention_hold(&store, &key)
                .await
                .unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_webhook_delivery_record_and_list() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "async-delivery".into(),
            9000,
        )
        .unwrap();

        assert!(
            AsyncIndexStore::record_webhook_delivery(&store, &delivery)
                .await
                .expect("record should succeed")
        );
        let deliveries = AsyncIndexStore::list_webhook_deliveries(&store)
            .await
            .expect("list should succeed");
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].delivery_id(), "async-delivery");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_webhook_delivery_delete() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "async-del-delivery".into(),
            10000,
        )
        .unwrap();

        AsyncIndexStore::record_webhook_delivery(&store, &delivery)
            .await
            .unwrap();
        assert!(
            AsyncIndexStore::delete_webhook_delivery(&store, &delivery)
                .await
                .unwrap()
        );
        assert!(
            !AsyncIndexStore::delete_webhook_delivery(&store, &delivery)
                .await
                .unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_provider_repository_state_upsert_and_read() {
        let store = make_store();
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "async-team".into(),
            "async-repo".into(),
            Some(1000),
            Some(2000),
            Some("refs/heads/main".into()),
        );

        AsyncIndexStore::upsert_provider_repository_state(&store, &state)
            .await
            .expect("upsert should succeed");
        let loaded = AsyncIndexStore::provider_repository_state(
            &store,
            RepositoryProvider::GitHub,
            "async-team",
            "async-repo",
        )
        .await
        .expect("lookup should succeed");
        assert_eq!(loaded, Some(state));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_provider_repository_state_delete() {
        let store = make_store();
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "del-team".into(),
            "del-repo".into(),
            None,
            None,
            None,
        );

        AsyncIndexStore::upsert_provider_repository_state(&store, &state)
            .await
            .unwrap();
        assert!(
            AsyncIndexStore::delete_provider_repository_state(
                &store,
                RepositoryProvider::GitHub,
                "del-team",
                "del-repo",
            )
            .await
            .unwrap()
        );
        assert!(
            !AsyncIndexStore::delete_provider_repository_state(
                &store,
                RepositoryProvider::GitHub,
                "del-team",
                "del-repo",
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_insert_xorb_then_contains_xorb() {
        let store = make_store();
        let hash = ShardlineHash::from_bytes([20; 32]);
        let xorb_id = crate::XorbId::new(hash);

        AsyncIndexStore::insert_xorb(&store, &xorb_id)
            .await
            .expect("insert_xorb should succeed");
        assert!(
            AsyncIndexStore::contains_xorb(&store, &xorb_id)
                .await
                .expect("contains_xorb should succeed")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_reconstruction_returns_none_for_missing() {
        let store = make_store();
        let file_id = FileId::new(ShardlineHash::from_bytes([99; 32]));
        let loaded = AsyncIndexStore::reconstruction(&store, &file_id)
            .await
            .expect("lookup should succeed");
        assert!(loaded.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_contains_object_returns_false_for_missing() {
        let store = make_store();
        let object_id = StoredObjectId::new(ShardlineHash::from_bytes([98; 32]));
        assert!(
            !AsyncIndexStore::contains_object(&store, &object_id)
                .await
                .unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_dedupe_shard_mapping_returns_none_for_missing() {
        let store = make_store();
        let chunk_hash = ShardlineHash::from_bytes([97; 32]);
        let loaded = AsyncIndexStore::dedupe_shard_mapping(&store, &chunk_hash)
            .await
            .expect("lookup should succeed");
        assert!(loaded.is_none());
    }
}
