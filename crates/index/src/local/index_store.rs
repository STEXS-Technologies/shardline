use std::ffi::OsStr;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use shardline_protocol::{HashParseError, ShardlineHash};
use shardline_storage::ObjectKey;

use super::error::LocalIndexStoreError;
use super::helpers::{
    ensure_parent_directory_path_components_are_not_symlinked, hex_encode_component,
    provider_repository_state_path, read_dir_if_exists, read_json_if_exists,
    read_quarantine_candidate_if_exists, read_retention_hold_if_exists,
    remove_empty_ancestors, visit_dedupe_shard_mappings_recursive,
    visit_provider_repository_states_recursive, visit_quarantine_candidates_recursive,
    visit_retention_holds_recursive, visit_webhook_deliveries_recursive, write_json_atomically,
    MAX_CONTROL_PLANE_METADATA_BYTES, MAX_RECONSTRUCTION_METADATA_BYTES,
};
use super::records::{
    DedupeShardRecord, FileReconstructionRecord, ProviderRepositoryStateRecord,
    QuarantineCandidateRecord, RetentionHoldRecord, WebhookDeliveryRecord,
};
use super::LocalIndexStore;
use crate::{
    parse_xet_hash_hex, xet_hash_hex_string, AsyncIndexStore, DedupeShardMapping, DedupeStore,
    FileId, FileReconstruction, IndexStoreFuture, LifecycleStore, ProviderRepositoryState,
    QuarantineCandidate, ReconstructionStore, ReconstructionTerm, RetentionHold, StoredObjectId,
    WebhookDelivery, XorbId,
};
use crate::local_fs::write_new_file;

impl ReconstructionStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    fn reconstruction(&self, file_id: &FileId) -> Result<Option<FileReconstruction>, Self::Error> {
        tokio::task::block_in_place(|| {
            read_json_if_exists::<FileReconstructionRecord>(
                &self.reconstruction_path(file_id),
                MAX_RECONSTRUCTION_METADATA_BYTES,
            )?
            .map(FileReconstructionRecord::into_domain)
            .transpose()
        })
    }

    fn list_reconstruction_file_ids(&self) -> Result<Vec<FileId>, Self::Error> {
        tokio::task::block_in_place(|| {
            let mut file_ids = Vec::new();
            let Some(entries) = read_dir_if_exists(&self.reconstructions_dir())? else {
                return Ok(file_ids);
            };

            for entry in entries {
                let entry = entry.map_err(LocalIndexStoreError::Io)?;
                if !entry
                    .file_type()
                    .map_err(LocalIndexStoreError::Io)?
                    .is_file()
                {
                    continue;
                }
                let path = entry.path();
                if path.extension().and_then(OsStr::to_str) != Some("json") {
                    continue;
                }
                let Some(stem) = path.file_stem().and_then(OsStr::to_str) else {
                    return Err(LocalIndexStoreError::HashParse(
                        HashParseError::InvalidLength,
                    ));
                };
                let hash = parse_xet_hash_hex(stem)?;
                file_ids.push(FileId::new(hash));
            }

            file_ids.sort_by(|left, right| xet_hash_hex_string(left.hash()).cmp(&xet_hash_hex_string(right.hash())));
            Ok(file_ids)
        })
    }

    fn delete_reconstruction(&self, file_id: &FileId) -> Result<bool, Self::Error> {
        tokio::task::block_in_place(|| {
            let path = self.reconstruction_path(file_id);
            ensure_parent_directory_path_components_are_not_symlinked(&path)?;
            match fs::remove_file(&path) {
                Ok(()) => Ok(true),
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
                Err(error) => Err(LocalIndexStoreError::Io(error)),
            }
        })
    }

    fn contains_object(&self, object_id: &StoredObjectId) -> Result<bool, Self::Error> {
        tokio::task::block_in_place(|| {
            let path = self.xorb_path(object_id);
            ensure_parent_directory_path_components_are_not_symlinked(&path)?;
            match fs::symlink_metadata(path) {
                Ok(metadata) => {
                    super::helpers::ensure_regular_metadata_file(&metadata)?;
                    Ok(true)
                }
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
                Err(error) => Err(LocalIndexStoreError::Io(error)),
            }
        })
    }
}

impl DedupeStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    fn dedupe_shard_mapping(
        &self,
        chunk_hash: &ShardlineHash,
    ) -> Result<Option<DedupeShardMapping>, Self::Error> {
        tokio::task::block_in_place(|| {
            read_json_if_exists::<DedupeShardRecord>(
                &self.dedupe_shard_path(*chunk_hash),
                MAX_CONTROL_PLANE_METADATA_BYTES,
            )?
            .map(DedupeShardRecord::into_domain)
            .transpose()
        })
    }

    fn list_dedupe_shard_mappings(&self) -> Result<Vec<DedupeShardMapping>, Self::Error> {
        tokio::task::block_in_place(|| {
            let mut collected = Vec::new();
            let mut visitor = |mapping| {
                collected.push(mapping);
                Ok::<(), LocalIndexStoreError>(())
            };
            visit_dedupe_shard_mappings_recursive(&self.dedupe_shards_dir(), &mut visitor)?;
            collected.sort_by(|left, right| {
                xet_hash_hex_string(left.chunk_hash()).cmp(&xet_hash_hex_string(right.chunk_hash()))
            });
            Ok(collected)
        })
    }

    fn visit_dedupe_shard_mappings<Visitor, VisitorError>(
        &self,
        visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(DedupeShardMapping) -> Result<(), VisitorError>,
    {
        tokio::task::block_in_place(|| {
            let mut visitor = visitor;
            visit_dedupe_shard_mappings_recursive(&self.dedupe_shards_dir(), &mut visitor)
        })
    }

    fn delete_dedupe_shard_mapping(&self, chunk_hash: &ShardlineHash) -> Result<bool, Self::Error> {
        tokio::task::block_in_place(|| {
            let path = self.dedupe_shard_path(*chunk_hash);
            ensure_parent_directory_path_components_are_not_symlinked(&path)?;
            match fs::remove_file(&path) {
                Ok(()) => {
                    remove_empty_ancestors(&path, &self.dedupe_shards_dir())?;
                    Ok(true)
                }
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
                Err(error) => Err(LocalIndexStoreError::Io(error)),
            }
        })
    }
}

impl LifecycleStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    fn quarantine_candidate(
        &self,
        object_key: &ObjectKey,
    ) -> Result<Option<QuarantineCandidate>, Self::Error> {
        tokio::task::block_in_place(|| {
            read_quarantine_candidate_if_exists(&self.quarantine_path(object_key))
        })
    }

    fn list_quarantine_candidates(&self) -> Result<Vec<QuarantineCandidate>, Self::Error> {
        tokio::task::block_in_place(|| {
            let mut collected = Vec::new();
            let mut visitor = |candidate| {
                collected.push(candidate);
                Ok::<(), LocalIndexStoreError>(())
            };
            visit_quarantine_candidates_recursive(&self.quarantine_dir(), &mut visitor)?;
            collected
                .sort_by(|left, right| left.object_key().as_str().cmp(right.object_key().as_str()));
            Ok(collected)
        })
    }

    fn visit_quarantine_candidates<Visitor, VisitorError>(
        &self,
        visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(QuarantineCandidate) -> Result<(), VisitorError>,
    {
        tokio::task::block_in_place(|| {
            let mut visitor = visitor;
            visit_quarantine_candidates_recursive(&self.quarantine_dir(), &mut visitor)
        })
    }

    fn upsert_quarantine_candidate(
        &self,
        candidate: &QuarantineCandidate,
    ) -> Result<(), Self::Error> {
        tokio::task::block_in_place(|| {
            let record = QuarantineCandidateRecord::from_domain(candidate);
            write_json_atomically(
                &self.root,
                &self.quarantine_path(candidate.object_key()),
                &record,
            )
        })
    }

    fn delete_quarantine_candidate(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
        tokio::task::block_in_place(|| {
            let path = self.quarantine_path(object_key);
            ensure_parent_directory_path_components_are_not_symlinked(&path)?;
            match fs::remove_file(&path) {
                Ok(()) => {
                    remove_empty_ancestors(&path, &self.quarantine_dir())?;
                    Ok(true)
                }
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
                Err(error) => Err(LocalIndexStoreError::Io(error)),
            }
        })
    }

    fn retention_hold(&self, object_key: &ObjectKey) -> Result<Option<RetentionHold>, Self::Error> {
        tokio::task::block_in_place(|| {
            read_retention_hold_if_exists(&self.retention_hold_path(object_key))
        })
    }

    fn list_retention_holds(&self) -> Result<Vec<RetentionHold>, Self::Error> {
        tokio::task::block_in_place(|| {
            let mut collected = Vec::new();
            let mut visitor = |hold| {
                collected.push(hold);
                Ok::<(), LocalIndexStoreError>(())
            };
            visit_retention_holds_recursive(&self.retention_holds_dir(), &mut visitor)?;
            collected
                .sort_by(|left, right| left.object_key().as_str().cmp(right.object_key().as_str()));
            Ok(collected)
        })
    }

    fn visit_retention_holds<Visitor, VisitorError>(
        &self,
        visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(RetentionHold) -> Result<(), VisitorError>,
    {
        tokio::task::block_in_place(|| {
            let mut visitor = visitor;
            visit_retention_holds_recursive(&self.retention_holds_dir(), &mut visitor)
        })
    }

    fn upsert_retention_hold(&self, hold: &RetentionHold) -> Result<(), Self::Error> {
        tokio::task::block_in_place(|| {
            let record = RetentionHoldRecord::from_domain(hold);
            write_json_atomically(
                &self.root,
                &self.retention_hold_path(hold.object_key()),
                &record,
            )
        })
    }

    fn delete_retention_hold(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
        tokio::task::block_in_place(|| {
            let path = self.retention_hold_path(object_key);
            ensure_parent_directory_path_components_are_not_symlinked(&path)?;
            match fs::remove_file(&path) {
                Ok(()) => {
                    remove_empty_ancestors(&path, &self.retention_holds_dir())?;
                    Ok(true)
                }
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
                Err(error) => Err(LocalIndexStoreError::Io(error)),
            }
        })
    }

    fn record_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error> {
        tokio::task::block_in_place(|| {
            let path = self.webhook_delivery_path(delivery);
            ensure_parent_directory_path_components_are_not_symlinked(&path)?;
            let bytes = serde_json::to_vec(&WebhookDeliveryRecord::from_domain(delivery))?;
            match write_new_file(&self.root, &path, &bytes).map_err(LocalIndexStoreError::Io) {
                Ok(()) => Ok(true),
                Err(LocalIndexStoreError::Io(error))
                    if error.kind() == ErrorKind::AlreadyExists =>
                {
                    Ok(false)
                }
                Err(error) => Err(error),
            }
        })
    }

    fn list_webhook_deliveries(&self) -> Result<Vec<WebhookDelivery>, Self::Error> {
        tokio::task::block_in_place(|| {
            let mut collected = Vec::new();
            let mut visitor = |delivery| {
                collected.push(delivery);
                Ok::<(), LocalIndexStoreError>(())
            };
            visit_webhook_deliveries_recursive(&self.webhook_deliveries_dir(), &mut visitor)?;
            collected.sort_by(|left, right| {
                left.provider()
                    .as_str()
                    .cmp(right.provider().as_str())
                    .then_with(|| left.owner().cmp(right.owner()))
                    .then_with(|| left.repo().cmp(right.repo()))
                    .then_with(|| left.delivery_id().cmp(right.delivery_id()))
            });
            Ok(collected)
        })
    }

    fn delete_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error> {
        tokio::task::block_in_place(|| {
            let path = self.webhook_delivery_path(delivery);
            ensure_parent_directory_path_components_are_not_symlinked(&path)?;
            match fs::remove_file(&path) {
                Ok(()) => {
                    remove_empty_ancestors(&path, &self.webhook_deliveries_dir())?;
                    Ok(true)
                }
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
                Err(error) => Err(LocalIndexStoreError::Io(error)),
            }
        })
    }

    fn provider_repository_state(
        &self,
        provider: shardline_protocol::RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<Option<ProviderRepositoryState>, Self::Error> {
        tokio::task::block_in_place(|| {
            read_json_if_exists::<ProviderRepositoryStateRecord>(
                &provider_repository_state_path(
                    &self.provider_repository_states_dir(),
                    provider,
                    owner,
                    repo,
                ),
                MAX_CONTROL_PLANE_METADATA_BYTES,
            )?
            .map(ProviderRepositoryStateRecord::into_domain)
            .transpose()
        })
    }

    fn list_provider_repository_states(&self) -> Result<Vec<ProviderRepositoryState>, Self::Error> {
        tokio::task::block_in_place(|| {
            let mut collected = Vec::new();
            let mut visitor = |state| {
                collected.push(state);
                Ok::<(), LocalIndexStoreError>(())
            };
            visit_provider_repository_states_recursive(
                &self.provider_repository_states_dir(),
                &mut visitor,
            )?;
            collected.sort_by(|left, right| {
                left.provider()
                    .as_str()
                    .cmp(right.provider().as_str())
                    .then_with(|| left.owner().cmp(right.owner()))
                    .then_with(|| left.repo().cmp(right.repo()))
            });
            Ok(collected)
        })
    }

    fn upsert_provider_repository_state(
        &self,
        state: &ProviderRepositoryState,
    ) -> Result<(), Self::Error> {
        tokio::task::block_in_place(|| {
            let record = ProviderRepositoryStateRecord::from_domain(state);
            write_json_atomically(
                &self.root,
                &self.provider_repository_state_path(state),
                &record,
            )
        })
    }

    fn delete_provider_repository_state(
        &self,
        provider: shardline_protocol::RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<bool, Self::Error> {
        tokio::task::block_in_place(|| {
            let path = provider_repository_state_path(
                &self.provider_repository_states_dir(),
                provider,
                owner,
                repo,
            );
            ensure_parent_directory_path_components_are_not_symlinked(&path)?;
            match fs::remove_file(&path) {
                Ok(()) => {
                    remove_empty_ancestors(&path, &self.provider_repository_states_dir())?;
                    Ok(true)
                }
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
                Err(error) => Err(LocalIndexStoreError::Io(error)),
            }
        })
    }
}

impl AsyncIndexStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    fn reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, Option<FileReconstruction>, Self::Error> {
        Box::pin(async move { tokio::task::block_in_place(|| ReconstructionStore::reconstruction(self, file_id)) })
    }

    fn insert_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
        reconstruction: &'operation FileReconstruction,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { tokio::task::block_in_place(|| self.insert_reconstruction(file_id, reconstruction)) })
    }

    fn list_reconstruction_file_ids(&self) -> IndexStoreFuture<'_, Vec<FileId>, Self::Error> {
        Box::pin(async move { tokio::task::block_in_place(|| ReconstructionStore::list_reconstruction_file_ids(self)) })
    }

    fn delete_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { tokio::task::block_in_place(|| ReconstructionStore::delete_reconstruction(self, file_id)) })
    }

    fn contains_object<'operation>(
        &'operation self,
        object_id: &'operation StoredObjectId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { tokio::task::block_in_place(|| ReconstructionStore::contains_object(self, object_id)) })
    }

    fn insert_object<'operation>(
        &'operation self,
        object_id: &'operation StoredObjectId,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { tokio::task::block_in_place(|| self.insert_object(object_id)) })
    }

    fn dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, Option<DedupeShardMapping>, Self::Error> {
        Box::pin(async move { tokio::task::block_in_place(|| DedupeStore::dedupe_shard_mapping(self, chunk_hash)) })
    }

    fn list_dedupe_shard_mappings(
        &self,
    ) -> IndexStoreFuture<'_, Vec<DedupeShardMapping>, Self::Error> {
        Box::pin(async move { tokio::task::block_in_place(|| DedupeStore::list_dedupe_shard_mappings(self)) })
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
        Box::pin(async move { tokio::task::block_in_place(|| DedupeStore::visit_dedupe_shard_mappings(self, visitor)) })
    }

    fn upsert_dedupe_shard_mapping<'operation>(
        &'operation self,
        mapping: &'operation DedupeShardMapping,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { tokio::task::block_in_place(|| self.upsert_dedupe_shard_mapping(mapping)) })
    }

    fn delete_dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { tokio::task::block_in_place(|| DedupeStore::delete_dedupe_shard_mapping(self, chunk_hash)) })
    }

    impl_async_lifecycle_delegation!(LocalIndexStore);
}
