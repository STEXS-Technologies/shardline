#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, symlink};
#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::{
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::{Error as IoError, ErrorKind, Read},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::{Error as SerdeJsonError, from_slice, to_vec};
use shardline_protocol::{
    ChunkRange, HashParseError, RangeError, RepositoryProvider, ShardlineHash,
};
use shardline_storage::{
    DirectoryPathError, ObjectKey, ObjectKeyError,
    ensure_directory_path_components_are_not_symlinked as ensure_directory_path_components_are_not_symlinked_shared,
};
use thiserror::Error;

use crate::local_fs::{write_file_atomically, write_new_file};
use crate::{
    AsyncIndexStore, DedupeShardMapping, FileId, FileReconstruction, IndexStore, IndexStoreFuture,
    ProviderRepositoryState, QuarantineCandidate, QuarantineCandidateError, ReconstructionTerm,
    RetentionHold, RetentionHoldError, WebhookDelivery, WebhookDeliveryError, XorbId,
    provider::parse_repository_provider,
};
const MAX_CONTROL_PLANE_METADATA_BYTES: u64 = 1_048_576;
const MAX_RECONSTRUCTION_METADATA_BYTES: u64 = 1_073_741_824;

#[cfg(test)]
type LocalMetadataReadHook = Box<dyn FnOnce() + Send>;

#[cfg(test)]
struct LocalMetadataReadHookRegistration {
    path: PathBuf,
    hook: LocalMetadataReadHook,
}

#[cfg(test)]
type LocalMetadataReadHookSlot = Option<LocalMetadataReadHookRegistration>;

#[cfg(test)]
static BEFORE_LOCAL_METADATA_READ_HOOK: LazyLock<Mutex<LocalMetadataReadHookSlot>> =
    LazyLock::new(|| Mutex::new(None));

/// Local filesystem implementation of [`IndexStore`].
#[derive(Debug, Clone)]
pub struct LocalIndexStore {
    root: PathBuf,
}

impl LocalIndexStore {
    /// Opens a local index store rooted at `root` without mutating the filesystem.
    #[must_use]
    pub const fn open(root: PathBuf) -> Self {
        Self { root }
    }

    /// Creates a local index store rooted at `root`.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when required directories cannot be created.
    pub fn new(root: PathBuf) -> Result<Self, LocalIndexStoreError> {
        let store = Self::open(root);
        ensure_directory_path_components_are_not_symlinked(&store.root)?;
        fs::create_dir_all(store.reconstructions_dir())?;
        fs::create_dir_all(store.xorbs_dir())?;
        fs::create_dir_all(store.dedupe_shards_dir())?;
        fs::create_dir_all(store.quarantine_dir())?;
        fs::create_dir_all(store.retention_holds_dir())?;
        fs::create_dir_all(store.webhook_deliveries_dir())?;
        fs::create_dir_all(store.provider_repository_states_dir())?;
        Ok(store)
    }

    /// Persists a file reconstruction.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when the reconstruction cannot be serialized or written.
    pub fn insert_reconstruction(
        &self,
        file_id: &FileId,
        reconstruction: &FileReconstruction,
    ) -> Result<(), LocalIndexStoreError> {
        let record = FileReconstructionRecord::from_domain(reconstruction);
        write_json_atomically(&self.root, &self.reconstruction_path(file_id), &record)
    }

    /// Persists xorb presence metadata.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when the xorb marker cannot be written.
    pub fn insert_xorb(&self, xorb_id: &XorbId) -> Result<(), LocalIndexStoreError> {
        write_json_atomically(
            &self.root,
            &self.xorb_path(xorb_id),
            &XorbPresenceRecord {
                hash: xorb_id.hash().api_hex_string(),
            },
        )
    }

    /// Persists a chunk-hash to retained-shard mapping.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when the mapping cannot be serialized or written.
    pub fn upsert_dedupe_shard_mapping(
        &self,
        mapping: &DedupeShardMapping,
    ) -> Result<(), LocalIndexStoreError> {
        write_json_atomically(
            &self.root,
            &self.dedupe_shard_path(mapping.chunk_hash()),
            &DedupeShardRecord {
                chunk_hash: mapping.chunk_hash().api_hex_string(),
                shard_object_key: mapping.shard_object_key().as_str().to_owned(),
            },
        )
    }

    fn reconstructions_dir(&self) -> PathBuf {
        self.root.join("reconstructions")
    }

    fn xorbs_dir(&self) -> PathBuf {
        self.root.join("xorbs")
    }

    fn dedupe_shards_dir(&self) -> PathBuf {
        self.root.join("dedupe-shards")
    }

    fn quarantine_dir(&self) -> PathBuf {
        self.root.join("quarantine")
    }

    fn retention_holds_dir(&self) -> PathBuf {
        self.root.join("retention-holds")
    }

    fn webhook_deliveries_dir(&self) -> PathBuf {
        self.root.join("webhook-deliveries")
    }

    fn provider_repository_states_dir(&self) -> PathBuf {
        self.root.join("provider-repository-states")
    }

    fn reconstruction_path(&self, file_id: &FileId) -> PathBuf {
        self.reconstructions_dir()
            .join(format!("{}.json", file_id.hash().api_hex_string()))
    }

    fn xorb_path(&self, xorb_id: &XorbId) -> PathBuf {
        self.xorbs_dir()
            .join(format!("{}.json", xorb_id.hash().api_hex_string()))
    }

    fn dedupe_shard_path(&self, chunk_hash: ShardlineHash) -> PathBuf {
        let hash = chunk_hash.api_hex_string();
        let prefix = hash.get(..2).unwrap_or_default();
        self.dedupe_shards_dir()
            .join(prefix)
            .join(format!("{hash}.json"))
    }

    fn quarantine_path(&self, object_key: &ObjectKey) -> PathBuf {
        self.quarantine_dir()
            .join(format!("{}.json", object_key.as_str()))
    }

    fn retention_hold_path(&self, object_key: &ObjectKey) -> PathBuf {
        self.retention_holds_dir()
            .join(format!("{}.json", object_key.as_str()))
    }

    fn webhook_delivery_path(&self, delivery: &WebhookDelivery) -> PathBuf {
        self.webhook_deliveries_dir()
            .join(delivery.provider().as_str())
            .join(hex_encode_component(delivery.owner()))
            .join(hex_encode_component(delivery.repo()))
            .join(format!(
                "{}.json",
                hex_encode_component(delivery.delivery_id())
            ))
    }

    fn provider_repository_state_path(&self, state: &ProviderRepositoryState) -> PathBuf {
        provider_repository_state_path(
            &self.provider_repository_states_dir(),
            state.provider(),
            state.owner(),
            state.repo(),
        )
    }
}

impl IndexStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    fn reconstruction(&self, file_id: &FileId) -> Result<Option<FileReconstruction>, Self::Error> {
        read_json_if_exists::<FileReconstructionRecord>(
            &self.reconstruction_path(file_id),
            MAX_RECONSTRUCTION_METADATA_BYTES,
        )?
        .map(FileReconstructionRecord::into_domain)
        .transpose()
    }

    fn list_reconstruction_file_ids(&self) -> Result<Vec<FileId>, Self::Error> {
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
            let hash = ShardlineHash::parse_api_hex(stem)?;
            file_ids.push(FileId::new(hash));
        }

        file_ids.sort_by(|left, right| {
            left.hash()
                .api_hex_string()
                .cmp(&right.hash().api_hex_string())
        });
        Ok(file_ids)
    }

    fn delete_reconstruction(&self, file_id: &FileId) -> Result<bool, Self::Error> {
        let path = self.reconstruction_path(file_id);
        ensure_parent_directory_path_components_are_not_symlinked(&path)?;
        match fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(LocalIndexStoreError::Io(error)),
        }
    }

    fn contains_xorb(&self, xorb_id: &XorbId) -> Result<bool, Self::Error> {
        let path = self.xorb_path(xorb_id);
        ensure_parent_directory_path_components_are_not_symlinked(&path)?;
        match fs::symlink_metadata(path) {
            Ok(metadata) => {
                ensure_regular_metadata_file(&metadata)?;
                Ok(true)
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(LocalIndexStoreError::Io(error)),
        }
    }

    fn dedupe_shard_mapping(
        &self,
        chunk_hash: &ShardlineHash,
    ) -> Result<Option<DedupeShardMapping>, Self::Error> {
        read_json_if_exists::<DedupeShardRecord>(
            &self.dedupe_shard_path(*chunk_hash),
            MAX_CONTROL_PLANE_METADATA_BYTES,
        )?
        .map(DedupeShardRecord::into_domain)
        .transpose()
    }

    fn list_dedupe_shard_mappings(&self) -> Result<Vec<DedupeShardMapping>, Self::Error> {
        let mut collected = Vec::new();
        let mut visitor = |mapping| {
            collected.push(mapping);
            Ok::<(), LocalIndexStoreError>(())
        };
        visit_dedupe_shard_mappings_recursive(&self.dedupe_shards_dir(), &mut visitor)?;
        collected.sort_by(|left, right| {
            left.chunk_hash()
                .api_hex_string()
                .cmp(&right.chunk_hash().api_hex_string())
        });
        Ok(collected)
    }

    fn visit_dedupe_shard_mappings<Visitor, VisitorError>(
        &self,
        visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(DedupeShardMapping) -> Result<(), VisitorError>,
    {
        let mut visitor = visitor;
        visit_dedupe_shard_mappings_recursive(&self.dedupe_shards_dir(), &mut visitor)
    }

    fn delete_dedupe_shard_mapping(&self, chunk_hash: &ShardlineHash) -> Result<bool, Self::Error> {
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
    }

    fn quarantine_candidate(
        &self,
        object_key: &ObjectKey,
    ) -> Result<Option<QuarantineCandidate>, Self::Error> {
        read_quarantine_candidate_if_exists(&self.quarantine_path(object_key))
    }

    fn list_quarantine_candidates(&self) -> Result<Vec<QuarantineCandidate>, Self::Error> {
        let mut collected = Vec::new();
        let mut visitor = |candidate| {
            collected.push(candidate);
            Ok::<(), LocalIndexStoreError>(())
        };
        visit_quarantine_candidates_recursive(&self.quarantine_dir(), &mut visitor)?;
        collected
            .sort_by(|left, right| left.object_key().as_str().cmp(right.object_key().as_str()));
        Ok(collected)
    }

    fn visit_quarantine_candidates<Visitor, VisitorError>(
        &self,
        visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(QuarantineCandidate) -> Result<(), VisitorError>,
    {
        let mut visitor = visitor;
        visit_quarantine_candidates_recursive(&self.quarantine_dir(), &mut visitor)
    }

    fn upsert_quarantine_candidate(
        &self,
        candidate: &QuarantineCandidate,
    ) -> Result<(), Self::Error> {
        let record = QuarantineCandidateRecord::from_domain(candidate);
        write_json_atomically(
            &self.root,
            &self.quarantine_path(candidate.object_key()),
            &record,
        )
    }

    fn delete_quarantine_candidate(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
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
    }

    fn retention_hold(&self, object_key: &ObjectKey) -> Result<Option<RetentionHold>, Self::Error> {
        read_retention_hold_if_exists(&self.retention_hold_path(object_key))
    }

    fn list_retention_holds(&self) -> Result<Vec<RetentionHold>, Self::Error> {
        let mut collected = Vec::new();
        let mut visitor = |hold| {
            collected.push(hold);
            Ok::<(), LocalIndexStoreError>(())
        };
        visit_retention_holds_recursive(&self.retention_holds_dir(), &mut visitor)?;
        collected
            .sort_by(|left, right| left.object_key().as_str().cmp(right.object_key().as_str()));
        Ok(collected)
    }

    fn visit_retention_holds<Visitor, VisitorError>(
        &self,
        visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(RetentionHold) -> Result<(), VisitorError>,
    {
        let mut visitor = visitor;
        visit_retention_holds_recursive(&self.retention_holds_dir(), &mut visitor)
    }

    fn upsert_retention_hold(&self, hold: &RetentionHold) -> Result<(), Self::Error> {
        let record = RetentionHoldRecord::from_domain(hold);
        write_json_atomically(
            &self.root,
            &self.retention_hold_path(hold.object_key()),
            &record,
        )
    }

    fn delete_retention_hold(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
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
    }

    fn record_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error> {
        let path = self.webhook_delivery_path(delivery);
        ensure_parent_directory_path_components_are_not_symlinked(&path)?;
        let bytes = to_vec(&WebhookDeliveryRecord::from_domain(delivery))?;
        match write_new_file(&self.root, &path, &bytes).map_err(LocalIndexStoreError::Io) {
            Ok(()) => Ok(true),
            Err(LocalIndexStoreError::Io(error))
                if error.kind() == ErrorKind::AlreadyExists =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    fn list_webhook_deliveries(&self) -> Result<Vec<WebhookDelivery>, Self::Error> {
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
    }

    fn delete_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error> {
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
    }

    fn provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<Option<ProviderRepositoryState>, Self::Error> {
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
    }

    fn list_provider_repository_states(&self) -> Result<Vec<ProviderRepositoryState>, Self::Error> {
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
    }

    fn upsert_provider_repository_state(
        &self,
        state: &ProviderRepositoryState,
    ) -> Result<(), Self::Error> {
        let record = ProviderRepositoryStateRecord::from_domain(state);
        write_json_atomically(
            &self.root,
            &self.provider_repository_state_path(state),
            &record,
        )
    }

    fn delete_provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<bool, Self::Error> {
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
    }
}

impl AsyncIndexStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

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

/// Local index-store failure.
#[derive(Debug, Error)]
pub enum LocalIndexStoreError {
    /// Local filesystem access failed.
    #[error("local index store operation failed")]
    Io(#[from] IoError),
    /// JSON serialization or deserialization failed.
    #[error("local index store json operation failed")]
    Json(#[from] SerdeJsonError),
    /// Stored metadata exceeded the bounded local parser ceiling.
    #[error("local index store metadata exceeded the bounded parser ceiling")]
    MetadataTooLarge {
        /// Observed file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted file length in bytes.
        maximum_bytes: u64,
    },
    /// Stored metadata changed after validation and was rejected.
    #[error("local index store metadata changed during bounded read")]
    MetadataLengthMismatch {
        /// Validated file length in bytes.
        expected_bytes: u64,
        /// Observed file length in bytes after bounded read.
        observed_bytes: u64,
    },
    /// A stored hash value was invalid.
    #[error("stored hash value was invalid")]
    HashParse(#[from] HashParseError),
    /// A stored object key was invalid.
    #[error("stored object key was invalid")]
    ObjectKey(#[from] ObjectKeyError),
    /// A stored chunk range was invalid.
    #[error("stored chunk range was invalid")]
    Range(#[from] RangeError),
    /// A stored retention hold was invalid.
    #[error("stored retention hold was invalid")]
    RetentionHold(#[from] RetentionHoldError),
    /// A stored quarantine candidate was invalid.
    #[error("stored quarantine candidate was invalid")]
    QuarantineCandidate(#[from] QuarantineCandidateError),
    /// A stored webhook delivery was invalid.
    #[error("stored webhook delivery was invalid")]
    WebhookDelivery(#[from] WebhookDeliveryError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileReconstructionRecord {
    terms: Vec<ReconstructionTermRecord>,
}

impl FileReconstructionRecord {
    fn from_domain(reconstruction: &FileReconstruction) -> Self {
        Self {
            terms: reconstruction
                .terms()
                .iter()
                .map(ReconstructionTermRecord::from_domain)
                .collect::<Vec<_>>(),
        }
    }

    fn into_domain(self) -> Result<FileReconstruction, LocalIndexStoreError> {
        let terms = self
            .terms
            .into_iter()
            .map(ReconstructionTermRecord::into_domain)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FileReconstruction::new(terms))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReconstructionTermRecord {
    xorb_hash: String,
    chunk_start: u32,
    chunk_end_exclusive: u32,
    unpacked_length: u64,
}

impl ReconstructionTermRecord {
    fn from_domain(term: &ReconstructionTerm) -> Self {
        Self {
            xorb_hash: term.xorb_id().hash().api_hex_string(),
            chunk_start: term.chunk_range().start(),
            chunk_end_exclusive: term.chunk_range().end_exclusive(),
            unpacked_length: term.unpacked_length(),
        }
    }

    fn into_domain(self) -> Result<ReconstructionTerm, LocalIndexStoreError> {
        let hash = ShardlineHash::parse_api_hex(&self.xorb_hash)?;
        let range = ChunkRange::new(self.chunk_start, self.chunk_end_exclusive)?;
        Ok(ReconstructionTerm::new(
            XorbId::new(hash),
            range,
            self.unpacked_length,
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QuarantineCandidateRecord {
    object_key: String,
    observed_length: u64,
    first_seen_unreachable_at_unix_seconds: u64,
    delete_after_unix_seconds: u64,
}

impl QuarantineCandidateRecord {
    fn from_domain(candidate: &QuarantineCandidate) -> Self {
        Self {
            object_key: candidate.object_key().as_str().to_owned(),
            observed_length: candidate.observed_length(),
            first_seen_unreachable_at_unix_seconds: candidate
                .first_seen_unreachable_at_unix_seconds(),
            delete_after_unix_seconds: candidate.delete_after_unix_seconds(),
        }
    }

    fn into_domain(self) -> Result<QuarantineCandidate, LocalIndexStoreError> {
        let object_key = ObjectKey::parse(&self.object_key)?;
        QuarantineCandidate::new(
            object_key,
            self.observed_length,
            self.first_seen_unreachable_at_unix_seconds,
            self.delete_after_unix_seconds,
        )
        .map_err(LocalIndexStoreError::from)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetentionHoldRecord {
    object_key: String,
    reason: String,
    held_at_unix_seconds: u64,
    release_after_unix_seconds: Option<u64>,
}

impl RetentionHoldRecord {
    fn from_domain(hold: &RetentionHold) -> Self {
        Self {
            object_key: hold.object_key().as_str().to_owned(),
            reason: hold.reason().to_owned(),
            held_at_unix_seconds: hold.held_at_unix_seconds(),
            release_after_unix_seconds: hold.release_after_unix_seconds(),
        }
    }

    fn into_domain(self) -> Result<RetentionHold, LocalIndexStoreError> {
        let object_key = ObjectKey::parse(&self.object_key)?;
        RetentionHold::new(
            object_key,
            self.reason,
            self.held_at_unix_seconds,
            self.release_after_unix_seconds,
        )
        .map_err(LocalIndexStoreError::from)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WebhookDeliveryRecord {
    provider: String,
    owner: String,
    repo: String,
    delivery_id: String,
    processed_at_unix_seconds: u64,
}

impl WebhookDeliveryRecord {
    fn from_domain(delivery: &WebhookDelivery) -> Self {
        Self {
            provider: delivery.provider().as_str().to_owned(),
            owner: delivery.owner().to_owned(),
            repo: delivery.repo().to_owned(),
            delivery_id: delivery.delivery_id().to_owned(),
            processed_at_unix_seconds: delivery.processed_at_unix_seconds(),
        }
    }

    fn into_domain(self) -> Result<WebhookDelivery, LocalIndexStoreError> {
        let provider = parse_repository_provider(&self.provider, || {
            LocalIndexStoreError::WebhookDelivery(WebhookDeliveryError::InvalidProvider)
        })?;
        WebhookDelivery::new(
            provider,
            self.owner,
            self.repo,
            self.delivery_id,
            self.processed_at_unix_seconds,
        )
        .map_err(LocalIndexStoreError::from)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProviderRepositoryStateRecord {
    provider: String,
    owner: String,
    repo: String,
    last_access_changed_at_unix_seconds: Option<u64>,
    last_revision_pushed_at_unix_seconds: Option<u64>,
    last_pushed_revision: Option<String>,
    #[serde(default)]
    last_cache_invalidated_at_unix_seconds: Option<u64>,
    #[serde(default)]
    last_authorization_rechecked_at_unix_seconds: Option<u64>,
    #[serde(default)]
    last_drift_checked_at_unix_seconds: Option<u64>,
}

impl ProviderRepositoryStateRecord {
    fn from_domain(state: &ProviderRepositoryState) -> Self {
        Self {
            provider: state.provider().as_str().to_owned(),
            owner: state.owner().to_owned(),
            repo: state.repo().to_owned(),
            last_access_changed_at_unix_seconds: state.last_access_changed_at_unix_seconds(),
            last_revision_pushed_at_unix_seconds: state.last_revision_pushed_at_unix_seconds(),
            last_pushed_revision: state.last_pushed_revision().map(ToOwned::to_owned),
            last_cache_invalidated_at_unix_seconds: state.last_cache_invalidated_at_unix_seconds(),
            last_authorization_rechecked_at_unix_seconds: state
                .last_authorization_rechecked_at_unix_seconds(),
            last_drift_checked_at_unix_seconds: state.last_drift_checked_at_unix_seconds(),
        }
    }

    fn into_domain(self) -> Result<ProviderRepositoryState, LocalIndexStoreError> {
        let provider = parse_repository_provider(&self.provider, || {
            LocalIndexStoreError::WebhookDelivery(WebhookDeliveryError::InvalidProvider)
        })?;
        Ok(ProviderRepositoryState::new(
            provider,
            self.owner,
            self.repo,
            self.last_access_changed_at_unix_seconds,
            self.last_revision_pushed_at_unix_seconds,
            self.last_pushed_revision,
        )
        .with_reconciliation(
            self.last_cache_invalidated_at_unix_seconds,
            self.last_authorization_rechecked_at_unix_seconds,
            self.last_drift_checked_at_unix_seconds,
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyQuarantineCandidateRecord {
    hash: String,
    bytes: u64,
    first_seen_unreachable_at_unix_seconds: u64,
    delete_after_unix_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct XorbPresenceRecord {
    hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DedupeShardRecord {
    chunk_hash: String,
    shard_object_key: String,
}

impl DedupeShardRecord {
    fn into_domain(self) -> Result<DedupeShardMapping, LocalIndexStoreError> {
        let chunk_hash = ShardlineHash::parse_api_hex(&self.chunk_hash)?;
        let shard_object_key = ObjectKey::parse(&self.shard_object_key)?;
        Ok(DedupeShardMapping::new(chunk_hash, shard_object_key))
    }
}

fn write_json_atomically<T>(root: &Path, path: &Path, value: &T) -> Result<(), LocalIndexStoreError>
where
    T: Serialize,
{
    ensure_directory_path_components_are_not_symlinked(root)?;
    ensure_parent_directory_path_components_are_not_symlinked(path)?;
    let bytes = to_vec(value)?;
    write_file_atomically(root, path, &bytes).map_err(LocalIndexStoreError::Io)
}

fn read_json_if_exists<T>(
    path: &Path,
    maximum_bytes: u64,
) -> Result<Option<T>, LocalIndexStoreError>
where
    T: for<'de> Deserialize<'de>,
{
    read_file_if_exists_bounded(path, maximum_bytes)?
        .map(|bytes| from_slice(&bytes))
        .transpose()
        .map_err(LocalIndexStoreError::from)
}

fn read_quarantine_candidate_if_exists(
    path: &Path,
) -> Result<Option<QuarantineCandidate>, LocalIndexStoreError> {
    let Some(bytes) = read_file_if_exists_bounded(path, MAX_CONTROL_PLANE_METADATA_BYTES)? else {
        return Ok(None);
    };
    if let Ok(record) = from_slice::<QuarantineCandidateRecord>(&bytes) {
        return Ok(Some(record.into_domain()?));
    }

    if let Ok(record) = from_slice::<LegacyQuarantineCandidateRecord>(&bytes) {
        let object_key = legacy_quarantine_object_key(&record.hash)?;
        let candidate = QuarantineCandidate::new(
            object_key,
            record.bytes,
            record.first_seen_unreachable_at_unix_seconds,
            record.delete_after_unix_seconds,
        )?;
        return Ok(Some(candidate));
    }

    let record = from_slice::<QuarantineCandidateRecord>(&bytes)?;
    Ok(Some(record.into_domain()?))
}

fn visit_quarantine_candidates_recursive<Visitor, VisitorError>(
    directory: &Path,
    visitor: &mut Visitor,
) -> Result<(), VisitorError>
where
    LocalIndexStoreError: Into<VisitorError>,
    Visitor: FnMut(QuarantineCandidate) -> Result<(), VisitorError>,
{
    let Some(entries) = read_dir_if_exists(directory).map_err(Into::into)? else {
        return Ok(());
    };

    for entry in entries {
        let entry = entry
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        if file_type.is_dir() {
            visit_quarantine_candidates_recursive(&path, visitor)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        if let Some(candidate) = read_quarantine_candidate_if_exists(&path).map_err(Into::into)? {
            visitor(candidate)?;
        }
    }

    Ok(())
}

fn read_retention_hold_if_exists(
    path: &Path,
) -> Result<Option<RetentionHold>, LocalIndexStoreError> {
    read_json_if_exists::<RetentionHoldRecord>(path, MAX_CONTROL_PLANE_METADATA_BYTES)?
        .map(RetentionHoldRecord::into_domain)
        .transpose()
}

fn visit_retention_holds_recursive<Visitor, VisitorError>(
    directory: &Path,
    visitor: &mut Visitor,
) -> Result<(), VisitorError>
where
    LocalIndexStoreError: Into<VisitorError>,
    Visitor: FnMut(RetentionHold) -> Result<(), VisitorError>,
{
    let Some(entries) = read_dir_if_exists(directory).map_err(Into::into)? else {
        return Ok(());
    };

    for entry in entries {
        let entry = entry
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        if file_type.is_dir() {
            visit_retention_holds_recursive(&path, visitor)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        if let Some(hold) = read_retention_hold_if_exists(&path).map_err(Into::into)? {
            visitor(hold)?;
        }
    }

    Ok(())
}

fn visit_dedupe_shard_mappings_recursive<Visitor, VisitorError>(
    directory: &Path,
    visitor: &mut Visitor,
) -> Result<(), VisitorError>
where
    LocalIndexStoreError: Into<VisitorError>,
    Visitor: FnMut(DedupeShardMapping) -> Result<(), VisitorError>,
{
    let Some(entries) = read_dir_if_exists(directory).map_err(Into::into)? else {
        return Ok(());
    };

    for entry in entries {
        let entry = entry
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        if file_type.is_dir() {
            visit_dedupe_shard_mappings_recursive(&path, visitor)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        if let Some(mapping) =
            read_json_if_exists::<DedupeShardRecord>(&path, MAX_CONTROL_PLANE_METADATA_BYTES)
                .map_err(Into::into)?
                .map(DedupeShardRecord::into_domain)
                .transpose()
                .map_err(Into::into)?
        {
            visitor(mapping)?;
        }
    }

    Ok(())
}

fn visit_webhook_deliveries_recursive<Visitor, VisitorError>(
    directory: &Path,
    visitor: &mut Visitor,
) -> Result<(), VisitorError>
where
    LocalIndexStoreError: Into<VisitorError>,
    Visitor: FnMut(WebhookDelivery) -> Result<(), VisitorError>,
{
    let Some(entries) = read_dir_if_exists(directory).map_err(Into::into)? else {
        return Ok(());
    };

    for entry in entries {
        let entry = entry
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        if file_type.is_dir() {
            visit_webhook_deliveries_recursive(&path, visitor)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        if let Some(delivery) =
            read_json_if_exists::<WebhookDeliveryRecord>(&path, MAX_CONTROL_PLANE_METADATA_BYTES)
                .map_err(Into::into)?
                .map(WebhookDeliveryRecord::into_domain)
                .transpose()
                .map_err(Into::into)?
        {
            visitor(delivery)?;
        }
    }

    Ok(())
}

fn visit_provider_repository_states_recursive<Visitor, VisitorError>(
    directory: &Path,
    visitor: &mut Visitor,
) -> Result<(), VisitorError>
where
    LocalIndexStoreError: Into<VisitorError>,
    Visitor: FnMut(ProviderRepositoryState) -> Result<(), VisitorError>,
{
    let Some(entries) = read_dir_if_exists(directory).map_err(Into::into)? else {
        return Ok(());
    };

    for entry in entries {
        let entry = entry
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        if file_type.is_dir() {
            visit_provider_repository_states_recursive(&path, visitor)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        if let Some(state) = read_json_if_exists::<ProviderRepositoryStateRecord>(
            &path,
            MAX_CONTROL_PLANE_METADATA_BYTES,
        )
        .map_err(Into::into)?
        .map(ProviderRepositoryStateRecord::into_domain)
        .transpose()
        .map_err(Into::into)?
        {
            visitor(state)?;
        }
    }

    Ok(())
}

fn provider_repository_state_path(
    root: &Path,
    provider: RepositoryProvider,
    owner: &str,
    repo: &str,
) -> PathBuf {
    root.join(provider.as_str())
        .join(hex_encode_component(owner))
        .join(format!("{}.json", hex_encode_component(repo)))
}

fn read_dir_if_exists(directory: &Path) -> Result<Option<fs::ReadDir>, LocalIndexStoreError> {
    ensure_directory_path_components_are_not_symlinked(directory)?;
    match fs::read_dir(directory) {
        Ok(entries) => Ok(Some(entries)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(LocalIndexStoreError::Io(error)),
    }
}

fn read_file_if_exists_bounded(
    path: &Path,
    maximum_bytes: u64,
) -> Result<Option<Vec<u8>>, LocalIndexStoreError> {
    ensure_parent_directory_path_components_are_not_symlinked(path)?;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(LocalIndexStoreError::Io(error)),
    };
    ensure_regular_metadata_file(&metadata)?;
    ensure_metadata_size_within_limit(metadata.len(), maximum_bytes)?;

    let mut file = match open_metadata_file(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(LocalIndexStoreError::Io(error)),
    };
    let opened_metadata = file.metadata()?;
    ensure_regular_metadata_file(&opened_metadata)?;
    ensure_metadata_size_within_limit(opened_metadata.len(), maximum_bytes)?;

    run_before_local_metadata_read_hook_for_tests(path);

    let bytes = read_bounded_metadata_file(&mut file, opened_metadata.len())?;
    let observed_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    ensure_metadata_size_within_limit(observed_bytes, maximum_bytes)?;

    Ok(Some(bytes))
}

fn read_bounded_metadata_file(
    file: &mut fs::File,
    expected_length: u64,
) -> Result<Vec<u8>, LocalIndexStoreError> {
    let capacity = usize::try_from(expected_length).unwrap_or(usize::MAX);
    let mut bytes = Vec::with_capacity(capacity);
    let mut limited = Read::by_ref(file).take(expected_length);
    limited.read_to_end(&mut bytes)?;

    let read_length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if read_length != expected_length {
        return Err(LocalIndexStoreError::MetadataLengthMismatch {
            expected_bytes: expected_length,
            observed_bytes: read_length,
        });
    }

    let mut trailing_byte = [0_u8; 1];
    if file.read(&mut trailing_byte)? != 0 {
        return Err(LocalIndexStoreError::MetadataLengthMismatch {
            expected_bytes: expected_length,
            observed_bytes: expected_length.saturating_add(1),
        });
    }

    let observed_metadata = file.metadata()?;
    let metadata_length = observed_metadata.len();
    if metadata_length != expected_length {
        return Err(LocalIndexStoreError::MetadataLengthMismatch {
            expected_bytes: expected_length,
            observed_bytes: metadata_length,
        });
    }

    Ok(bytes)
}

const fn ensure_metadata_size_within_limit(
    observed_bytes: u64,
    maximum_bytes: u64,
) -> Result<(), LocalIndexStoreError> {
    if observed_bytes > maximum_bytes {
        return Err(LocalIndexStoreError::MetadataTooLarge {
            observed_bytes,
            maximum_bytes,
        });
    }

    Ok(())
}

#[cfg(unix)]
fn open_metadata_file(path: &Path) -> Result<fs::File, IoError> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

#[cfg(not(unix))]
fn open_metadata_file(path: &Path) -> Result<fs::File, IoError> {
    OpenOptions::new().read(true).open(path)
}

fn ensure_regular_metadata_file(metadata: &fs::Metadata) -> Result<(), LocalIndexStoreError> {
    if !metadata.file_type().is_file() {
        return Err(invalid_metadata_path_error());
    }
    Ok(())
}

fn invalid_metadata_path_error() -> LocalIndexStoreError {
    LocalIndexStoreError::Io(IoError::new(
        ErrorKind::InvalidData,
        "local index metadata path must be a regular file and must not be a symlink",
    ))
}

fn ensure_parent_directory_path_components_are_not_symlinked(
    path: &Path,
) -> Result<(), LocalIndexStoreError> {
    let parent = path.parent().ok_or_else(invalid_metadata_path_error)?;
    ensure_directory_path_components_are_not_symlinked(parent)
}

fn ensure_directory_path_components_are_not_symlinked(
    path: &Path,
) -> Result<(), LocalIndexStoreError> {
    ensure_directory_path_components_are_not_symlinked_shared(path)
        .map_err(map_directory_path_error)
}

fn map_directory_path_error(error: DirectoryPathError) -> LocalIndexStoreError {
    match error {
        DirectoryPathError::UnsupportedPrefix
        | DirectoryPathError::SymlinkedComponent(_)
        | DirectoryPathError::NonDirectoryComponent(_) => invalid_metadata_path_error(),
        DirectoryPathError::Io(error) => LocalIndexStoreError::Io(error),
    }
}

#[cfg(test)]
fn set_before_local_metadata_read_hook(path: PathBuf, hook: impl FnOnce() + Send + 'static) {
    let mut slot = match BEFORE_LOCAL_METADATA_READ_HOOK.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *slot = Some(LocalMetadataReadHookRegistration {
        path,
        hook: Box::new(hook),
    });
}

#[cfg(test)]
fn run_before_local_metadata_read_hook_for_tests(path: &Path) {
    let hook = match BEFORE_LOCAL_METADATA_READ_HOOK.lock() {
        Ok(mut guard) => take_matching_local_metadata_read_hook(&mut guard, path),
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            take_matching_local_metadata_read_hook(&mut guard, path)
        }
    };

    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn take_matching_local_metadata_read_hook(
    slot: &mut LocalMetadataReadHookSlot,
    path: &Path,
) -> Option<LocalMetadataReadHook> {
    if slot
        .as_ref()
        .is_none_or(|registration| registration.path != path)
    {
        return None;
    }

    slot.take().map(|registration| registration.hook)
}

#[cfg(not(test))]
const fn run_before_local_metadata_read_hook_for_tests(_path: &Path) {}

fn legacy_quarantine_object_key(hash: &str) -> Result<ObjectKey, LocalIndexStoreError> {
    let prefix = hash.get(..2).ok_or(ObjectKeyError::UnsafePath)?;
    let value = format!("{prefix}/{hash}");
    ObjectKey::parse(&value).map_err(LocalIndexStoreError::from)
}

fn remove_empty_ancestors(path: &Path, root: &Path) -> Result<(), LocalIndexStoreError> {
    let mut current = path.parent();
    while let Some(directory) = current {
        if directory == root {
            break;
        }

        match fs::remove_dir(directory) {
            Ok(()) => {
                current = directory.parent();
            }
            Err(error) if error.kind() == ErrorKind::DirectoryNotEmpty => break,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                current = directory.parent();
            }
            Err(error) => return Err(LocalIndexStoreError::Io(error)),
        }
    }

    Ok(())
}

fn hex_encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len().saturating_mul(2));
    for byte in value.as_bytes() {
        let high = byte >> 4;
        let low = byte & 0x0f;
        encoded.push(char::from(nibble_to_hex(high)));
        encoded.push(char::from(nibble_to_hex(low)));
    }
    encoded
}

const fn nibble_to_hex(value: u8) -> u8 {
    match value {
        0 => b'0',
        1 => b'1',
        2 => b'2',
        3 => b'3',
        4 => b'4',
        5 => b'5',
        6 => b'6',
        7 => b'7',
        8 => b'8',
        9 => b'9',
        10 => b'a',
        11 => b'b',
        12 => b'c',
        13 => b'd',
        14 => b'e',
        15 => b'f',
        _other => b'0',
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::io::Write as _;
    use std::path::PathBuf;

    use serde::Serialize;
    use shardline_protocol::{ChunkRange, RepositoryProvider, ShardlineHash};
    use shardline_storage::ObjectKey;

    use super::{
        LocalIndexStore, LocalIndexStoreError, MAX_CONTROL_PLANE_METADATA_BYTES,
        MAX_RECONSTRUCTION_METADATA_BYTES, hex_encode_component, read_file_if_exists_bounded,
        set_before_local_metadata_read_hook,
    };
    use crate::local_fs::set_before_local_write_hook;
    use crate::{
        DedupeShardMapping, FileId, FileReconstruction, IndexStore, QuarantineCandidate,
        QuarantineCandidateError, ReconstructionTerm, RetentionHold, WebhookDelivery, XorbId,
    };

    #[test]
    fn local_index_store_roundtrips_reconstruction_xorb_and_quarantine_state() {
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

        let hash = ShardlineHash::from_bytes([3; 32]);
        let file_id = FileId::new(hash);
        let xorb_id = XorbId::new(hash);
        let range = ChunkRange::new(1, 3);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };
        let reconstruction =
            FileReconstruction::new(vec![ReconstructionTerm::new(xorb_id, range, 64)]);
        let inserted = store.insert_reconstruction(&file_id, &reconstruction);
        assert!(inserted.is_ok());
        let xorb_inserted = store.insert_xorb(&xorb_id);
        assert!(xorb_inserted.is_ok());
        let dedupe_key = ObjectKey::parse("shards/aa/hash.shard");
        assert!(dedupe_key.is_ok());
        let Ok(dedupe_key) = dedupe_key else {
            return;
        };
        let dedupe_mapping = DedupeShardMapping::new(hash, dedupe_key);
        let dedupe_inserted = store.upsert_dedupe_shard_mapping(&dedupe_mapping);
        assert!(dedupe_inserted.is_ok());

        let key = ObjectKey::parse("xorbs/default/aa/hash.xorb");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let candidate = QuarantineCandidate::new(key.clone(), 64, 10, 20);
        assert!(candidate.is_ok());
        let Ok(candidate) = candidate else {
            return;
        };
        let upserted = store.upsert_quarantine_candidate(&candidate);
        assert!(upserted.is_ok());
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
        let hold_upserted = store.upsert_retention_hold(&hold);
        assert!(hold_upserted.is_ok());

        let loaded_reconstruction = store.reconstruction(&file_id);
        assert!(matches!(loaded_reconstruction, Ok(Some(_))));
        if let Ok(Some(loaded_reconstruction)) = loaded_reconstruction {
            assert_eq!(loaded_reconstruction, reconstruction);
        }
        assert!(matches!(store.contains_xorb(&xorb_id), Ok(true)));
        let loaded_dedupe_key = store.dedupe_shard_mapping(&hash);
        assert!(matches!(loaded_dedupe_key, Ok(Some(_))));
        if let Ok(Some(loaded_dedupe_key)) = loaded_dedupe_key {
            assert_eq!(loaded_dedupe_key, dedupe_mapping);
        }
        let listed_dedupe = store.list_dedupe_shard_mappings();
        assert!(listed_dedupe.is_ok());
        if let Ok(listed_dedupe) = listed_dedupe {
            assert_eq!(listed_dedupe, vec![dedupe_mapping.clone()]);
        }
        let mut visited_dedupe = Vec::new();
        let visited_dedupe_result = store.visit_dedupe_shard_mappings(|mapping| {
            visited_dedupe.push(mapping);
            Ok::<(), LocalIndexStoreError>(())
        });
        assert!(visited_dedupe_result.is_ok());
        assert_eq!(visited_dedupe, vec![dedupe_mapping]);
        let loaded_candidate = store.quarantine_candidate(&key);
        assert!(matches!(loaded_candidate, Ok(Some(_))));
        if let Ok(Some(loaded_candidate)) = loaded_candidate {
            assert_eq!(loaded_candidate, candidate);
        }
        let listed_candidates = store.list_quarantine_candidates();
        assert!(listed_candidates.is_ok());
        if let Ok(listed_candidates) = listed_candidates {
            assert_eq!(listed_candidates, vec![candidate.clone()]);
        }
        let mut visited_candidates = Vec::new();
        let visited_candidates_result = store.visit_quarantine_candidates(|entry| {
            visited_candidates.push(entry);
            Ok::<(), LocalIndexStoreError>(())
        });
        assert!(visited_candidates_result.is_ok());
        assert_eq!(visited_candidates, vec![candidate]);
        let loaded_hold = store.retention_hold(&key);
        assert!(matches!(loaded_hold, Ok(Some(_))));
        if let Ok(Some(loaded_hold)) = loaded_hold {
            assert_eq!(loaded_hold, hold);
        }
        let listed_holds = store.list_retention_holds();
        assert!(listed_holds.is_ok());
        if let Ok(listed_holds) = listed_holds {
            assert_eq!(listed_holds, vec![hold.clone()]);
        }
        let mut visited_holds = Vec::new();
        let visited_holds_result = store.visit_retention_holds(|entry| {
            visited_holds.push(entry);
            Ok::<(), LocalIndexStoreError>(())
        });
        assert!(visited_holds_result.is_ok());
        assert_eq!(visited_holds, vec![hold]);
        assert!(matches!(store.delete_quarantine_candidate(&key), Ok(true)));
        assert!(matches!(store.delete_quarantine_candidate(&key), Ok(false)));
        assert!(matches!(store.quarantine_candidate(&key), Ok(None)));
        assert!(matches!(store.delete_retention_hold(&key), Ok(true)));
        assert!(matches!(store.delete_retention_hold(&key), Ok(false)));
        assert!(matches!(store.retention_hold(&key), Ok(None)));
    }

    #[test]
    fn local_index_store_rejects_invalid_stored_range() {
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

        let hash = ShardlineHash::from_bytes([4; 32]);
        let path = store
            .root
            .join("reconstructions")
            .join(format!("{}.json", hash.api_hex_string()));
        let Some(parent) = path.parent() else {
            return;
        };
        let created = fs::create_dir_all(parent);
        assert!(created.is_ok());
        let written = fs::write(
            path,
            r#"{"terms":[{"xorb_hash":"0404040404040404040404040404040404040404040404040404040404040404","chunk_start":2,"chunk_end_exclusive":2,"unpacked_length":5}]}"#,
        );
        assert!(written.is_ok());

        let file_id = FileId::new(hash);
        let loaded = store.reconstruction(&file_id);
        assert!(matches!(loaded, Err(LocalIndexStoreError::Range(_))));
    }

    #[cfg(unix)]
    #[test]
    fn local_index_store_rejects_symlinked_reconstruction_metadata() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let outside = tempfile::NamedTempFile::new();
        assert!(outside.is_ok());
        let Ok(outside) = outside else {
            return;
        };
        let written = fs::write(outside.path(), r#"{"terms":[]}"#);
        assert!(written.is_ok());
        let store = LocalIndexStore::new(storage.path().to_path_buf());
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };
        let hash = ShardlineHash::from_bytes([11; 32]);
        let file_id = FileId::new(hash);
        let path = store.reconstruction_path(&file_id);
        let Some(parent) = path.parent() else {
            return;
        };
        let created = fs::create_dir_all(parent);
        assert!(created.is_ok());
        let linked = symlink(outside.path(), &path);
        assert!(linked.is_ok());

        let loaded = store.reconstruction(&file_id);

        assert!(
            loaded.is_err(),
            "local index read followed symlinked reconstruction metadata"
        );
    }

    #[cfg(unix)]
    #[test]
    fn local_index_store_rejects_symlinked_reconstruction_parent_directory() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let outside = tempfile::tempdir();
        assert!(outside.is_ok());
        let Ok(outside) = outside else {
            return;
        };
        let store = LocalIndexStore::open(storage.path().to_path_buf());
        let link = storage.path().join("reconstructions");
        let linked = symlink(outside.path(), &link);
        assert!(linked.is_ok());

        let hash = ShardlineHash::from_bytes([12; 32]);
        let file_id = FileId::new(hash);
        let reconstruction = FileReconstruction::new(Vec::new());
        let inserted = store.insert_reconstruction(&file_id, &reconstruction);

        assert!(
            inserted.is_err(),
            "local index write followed a symlinked reconstruction parent directory"
        );
        let escaped = outside
            .path()
            .join(format!("{}.json", hash.api_hex_string()));
        assert!(
            !escaped.exists(),
            "local index write escaped into a symlink target outside the index root"
        );
    }

    #[cfg(unix)]
    #[test]
    fn local_index_store_rejects_reconstruction_parent_swap_race() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let outside = tempfile::tempdir();
        assert!(outside.is_ok());
        let Ok(outside) = outside else {
            return;
        };
        let store = LocalIndexStore::open(storage.path().to_path_buf());
        let hash = ShardlineHash::from_bytes([11; 32]);
        let file_id = FileId::new(hash);
        let path = store.reconstruction_path(&file_id);
        let parent = path.parent().map(PathBuf::from);
        assert!(parent.is_some());
        let Some(parent) = parent else {
            return;
        };
        let moved_parent = storage.path().join("swapped-reconstruction-parent");
        let moved_parent_for_hook = moved_parent.clone();
        let escape_dir = outside.path().to_path_buf();

        set_before_local_write_hook(path, move || {
            let renamed = fs::rename(&parent, &moved_parent_for_hook);
            assert!(renamed.is_ok());
            let linked = symlink(&escape_dir, &parent);
            assert!(linked.is_ok());
        });

        let reconstruction = FileReconstruction::new(Vec::new());
        let inserted = store.insert_reconstruction(&file_id, &reconstruction);

        assert!(matches!(
            inserted,
            Err(LocalIndexStoreError::Io(error)) if error.kind() == ErrorKind::InvalidData
        ));
        assert!(
            !outside
                .path()
                .join(format!("{}.json", hash.api_hex_string()))
                .exists(),
            "local index write escaped into an attacker-controlled symlink target"
        );
        assert!(
            !moved_parent
                .join(format!("{}.json", hash.api_hex_string()))
                .exists(),
            "local index write left a committed file behind in the detached original directory"
        );
    }

    #[cfg(unix)]
    #[test]
    fn local_index_store_new_rejects_symlinked_root_ancestor() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let target = storage.path().join("target");
        let created = fs::create_dir_all(&target);
        assert!(created.is_ok());
        let link = storage.path().join("link");
        let linked = symlink(&target, &link);
        assert!(linked.is_ok());

        let store = LocalIndexStore::new(link.join("gc"));

        assert!(matches!(
            store,
            Err(LocalIndexStoreError::Io(error))
                if error.kind() == ErrorKind::InvalidData
        ));
    }

    #[cfg(unix)]
    #[test]
    fn local_index_store_list_reconstruction_file_ids_rejects_symlinked_root_ancestor() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let target = storage.path().join("target");
        let directory = target.join("gc/reconstructions");
        let created = fs::create_dir_all(&directory);
        assert!(created.is_ok());
        let record_path =
            directory.join("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.json");
        let written = fs::write(&record_path, br#"{"terms":[]}"#);
        assert!(written.is_ok());
        let link = storage.path().join("link");
        let linked = symlink(&target, &link);
        assert!(linked.is_ok());
        let store = LocalIndexStore::open(link.join("gc"));

        let listed = store.list_reconstruction_file_ids();

        assert!(matches!(
            listed,
            Err(LocalIndexStoreError::Io(error))
                if error.kind() == ErrorKind::InvalidData
        ));
    }

    #[test]
    fn local_index_store_open_is_non_mutating_until_write() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let root = storage.path().join("index");

        let store = LocalIndexStore::open(root.clone());
        assert!(!root.exists());

        let hash = ShardlineHash::from_bytes([7; 32]);
        let xorb_id = XorbId::new(hash);
        let inserted = store.insert_xorb(&xorb_id);

        assert!(inserted.is_ok());
        assert!(root.join("xorbs").is_dir());
    }

    #[test]
    fn local_index_store_rejects_invalid_stored_dedupe_shard_object_key() {
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

        let hash = ShardlineHash::from_bytes([8; 32]);
        let hash_hex = hash.api_hex_string();
        let path = storage
            .path()
            .join("dedupe-shards")
            .join(&hash_hex[..2])
            .join(format!("{hash_hex}.json"));
        let Some(parent) = path.parent() else {
            return;
        };
        let created = fs::create_dir_all(parent);
        assert!(created.is_ok());
        let written = fs::write(
            path,
            format!("{{\"chunk_hash\":\"{hash_hex}\",\"shard_object_key\":\"../invalid\"}}"),
        );
        assert!(written.is_ok());

        let loaded = store.dedupe_shard_mapping(&hash);
        assert!(matches!(loaded, Err(LocalIndexStoreError::ObjectKey(_))));
    }

    #[test]
    fn local_index_store_reads_legacy_quarantine_record_shape() {
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
        let hash = "de".repeat(32);
        let path = storage
            .path()
            .join("quarantine")
            .join("de")
            .join(format!("{hash}.json"));
        let parent = path.parent();
        assert!(parent.is_some());
        let Some(parent) = parent else {
            return;
        };
        let created = fs::create_dir_all(parent);
        assert!(created.is_ok());
        let written = fs::write(
            &path,
            to_vec(&LegacyQuarantineCandidateRecordForTest {
                hash: hash.clone(),
                bytes: 64,
                first_seen_unreachable_at_unix_seconds: 10,
                delete_after_unix_seconds: 20,
            })
            .unwrap_or_default(),
        );
        assert!(written.is_ok());

        let listed = store.list_quarantine_candidates();

        assert!(matches!(listed, Ok(ref candidates) if candidates.len() == 1));
        if let Ok(candidates) = listed {
            let expected_key = ObjectKey::parse(&format!("de/{hash}"));
            assert!(expected_key.is_ok());
            let Ok(expected_key) = expected_key else {
                return;
            };
            let expected = QuarantineCandidate::new(expected_key, 64, 10, 20);
            assert!(expected.is_ok());
            let Ok(expected) = expected else {
                return;
            };
            assert_eq!(candidates, vec![expected]);
        }
    }

    #[test]
    fn local_index_store_rejects_inverted_quarantine_timeline() {
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
        let hash = "ab".repeat(32);
        let path = storage
            .path()
            .join("quarantine")
            .join("ab")
            .join(format!("{hash}.json"));
        let parent = path.parent();
        assert!(parent.is_some());
        let Some(parent) = parent else {
            return;
        };
        let created = fs::create_dir_all(parent);
        assert!(created.is_ok());
        let record = format!(
            "{{\"object_key\":\"ab/{hash}\",\"observed_length\":64,\
             \"first_seen_unreachable_at_unix_seconds\":20,\
             \"delete_after_unix_seconds\":10}}"
        );
        let written = fs::write(&path, record);
        assert!(written.is_ok());

        let listed = store.list_quarantine_candidates();

        assert!(matches!(
            listed,
            Err(LocalIndexStoreError::QuarantineCandidate(
                QuarantineCandidateError::InvertedTimeline
            ))
        ));
    }

    #[test]
    fn local_index_store_rejects_oversized_webhook_delivery_metadata_before_reading() {
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
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets".to_owned(),
            "delivery-oversized".to_owned(),
            100,
        );
        assert!(delivery.is_ok());
        let Ok(delivery) = delivery else {
            return;
        };
        let recorded = store.record_webhook_delivery(&delivery);
        assert!(matches!(recorded, Ok(true)));
        let path = storage
            .path()
            .join("webhook-deliveries")
            .join("github")
            .join(hex_encode_component(delivery.owner()))
            .join(hex_encode_component(delivery.repo()))
            .join(format!(
                "{}.json",
                hex_encode_component(delivery.delivery_id())
            ));
        let written = fs::write(
            &path,
            vec![b'{'; MAX_CONTROL_PLANE_METADATA_BYTES as usize + 1],
        );
        assert!(written.is_ok());

        let listed = store.list_webhook_deliveries();

        assert!(matches!(
            listed,
            Err(LocalIndexStoreError::MetadataTooLarge {
                maximum_bytes: MAX_CONTROL_PLANE_METADATA_BYTES,
                ..
            })
        ));
    }

    #[test]
    fn local_index_store_rejects_oversized_reconstruction_metadata_before_reading() {
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

        let hash = ShardlineHash::from_bytes([9; 32]);
        let path = store
            .root
            .join("reconstructions")
            .join(format!("{}.json", hash.api_hex_string()));
        let file = fs::File::create(&path);
        assert!(file.is_ok());
        let Ok(file) = file else {
            return;
        };
        let resized = file.set_len(MAX_RECONSTRUCTION_METADATA_BYTES + 1);
        assert!(resized.is_ok());

        let loaded = store.reconstruction(&FileId::new(hash));

        assert!(matches!(
            loaded,
            Err(LocalIndexStoreError::MetadataTooLarge {
                maximum_bytes: MAX_RECONSTRUCTION_METADATA_BYTES,
                ..
            })
        ));
    }

    #[test]
    fn local_index_store_rejects_metadata_growth_after_length_validation() {
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

        let hash = ShardlineHash::from_bytes([10; 32]);
        let path = store
            .root
            .join("reconstructions")
            .join(format!("{}.json", hash.api_hex_string()));
        let written = fs::write(&path, br#"{"terms":[]}"#);
        assert!(written.is_ok());

        let append_path = path.clone();
        set_before_local_metadata_read_hook(path.clone(), move || {
            let opened = OpenOptions::new().append(true).open(&append_path);
            assert!(opened.is_ok());
            let Ok(mut file) = opened else {
                return;
            };
            let appended = file.write_all(b"\n");
            assert!(appended.is_ok());
            let synced = file.sync_all();
            assert!(synced.is_ok());
        });

        let loaded = read_file_if_exists_bounded(&path, MAX_RECONSTRUCTION_METADATA_BYTES);

        assert!(matches!(
            loaded,
            Err(LocalIndexStoreError::MetadataLengthMismatch { .. })
        ));
    }

    #[derive(Debug, Clone, Serialize)]
    struct LegacyQuarantineCandidateRecordForTest {
        hash: String,
        bytes: u64,
        first_seen_unreachable_at_unix_seconds: u64,
        delete_after_unix_seconds: u64,
    }
}
