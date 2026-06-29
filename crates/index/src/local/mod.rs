#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::{fs, path::PathBuf};

use shardline_storage::ObjectKey;

use crate::{
    FileId, FileReconstruction, StoredObjectId, WebhookDelivery, XorbId, xet_hash_hex_string,
    provider::parse_repository_provider,
};

mod error;
mod helpers;
mod index_store;
mod records;
#[cfg(test)]
mod tests;

pub(crate) use error::LocalIndexStoreError;
pub(crate) use helpers::{
    ensure_parent_directory_path_components_are_not_symlinked, hex_encode_component,
    read_file_if_exists_bounded, read_json_if_exists, remove_empty_ancestors,
    set_before_local_metadata_read_hook, MAX_CONTROL_PLANE_METADATA_BYTES,
    MAX_RECONSTRUCTION_METADATA_BYTES,
};

/// Local filesystem implementation of [`IndexStore`].
#[derive(Debug, Clone)]
pub struct LocalIndexStore {
    pub(crate) root: PathBuf,
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
        helpers::ensure_directory_path_components_are_not_symlinked(&store.root)?;
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
        let record = records::FileReconstructionRecord::from_domain(reconstruction);
        helpers::write_json_atomically(&self.root, &self.reconstruction_path(file_id), &record)
    }

    /// Persists stored-object presence metadata.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when the object marker cannot be written.
    pub fn insert_object(&self, object_id: &StoredObjectId) -> Result<(), LocalIndexStoreError> {
        helpers::write_json_atomically(
            &self.root,
            &self.xorb_path(object_id),
            &records::StoredObjectPresenceRecord {
                hash: xet_hash_hex_string(object_id.hash()),
            },
        )
    }

    /// Persists Xet xorb presence metadata.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when the xorb marker cannot be written.
    pub fn insert_xorb(&self, xorb_id: &XorbId) -> Result<(), LocalIndexStoreError> {
        self.insert_object(xorb_id)
    }

    /// Persists a chunk-hash to retained-shard mapping.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when the mapping cannot be serialized or written.
    pub fn upsert_dedupe_shard_mapping(
        &self,
        mapping: &crate::DedupeShardMapping,
    ) -> Result<(), LocalIndexStoreError> {
        helpers::write_json_atomically(
            &self.root,
            &self.dedupe_shard_path(mapping.chunk_hash()),
            &records::DedupeShardRecord {
                chunk_hash: xet_hash_hex_string(mapping.chunk_hash()),
                shard_object_key: mapping.shard_object_key().as_str().to_owned(),
            },
        )
    }

    pub(crate) fn reconstructions_dir(&self) -> PathBuf {
        self.root.join("reconstructions")
    }

    fn xorbs_dir(&self) -> PathBuf {
        self.root.join("xorbs")
    }

    pub(crate) fn dedupe_shards_dir(&self) -> PathBuf {
        self.root.join("dedupe-shards")
    }

    fn quarantine_dir(&self) -> PathBuf {
        self.root.join("quarantine")
    }

    pub(crate) fn retention_holds_dir(&self) -> PathBuf {
        self.root.join("retention-holds")
    }

    pub(crate) fn webhook_deliveries_dir(&self) -> PathBuf {
        self.root.join("webhook-deliveries")
    }

    pub(crate) fn provider_repository_states_dir(&self) -> PathBuf {
        self.root.join("provider-repository-states")
    }

    pub(crate) fn reconstruction_path(&self, file_id: &FileId) -> PathBuf {
        self.reconstructions_dir()
            .join(format!("{}.json", xet_hash_hex_string(file_id.hash())))
    }

    fn xorb_path(&self, object_id: &StoredObjectId) -> PathBuf {
        self.xorbs_dir()
            .join(format!("{}.json", xet_hash_hex_string(object_id.hash())))
    }

    fn dedupe_shard_path(&self, chunk_hash: shardline_protocol::ShardlineHash) -> PathBuf {
        let hash = xet_hash_hex_string(&chunk_hash);
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

    fn provider_repository_state_path(&self, state: &crate::ProviderRepositoryState) -> PathBuf {
        helpers::provider_repository_state_path(
            &self.provider_repository_states_dir(),
            state.provider(),
            state.owner(),
            state.repo(),
        )
    }
}
