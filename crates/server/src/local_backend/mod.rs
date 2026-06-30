use std::{num::NonZeroUsize, path::PathBuf};

mod files;
mod objects;
pub(crate) mod records;
mod xorbs;

use shardline_index::{
    FileChunkRecord, IndexStore, LocalIndexStore, LocalRecordStore, RecordStore,
};
use shardline_storage::ObjectPrefix;

use crate::{
    ServerError, ServerFrontend,
    config::default_upload_max_in_flight_chunks,
    local_path::ensure_directory_path_components_are_not_symlinked,
    model::ServerStatsResponse,
    object_store::ServerObjectStore,
    overflow::{checked_add, checked_increment},
    validation::ensure_directory,
};
use records::read_record;

/// Local filesystem backend for file chunk storage and reconstruction metadata.
#[derive(Debug, Clone)]
pub struct LocalBackend {
    pub(super) public_base_url: String,
    pub(super) chunk_size: NonZeroUsize,
    pub(super) upload_max_in_flight_chunks: NonZeroUsize,
    pub(super) server_frontends: Vec<ServerFrontend>,
    pub(super) index_store: LocalIndexStore,
    pub(super) record_store: LocalRecordStore,
    pub(super) object_store: ServerObjectStore,
}

impl LocalBackend {
    /// Creates a local backend and initializes its directory structure.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the local directories cannot be created.
    pub async fn new(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
    ) -> Result<Self, ServerError> {
        let object_store = ServerObjectStore::local(root.join("chunks"))?;
        Self::new_with_object_store(root, public_base_url, chunk_size, object_store).await
    }

    /// Creates a local backend with explicit upload chunk parallelism.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the local directories cannot be created.
    pub async fn new_with_upload_parallelism(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        upload_max_in_flight_chunks: NonZeroUsize,
    ) -> Result<Self, ServerError> {
        let object_store = ServerObjectStore::local(root.join("chunks"))?;
        Self::new_with_object_store_and_upload_parallelism(
            root,
            public_base_url,
            chunk_size,
            upload_max_in_flight_chunks,
            object_store,
        )
        .await
    }

    pub(crate) async fn new_with_object_store(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        object_store: ServerObjectStore,
    ) -> Result<Self, ServerError> {
        Self::new_with_object_store_and_upload_parallelism_with_frontends(
            root,
            public_base_url,
            chunk_size,
            default_upload_max_in_flight_chunks(),
            object_store,
            &[ServerFrontend::Xet],
        )
        .await
    }

    pub(crate) async fn new_with_object_store_and_upload_parallelism(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        upload_max_in_flight_chunks: NonZeroUsize,
        object_store: ServerObjectStore,
    ) -> Result<Self, ServerError> {
        Self::new_with_object_store_and_upload_parallelism_with_frontends(
            root,
            public_base_url,
            chunk_size,
            upload_max_in_flight_chunks,
            object_store,
            &[ServerFrontend::Xet],
        )
        .await
    }

    pub(crate) async fn new_with_object_store_and_upload_parallelism_with_frontends(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        upload_max_in_flight_chunks: NonZeroUsize,
        object_store: ServerObjectStore,
        server_frontends: &[ServerFrontend],
    ) -> Result<Self, ServerError> {
        ensure_directory_path_components_are_not_symlinked(&root)?;
        let backend = Self {
            index_store: LocalIndexStore::open(root.clone()),
            record_store: LocalRecordStore::open(root),
            public_base_url,
            chunk_size,
            upload_max_in_flight_chunks,
            server_frontends: server_frontends.to_vec(),
            object_store,
        };
        Ok(backend)
    }

    /// Returns the public base URL used in generated download links.
    #[must_use]
    pub fn public_base_url(&self) -> &str {
        &self.public_base_url
    }

    pub(crate) const fn object_backend_name(&self) -> &'static str {
        self.object_store.backend_name()
    }

    /// Verifies that local storage paths remain reachable.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the local object store or metadata roots cannot be
    /// traversed.
    pub async fn ready(&self) -> Result<(), ServerError> {
        let object_store = self.object_store();
        if let Some(local_root) = object_store.local_root() {
            ensure_directory(local_root).await?;
        } else {
            let probe_key = shardline_storage::ObjectKey::parse("health/probe")
                .map_err(|_error| ServerError::InvalidContentHash)?;
            let _object_store_reachable = object_store.metadata(&probe_key)?;
        }
        let _latest = RecordStore::list_latest_record_locators(&self.record_store).await?;
        let _reconstructions = IndexStore::list_reconstruction_file_ids(&self.index_store)?;
        Ok(())
    }

    /// Returns local backend storage stats.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when local metadata cannot be traversed.
    pub async fn stats(&self) -> Result<ServerStatsResponse, ServerError> {
        let object_store = self.object_store();
        let prefix = ObjectPrefix::parse("").map_err(|_error| ServerError::InvalidContentHash)?;
        let mut chunks = 0_u64;
        let mut chunk_bytes = 0_u64;
        crate::object_store::visit_object_prefix(&object_store, &prefix, |metadata| {
            let is_chunk = crate::chunk_store::chunk_hash_from_chunk_object_key_if_present(
                metadata.key(),
            )?
            .is_some();
            if is_chunk {
                chunks = checked_increment(chunks)?;
                chunk_bytes = checked_add(chunk_bytes, metadata.length())?;
            }

            Ok(())
        })?;
        let files = u64::try_from(
            RecordStore::list_latest_record_locators(&self.record_store)
                .await?
                .len(),
        )?;

        Ok(ServerStatsResponse {
            chunks,
            chunk_bytes,
            files,
        })
    }

    pub(crate) fn object_store(&self) -> ServerObjectStore {
        self.object_store.clone()
    }

    pub(super) async fn read_record(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        repository_scope: Option<&shardline_protocol::RepositoryScope>,
    ) -> Result<shardline_index::FileRecord, ServerError> {
        read_record(&self.record_store, file_id, content_hash, repository_scope).await
    }
}

pub fn chunk_hash(bytes: &[u8]) -> shardline_protocol::ShardlineHash {
    let digest = blake3::hash(bytes);
    shardline_protocol::ShardlineHash::from_bytes(*digest.as_bytes())
}

pub(crate) fn content_hash(
    total_bytes: u64,
    chunk_size: u64,
    chunks: &[FileChunkRecord],
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&total_bytes.to_le_bytes());
    hasher.update(&chunk_size.to_le_bytes());
    for chunk in chunks {
        hasher.update(chunk.hash.as_bytes());
        hasher.update(&chunk.offset.to_le_bytes());
        hasher.update(&chunk.length.to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests;
