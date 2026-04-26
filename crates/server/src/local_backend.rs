use std::{num::NonZeroUsize, path::PathBuf};

use axum::body::Bytes;
mod records;

use shardline_index::{
    FileChunkRecord, FileRecord, IndexStore, LocalIndexStore, LocalRecordStore, RecordStore,
};
use shardline_protocol::{ByteRange, RepositoryScope, ShardlineHash};
use shardline_storage::{
    DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata, ObjectPrefix, PutOutcome,
};
use tokio::task;

use crate::{
    ServerError, ServerFrontend, ShardMetadataLimits,
    chunk_store::{chunk_hash_from_chunk_object_key_if_present, chunk_object_key},
    config::default_upload_max_in_flight_chunks,
    download_stream::{ServerByteStream, object_byte_range_stream, object_byte_stream},
    local_path::ensure_directory_path_components_are_not_symlinked,
    model::{
        FileReconstructionResponse, ServerStatsResponse, ShardUploadResponse, UploadFileResponse,
        XorbUploadResponse,
    },
    object_store::{ServerObjectStore, read_full_object, reconstruct_file_record_bytes},
    overflow::{checked_add, checked_increment},
    protocol_support::shared_sha256_object_key,
    upload_ingest::{FileUploadIngestor, RequestBodyReader},
    validation::{ensure_directory, validate_identifier},
    xet_adapter::{
        build_reconstruction_response, register_uploaded_shard_stream, resolve_dedupe_shard_object,
        store_uploaded_xorb_stream, xorb_object_key,
    },
};
use records::{read_record, repository_references_xorb};

/// Local filesystem backend for file chunk storage and reconstruction metadata.
#[derive(Debug, Clone)]
pub struct LocalBackend {
    public_base_url: String,
    chunk_size: NonZeroUsize,
    upload_max_in_flight_chunks: NonZeroUsize,
    server_frontends: Vec<ServerFrontend>,
    index_store: LocalIndexStore,
    record_store: LocalRecordStore,
    object_store: ServerObjectStore,
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
            let probe_key = ObjectKey::parse("health/probe")
                .map_err(|_error| ServerError::InvalidContentHash)?;
            let _object_store_reachable = object_store.metadata(&probe_key)?;
        }
        let _latest = RecordStore::list_latest_record_locators(&self.record_store).await?;
        let _reconstructions = IndexStore::list_reconstruction_file_ids(&self.index_store)?;
        Ok(())
    }

    /// Stores a file version as deduplicated content chunks.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when file identifier validation, chunk persistence, or
    /// metadata persistence fails.
    pub async fn upload_file(
        &self,
        file_id: &str,
        body: Bytes,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<UploadFileResponse, ServerError> {
        self.upload_file_stream(
            file_id,
            RequestBodyReader::from_bytes(body),
            repository_scope,
            None,
        )
        .await
    }

    /// Stores a streamed file version as deduplicated content chunks.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when request streaming, file identifier validation,
    /// chunk persistence, source digest validation, or metadata persistence fails.
    pub(crate) async fn upload_file_stream(
        &self,
        file_id: &str,
        mut body: RequestBodyReader,
        repository_scope: Option<&RepositoryScope>,
        expected_sha256: Option<&str>,
    ) -> Result<UploadFileResponse, ServerError> {
        validate_identifier(file_id)?;

        let object_store = self.object_store();
        let mut ingestor = FileUploadIngestor::new_with_parallelism(
            self.chunk_size,
            expected_sha256.is_some(),
            self.upload_max_in_flight_chunks,
        );
        while let Some(bytes) = body.next_bytes().await? {
            ingestor.ingest_body_chunk(&object_store, &bytes).await?;
        }

        let (record, response) = ingestor
            .finish(&object_store, file_id, repository_scope, expected_sha256)
            .await?;
        self.record_store
            .commit_file_version_metadata(&record)
            .await?;

        Ok(response)
    }

    pub(crate) fn put_object_bytes_if_absent(
        &self,
        object_key: &ObjectKey,
        bytes: Vec<u8>,
    ) -> Result<PutOutcome, ServerError> {
        let integrity = ObjectIntegrity::new(chunk_hash(&bytes), u64::try_from(bytes.len())?);
        self.object_store()
            .put_if_absent(object_key, ObjectBody::from_vec(bytes), &integrity)
    }

    pub(crate) fn put_sha256_addressed_object_bytes_if_absent(
        &self,
        object_key: &ObjectKey,
        digest_hex: &str,
        bytes: Vec<u8>,
    ) -> Result<PutOutcome, ServerError> {
        let canonical_key = shared_sha256_object_key(digest_hex)?;
        let integrity = ObjectIntegrity::new(chunk_hash(&bytes), u64::try_from(bytes.len())?);
        let canonical_outcome = self.object_store().put_if_absent(
            &canonical_key,
            ObjectBody::from_vec(bytes),
            &integrity,
        )?;
        if canonical_key == *object_key {
            return Ok(canonical_outcome);
        }
        self.object_store()
            .copy_if_absent(&canonical_key, object_key)
    }

    pub(crate) fn copy_object_if_absent(
        &self,
        source: &ObjectKey,
        destination: &ObjectKey,
    ) -> Result<PutOutcome, ServerError> {
        self.object_store().copy_if_absent(source, destination)
    }

    pub(crate) fn put_object_bytes_overwrite(
        &self,
        object_key: &ObjectKey,
        bytes: Vec<u8>,
    ) -> Result<(), ServerError> {
        let integrity = ObjectIntegrity::new(chunk_hash(&bytes), u64::try_from(bytes.len())?);
        self.object_store()
            .put_overwrite(object_key, ObjectBody::from_vec(bytes), &integrity)
    }

    pub(crate) fn put_sha256_addressed_object_file(
        &self,
        object_key: &ObjectKey,
        digest_hex: &str,
        path: &std::path::Path,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, ServerError> {
        let canonical_key = shared_sha256_object_key(digest_hex)?;
        let canonical_outcome =
            self.object_store()
                .put_content_addressed_file(&canonical_key, path, integrity)?;
        if canonical_key == *object_key {
            return Ok(canonical_outcome);
        }
        self.object_store()
            .copy_if_absent(&canonical_key, object_key)
    }

    /// Stores a raw xorb body under its content hash.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the supplied hash is invalid, the body hash does not
    /// match, or persistence fails.
    pub async fn upload_xorb(
        &self,
        expected_hash: &str,
        body: Bytes,
    ) -> Result<XorbUploadResponse, ServerError> {
        self.upload_xorb_stream(expected_hash, RequestBodyReader::from_bytes(body))
            .await
    }

    /// Stores a bounded raw xorb body under its content hash.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when request streaming, hash validation, or persistence
    /// fails.
    pub(crate) async fn upload_xorb_stream(
        &self,
        expected_hash: &str,
        body: RequestBodyReader,
    ) -> Result<XorbUploadResponse, ServerError> {
        let object_store = self.object_store();
        store_uploaded_xorb_stream(&object_store, expected_hash, body).await
    }

    /// Stores a bounded native Xet shard and indexes the contained file reconstructions.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when request streaming, shard validation, referenced xorb
    /// validation, or metadata persistence fails.
    pub(crate) async fn upload_shard_stream(
        &self,
        body: RequestBodyReader,
        repository_scope: Option<&RepositoryScope>,
        shard_metadata_limits: ShardMetadataLimits,
    ) -> Result<ShardUploadResponse, ServerError> {
        let record_store = self.record_store.clone();
        let object_store = self.object_store();
        register_uploaded_shard_stream(
            &object_store,
            body,
            repository_scope,
            shard_metadata_limits,
            move |records, mappings| async move {
                record_store
                    .commit_native_shard_metadata(&records, &mappings)
                    .await?;
                Ok(())
            },
        )
        .await
    }

    /// Loads reconstruction metadata for a file.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the file identifier is invalid or the record is
    /// missing or unreadable.
    pub async fn reconstruction(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        requested_range: Option<ByteRange>,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<FileReconstructionResponse, ServerError> {
        let record = self
            .read_record(file_id, content_hash, repository_scope)
            .await?;
        build_reconstruction_response(self.public_base_url(), &record, requested_range)
    }

    /// Loads the logical byte length for a file version.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the file identifier is invalid or the record is
    /// missing or unreadable.
    pub async fn file_total_bytes(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<u64, ServerError> {
        let record = self
            .read_record(file_id, content_hash, repository_scope)
            .await?;
        Ok(record.total_bytes)
    }

    /// Loads the file-version record used by streaming transfer paths.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the file identifier is invalid or the record is
    /// missing or unreadable.
    #[cfg(test)]
    pub(crate) async fn file_record(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<FileRecord, ServerError> {
        self.read_record(file_id, content_hash, repository_scope)
            .await
    }

    /// Reconstructs a file into a contiguous byte vector.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when metadata or chunk bytes cannot be read.
    pub async fn download_file(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<Vec<u8>, ServerError> {
        let record = self
            .read_record(file_id, content_hash, repository_scope)
            .await?;
        let object_store = self.object_store();
        let server_frontends = self.server_frontends.clone();
        task::spawn_blocking(move || {
            reconstruct_file_record_bytes(&object_store, &server_frontends, &record)
        })
        .await
        .map_err(ServerError::BlockingTask)?
    }

    /// Reads a stored chunk by hash.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the hash is invalid or the chunk is missing.
    pub async fn read_chunk(&self, hash_hex: &str) -> Result<Vec<u8>, ServerError> {
        let object_store = self.object_store();
        let object_key = chunk_object_key(hash_hex)?;
        let metadata = object_store.metadata(&object_key)?;
        let Some(metadata) = metadata else {
            return Err(ServerError::NotFound);
        };

        task::spawn_blocking(move || {
            read_full_object(&object_store, &object_key, metadata.length())
        })
        .await
        .map_err(ServerError::BlockingTask)?
    }

    pub(crate) async fn object_length(&self, object_key: &ObjectKey) -> Result<u64, ServerError> {
        let metadata = self.object_store().metadata(object_key)?;
        let Some(metadata) = metadata else {
            return Err(ServerError::NotFound);
        };
        Ok(metadata.length())
    }

    pub(crate) async fn read_object(&self, object_key: &ObjectKey) -> Result<Vec<u8>, ServerError> {
        let object_store = self.object_store();
        let metadata = object_store.metadata(object_key)?;
        let Some(metadata) = metadata else {
            return Err(ServerError::NotFound);
        };
        let object_key = object_key.clone();
        task::spawn_blocking(move || {
            read_full_object(&object_store, &object_key, metadata.length())
        })
        .await
        .map_err(ServerError::BlockingTask)?
    }

    pub(crate) async fn read_object_stream(
        &self,
        object_key: &ObjectKey,
        total_length: u64,
        range: Option<ByteRange>,
    ) -> Result<ServerByteStream, ServerError> {
        let object_store = self.object_store();
        if let Some(range) = range {
            return object_byte_range_stream(object_store, object_key.clone(), total_length, range)
                .await;
        }

        object_byte_stream(object_store, object_key.clone(), total_length).await
    }

    pub(crate) fn visit_object_prefix<Visitor>(
        &self,
        prefix: &ObjectPrefix,
        visitor: Visitor,
    ) -> Result<(), ServerError>
    where
        Visitor: FnMut(ObjectMetadata) -> Result<(), ServerError>,
    {
        self.object_store().visit_prefix(prefix, visitor)
    }

    pub(crate) fn list_object_flat_namespace_page(
        &self,
        prefix: &ObjectPrefix,
        start_after: Option<&ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, ServerError> {
        self.object_store()
            .list_flat_namespace_page(prefix, start_after, limit)
    }

    pub(crate) async fn delete_object_if_present(
        &self,
        object_key: &ObjectKey,
    ) -> Result<DeleteOutcome, ServerError> {
        self.object_store().delete_if_present(object_key)
    }

    /// Loads the stored byte length for a chunk object.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the hash is invalid or the chunk is missing.
    #[cfg(test)]
    pub async fn chunk_length(&self, hash_hex: &str) -> Result<u64, ServerError> {
        let object_store = self.object_store();
        let object_key = chunk_object_key(hash_hex)?;
        let metadata = object_store.metadata(&object_key)?;
        let Some(metadata) = metadata else {
            return Err(ServerError::NotFound);
        };

        Ok(metadata.length())
    }

    pub(crate) async fn read_dedupe_shard_stream(
        &self,
        hash_hex: &str,
    ) -> Result<(ServerByteStream, u64), ServerError> {
        let object_store = self.object_store();
        let (object_key, total_length) =
            resolve_dedupe_shard_object(&self.index_store, &object_store, hash_hex).await?;
        let byte_stream = object_byte_stream(object_store, object_key, total_length).await?;

        Ok((byte_stream, total_length))
    }

    pub(crate) async fn dedupe_shard_length(&self, hash_hex: &str) -> Result<u64, ServerError> {
        let object_store = self.object_store();
        let (_object_key, total_length) =
            resolve_dedupe_shard_object(&self.index_store, &object_store, hash_hex).await?;

        Ok(total_length)
    }

    /// Streams a stored xorb byte range by hash.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the hash is invalid, the xorb is missing, or the
    /// requested byte range cannot be served.
    pub(crate) async fn read_xorb_range_stream(
        &self,
        hash_hex: &str,
        total_length: u64,
        range: ByteRange,
    ) -> Result<ServerByteStream, ServerError> {
        let object_store = self.object_store();
        let object_key = xorb_object_key(hash_hex)?;

        object_byte_range_stream(object_store, object_key, total_length, range).await
    }

    /// Reads a stored chunk only when it is reachable from a concrete file version.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the hash, file identifier, or content hash are
    /// invalid, when the file version is missing, or when the chunk is not referenced
    /// by that version.
    pub async fn read_chunk_for_file_version(
        &self,
        hash_hex: &str,
        file_id: &str,
        content_hash: &str,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<Vec<u8>, ServerError> {
        let record = self
            .read_record(file_id, Some(content_hash), repository_scope)
            .await?;
        if !record.chunks.iter().any(|chunk| chunk.hash == hash_hex) {
            return Err(ServerError::NotFound);
        }

        self.read_chunk(hash_hex).await
    }

    /// Loads the stored byte length for a serialized xorb object.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the hash is invalid or the xorb is missing.
    pub async fn xorb_length(&self, hash_hex: &str) -> Result<u64, ServerError> {
        let object_store = self.object_store();
        let object_key = xorb_object_key(hash_hex)?;
        let metadata = object_store.metadata(&object_key)?;
        let Some(metadata) = metadata else {
            return Err(ServerError::NotFound);
        };

        Ok(metadata.length())
    }

    pub(crate) async fn repository_references_xorb(
        &self,
        hash_hex: &str,
        repository_scope: &RepositoryScope,
    ) -> Result<bool, ServerError> {
        repository_references_xorb(&self.record_store, hash_hex, repository_scope).await
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
        object_store.visit_prefix(&prefix, |metadata| {
            let is_chunk = chunk_hash_from_chunk_object_key_if_present(metadata.key())?.is_some();
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

    async fn read_record(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<FileRecord, ServerError> {
        read_record(&self.record_store, file_id, content_hash, repository_scope).await
    }
}

pub(crate) fn chunk_hash(bytes: &[u8]) -> ShardlineHash {
    let digest = blake3::hash(bytes);
    ShardlineHash::from_bytes(*digest.as_bytes())
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
