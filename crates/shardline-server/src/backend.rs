use std::{
    io::{Error, ErrorKind},
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use axum::body::Bytes;
use sha2::{Digest, Sha256};
use shardline_protocol::{ByteRange, RepositoryScope, ShardlineHash};
use shardline_storage::{
    AsyncObjectStore, BeginMultipartUploadResult, DeleteOutcome, ObjectBody, ObjectIntegrity,
    ObjectKey, ObjectMetadata, ObjectPrefix, PutOutcome,
};

use std::sync::{
    Arc, LazyLock, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::{
    LocalBackend, ObjectStorageAdapter, ObjectStoreError, PostgresBackend, ServerConfig,
    ServerError, ShardMetadataLimits,
    download_stream::ServerByteStream,
    model::{ServerStatsResponse, UploadFileResponse},
    object_store::{ServerObjectStore, object_store_from_config},
    overflow::checked_add,
    protocol_support::shared_sha256_object_key,
    reconstruction_cache::{
        ReconstructionCacheBenchReport, benchmark_memory_reconstruction_cache_with_loader,
    },
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
    xet_adapter::{FileReconstructionResponse, ShardUploadResponse, XorbUploadResponse},
};

#[derive(Debug, Clone)]
pub enum ServerBackend {
    Local(LocalBackend),
    Postgres(PostgresBackend),
}

/// Public benchmark-facing backend wrapper that resolves the active metadata and object
/// adapters without exposing the private server runtime enum.
#[derive(Debug, Clone)]
pub struct BenchmarkBackend {
    backend: ServerBackend,
}

static REPOSITORY_REFERENCE_PROBE_COUNT: AtomicUsize = AtomicUsize::new(0);
static REPOSITORY_REFERENCE_PROBE_FILTER: LazyLock<Mutex<Option<String>>> =
    LazyLock::new(|| Mutex::new(None));
static REPOSITORY_REFERENCE_PROBE_TEST_LOCK: LazyLock<Arc<AsyncMutex<()>>> =
    LazyLock::new(|| Arc::new(AsyncMutex::new(())));

impl ServerBackend {
    /// Build a [`ServerBackend`] from a [`ServerConfig`] by resolving the object store
    /// and metadata backend (local or Postgres).
    ///
    /// Probes both the storage and metadata backends at startup to verify
    /// connectivity before accepting traffic.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the object store or Postgres backend fails to initialise.
    pub async fn from_config(config: &ServerConfig) -> Result<Self, ServerError> {
        let object_store = object_store_from_config(config)?;

        // Probe object storage — warn but don't fail (S3 may be slow to start)
        match object_store.probe() {
            Ok(()) => {
                tracing::info!(
                    backend = object_store.backend_name(),
                    "startup probe: object storage OK"
                );
            }
            Err(reason) => {
                tracing::warn!(
                    backend = object_store.backend_name(),
                    reason,
                    "startup probe: object storage unreachable (will retry on first request)"
                );
            }
        }

        if let Some(index_postgres_url) = config.index_postgres_url() {
            let backend =
                PostgresBackend::new_with_object_store_and_upload_parallelism_with_frontends(
                    config.root_dir().to_path_buf(),
                    config.public_base_url().to_owned(),
                    config.chunk_size(),
                    config.upload_max_in_flight_chunks(),
                    index_postgres_url,
                    object_store,
                    config.server_frontends(),
                )
                .await?;

            // Probe Postgres metadata — fail startup if unreachable (metadata is critical)
            backend.probe_metadata().await.map_err(|e| {
                tracing::error!(reason = %e, "startup probe: postgres metadata FAILED");
                ServerError::Io(Error::new(ErrorKind::ConnectionRefused, e))
            })?;
            tracing::info!("startup probe: postgres metadata OK");

            // Reconcile any stuck upload intents from a previous crash
            backend.reconcile_stuck_upload_intents().await?;

            return Ok(Self::Postgres(backend));
        }

        let backend = LocalBackend::new_with_object_store_and_upload_parallelism_with_frontends(
            config.root_dir().to_path_buf(),
            config.public_base_url().to_owned(),
            config.chunk_size(),
            config.upload_max_in_flight_chunks(),
            object_store,
            config.server_frontends(),
        )
        .await?;

        // Probe SQLite metadata — fail startup if unreachable (metadata is critical)
        backend.probe_metadata().map_err(|e| {
            tracing::error!(reason = %e, "startup probe: sqlite metadata FAILED");
            ServerError::Io(std::io::Error::new(ErrorKind::ConnectionRefused, e))
        })?;
        tracing::info!("startup probe: sqlite metadata OK");

        // Reconcile any stuck upload intents from a previous crash
        backend.reconcile_stuck_upload_intents().await?;

        Ok(Self::Local(backend))
    }

    pub(crate) async fn upload_xorb_stream(
        &self,
        expected_hash: &str,
        body: RequestBodyReader,
    ) -> Result<XorbUploadResponse, ServerError> {
        match self {
            Self::Local(backend) => backend.upload_xorb_stream(expected_hash, body).await,
            Self::Postgres(backend) => backend.upload_xorb_stream(expected_hash, body).await,
        }
    }

    pub(crate) async fn upload_shard_stream(
        &self,
        body: RequestBodyReader,
        repository_scope: Option<&RepositoryScope>,
        shard_metadata_limits: ShardMetadataLimits,
    ) -> Result<ShardUploadResponse, ServerError> {
        match self {
            Self::Local(backend) => {
                backend
                    .upload_shard_stream(body, repository_scope, shard_metadata_limits)
                    .await
            }
            Self::Postgres(backend) => {
                backend
                    .upload_shard_stream(body, repository_scope, shard_metadata_limits)
                    .await
            }
        }
    }

    pub(crate) async fn put_sha256_addressed_object_stream_if_absent(
        &self,
        object_key: &ObjectKey,
        digest_hex: &str,
        body: RequestBodyReader,
    ) -> Result<PutOutcome, ServerError> {
        match self {
            Self::Local(backend) => {
                put_sha256_addressed_object_stream_if_absent_with_object_store(
                    &backend.object_store(),
                    object_key,
                    digest_hex,
                    body,
                )
                .await
            }
            Self::Postgres(backend) => {
                put_sha256_addressed_object_stream_if_absent_with_object_store(
                    &backend.object_store(),
                    object_key,
                    digest_hex,
                    body,
                )
                .await
            }
        }
    }

    pub(crate) async fn reconstruction(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        requested_range: Option<ByteRange>,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<FileReconstructionResponse, ServerError> {
        match self {
            Self::Local(backend) => {
                backend
                    .reconstruction(file_id, content_hash, requested_range, repository_scope)
                    .await
            }
            Self::Postgres(backend) => {
                backend
                    .reconstruction(file_id, content_hash, requested_range, repository_scope)
                    .await
            }
        }
    }

    pub(crate) async fn file_total_bytes(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<u64, ServerError> {
        match self {
            Self::Local(backend) => {
                backend
                    .file_total_bytes(file_id, content_hash, repository_scope)
                    .await
            }
            Self::Postgres(backend) => {
                backend
                    .file_total_bytes(file_id, content_hash, repository_scope)
                    .await
            }
        }
    }

    pub(crate) async fn chunk_length(&self, hash_hex: &str) -> Result<u64, ServerError> {
        match self {
            Self::Local(backend) => backend.chunk_length(hash_hex).await,
            Self::Postgres(backend) => backend.chunk_length(hash_hex).await,
        }
    }

    pub(crate) async fn xorb_length(&self, hash_hex: &str) -> Result<u64, ServerError> {
        match self {
            Self::Local(backend) => backend.xorb_length(hash_hex).await,
            Self::Postgres(backend) => backend.xorb_length(hash_hex).await,
        }
    }

    pub(crate) async fn read_xorb_range_stream(
        &self,
        hash_hex: &str,
        total_length: u64,
        range: ByteRange,
    ) -> Result<ServerByteStream, ServerError> {
        match self {
            Self::Local(backend) => {
                backend
                    .read_xorb_range_stream(hash_hex, total_length, range)
                    .await
            }
            Self::Postgres(backend) => {
                backend
                    .read_xorb_range_stream(hash_hex, total_length, range)
                    .await
            }
        }
    }

    pub(crate) async fn read_dedupe_shard_stream(
        &self,
        hash_hex: &str,
    ) -> Result<(ServerByteStream, u64), ServerError> {
        match self {
            Self::Local(backend) => backend.read_dedupe_shard_stream(hash_hex).await,
            Self::Postgres(backend) => backend.read_dedupe_shard_stream(hash_hex).await,
        }
    }

    pub(crate) async fn repository_references_xorb(
        &self,
        hash_hex: &str,
        repository_scope: &RepositoryScope,
    ) -> Result<bool, ServerError> {
        #[cfg(test)]
        count_repository_reference_probe_for_tests(hash_hex);
        match self {
            Self::Local(backend) => {
                backend
                    .repository_references_xorb(hash_hex, repository_scope)
                    .await
            }
            Self::Postgres(backend) => {
                backend
                    .repository_references_xorb(hash_hex, repository_scope)
                    .await
            }
        }
    }

    pub(crate) async fn stats(&self) -> Result<ServerStatsResponse, ServerError> {
        match self {
            Self::Local(backend) => backend.stats().await,
            Self::Postgres(backend) => backend.stats().await,
        }
    }

    pub(crate) async fn ready(&self) -> Result<(), ServerError> {
        match self {
            Self::Local(backend) => backend.ready().await,
            Self::Postgres(backend) => backend.ready().await,
        }
    }

    pub(crate) fn object_store(&self) -> ServerObjectStore {
        match self {
            Self::Local(backend) => backend.object_store(),
            Self::Postgres(backend) => backend.object_store(),
        }
    }

    pub(crate) const fn backend_name(&self) -> &'static str {
        match self {
            Self::Local(_backend) => "local",
            Self::Postgres(_backend) => "postgres",
        }
    }

    #[must_use]
    pub const fn object_backend_name(&self) -> &'static str {
        match self {
            Self::Local(backend) => backend.object_backend_name(),
            Self::Postgres(backend) => backend.object_backend_name(),
        }
    }

    pub(crate) fn uses_s3_object_store(&self) -> bool {
        match self {
            Self::Local(backend) => matches!(backend.object_store(), ServerObjectStore::S3(_)),
            Self::Postgres(backend) => matches!(backend.object_store(), ServerObjectStore::S3(_)),
        }
    }

    pub(crate) async fn put_object_bytes_if_absent(
        &self,
        object_key: &ObjectKey,
        bytes: Vec<u8>,
    ) -> Result<PutOutcome, ServerError> {
        let object_store = self.object_store();
        let integrity = ObjectIntegrity::new(
            crate::local_backend::chunk_hash(&bytes),
            u64::try_from(bytes.len())?,
        );
        Ok(AsyncObjectStore::put_if_absent(
            &object_store,
            object_key,
            ObjectBody::from_vec(bytes),
            &integrity,
        )
        .await?)
    }

    pub(crate) async fn put_sha256_addressed_object_bytes_if_absent(
        &self,
        object_key: &ObjectKey,
        digest_hex: &str,
        bytes: Vec<u8>,
    ) -> Result<PutOutcome, ServerError> {
        let object_store = self.object_store();
        let canonical_key = crate::protocol_support::shared_sha256_object_key(digest_hex)?;
        let integrity = ObjectIntegrity::new(
            crate::local_backend::chunk_hash(&bytes),
            u64::try_from(bytes.len())?,
        );
        let canonical_outcome = AsyncObjectStore::put_if_absent(
            &object_store,
            &canonical_key,
            ObjectBody::from_vec(bytes),
            &integrity,
        )
        .await?;
        if canonical_key == *object_key {
            return Ok(canonical_outcome);
        }
        Ok(object_store.copy_if_absent(&canonical_key, object_key)?)
    }

    pub(crate) async fn copy_object_if_absent(
        &self,
        source: &ObjectKey,
        destination: &ObjectKey,
    ) -> Result<PutOutcome, ServerError> {
        let object_store = self.object_store();
        let source = source.clone();
        let destination = destination.clone();
        tokio::task::spawn_blocking(move || Ok(object_store.copy_if_absent(&source, &destination)?))
            .await
            .map_err(ServerError::BlockingTask)?
    }

    pub(crate) async fn put_object_bytes_overwrite(
        &self,
        object_key: &ObjectKey,
        bytes: Vec<u8>,
    ) -> Result<(), ServerError> {
        let object_store = self.object_store();
        let object_key = object_key.clone();
        let integrity = ObjectIntegrity::new(
            crate::local_backend::chunk_hash(&bytes),
            u64::try_from(bytes.len())?,
        );
        tokio::task::spawn_blocking(move || {
            Ok(object_store.put_overwrite(&object_key, ObjectBody::from_vec(bytes), &integrity)?)
        })
        .await
        .map_err(ServerError::BlockingTask)?
    }

    pub(crate) async fn put_sha256_addressed_object_file(
        &self,
        object_key: &ObjectKey,
        digest_hex: &str,
        path: &Path,
        integrity: &shardline_storage::ObjectIntegrity,
    ) -> Result<PutOutcome, ServerError> {
        let object_store = self.object_store();
        let canonical_key = crate::protocol_support::shared_sha256_object_key(digest_hex)?;
        let object_key = object_key.clone();
        let path = path.to_path_buf();
        let integrity = ObjectIntegrity::new(integrity.hash(), integrity.length());
        tokio::task::spawn_blocking(move || {
            let canonical_outcome =
                object_store.put_content_addressed_file(&canonical_key, &path, &integrity)?;
            if canonical_key == object_key {
                return Ok(canonical_outcome);
            }
            Ok(object_store.copy_if_absent(&canonical_key, &object_key)?)
        })
        .await
        .map_err(ServerError::BlockingTask)?
    }

    pub(crate) async fn object_length(&self, object_key: &ObjectKey) -> Result<u64, ServerError> {
        match self {
            Self::Local(backend) => backend.object_length(object_key).await,
            Self::Postgres(backend) => backend.object_length(object_key).await,
        }
    }

    pub(crate) async fn read_object(&self, object_key: &ObjectKey) -> Result<Vec<u8>, ServerError> {
        match self {
            Self::Local(backend) => backend.read_object(object_key).await,
            Self::Postgres(backend) => backend.read_object(object_key).await,
        }
    }

    pub(crate) async fn read_object_stream(
        &self,
        object_key: &ObjectKey,
        total_length: u64,
        range: Option<ByteRange>,
    ) -> Result<ServerByteStream, ServerError> {
        match self {
            Self::Local(backend) => {
                backend
                    .read_object_stream(object_key, total_length, range)
                    .await
            }
            Self::Postgres(backend) => {
                backend
                    .read_object_stream(object_key, total_length, range)
                    .await
            }
        }
    }

    pub(crate) fn visit_object_prefix<Visitor>(
        &self,
        prefix: &ObjectPrefix,
        visitor: Visitor,
    ) -> Result<(), ServerError>
    where
        Visitor: FnMut(ObjectMetadata) -> Result<(), ServerError>,
    {
        match self {
            Self::Local(backend) => backend.visit_object_prefix(prefix, visitor),
            Self::Postgres(backend) => backend.visit_object_prefix(prefix, visitor),
        }
    }

    pub(crate) fn list_object_flat_namespace_page(
        &self,
        prefix: &ObjectPrefix,
        start_after: Option<&ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, ServerError> {
        match self {
            Self::Local(backend) => {
                backend.list_object_flat_namespace_page(prefix, start_after, limit)
            }
            Self::Postgres(backend) => {
                backend.list_object_flat_namespace_page(prefix, start_after, limit)
            }
        }
    }

    pub(crate) async fn delete_object_if_present(
        &self,
        object_key: &ObjectKey,
    ) -> Result<DeleteOutcome, ServerError> {
        match self {
            Self::Local(backend) => backend.delete_object_if_present(object_key).await,
            Self::Postgres(backend) => backend.delete_object_if_present(object_key).await,
        }
    }
}

impl shardline_oci_adapter::OciBackend for ServerBackend {
    async fn create_resumable_object_upload(
        &self,
        object_key: &ObjectKey,
    ) -> Result<Option<String>, shardline_oci_adapter::OciAdapterError> {
        match self {
            Self::Local(backend) => match backend.object_store() {
                ServerObjectStore::S3(store) => Ok(Some(
                    store
                        .create_resumable_upload(object_key)
                        .await
                        .map_err(shardline_oci_adapter::OciAdapterError::from)?,
                )),
                ServerObjectStore::Local(_) | ServerObjectStore::Blackhole => Ok(None),
            },
            Self::Postgres(backend) => match backend.object_store() {
                ServerObjectStore::S3(store) => Ok(Some(
                    store
                        .create_resumable_upload(object_key)
                        .await
                        .map_err(shardline_oci_adapter::OciAdapterError::from)?,
                )),
                ServerObjectStore::Local(_) | ServerObjectStore::Blackhole => Ok(None),
            },
        }
    }

    async fn upload_resumable_object_part(
        &self,
        object_key: &ObjectKey,
        upload_id: &str,
        part_idx: usize,
        bytes: Bytes,
    ) -> Result<String, shardline_oci_adapter::OciAdapterError> {
        match self {
            Self::Local(backend) => match backend.object_store() {
                ServerObjectStore::S3(store) => store
                    .upload_resumable_part(object_key, upload_id, part_idx, bytes)
                    .await
                    .map_err(shardline_oci_adapter::OciAdapterError::from),
                ServerObjectStore::Local(_) | ServerObjectStore::Blackhole => {
                    Err(shardline_oci_adapter::OciAdapterError::NotFound)
                }
            },
            Self::Postgres(backend) => match backend.object_store() {
                ServerObjectStore::S3(store) => store
                    .upload_resumable_part(object_key, upload_id, part_idx, bytes)
                    .await
                    .map_err(shardline_oci_adapter::OciAdapterError::from),
                ServerObjectStore::Local(_) | ServerObjectStore::Blackhole => {
                    Err(shardline_oci_adapter::OciAdapterError::NotFound)
                }
            },
        }
    }

    async fn complete_resumable_object_upload(
        &self,
        object_key: &ObjectKey,
        upload_id: &str,
        parts: Vec<(usize, String)>,
    ) -> Result<(), shardline_oci_adapter::OciAdapterError> {
        match self {
            Self::Local(backend) => match backend.object_store() {
                ServerObjectStore::S3(store) => store
                    .complete_resumable_upload(object_key, upload_id, parts)
                    .await
                    .map_err(shardline_oci_adapter::OciAdapterError::from),
                ServerObjectStore::Local(_) | ServerObjectStore::Blackhole => {
                    Err(shardline_oci_adapter::OciAdapterError::NotFound)
                }
            },
            Self::Postgres(backend) => match backend.object_store() {
                ServerObjectStore::S3(store) => store
                    .complete_resumable_upload(object_key, upload_id, parts)
                    .await
                    .map_err(shardline_oci_adapter::OciAdapterError::from),
                ServerObjectStore::Local(_) | ServerObjectStore::Blackhole => {
                    Err(shardline_oci_adapter::OciAdapterError::NotFound)
                }
            },
        }
    }

    async fn abort_resumable_object_upload(
        &self,
        object_key: &ObjectKey,
        upload_id: &str,
    ) -> Result<(), shardline_oci_adapter::OciAdapterError> {
        match self {
            Self::Local(backend) => match backend.object_store() {
                ServerObjectStore::S3(store) => store
                    .abort_resumable_upload(object_key, upload_id)
                    .await
                    .map_err(shardline_oci_adapter::OciAdapterError::from),
                ServerObjectStore::Local(_) | ServerObjectStore::Blackhole => Ok(()),
            },
            Self::Postgres(backend) => match backend.object_store() {
                ServerObjectStore::S3(store) => store
                    .abort_resumable_upload(object_key, upload_id)
                    .await
                    .map_err(shardline_oci_adapter::OciAdapterError::from),
                ServerObjectStore::Local(_) | ServerObjectStore::Blackhole => Ok(()),
            },
        }
    }

    fn put_sha256_addressed_object_bytes_if_absent(
        &self,
        object_key: &ObjectKey,
        digest_hex: &str,
        bytes: Vec<u8>,
    ) -> Result<PutOutcome, shardline_oci_adapter::OciAdapterError> {
        let object_key = object_key.clone();
        let digest_hex = digest_hex.to_owned();
        let result = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(
                ServerBackend::put_sha256_addressed_object_bytes_if_absent(
                    self,
                    &object_key,
                    &digest_hex,
                    bytes,
                ),
            )
        });
        result.map_err(server_error_to_oci)
    }

    fn copy_object_if_absent(
        &self,
        source: &ObjectKey,
        destination: &ObjectKey,
    ) -> Result<PutOutcome, shardline_oci_adapter::OciAdapterError> {
        let source = source.clone();
        let destination = destination.clone();
        let result = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(ServerBackend::copy_object_if_absent(
                self,
                &source,
                &destination,
            ))
        });
        result.map_err(server_error_to_oci)
    }

    async fn delete_object_if_present(
        &self,
        object_key: &ObjectKey,
    ) -> Result<DeleteOutcome, shardline_oci_adapter::OciAdapterError> {
        match self {
            Self::Local(backend) => backend
                .delete_object_if_present(object_key)
                .await
                .map_err(server_error_to_oci),
            Self::Postgres(backend) => backend
                .delete_object_if_present(object_key)
                .await
                .map_err(server_error_to_oci),
        }
    }
}

fn server_error_to_oci(error: ServerError) -> shardline_oci_adapter::OciAdapterError {
    use shardline_oci_adapter::OciAdapterError;
    match error {
        ServerError::Io(e) => OciAdapterError::Io(e),
        ServerError::Json(e) => OciAdapterError::Json(e),
        ServerError::NumericConversion(e) => OciAdapterError::NumericConversion(e),
        ServerError::ObjectStore(ObjectStoreError::Local(e)) => {
            OciAdapterError::LocalObjectStore(e)
        }
        ServerError::ObjectStore(ObjectStoreError::S3(e)) => OciAdapterError::S3ObjectStore(e),
        ServerError::ObjectStore(ObjectStoreError::Prefix(e)) => OciAdapterError::ObjectPrefix(e),
        ServerError::NotFound => OciAdapterError::NotFound,
        ServerError::Overflow => OciAdapterError::Overflow,
        ServerError::InvalidContentHash => OciAdapterError::InvalidContentHash,
        ServerError::InvalidDigest => OciAdapterError::InvalidDigest,
        ServerError::InvalidRepositoryName => OciAdapterError::InvalidRepositoryName,
        ServerError::InvalidManifestReference => OciAdapterError::InvalidManifestReference,
        ServerError::InvalidUploadSession => OciAdapterError::InvalidUploadSession,
        ServerError::TooManyUploadSessions => OciAdapterError::TooManyUploadSessions,
        ServerError::ExpectedBodyHashMismatch => OciAdapterError::ExpectedBodyHashMismatch,
        ServerError::BlockingTask(e) => OciAdapterError::BlockingTask(e),
        ref other @ (ServerError::RequestBodyRead(_)
        | ServerError::RequestBodyTooLarge
        | ServerError::RequestQueryTooLarge
        | ServerError::RequestBodyFrameOutOfBounds
        | ServerError::HashParse(_)
        | ServerError::ObjectStore(
            ObjectStoreError::MissingS3Config
            | ObjectStoreError::StoredLengthMismatch
            | ObjectStoreError::MigrationSourceHashMismatch { .. },
        )
        | ServerError::Index(_)
        | ServerError::StoredFileMetadataTooLarge { .. }
        | ServerError::StoredFileMetadataLengthMismatch
        | ServerError::InvalidFileId
        | ServerError::InvalidXorbPrefix
        | ServerError::XorbHashMismatch
        | ServerError::InvalidSerializedXorb
        | ServerError::InvalidSerializedShard(_)
        | ServerError::MissingReferencedXorb
        | ServerError::TooManyShardTerms
        | ServerError::TooManyBatchReconstructionFileIds
        | ServerError::InvalidRangeHeader
        | ServerError::RangeNotSatisfiable
        | ServerError::MissingAuthorization
        | ServerError::InvalidAuthorizationHeader
        | ServerError::InvalidToken(_)
        | ServerError::InsufficientScope
        | ServerError::ProviderTokensDisabled
        | ServerError::MissingProviderApiKey
        | ServerError::InvalidProviderApiKey
        | ServerError::MissingProviderSubject
        | ServerError::InvalidProviderTokenRequest
        | ServerError::MissingProviderWebhookAuthentication
        | ServerError::InvalidProviderWebhookAuthentication
        | ServerError::InvalidProviderWebhookPayload
        | ServerError::UnknownProvider
        | ServerError::ProviderDenied
        | ServerError::Provider(_)
        | ServerError::ReconstructionCache(_)
        | ServerError::Config(_)
        | ServerError::NotAcceptable
        | ServerError::UnauthorizedChallenge(_)
        | ServerError::TooManyRegistryTokenRequests
        | ServerError::MissingReconstructionCacheRedisUrl
        | ServerError::TransferLimiterClosed
        | ServerError::TransferLimiterTimedOut
        | ServerError::SigningKeyError(_)) => OciAdapterError::Io(Error::other(other.to_string())),
    }
}

async fn put_sha256_addressed_object_stream_if_absent_with_object_store(
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    digest_hex: &str,
    mut body: RequestBodyReader,
) -> Result<PutOutcome, ServerError> {
    let canonical_key = shared_sha256_object_key(digest_hex)?;
    match object_store {
        ServerObjectStore::S3(store) => {
            let canonical_outcome =
                match store.begin_content_addressed_upload(&canonical_key).await? {
                    BeginMultipartUploadResult::AlreadyExists => PutOutcome::AlreadyExists,
                    BeginMultipartUploadResult::Upload(mut upload, temp_key) => {
                        let mut sha256 = Sha256::new();
                        let mut total_length = 0_u64;
                        while let Some(bytes) = body.next_bytes().await? {
                            sha256.update(&bytes);
                            total_length = checked_add(total_length, u64::try_from(bytes.len())?)?;
                            upload.write(&bytes);
                            if let Err(error) = upload.wait_for_capacity(4).await {
                                let _ignored = upload.abort().await;
                                return Err(ServerError::from(error));
                            }
                        }
                        let observed = hex::encode(sha256.finalize());
                        if observed != digest_hex {
                            let _ignored = upload.abort().await;
                            return Err(ServerError::ExpectedBodyHashMismatch);
                        }
                        let _total_length = total_length;
                        store
                            .finish_content_addressed_upload(upload, &temp_key, &canonical_key)
                            .await?
                    }
                };
            if canonical_key == *object_key {
                return Ok(canonical_outcome);
            }
            Ok(object_store.copy_if_absent(&canonical_key, object_key)?)
        }
        ServerObjectStore::Local(_) | ServerObjectStore::Blackhole => {
            let bytes = read_body_to_bytes(&mut body).await?;
            let observed = hex::encode(Sha256::digest(&bytes));
            if observed != digest_hex {
                return Err(ServerError::ExpectedBodyHashMismatch);
            }
            let integrity = ObjectIntegrity::new(
                ShardlineHash::from_bytes(*blake3::hash(&bytes).as_bytes()),
                u64::try_from(bytes.len())?,
            );
            let canonical_outcome = shardline_storage::ObjectStore::put_if_absent(
                object_store,
                &canonical_key,
                ObjectBody::from_vec(bytes),
                &integrity,
            )?;
            if canonical_key == *object_key {
                return Ok(canonical_outcome);
            }
            Ok(object_store.copy_if_absent(&canonical_key, object_key)?)
        }
    }
}

impl BenchmarkBackend {
    /// Creates an isolated local benchmark backend rooted at the supplied directory.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when local metadata or object storage cannot initialize.
    pub async fn isolated_local(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        upload_max_in_flight_chunks: NonZeroUsize,
    ) -> Result<Self, ServerError> {
        let backend = LocalBackend::new_with_upload_parallelism(
            root,
            public_base_url,
            chunk_size,
            upload_max_in_flight_chunks,
        )
        .await?;
        Ok(Self {
            backend: ServerBackend::Local(backend),
        })
    }

    /// Creates a benchmark backend from the effective runtime configuration.
    ///
    /// The benchmark namespace is appended to any configured S3 key prefix so benchmark
    /// objects cannot collide with non-benchmark data.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the configured metadata or object adapters cannot
    /// initialize.
    pub async fn from_config(
        config: &ServerConfig,
        root: PathBuf,
        benchmark_namespace: &str,
    ) -> Result<Self, ServerError> {
        let mut configured = config.clone().with_root_dir(root);
        if configured.object_storage_adapter() == ObjectStorageAdapter::S3
            && let Some(s3_config) = configured.s3_object_store_config().cloned()
        {
            let key_prefix =
                compose_benchmark_object_key_prefix(s3_config.key_prefix(), benchmark_namespace);
            configured = configured.with_object_storage(
                ObjectStorageAdapter::S3,
                Some(s3_config.with_key_prefix(Some(&key_prefix))),
            );
        }

        let backend = ServerBackend::from_config(&configured).await?;
        Ok(Self { backend })
    }

    /// Stores one logical file version.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when validation, object persistence, or metadata updates
    /// fail.
    pub async fn upload_file(
        &self,
        file_id: &str,
        body: Bytes,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<UploadFileResponse, ServerError> {
        match &self.backend {
            ServerBackend::Local(backend) => {
                backend.upload_file(file_id, body, repository_scope).await
            }
            ServerBackend::Postgres(backend) => {
                backend.upload_file(file_id, body, repository_scope).await
            }
        }
    }

    /// Reconstructs one logical file or range.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the requested version is missing or any referenced
    /// object cannot be loaded.
    pub async fn reconstruction(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        requested_range: Option<ByteRange>,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<FileReconstructionResponse, ServerError> {
        self.backend
            .reconstruction(file_id, content_hash, requested_range, repository_scope)
            .await
    }

    /// Downloads one logical file version into full bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the requested version is missing or any referenced
    /// object cannot be loaded.
    pub async fn download_file(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<Vec<u8>, ServerError> {
        match &self.backend {
            ServerBackend::Local(backend) => {
                backend
                    .download_file(file_id, content_hash, repository_scope)
                    .await
            }
            ServerBackend::Postgres(backend) => {
                backend
                    .download_file(file_id, content_hash, repository_scope)
                    .await
            }
        }
    }

    /// Returns storage statistics from the active backend.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when metadata or object storage cannot be traversed.
    pub async fn stats(&self) -> Result<ServerStatsResponse, ServerError> {
        self.backend.stats().await
    }

    /// Measures one cold reconstruction lookup followed by one hot memory-cache hit.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when reconstruction loading or response serialization
    /// fails.
    pub async fn benchmark_memory_reconstruction_cache(
        &self,
        file_id: &str,
        content_hash: &str,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<ReconstructionCacheBenchReport, ServerError> {
        benchmark_memory_reconstruction_cache_with_loader(
            file_id,
            content_hash,
            repository_scope,
            || async move {
                self.reconstruction(file_id, Some(content_hash), None, repository_scope)
                    .await
            },
        )
        .await
    }

    /// Returns the metadata backend name used by this benchmark backend.
    #[must_use]
    pub const fn metadata_backend_name(&self) -> &'static str {
        self.backend.backend_name()
    }

    /// Returns the immutable object-storage backend name used by this benchmark backend.
    #[must_use]
    pub const fn object_backend_name(&self) -> &'static str {
        self.backend.object_backend_name()
    }
}

fn compose_benchmark_object_key_prefix(
    existing_key_prefix: Option<&str>,
    benchmark_namespace: &str,
) -> String {
    match existing_key_prefix {
        Some(existing_key_prefix) if !existing_key_prefix.is_empty() => {
            format!("{existing_key_prefix}/bench/{benchmark_namespace}")
        }
        Some(_existing_key_prefix) => format!("bench/{benchmark_namespace}"),
        None => format!("bench/{benchmark_namespace}"),
    }
}

pub fn reset_repository_reference_probe_count_for_hash(hash_hex: &str) {
    REPOSITORY_REFERENCE_PROBE_COUNT.store(0, Ordering::Relaxed);
    let filter = REPOSITORY_REFERENCE_PROBE_FILTER.lock();
    match filter {
        Ok(mut filter) => *filter = Some(hash_hex.to_owned()),
        Err(poisoned) => *poisoned.into_inner() = Some(hash_hex.to_owned()),
    }
}

pub fn repository_reference_probe_count() -> usize {
    REPOSITORY_REFERENCE_PROBE_COUNT.load(Ordering::Relaxed)
}

pub fn clear_repository_reference_probe_filter() {
    let filter = REPOSITORY_REFERENCE_PROBE_FILTER.lock();
    match filter {
        Ok(mut filter) => *filter = None,
        Err(poisoned) => *poisoned.into_inner() = None,
    }
}

pub async fn lock_repository_reference_probe_test() -> OwnedMutexGuard<()> {
    REPOSITORY_REFERENCE_PROBE_TEST_LOCK
        .clone()
        .lock_owned()
        .await
}

#[cfg(test)]
fn count_repository_reference_probe_for_tests(hash_hex: &str) {
    let filter = REPOSITORY_REFERENCE_PROBE_FILTER.lock();
    let matches_filter = match filter {
        Ok(filter) => filter
            .as_deref()
            .is_none_or(|expected| expected == hash_hex),
        Err(poisoned) => poisoned
            .into_inner()
            .as_deref()
            .is_none_or(|expected| expected == hash_hex),
    };

    if matches_filter {
        REPOSITORY_REFERENCE_PROBE_COUNT.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, path::PathBuf, sync::atomic::Ordering};

    use super::{
        BenchmarkBackend, REPOSITORY_REFERENCE_PROBE_COUNT, REPOSITORY_REFERENCE_PROBE_FILTER,
        ServerBackend, clear_repository_reference_probe_filter,
        compose_benchmark_object_key_prefix, count_repository_reference_probe_for_tests,
        lock_repository_reference_probe_test, repository_reference_probe_count,
        reset_repository_reference_probe_count_for_hash, server_error_to_oci,
    };

    /// Poison the static probe-filter mutex by panicking in a helper thread
    /// that is holding the lock.
    ///
    /// After this call returns, every subsequent `lock()` on the static will
    /// produce `Err(PoisonError)`.
    #[allow(clippy::panic)]
    fn poison_probe_filter_mutex() {
        // Handle initial poison if a previous test left the mutex poisoned.
        REPOSITORY_REFERENCE_PROBE_FILTER.clear_poison();
        let handle = std::thread::spawn(|| {
            // Acquire the lock; if already poisoned, recover first.
            let _guard = match REPOSITORY_REFERENCE_PROBE_FILTER.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            panic!("intentional panic to poison the probe filter mutex");
        });
        let _ = handle.join();
    }
    use crate::ServerConfig;
    use crate::ServerError;
    use crate::error::ObjectStoreError;
    use crate::local_backend::LocalBackend;
    use shardline_oci_adapter::OciAdapterError;
    use shardline_storage::{DeleteOutcome, ObjectKey, ObjectPrefix};

    #[test]
    fn benchmark_object_key_prefix_appends_namespace() {
        assert_eq!(
            compose_benchmark_object_key_prefix(Some("tenant-a"), "run-0001"),
            "tenant-a/bench/run-0001"
        );
        assert_eq!(
            compose_benchmark_object_key_prefix(None, "run-0001"),
            "bench/run-0001"
        );
    }

    #[test]
    fn benchmark_object_key_prefix_handles_empty_existing() {
        assert_eq!(
            compose_benchmark_object_key_prefix(Some(""), "bench-1"),
            "bench/bench-1"
        );
        // Also verify with None prefix (different branch)
        assert_eq!(
            compose_benchmark_object_key_prefix(Some(""), "bench-2"),
            "bench/bench-2"
        );
    }

    #[test]
    fn benchmark_object_key_prefix_none_existing() {
        assert_eq!(
            compose_benchmark_object_key_prefix(None, "bench-3"),
            "bench/bench-3"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn configured_benchmark_backend_uses_local_runtime_configuration() {
        let chunk_size = NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN);
        let upload_budget = NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN);
        let bind_addr = "127.0.0.1:8080".parse();
        assert!(bind_addr.is_ok());
        let Ok(bind_addr) = bind_addr else {
            return;
        };
        let config = ServerConfig::new(
            bind_addr,
            "http://127.0.0.1:8080".to_owned(),
            PathBuf::from("/tmp/ignored"),
            chunk_size,
        )
        .with_chunk_size(chunk_size)
        .with_upload_max_in_flight_chunks(upload_budget);
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };

        let backend =
            BenchmarkBackend::from_config(&config, storage.path().to_path_buf(), "run-0001").await;
        assert!(backend.is_ok());
        let Ok(backend) = backend else {
            return;
        };

        assert_eq!(backend.metadata_backend_name(), "local");
        assert_eq!(backend.object_backend_name(), "local");
    }

    // ── Repository reference probe helpers ─────────────────────────────────

    #[test]
    fn repository_reference_probe_count_starts_at_zero() {
        clear_repository_reference_probe_filter();
        assert_eq!(repository_reference_probe_count(), 0);
    }

    #[test]
    fn repository_reference_probe_count_increments_when_filter_matches() {
        clear_repository_reference_probe_filter();
        reset_repository_reference_probe_count_for_hash("aabb");
        // count_repository_reference_probe_for_tests is called by
        // repository_references_xorb, but we can't easily test that async path
        // without a full backend. Instead verify that reset/clear round-trip.
        assert_eq!(repository_reference_probe_count(), 0);
        clear_repository_reference_probe_filter();
    }

    #[test]
    #[serial_test::serial]
    fn poisoned_mutex_reset_repository_reference_probe_count_recovers() {
        poison_probe_filter_mutex();

        // The recovery path (line 902) should still reset the filter without
        // propagating the poison error.
        reset_repository_reference_probe_count_for_hash("recovered");

        // After recovery the mutex is still poisoned; lock() returns Err,
        // but we can retrieve the value via into_inner.
        let val = match REPOSITORY_REFERENCE_PROBE_FILTER.lock() {
            Ok(g) => g.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        assert_eq!(val, Some("recovered".to_owned()));
        REPOSITORY_REFERENCE_PROBE_FILTER.clear_poison();
        *REPOSITORY_REFERENCE_PROBE_FILTER.lock().unwrap() = None;
    }

    #[test]
    #[serial_test::serial]
    fn poisoned_mutex_clear_repository_reference_probe_filter_recovers() {
        poison_probe_filter_mutex();

        // The recovery path (line 914) should clear the filter.
        clear_repository_reference_probe_filter();

        let val = match REPOSITORY_REFERENCE_PROBE_FILTER.lock() {
            Ok(g) => g.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        assert!(val.is_none());
        REPOSITORY_REFERENCE_PROBE_FILTER.clear_poison();
    }

    #[test]
    #[serial_test::serial]
    fn poisoned_mutex_count_repository_reference_probe_recovers() {
        // First set a normal value so we can verify the poisoned-path read.
        REPOSITORY_REFERENCE_PROBE_FILTER.clear_poison();
        *REPOSITORY_REFERENCE_PROBE_FILTER.lock().unwrap() = Some("target".to_owned());
        REPOSITORY_REFERENCE_PROBE_COUNT.store(0, Ordering::Relaxed);

        poison_probe_filter_mutex();

        // After poisoning, count_repository_reference_probe_for_tests enters
        // the Err(poisoned) => poisoned.into_inner() path (lines 931-934).
        count_repository_reference_probe_for_tests("target");
        assert_eq!(repository_reference_probe_count(), 1);

        // Clean up.
        REPOSITORY_REFERENCE_PROBE_FILTER.clear_poison();
        *REPOSITORY_REFERENCE_PROBE_FILTER.lock().unwrap() = None;
        REPOSITORY_REFERENCE_PROBE_COUNT.store(0, Ordering::Relaxed);
    }

    // ── server_error_to_oci conversion ─────────────────────────────────────

    #[test]
    fn server_error_to_oci_maps_not_found() {
        let err = ServerError::NotFound;
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::NotFound));
    }

    #[test]
    fn server_error_to_oci_maps_overflow() {
        let err = ServerError::Overflow;
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::Overflow));
    }

    #[test]
    fn server_error_to_oci_maps_invalid_content_hash() {
        let err = ServerError::InvalidContentHash;
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::InvalidContentHash));
    }

    #[test]
    fn server_error_to_oci_maps_io_error() {
        let io_err = std::io::Error::other("disk error");
        let err = ServerError::Io(io_err);
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::Io(_)));
    }

    #[test]
    fn server_error_to_oci_maps_local_object_store_error() {
        let io_err = std::io::Error::other("local error");
        let err = ServerError::ObjectStore(ObjectStoreError::Local(io_err.into()));
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::LocalObjectStore(_)));
    }

    #[test]
    fn server_error_to_oci_maps_object_store_prefix_error() {
        use shardline_storage::ObjectPrefixError;
        let err = ServerError::ObjectStore(ObjectStoreError::Prefix(ObjectPrefixError::UnsafePath));
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::ObjectPrefix(_)));
    }

    #[test]
    fn server_error_to_oci_maps_invalid_digest() {
        let err = ServerError::InvalidDigest;
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::InvalidDigest));
    }

    #[test]
    fn server_error_to_oci_maps_invalid_repository_name() {
        let err = ServerError::InvalidRepositoryName;
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::InvalidRepositoryName));
    }

    #[test]
    fn server_error_to_oci_maps_catch_all_to_io() {
        let err = ServerError::RequestBodyTooLarge;
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::Io(_)));
    }

    // ── ServerBackend construction and identity ─────────────────────────────

    async fn make_backend() -> (ServerBackend, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let chunk_size = NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN);
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            chunk_size,
        )
        .await
        .unwrap();
        (ServerBackend::Local(backend), tmp)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_from_config_creates_local_backend() {
        let tmp = tempfile::tempdir().unwrap();
        let bind_addr = "127.0.0.1:0".parse().unwrap();
        let chunk_size = NonZeroUsize::new(65536).unwrap();
        let config = ServerConfig::new(
            bind_addr,
            "http://127.0.0.1:8080".to_owned(),
            tmp.path().to_path_buf(),
            chunk_size,
        );
        let result = ServerBackend::from_config(&config).await;
        assert!(result.is_ok());
        let backend = result.unwrap();
        assert!(matches!(backend, ServerBackend::Local(_)));
        assert_eq!(backend.backend_name(), "local");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_backend_name_returns_local() {
        let (backend, _tmp) = make_backend().await;
        assert_eq!(backend.backend_name(), "local");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_object_backend_name_returns_local() {
        let (backend, _tmp) = make_backend().await;
        assert_eq!(backend.object_backend_name(), "local");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_uses_s3_object_store_returns_false_for_local() {
        let (backend, _tmp) = make_backend().await;
        assert!(!backend.uses_s3_object_store());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_ready_succeeds_with_empty_store() {
        let (backend, _tmp) = make_backend().await;
        let result = backend.ready().await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_stats_returns_zero_counts() {
        let (backend, _tmp) = make_backend().await;
        let stats = backend.stats().await.unwrap();
        assert_eq!(stats.chunks, 0);
        assert_eq!(stats.chunk_bytes, 0);
        assert_eq!(stats.files, 0);
    }

    // ── Object CRUD operations ──────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_put_object_bytes_if_absent_stores_and_returns_outcome() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("backend-test-key").unwrap();
        let result = backend
            .put_object_bytes_if_absent(&key, b"hello-backend".to_vec())
            .await;
        assert!(result.is_ok());
        let length = backend.object_length(&key).await.unwrap();
        assert_eq!(length, 13);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_put_object_bytes_if_absent_idempotent() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("backend-idempotent").unwrap();
        let first = backend
            .put_object_bytes_if_absent(&key, b"data".to_vec())
            .await;
        assert!(first.is_ok());
        let second = backend
            .put_object_bytes_if_absent(&key, b"data".to_vec())
            .await;
        assert!(second.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_put_object_bytes_overwrite_stores_object() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("backend-overwrite").unwrap();
        let result = backend
            .put_object_bytes_overwrite(&key, b"overwrite-data".to_vec())
            .await;
        assert!(result.is_ok());
        // Verify stored content
        let read_back = backend.read_object(&key).await.unwrap();
        assert_eq!(read_back.as_slice(), b"overwrite-data");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_put_sha256_addressed_object_bytes_if_absent_stores_object() {
        let (backend, _tmp) = make_backend().await;
        let body = b"sha256-payload-backend";
        let digest_hex = "ab".repeat(32);
        let canonical_key = crate::protocol_support::shared_sha256_object_key(&digest_hex).unwrap();
        let result = backend
            .put_sha256_addressed_object_bytes_if_absent(&canonical_key, &digest_hex, body.to_vec())
            .await;
        assert!(result.is_ok());
        let length = backend.object_length(&canonical_key).await.unwrap();
        assert_eq!(length, body.len() as u64);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_copy_object_if_absent_copies_object() {
        let (backend, _tmp) = make_backend().await;
        let src = ObjectKey::parse("backend-src").unwrap();
        let dst = ObjectKey::parse("backend-dst").unwrap();
        backend
            .put_object_bytes_if_absent(&src, b"copy-source".to_vec())
            .await
            .unwrap();
        let result = backend.copy_object_if_absent(&src, &dst).await;
        assert!(result.is_ok());
        let src_len = backend.object_length(&src).await.unwrap();
        let dst_len = backend.object_length(&dst).await.unwrap();
        assert_eq!(src_len, dst_len);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_read_object_returns_stored_bytes() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("backend-read").unwrap();
        let data = b"readable-content";
        backend
            .put_object_bytes_if_absent(&key, data.to_vec())
            .await
            .unwrap();
        let result = backend.read_object(&key).await.unwrap();
        assert_eq!(result.as_slice(), data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_read_object_returns_not_found_for_missing() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("backend-missing").unwrap();
        let result = backend.read_object(&key).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_object_length_returns_stored_length() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("backend-len").unwrap();
        let data = b"length-check";
        backend
            .put_object_bytes_if_absent(&key, data.to_vec())
            .await
            .unwrap();
        let length = backend.object_length(&key).await.unwrap();
        assert_eq!(length, data.len() as u64);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_object_length_returns_not_found_for_missing() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("backend-missing-len").unwrap();
        let result = backend.object_length(&key).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_delete_object_if_present_removes_object() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("backend-delete").unwrap();
        backend
            .put_object_bytes_if_absent(&key, b"to-delete".to_vec())
            .await
            .unwrap();
        let outcome = backend.delete_object_if_present(&key).await.unwrap();
        assert_eq!(outcome, DeleteOutcome::Deleted);
        let length = backend.object_length(&key).await;
        assert!(matches!(length, Err(ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_delete_object_if_present_returns_not_found_for_missing() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("backend-already-missing").unwrap();
        let outcome = backend.delete_object_if_present(&key).await.unwrap();
        assert_eq!(outcome, DeleteOutcome::NotFound);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_visit_object_prefix_lists_stored_objects() {
        let (backend, _tmp) = make_backend().await;
        let k1 = ObjectKey::parse("prefix-a/obj1").unwrap();
        let k2 = ObjectKey::parse("prefix-a/obj2").unwrap();
        backend
            .put_object_bytes_if_absent(&k1, b"d1".to_vec())
            .await
            .unwrap();
        backend
            .put_object_bytes_if_absent(&k2, b"d2".to_vec())
            .await
            .unwrap();
        let prefix = ObjectPrefix::parse("prefix-a").unwrap();
        let mut keys = Vec::new();
        backend
            .visit_object_prefix(&prefix, |meta| {
                keys.push(meta.key().as_str().to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_list_object_flat_namespace_page_returns_results() {
        let (backend, _tmp) = make_backend().await;
        let prefix = ObjectPrefix::parse("list-be").unwrap();
        for i in 0..3 {
            let key = ObjectKey::parse(&format!("list-be/obj{i}")).unwrap();
            backend
                .put_object_bytes_if_absent(&key, b"d".to_vec())
                .await
                .unwrap();
        }
        let page = backend
            .list_object_flat_namespace_page(&prefix, None, 10)
            .unwrap();
        assert_eq!(page.len(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_read_object_stream_returns_stream_for_existing_object() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("backend-stream").unwrap();
        let data = b"stream-content-backend";
        backend
            .put_object_bytes_if_absent(&key, data.to_vec())
            .await
            .unwrap();
        let result = backend
            .read_object_stream(&key, data.len() as u64, None)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_chunk_length_returns_not_found_for_missing() {
        let (backend, _tmp) = make_backend().await;
        // Use a valid 64-char hex hash that doesn't exist
        let hash = "a".repeat(64);
        let result = backend.chunk_length(&hash).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_xorb_length_returns_not_found_for_missing() {
        let (backend, _tmp) = make_backend().await;
        // Use a valid 64-char hex hash that doesn't exist
        let hash = "b".repeat(64);
        let result = backend.xorb_length(&hash).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_chunk_length_rejects_invalid_hash() {
        let (backend, _tmp) = make_backend().await;
        let result = backend.chunk_length("invalid-hash").await;
        assert!(result.is_err());
    }

    // ── OciBackend trait implementation ─────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_oci_create_resumable_upload_returns_none_for_local_store() {
        use shardline_oci_adapter::OciBackend;
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("oci-test-key").unwrap();
        let result = OciBackend::create_resumable_object_upload(&backend, &key).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_oci_upload_resumable_part_returns_not_found_for_local_store() {
        use shardline_oci_adapter::OciBackend;
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("oci-upload-key").unwrap();
        let result = OciBackend::upload_resumable_object_part(
            &backend,
            &key,
            "upload-id",
            0,
            axum::body::Bytes::from_static(b"part-data"),
        )
        .await;
        assert!(matches!(
            result,
            Err(shardline_oci_adapter::OciAdapterError::NotFound)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_oci_complete_resumable_upload_returns_not_found_for_local_store() {
        use shardline_oci_adapter::OciBackend;
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("oci-complete-key").unwrap();
        let result = OciBackend::complete_resumable_object_upload(
            &backend,
            &key,
            "upload-id",
            vec![(0, "part-etag".to_owned())],
        )
        .await;
        assert!(matches!(
            result,
            Err(shardline_oci_adapter::OciAdapterError::NotFound)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_oci_abort_resumable_upload_succeeds_for_local_store() {
        use shardline_oci_adapter::OciBackend;
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("oci-abort-key").unwrap();
        let result = OciBackend::abort_resumable_object_upload(&backend, &key, "upload-id").await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_oci_delete_object_if_present_delegates() {
        use shardline_oci_adapter::OciBackend;
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("oci-delete-key").unwrap();
        backend
            .put_object_bytes_if_absent(&key, b"oci-delete".to_vec())
            .await
            .unwrap();
        let outcome = OciBackend::delete_object_if_present(&backend, &key).await;
        assert!(outcome.is_ok());
        assert_eq!(outcome.unwrap(), DeleteOutcome::Deleted);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_oci_copy_object_if_absent_delegates() {
        use shardline_oci_adapter::OciBackend;
        let (backend, _tmp) = make_backend().await;
        let src = ObjectKey::parse("oci-copy-src").unwrap();
        let dst = ObjectKey::parse("oci-copy-dst").unwrap();
        backend
            .put_object_bytes_if_absent(&src, b"oci-copy-data".to_vec())
            .await
            .unwrap();
        let result = OciBackend::copy_object_if_absent(&backend, &src, &dst);
        assert!(result.is_ok());
    }

    // ── OciBackend put_sha256_addressed_object_bytes_if_absent ───────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_backend_oci_put_sha256_addressed_object_bytes_if_absent_local() {
        use shardline_oci_adapter::OciBackend;
        let (backend, _tmp) = make_backend().await;
        let body = b"oci-sha256-payload";
        let digest_hex = "cd".repeat(32);
        let canonical_key = crate::protocol_support::shared_sha256_object_key(&digest_hex).unwrap();
        let result = OciBackend::put_sha256_addressed_object_bytes_if_absent(
            &backend,
            &canonical_key,
            &digest_hex,
            body.to_vec(),
        );
        assert!(result.is_ok());
        let put_outcome = result.unwrap();
        use shardline_storage::PutOutcome;
        assert_eq!(put_outcome, PutOutcome::Inserted);
        // Verify the object can be read back
        let length = backend.object_length(&canonical_key).await.unwrap();
        assert_eq!(length, body.len() as u64);
    }

    // ── Repository reference probe tests ─────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn count_repository_reference_probe_directly() {
        let _guard = lock_repository_reference_probe_test().await;
        // Reset count and filter so other tests aren't affected
        reset_repository_reference_probe_count_for_hash("deadbeef");
        assert_eq!(repository_reference_probe_count(), 0);

        // Call the count function directly with matching hash
        count_repository_reference_probe_for_tests("deadbeef");
        assert_eq!(repository_reference_probe_count(), 1);

        // Call with non-matching hash — should NOT increment
        count_repository_reference_probe_for_tests("not-matching");
        assert_eq!(repository_reference_probe_count(), 1);

        // Call with matching hash again
        count_repository_reference_probe_for_tests("deadbeef");
        assert_eq!(repository_reference_probe_count(), 2);

        // Clean up: reset count to 0 and clear filter
        reset_repository_reference_probe_count_for_hash("cleanup");
        clear_repository_reference_probe_filter();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn count_repository_reference_probe_without_filter_counts_all() {
        let _guard = lock_repository_reference_probe_test().await;
        // Reset count and filter so other tests aren't affected
        reset_repository_reference_probe_count_for_hash("x");
        clear_repository_reference_probe_filter();
        assert_eq!(repository_reference_probe_count(), 0);

        count_repository_reference_probe_for_tests("any-hash");
        assert_eq!(repository_reference_probe_count(), 1);

        count_repository_reference_probe_for_tests("another-hash");
        assert_eq!(repository_reference_probe_count(), 2);

        // Clean up: reset count to 0 and clear filter
        reset_repository_reference_probe_count_for_hash("cleanup");
        clear_repository_reference_probe_filter();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lock_repository_reference_probe_test_acquires_lock() {
        let guard = lock_repository_reference_probe_test().await;
        drop(guard);
    }

    // ── server_error_to_oci remaining variants ───────────────────────────

    #[test]
    fn server_error_to_oci_maps_json() {
        let err = ServerError::Json(serde_json::from_str::<()>("invalid").unwrap_err());
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::Json(_)));
    }

    #[test]
    fn server_error_to_oci_maps_numeric_conversion() {
        // Construct a TryFromIntError by attempting an invalid conversion
        let result: Result<u32, _> = (u64::MAX).try_into();
        let try_from_err = result.unwrap_err();
        let err = ServerError::NumericConversion(try_from_err);
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::NumericConversion(_)));
    }

    #[test]
    fn server_error_to_oci_maps_s3_object_store_error() {
        let err = ServerError::ObjectStore(ObjectStoreError::S3(
            shardline_storage::S3ObjectStoreError::EmptyBucket,
        ));
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::S3ObjectStore(_)));
    }

    #[test]
    fn server_error_to_oci_maps_invalid_manifest_reference() {
        let err = ServerError::InvalidManifestReference;
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::InvalidManifestReference));
    }

    #[test]
    fn server_error_to_oci_maps_invalid_upload_session() {
        let err = ServerError::InvalidUploadSession;
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::InvalidUploadSession));
    }

    #[test]
    fn server_error_to_oci_maps_too_many_upload_sessions() {
        let err = ServerError::TooManyUploadSessions;
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::TooManyUploadSessions));
    }

    #[test]
    fn server_error_to_oci_maps_expected_body_hash_mismatch() {
        let err = ServerError::ExpectedBodyHashMismatch;
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::ExpectedBodyHashMismatch));
    }

    #[test]
    #[allow(clippy::panic)]
    fn server_error_to_oci_maps_blocking_task() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let join_err = rt.block_on(async {
            tokio::spawn(async { panic!("intentional panic for test") })
                .await
                .unwrap_err()
        });
        let err = ServerError::BlockingTask(join_err);
        let oci = server_error_to_oci(err);
        assert!(matches!(oci, OciAdapterError::BlockingTask(_)));
    }
}
