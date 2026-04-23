#[cfg(test)]
use std::sync::{
    Arc, LazyLock, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::{num::NonZeroUsize, path::PathBuf};

use axum::body::Bytes;
use shardline_protocol::{ByteRange, RepositoryScope};
#[cfg(test)]
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::{
    LocalBackend, ObjectStorageAdapter, PostgresBackend, ServerConfig, ServerError,
    ShardMetadataLimits,
    download_stream::ServerByteStream,
    model::{
        FileReconstructionResponse, ServerStatsResponse, ShardUploadResponse, UploadFileResponse,
        XorbUploadResponse,
    },
    object_store::object_store_from_config,
    reconstruction_cache::{
        ReconstructionCacheBenchReport, benchmark_memory_reconstruction_cache_with_loader,
    },
    upload_ingest::RequestBodyReader,
};

#[derive(Debug, Clone)]
pub(crate) enum ServerBackend {
    Local(LocalBackend),
    Postgres(PostgresBackend),
}

/// Public benchmark-facing backend wrapper that resolves the active metadata and object
/// adapters without exposing the private server runtime enum.
#[derive(Debug, Clone)]
pub struct BenchmarkBackend {
    backend: ServerBackend,
}

#[cfg(test)]
static REPOSITORY_REFERENCE_PROBE_COUNT: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static REPOSITORY_REFERENCE_PROBE_FILTER: LazyLock<Mutex<Option<String>>> =
    LazyLock::new(|| Mutex::new(None));
#[cfg(test)]
static REPOSITORY_REFERENCE_PROBE_TEST_LOCK: LazyLock<Arc<AsyncMutex<()>>> =
    LazyLock::new(|| Arc::new(AsyncMutex::new(())));

impl ServerBackend {
    pub(crate) async fn from_config(config: &ServerConfig) -> Result<Self, ServerError> {
        let object_store = object_store_from_config(config)?;
        if let Some(index_postgres_url) = config.index_postgres_url() {
            let backend = PostgresBackend::new_with_object_store_and_upload_parallelism(
                config.root_dir().to_path_buf(),
                config.public_base_url().to_owned(),
                config.chunk_size(),
                config.upload_max_in_flight_chunks(),
                index_postgres_url,
                object_store,
            )
            .await?;
            return Ok(Self::Postgres(backend));
        }

        let backend = LocalBackend::new_with_object_store_and_upload_parallelism(
            config.root_dir().to_path_buf(),
            config.public_base_url().to_owned(),
            config.chunk_size(),
            config.upload_max_in_flight_chunks(),
            object_store,
        )
        .await?;
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

    #[cfg(test)]
    pub(crate) async fn chunk_length(&self, hash_hex: &str) -> Result<u64, ServerError> {
        match self {
            Self::Local(backend) => backend.chunk_length(hash_hex).await,
            Self::Postgres(backend) => backend.chunk_length(hash_hex).await,
        }
    }

    pub(crate) async fn dedupe_shard_length(&self, hash_hex: &str) -> Result<u64, ServerError> {
        match self {
            Self::Local(backend) => backend.dedupe_shard_length(hash_hex).await,
            Self::Postgres(backend) => backend.dedupe_shard_length(hash_hex).await,
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

    pub(crate) const fn backend_name(&self) -> &'static str {
        match self {
            Self::Local(_backend) => "local",
            Self::Postgres(_backend) => "postgres",
        }
    }

    pub(crate) const fn object_backend_name(&self) -> &'static str {
        match self {
            Self::Local(backend) => backend.object_backend_name(),
            Self::Postgres(backend) => backend.object_backend_name(),
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

#[cfg(test)]
pub(crate) fn reset_repository_reference_probe_count_for_hash(hash_hex: &str) {
    REPOSITORY_REFERENCE_PROBE_COUNT.store(0, Ordering::Relaxed);
    let filter = REPOSITORY_REFERENCE_PROBE_FILTER.lock();
    match filter {
        Ok(mut filter) => *filter = Some(hash_hex.to_owned()),
        Err(poisoned) => *poisoned.into_inner() = Some(hash_hex.to_owned()),
    }
}

#[cfg(test)]
pub(crate) fn repository_reference_probe_count() -> usize {
    REPOSITORY_REFERENCE_PROBE_COUNT.load(Ordering::Relaxed)
}

#[cfg(test)]
pub(crate) fn clear_repository_reference_probe_filter() {
    let filter = REPOSITORY_REFERENCE_PROBE_FILTER.lock();
    match filter {
        Ok(mut filter) => *filter = None,
        Err(poisoned) => *poisoned.into_inner() = None,
    }
}

#[cfg(test)]
pub(crate) async fn lock_repository_reference_probe_test() -> OwnedMutexGuard<()> {
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
    use std::{num::NonZeroUsize, path::PathBuf};

    use super::{BenchmarkBackend, compose_benchmark_object_key_prefix};
    use crate::ServerConfig;

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn configured_benchmark_backend_uses_local_runtime_configuration() {
        let chunk_size = NonZeroUsize::new(4).map_or(NonZeroUsize::MIN, |value| value);
        let upload_budget = NonZeroUsize::new(4).map_or(NonZeroUsize::MIN, |value| value);
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
}
