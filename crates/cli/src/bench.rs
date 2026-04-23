use std::{
    fs as std_fs,
    io::{Error as IoError, ErrorKind},
    iter::repeat_n,
    num::{NonZeroUsize, TryFromIntError},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use shardline_protocol::{
    ByteRange, RangeError, RepositoryProvider, RepositoryScope, TokenClaimsError,
};
use shardline_server::{
    BenchmarkBackend, ReconstructionCacheBenchReport, ServerConfig, ServerConfigError, ServerError,
    ingest_without_storage_with_parallelism,
};
use thiserror::Error;
use tokio::{
    fs,
    task::{JoinError, JoinSet},
};

#[cfg(test)]
const DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS: usize = 64;

/// One benchmark iteration report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchIterationReport {
    /// Iteration number starting at one.
    pub iteration: u32,
    /// Storage root used for this isolated iteration.
    pub storage_dir: PathBuf,
    /// Initial upload latency in microseconds.
    pub initial_upload_micros: u64,
    /// Sparse-update upload latency in microseconds.
    pub sparse_update_upload_micros: u64,
    /// Latest-version download latency in microseconds.
    pub latest_download_micros: u64,
    /// Previous-version download latency in microseconds.
    pub previous_download_micros: u64,
    /// Ranged reconstruction-planning latency in microseconds.
    pub ranged_reconstruction_micros: u64,
    /// Concurrent latest-download wall-clock latency in microseconds.
    pub concurrent_latest_download_micros: u64,
    /// Concurrent upload wall-clock latency in microseconds.
    pub concurrent_upload_micros: u64,
    /// Cross-repository upload latency in microseconds.
    pub cross_repository_upload_micros: u64,
    /// Cold reconstruction-cache fill latency in microseconds.
    pub cached_latest_reconstruction_cold_micros: u64,
    /// Hot reconstruction-cache hit latency in microseconds.
    pub cached_latest_reconstruction_hot_micros: u64,
    /// Total bytes uploaded in this iteration.
    pub uploaded_bytes: u64,
    /// Total bytes downloaded in this iteration.
    pub downloaded_bytes: u64,
    /// Serialized cached reconstruction response bytes measured in this iteration.
    pub cached_reconstruction_response_bytes: u64,
    /// Whether the hot cached reconstruction avoided the backend loader.
    pub cached_latest_reconstruction_cache_hit: bool,
    /// Bytes downloaded by concurrent latest-download workers.
    pub concurrent_downloaded_bytes: u64,
    /// Bytes uploaded by concurrent upload workers.
    pub concurrent_uploaded_bytes: u64,
    /// New bytes written by concurrent upload workers.
    pub concurrent_newly_stored_bytes: u64,
    /// New bytes written to storage in this iteration.
    pub newly_stored_bytes: u64,
    /// Number of chunks inserted during the initial upload.
    pub initial_inserted_chunks: u64,
    /// Number of chunks inserted during the sparse update.
    pub sparse_update_inserted_chunks: u64,
    /// Number of chunks reused during the sparse update.
    pub sparse_update_reused_chunks: u64,
    /// Number of chunks inserted during concurrent uploads.
    pub concurrent_upload_inserted_chunks: u64,
    /// Number of chunks reused during concurrent uploads.
    pub concurrent_upload_reused_chunks: u64,
    /// Number of chunks inserted during the cross-repository upload.
    pub cross_repository_inserted_chunks: u64,
    /// Number of chunks reused during the cross-repository upload.
    pub cross_repository_reused_chunks: u64,
    /// New bytes written during the cross-repository upload.
    pub cross_repository_newly_stored_bytes: u64,
    /// Chunk object count after the iteration completes.
    pub chunk_objects: u64,
    /// Chunk object bytes after the iteration completes.
    pub chunk_bytes: u64,
    /// Visible file-record count after the iteration completes.
    pub visible_files: u64,
    /// Total process CPU time consumed while executing this iteration workload.
    pub process_cpu_micros: u64,
    /// Average CPU cores consumed during this iteration, in per-mille cores.
    pub process_cpu_cores_per_mille: u64,
    /// Fraction of host CPU capacity consumed during this iteration, in per-mille.
    pub process_host_utilization_per_mille: u64,
}

/// Aggregate benchmark report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchReport {
    /// Focused benchmark scenario.
    pub scenario: BenchScenario,
    /// Backend target exercised by this report.
    pub deployment_target: BenchDeploymentTarget,
    /// Metadata backend selected for this run.
    pub metadata_backend: String,
    /// Immutable object-storage backend selected for this run.
    pub object_backend: String,
    /// Scope of the reported inventory counters.
    pub inventory_scope: BenchInventoryScope,
    /// Root directory that contains isolated iteration stores.
    pub storage_dir: PathBuf,
    /// Number of benchmark iterations.
    pub iterations: u32,
    /// Chunk size used for all iterations.
    pub chunk_size_bytes: u64,
    /// Concurrency used for concurrent benchmark sub-scenarios.
    pub concurrency: u32,
    /// Maximum upload chunks processed in parallel per upload.
    pub upload_max_in_flight_chunks: u64,
    /// Base asset size used for all iterations.
    pub base_bytes: u64,
    /// Mutation window size used for all iterations.
    pub mutated_bytes: u64,
    /// CPU threads available to the benchmark process.
    pub available_parallelism: u64,
    /// Average initial upload latency in microseconds.
    pub average_initial_upload_micros: u64,
    /// Average sparse-update upload latency in microseconds.
    pub average_sparse_update_upload_micros: u64,
    /// Average latest-version download latency in microseconds.
    pub average_latest_download_micros: u64,
    /// Average previous-version download latency in microseconds.
    pub average_previous_download_micros: u64,
    /// Average ranged reconstruction-planning latency in microseconds.
    pub average_ranged_reconstruction_micros: u64,
    /// Average concurrent latest-download latency in microseconds.
    pub average_concurrent_latest_download_micros: u64,
    /// Average concurrent upload latency in microseconds.
    pub average_concurrent_upload_micros: u64,
    /// Average cross-repository upload latency in microseconds.
    pub average_cross_repository_upload_micros: u64,
    /// Average cold reconstruction-cache fill latency in microseconds.
    pub average_cached_latest_reconstruction_cold_micros: u64,
    /// Average hot reconstruction-cache hit latency in microseconds.
    pub average_cached_latest_reconstruction_hot_micros: u64,
    /// Average initial upload throughput in bytes per second.
    pub average_initial_upload_bytes_per_second: u64,
    /// Average sparse-update upload throughput in bytes per second.
    pub average_sparse_update_upload_bytes_per_second: u64,
    /// Average latest-download throughput in bytes per second.
    pub average_latest_download_bytes_per_second: u64,
    /// Average previous-download throughput in bytes per second.
    pub average_previous_download_bytes_per_second: u64,
    /// Average concurrent latest-download throughput in bytes per second.
    pub average_concurrent_latest_download_bytes_per_second: u64,
    /// Average concurrent upload throughput in bytes per second.
    pub average_concurrent_upload_bytes_per_second: u64,
    /// Average cross-repository upload throughput in bytes per second.
    pub average_cross_repository_upload_bytes_per_second: u64,
    /// Average hot cached-reconstruction throughput in bytes per second.
    pub average_cached_latest_reconstruction_hit_bytes_per_second: u64,
    /// Average process CPU time consumed per iteration.
    pub average_process_cpu_micros: u64,
    /// Average CPU cores consumed per iteration, in per-mille cores.
    pub average_process_cpu_cores_per_mille: u64,
    /// Average fraction of host CPU capacity consumed per iteration, in per-mille.
    pub average_process_host_utilization_per_mille: u64,
    /// Concurrent latest-download scaling efficiency in per-mille, where 1000 is ideal linear scaling.
    pub concurrent_latest_download_scaling_per_mille: u64,
    /// Concurrent upload scaling efficiency in per-mille, where 1000 is ideal linear scaling.
    pub concurrent_upload_scaling_per_mille: u64,
    /// Total uploaded bytes across all iterations.
    pub total_uploaded_bytes: u64,
    /// Total downloaded bytes across all iterations.
    pub total_downloaded_bytes: u64,
    /// Total serialized cached reconstruction response bytes across all iterations.
    pub total_cached_reconstruction_response_bytes: u64,
    /// Number of iterations whose second reconstruction lookup hit cache.
    pub cache_hit_iterations: u64,
    /// Total bytes downloaded across all concurrent latest-download runs.
    pub total_concurrent_downloaded_bytes: u64,
    /// Total bytes uploaded across all concurrent upload runs.
    pub total_concurrent_uploaded_bytes: u64,
    /// Total newly stored bytes across all concurrent upload runs.
    pub total_concurrent_newly_stored_bytes: u64,
    /// Total newly stored bytes across all iterations.
    pub total_newly_stored_bytes: u64,
    /// Total chunks inserted across all initial uploads.
    pub total_initial_inserted_chunks: u64,
    /// Total chunks inserted across all sparse updates.
    pub total_sparse_update_inserted_chunks: u64,
    /// Total chunks reused across all sparse updates.
    pub total_sparse_update_reused_chunks: u64,
    /// Total chunks inserted across all concurrent upload runs.
    pub total_concurrent_upload_inserted_chunks: u64,
    /// Total chunks reused across all concurrent upload runs.
    pub total_concurrent_upload_reused_chunks: u64,
    /// Total chunks inserted across all cross-repository upload runs.
    pub total_cross_repository_inserted_chunks: u64,
    /// Total chunks reused across all cross-repository upload runs.
    pub total_cross_repository_reused_chunks: u64,
    /// Total newly stored bytes across all cross-repository upload runs.
    pub total_cross_repository_newly_stored_bytes: u64,
    /// Per-iteration detail.
    pub iterations_detail: Vec<BenchIterationReport>,
}

/// One zero-storage ingest benchmark iteration report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestBenchIterationReport {
    /// Iteration number starting at one.
    pub iteration: u32,
    /// Initial upload latency in microseconds.
    pub initial_upload_micros: u64,
    /// Sparse-update upload latency in microseconds.
    pub sparse_update_upload_micros: u64,
    /// Concurrent upload wall-clock latency in microseconds.
    pub concurrent_upload_micros: u64,
    /// Total bytes processed by uploads in this iteration.
    pub uploaded_bytes: u64,
    /// Bytes processed by concurrent upload workers.
    pub concurrent_uploaded_bytes: u64,
    /// Chunks processed by the initial upload.
    pub initial_inserted_chunks: u64,
    /// Chunks processed by the sparse update upload.
    pub sparse_update_inserted_chunks: u64,
    /// Chunks processed by concurrent upload workers.
    pub concurrent_upload_inserted_chunks: u64,
    /// Process CPU time consumed by the timed concurrent upload window.
    pub concurrent_upload_process_cpu_micros: u64,
    /// Average CPU cores consumed during the timed concurrent upload window, in per-mille cores.
    pub concurrent_upload_process_cpu_cores_per_mille: u64,
    /// Fraction of host CPU capacity consumed by the timed concurrent upload window, in per-mille.
    pub concurrent_upload_process_host_utilization_per_mille: u64,
    /// Total process CPU time consumed while executing this iteration workload.
    pub process_cpu_micros: u64,
    /// Average CPU cores consumed during this iteration, in per-mille cores.
    pub process_cpu_cores_per_mille: u64,
    /// Fraction of host CPU capacity consumed during this iteration, in per-mille.
    pub process_host_utilization_per_mille: u64,
}

/// Aggregate zero-storage ingest benchmark report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestBenchReport {
    /// Focused benchmark scenario.
    pub scenario: BenchScenario,
    /// Number of benchmark iterations.
    pub iterations: u32,
    /// Chunk size used for all iterations.
    pub chunk_size_bytes: u64,
    /// Concurrency used for concurrent upload sub-scenarios.
    pub concurrency: u32,
    /// Maximum upload chunks processed in parallel per upload.
    pub upload_max_in_flight_chunks: u64,
    /// Base asset size used for all iterations.
    pub base_bytes: u64,
    /// Mutation window size used for all iterations.
    pub mutated_bytes: u64,
    /// CPU threads available to the benchmark process.
    pub available_parallelism: u64,
    /// Average initial upload latency in microseconds.
    pub average_initial_upload_micros: u64,
    /// Average sparse-update upload latency in microseconds.
    pub average_sparse_update_upload_micros: u64,
    /// Average concurrent upload latency in microseconds.
    pub average_concurrent_upload_micros: u64,
    /// Average initial upload throughput in bytes per second.
    pub average_initial_upload_bytes_per_second: u64,
    /// Average sparse-update upload throughput in bytes per second.
    pub average_sparse_update_upload_bytes_per_second: u64,
    /// Average concurrent upload throughput in bytes per second.
    pub average_concurrent_upload_bytes_per_second: u64,
    /// Average process CPU time consumed by timed concurrent upload windows.
    pub average_concurrent_upload_process_cpu_micros: u64,
    /// Average CPU cores consumed by timed concurrent upload windows, in per-mille cores.
    pub average_concurrent_upload_process_cpu_cores_per_mille: u64,
    /// Average fraction of host CPU capacity consumed by timed concurrent upload windows, in per-mille.
    pub average_concurrent_upload_process_host_utilization_per_mille: u64,
    /// Average process CPU time consumed per iteration.
    pub average_process_cpu_micros: u64,
    /// Average CPU cores consumed per iteration, in per-mille cores.
    pub average_process_cpu_cores_per_mille: u64,
    /// Average fraction of host CPU capacity consumed per iteration, in per-mille.
    pub average_process_host_utilization_per_mille: u64,
    /// Concurrent upload scaling efficiency in per-mille, where 1000 is ideal linear scaling.
    pub concurrent_upload_scaling_per_mille: u64,
    /// Total processed bytes across all iterations.
    pub total_uploaded_bytes: u64,
    /// Total bytes processed across all concurrent upload runs.
    pub total_concurrent_uploaded_bytes: u64,
    /// Total chunks processed across all initial uploads.
    pub total_initial_inserted_chunks: u64,
    /// Total chunks processed across all sparse updates.
    pub total_sparse_update_inserted_chunks: u64,
    /// Total chunks processed across all concurrent upload runs.
    pub total_concurrent_upload_inserted_chunks: u64,
    /// Per-iteration detail.
    pub iterations_detail: Vec<IngestBenchIterationReport>,
}

/// Supported benchmark scenarios.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum BenchScenario {
    /// Run the full benchmark suite.
    Full,
    /// Measure only the initial upload hot path.
    InitialUpload,
    /// Measure only the sparse-update upload hot path.
    SparseUpdateUpload,
    /// Measure reconstruction of the latest version into full file bytes.
    LatestDownload,
    /// Measure reconstruction of a previous version into full file bytes.
    PreviousDownload,
    /// Measure ranged reconstruction planning for a logical file byte range.
    RangedReconstruction,
    /// Measure concurrent latest-version downloads.
    ConcurrentLatestDownload,
    /// Measure concurrent uploads with chunk reuse.
    ConcurrentUpload,
    /// Measure cross-repository dedupe reuse during upload.
    CrossRepositoryUpload,
    /// Measure hot reconstruction served from the memory cache after a cold fill.
    CachedLatestReconstruction,
}

impl BenchScenario {
    /// Returns the stable CLI/documentation name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::InitialUpload => "initial-upload",
            Self::SparseUpdateUpload => "sparse-update-upload",
            Self::LatestDownload => "latest-download",
            Self::PreviousDownload => "previous-download",
            Self::RangedReconstruction => "ranged-reconstruction",
            Self::ConcurrentLatestDownload => "concurrent-latest-download",
            Self::ConcurrentUpload => "concurrent-upload",
            Self::CrossRepositoryUpload => "cross-repository-upload",
            Self::CachedLatestReconstruction => "cached-latest-reconstruction",
        }
    }

    #[must_use]
    const fn supports_ingest(self) -> bool {
        matches!(
            self,
            Self::Full | Self::InitialUpload | Self::SparseUpdateUpload | Self::ConcurrentUpload
        )
    }
}

/// Supported end-to-end benchmark deployment targets.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum BenchDeploymentTarget {
    /// Create a fresh local SQLite and local-object-store deployment under `--storage-dir`.
    IsolatedLocal,
    /// Use the active `SHARDLINE_*` runtime config, with per-run benchmark namespacing.
    Configured,
}

impl BenchDeploymentTarget {
    /// Returns the stable kebab-case target name used in reports.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IsolatedLocal => "isolated-local",
            Self::Configured => "configured",
        }
    }
}

/// Scope of the inventory counters recorded in the benchmark report.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum BenchInventoryScope {
    /// Inventory counters reflect only the benchmark's isolated local store.
    Isolated,
    /// Inventory counters may combine isolated and shared adapters.
    Mixed,
    /// Inventory counters come from shared configured adapters.
    BackendGlobal,
}

impl BenchInventoryScope {
    /// Returns the stable kebab-case inventory scope name used in reports.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Isolated => "isolated",
            Self::Mixed => "mixed",
            Self::BackendGlobal => "backend-global",
        }
    }
}

/// Benchmark execution parameters.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchConfig {
    /// End-to-end backend target.
    pub deployment_target: BenchDeploymentTarget,
    /// Focused benchmark scenario.
    pub scenario: BenchScenario,
    /// Number of benchmark iterations to run.
    pub iterations: u32,
    /// Number of concurrent workers used by concurrent sub-scenarios.
    pub concurrency: u32,
    /// Maximum upload chunks processed in parallel per upload.
    pub upload_max_in_flight_chunks: usize,
    /// Chunk size in bytes used by the benchmark backend.
    pub chunk_size_bytes: usize,
    /// Logical size of the benchmark asset in bytes.
    pub base_bytes: usize,
    /// Number of bytes changed in the sparse-update step.
    pub mutated_bytes: usize,
}

#[derive(Debug, Clone)]
struct BenchFixture<'asset> {
    chunk_size: NonZeroUsize,
    upload_max_in_flight_chunks: NonZeroUsize,
    concurrency: u32,
    base: Bytes,
    updated: Bytes,
    ranged_reconstruction: ByteRange,
    concurrent_upload_cases: &'asset [ConcurrentUploadCase],
    cross_repository_base: Bytes,
    cross_repository_updated: Bytes,
}

#[derive(Debug, Clone)]
struct IngestBenchScenario<'asset> {
    chunk_size: NonZeroUsize,
    upload_max_in_flight_chunks: NonZeroUsize,
    concurrent_upload_cases: &'asset [ConcurrentIngestUploadCase],
    base: Bytes,
    updated: Bytes,
}

#[derive(Debug, Clone)]
enum BenchBackendSetup {
    IsolatedLocal,
    Configured(Box<ServerConfig>),
}

/// Benchmark runtime failure.
#[derive(Debug, Error)]
pub enum BenchRuntimeError {
    /// The benchmark chunk size must be positive.
    #[error("benchmark chunk size must be greater than zero")]
    ZeroChunkSize,
    /// The benchmark iteration count must be positive.
    #[error("benchmark iteration count must be greater than zero")]
    ZeroIterations,
    /// The benchmark concurrency must be positive.
    #[error("benchmark concurrency must be greater than zero")]
    ZeroConcurrency,
    /// Upload chunk parallelism must be positive.
    #[error("benchmark upload-max-in-flight-chunks must be greater than zero")]
    ZeroUploadMaxInFlightChunks,
    /// The mutation window must be positive.
    #[error("benchmark mutated-bytes must be greater than zero")]
    ZeroMutatedBytes,
    /// The mutation window cannot exceed the asset size.
    #[error("benchmark mutated-bytes must not exceed base-bytes")]
    MutatedBytesExceedBaseBytes,
    /// The chosen benchmark scenario is not supported by the selected mode.
    #[error("benchmark scenario is not supported by the selected mode")]
    UnsupportedScenarioForMode,
    /// Filesystem access failed.
    #[error(transparent)]
    Io(#[from] IoError),
    /// Numeric conversion exceeded the supported range.
    #[error(transparent)]
    NumericConversion(#[from] TryFromIntError),
    /// Backend operation failed.
    #[error(transparent)]
    Server(#[from] ServerError),
    /// Loading runtime configuration failed.
    #[error(transparent)]
    ServerConfig(#[from] ServerConfigError),
    /// Repository scope construction failed.
    #[error(transparent)]
    TokenClaims(#[from] TokenClaimsError),
    /// The run root path did not contain a valid final path component.
    #[error("benchmark run root did not produce a stable namespace")]
    MissingRunNamespace,
    /// The iteration loop did not report backend names.
    #[error("benchmark iterations did not report backend names")]
    MissingBenchmarkBackendNames,
    /// The ranged reconstruction request did not produce reconstruction terms.
    #[error("ranged reconstruction did not return any reconstruction terms")]
    EmptyRangedReconstruction,
    /// The latest download payload differed from the uploaded sparse update.
    #[error("latest download did not match updated asset bytes")]
    LatestDownloadMismatch,
    /// The previous download payload differed from the uploaded base asset.
    #[error("previous download did not match initial asset bytes")]
    PreviousDownloadMismatch,
    /// The left scoped repository download differed from the seeded asset.
    #[error("cross-repository left download did not match seeded asset bytes")]
    CrossRepositoryLeftDownloadMismatch,
    /// The right scoped repository download differed from the updated asset.
    #[error("cross-repository right download did not match updated asset bytes")]
    CrossRepositoryRightDownloadMismatch,
    /// The cross-repository upload did not reuse stored chunks.
    #[error("cross-repository upload did not reuse any chunks")]
    CrossRepositoryUploadWithoutReusedChunks,
    /// A concurrent latest download returned unexpected bytes.
    #[error("concurrent latest download did not match updated asset bytes")]
    ConcurrentLatestDownloadMismatch,
    /// A concurrent upload verification download returned unexpected bytes.
    #[error("concurrent upload verification download did not match uploaded bytes")]
    ConcurrentUploadVerificationMismatch,
    /// Concurrent upload chunk selection failed.
    #[error("concurrent upload chunk selection failed")]
    ConcurrentUploadChunkSelectionFailed,
    /// Calculating a chunk start overflowed.
    #[error("chunk start overflowed")]
    ChunkStartOverflow,
    /// Calculating a chunk end overflowed.
    #[error("chunk end overflowed")]
    ChunkEndOverflow,
    /// Calculating a chunk window underflowed.
    #[error("chunk window underflowed")]
    ChunkWindowUnderflow,
    /// Calculating a worker mutation window overflowed.
    #[error("worker mutation window overflowed")]
    WorkerMutationWindowOverflow,
    /// Calculating a worker mutation window selected an invalid slice.
    #[error("worker mutation window was out of bounds")]
    WorkerMutationWindowOutOfBounds,
    /// Calculating worker byte deltas overflowed.
    #[error("worker delta overflowed")]
    WorkerDeltaOverflow,
    /// A benchmark divisor was zero.
    #[error("benchmark divisor was zero")]
    BenchmarkDivisorZero,
    /// Calculating a sparse mutation window overflowed.
    #[error("mutation window overflowed")]
    MutationWindowOverflow,
    /// Calculating a sparse mutation window selected an invalid slice.
    #[error("mutation window was out of bounds")]
    MutationWindowOutOfBounds,
    /// Calculating a sparse mutation byte range overflowed.
    #[error("mutation range overflowed")]
    MutationRangeOverflow,
    /// Constructing a sparse mutation byte range failed.
    #[error("mutation range was invalid")]
    MutationRangeInvalid(#[source] RangeError),
    /// Building the cross-repository fixture overflowed.
    #[error("cross-repository asset overflowed")]
    CrossRepositoryAssetOverflow,
    /// Building the cross-repository fixture selected an invalid middle chunk.
    #[error("cross-repository middle chunk was out of bounds")]
    CrossRepositoryMiddleChunkOutOfBounds,
    /// A `u64` benchmark counter overflowed.
    #[error("benchmark counter overflowed u64")]
    BenchmarkCounterU64Overflow,
    /// A `u32` benchmark counter overflowed.
    #[error("benchmark counter overflowed u32")]
    BenchmarkCounterU32Overflow,
    /// A spawned benchmark task failed to join.
    #[error("benchmark task failed to join")]
    BenchmarkTaskJoin(#[from] JoinError),
}

impl BenchBackendSetup {
    async fn create_backend(
        &self,
        root: PathBuf,
        chunk_size: NonZeroUsize,
        upload_max_in_flight_chunks: NonZeroUsize,
        benchmark_namespace: &str,
    ) -> Result<BenchmarkBackend, BenchRuntimeError> {
        match self {
            Self::IsolatedLocal => Ok(BenchmarkBackend::isolated_local(
                root,
                "http://127.0.0.1:8080".to_owned(),
                chunk_size,
                upload_max_in_flight_chunks,
            )
            .await?),
            Self::Configured(config) => {
                let configured = config
                    .as_ref()
                    .clone()
                    .with_root_dir(root)
                    .with_chunk_size(chunk_size)
                    .with_upload_max_in_flight_chunks(upload_max_in_flight_chunks);
                Ok(BenchmarkBackend::from_config(
                    &configured,
                    configured.root_dir().to_path_buf(),
                    benchmark_namespace,
                )
                .await?)
            }
        }
    }
}

/// Runs the local sparse-update benchmark suite.
///
/// # Errors
///
/// Returns [`BenchRuntimeError`] when the benchmark parameters are invalid, storage
/// roots cannot be created, or the backend violates the expected sparse-update flow.
pub async fn run_bench(
    storage_dir: &Path,
    config: BenchConfig,
) -> Result<BenchReport, BenchRuntimeError> {
    let deployment_target = config.deployment_target;
    let scenario = config.scenario;
    let iterations = config.iterations;
    let concurrency = config.concurrency;
    let upload_max_in_flight_chunks = config.upload_max_in_flight_chunks;
    let chunk_size_bytes = config.chunk_size_bytes;
    let base_bytes = config.base_bytes;
    let mutated_bytes = config.mutated_bytes;

    if iterations == 0 {
        return Err(BenchRuntimeError::ZeroIterations);
    }
    if concurrency == 0 {
        return Err(BenchRuntimeError::ZeroConcurrency);
    }
    if upload_max_in_flight_chunks == 0 {
        return Err(BenchRuntimeError::ZeroUploadMaxInFlightChunks);
    }
    if chunk_size_bytes == 0 {
        return Err(BenchRuntimeError::ZeroChunkSize);
    }
    if mutated_bytes == 0 {
        return Err(BenchRuntimeError::ZeroMutatedBytes);
    }
    if mutated_bytes > base_bytes {
        return Err(BenchRuntimeError::MutatedBytesExceedBaseBytes);
    }

    fs::create_dir_all(storage_dir).await?;
    let run_root = allocate_bench_run_root(storage_dir).await?;
    fs::create_dir_all(&run_root).await?;

    let base = build_base_asset(base_bytes)?;
    let updated = build_sparse_update(&base, mutated_bytes)?;
    let chunk_size = NonZeroUsize::new(chunk_size_bytes).ok_or(BenchRuntimeError::ZeroChunkSize)?;
    let upload_max_in_flight_chunks = NonZeroUsize::new(upload_max_in_flight_chunks)
        .ok_or(BenchRuntimeError::ZeroUploadMaxInFlightChunks)?;
    let backend_setup = match deployment_target {
        BenchDeploymentTarget::IsolatedLocal => BenchBackendSetup::IsolatedLocal,
        BenchDeploymentTarget::Configured => BenchBackendSetup::Configured(Box::new(
            ServerConfig::from_env()?
                .with_chunk_size(chunk_size)
                .with_upload_max_in_flight_chunks(upload_max_in_flight_chunks),
        )),
    };
    let concurrent_upload_cases =
        build_concurrent_upload_cases(&updated, mutated_bytes, chunk_size.get(), concurrency)?;
    let (cross_repository_base, cross_repository_updated) =
        build_cross_repository_assets(chunk_size.get())?;

    let fixture = BenchFixture {
        chunk_size,
        upload_max_in_flight_chunks,
        concurrency,
        base: Bytes::from(base),
        updated: Bytes::from(updated),
        ranged_reconstruction: build_mutation_range(base_bytes, mutated_bytes)?,
        concurrent_upload_cases: &concurrent_upload_cases,
        cross_repository_base: Bytes::from(cross_repository_base),
        cross_repository_updated: Bytes::from(cross_repository_updated),
    };
    let run_namespace = run_root
        .file_name()
        .and_then(|component| component.to_str())
        .ok_or(BenchRuntimeError::MissingRunNamespace)?
        .to_owned();

    let mut detail = Vec::with_capacity(usize::try_from(iterations)?);
    let mut benchmark_backend_names: Option<(String, String)> = None;
    let mut total_initial_upload_micros = 0_u64;
    let mut total_sparse_update_upload_micros = 0_u64;
    let mut total_latest_download_micros = 0_u64;
    let mut total_previous_download_micros = 0_u64;
    let mut total_ranged_reconstruction_micros = 0_u64;
    let mut total_concurrent_latest_download_micros = 0_u64;
    let mut total_concurrent_upload_micros = 0_u64;
    let mut total_cross_repository_upload_micros = 0_u64;
    let mut total_cached_latest_reconstruction_cold_micros = 0_u64;
    let mut total_cached_latest_reconstruction_hot_micros = 0_u64;
    let mut total_uploaded_bytes = 0_u64;
    let mut total_downloaded_bytes = 0_u64;
    let mut total_cached_reconstruction_response_bytes = 0_u64;
    let mut total_concurrent_downloaded_bytes = 0_u64;
    let mut total_concurrent_uploaded_bytes = 0_u64;
    let mut total_concurrent_newly_stored_bytes = 0_u64;
    let mut total_cross_repository_newly_stored_bytes = 0_u64;
    let mut total_newly_stored_bytes = 0_u64;
    let mut total_initial_inserted_chunks = 0_u64;
    let mut total_sparse_update_inserted_chunks = 0_u64;
    let mut total_sparse_update_reused_chunks = 0_u64;
    let mut total_concurrent_upload_inserted_chunks = 0_u64;
    let mut total_concurrent_upload_reused_chunks = 0_u64;
    let mut total_cross_repository_inserted_chunks = 0_u64;
    let mut total_cross_repository_reused_chunks = 0_u64;
    let mut total_process_cpu_micros = 0_u64;
    let mut total_process_cpu_cores_per_mille = 0_u64;
    let mut total_process_host_utilization_per_mille = 0_u64;
    let mut cache_hit_iterations = 0_u64;

    for index in 0..iterations {
        let iteration_number = checked_add_u32(index, 1)?;
        let iteration_root = run_root.join(format!("iteration-{index:04}"));

        let (report, metadata_backend, object_backend) = run_bench_iteration(
            iteration_number,
            iteration_root,
            &run_namespace,
            fixture.clone(),
            scenario,
            &backend_setup,
        )
        .await?;
        if benchmark_backend_names.is_none() {
            benchmark_backend_names = Some((metadata_backend, object_backend));
        }

        total_initial_upload_micros =
            checked_add_u64(total_initial_upload_micros, report.initial_upload_micros)?;
        total_sparse_update_upload_micros = checked_add_u64(
            total_sparse_update_upload_micros,
            report.sparse_update_upload_micros,
        )?;
        total_latest_download_micros =
            checked_add_u64(total_latest_download_micros, report.latest_download_micros)?;
        total_previous_download_micros = checked_add_u64(
            total_previous_download_micros,
            report.previous_download_micros,
        )?;
        total_ranged_reconstruction_micros = checked_add_u64(
            total_ranged_reconstruction_micros,
            report.ranged_reconstruction_micros,
        )?;
        total_concurrent_latest_download_micros = checked_add_u64(
            total_concurrent_latest_download_micros,
            report.concurrent_latest_download_micros,
        )?;
        total_concurrent_upload_micros = checked_add_u64(
            total_concurrent_upload_micros,
            report.concurrent_upload_micros,
        )?;
        total_cross_repository_upload_micros = checked_add_u64(
            total_cross_repository_upload_micros,
            report.cross_repository_upload_micros,
        )?;
        total_cached_latest_reconstruction_cold_micros = checked_add_u64(
            total_cached_latest_reconstruction_cold_micros,
            report.cached_latest_reconstruction_cold_micros,
        )?;
        total_cached_latest_reconstruction_hot_micros = checked_add_u64(
            total_cached_latest_reconstruction_hot_micros,
            report.cached_latest_reconstruction_hot_micros,
        )?;
        total_uploaded_bytes = checked_add_u64(total_uploaded_bytes, report.uploaded_bytes)?;
        total_downloaded_bytes = checked_add_u64(total_downloaded_bytes, report.downloaded_bytes)?;
        total_cached_reconstruction_response_bytes = checked_add_u64(
            total_cached_reconstruction_response_bytes,
            report.cached_reconstruction_response_bytes,
        )?;
        total_concurrent_downloaded_bytes = checked_add_u64(
            total_concurrent_downloaded_bytes,
            report.concurrent_downloaded_bytes,
        )?;
        total_concurrent_uploaded_bytes = checked_add_u64(
            total_concurrent_uploaded_bytes,
            report.concurrent_uploaded_bytes,
        )?;
        total_concurrent_newly_stored_bytes = checked_add_u64(
            total_concurrent_newly_stored_bytes,
            report.concurrent_newly_stored_bytes,
        )?;
        total_cross_repository_newly_stored_bytes = checked_add_u64(
            total_cross_repository_newly_stored_bytes,
            report.cross_repository_newly_stored_bytes,
        )?;
        total_newly_stored_bytes =
            checked_add_u64(total_newly_stored_bytes, report.newly_stored_bytes)?;
        total_initial_inserted_chunks = checked_add_u64(
            total_initial_inserted_chunks,
            report.initial_inserted_chunks,
        )?;
        total_sparse_update_inserted_chunks = checked_add_u64(
            total_sparse_update_inserted_chunks,
            report.sparse_update_inserted_chunks,
        )?;
        total_sparse_update_reused_chunks = checked_add_u64(
            total_sparse_update_reused_chunks,
            report.sparse_update_reused_chunks,
        )?;
        total_concurrent_upload_inserted_chunks = checked_add_u64(
            total_concurrent_upload_inserted_chunks,
            report.concurrent_upload_inserted_chunks,
        )?;
        total_concurrent_upload_reused_chunks = checked_add_u64(
            total_concurrent_upload_reused_chunks,
            report.concurrent_upload_reused_chunks,
        )?;
        total_cross_repository_inserted_chunks = checked_add_u64(
            total_cross_repository_inserted_chunks,
            report.cross_repository_inserted_chunks,
        )?;
        total_cross_repository_reused_chunks = checked_add_u64(
            total_cross_repository_reused_chunks,
            report.cross_repository_reused_chunks,
        )?;
        total_process_cpu_micros =
            checked_add_u64(total_process_cpu_micros, report.process_cpu_micros)?;
        total_process_cpu_cores_per_mille = checked_add_u64(
            total_process_cpu_cores_per_mille,
            report.process_cpu_cores_per_mille,
        )?;
        total_process_host_utilization_per_mille = checked_add_u64(
            total_process_host_utilization_per_mille,
            report.process_host_utilization_per_mille,
        )?;
        cache_hit_iterations = checked_add_u64(
            cache_hit_iterations,
            if report.cached_latest_reconstruction_cache_hit {
                1
            } else {
                0
            },
        )?;
        detail.push(report);
    }

    let iterations_u64 = u64::from(iterations);
    let base_bytes_u64 = u64::try_from(base_bytes)?;
    let chunk_size_bytes_u64 = u64::try_from(chunk_size_bytes)?;
    let measured_initial_upload_bytes = checked_mul_u64(
        base_bytes_u64,
        measured_iteration_count(total_initial_upload_micros, iterations_u64),
    )?;
    let measured_sparse_update_upload_bytes = checked_mul_u64(
        base_bytes_u64,
        measured_iteration_count(total_sparse_update_upload_micros, iterations_u64),
    )?;
    let measured_latest_download_bytes = checked_mul_u64(
        base_bytes_u64,
        measured_iteration_count(total_latest_download_micros, iterations_u64),
    )?;
    let measured_previous_download_bytes = checked_mul_u64(
        base_bytes_u64,
        measured_iteration_count(total_previous_download_micros, iterations_u64),
    )?;
    let cross_repository_asset_bytes = checked_mul_u64(chunk_size_bytes_u64, 3)?;
    let measured_cross_repository_upload_bytes = checked_mul_u64(
        cross_repository_asset_bytes,
        measured_iteration_count(total_cross_repository_upload_micros, iterations_u64),
    )?;
    let initial_upload_bytes_per_second =
        throughput_bytes_per_second(measured_initial_upload_bytes, total_initial_upload_micros);
    let sparse_update_upload_bytes_per_second = throughput_bytes_per_second(
        measured_sparse_update_upload_bytes,
        total_sparse_update_upload_micros,
    );
    let latest_download_bytes_per_second =
        throughput_bytes_per_second(measured_latest_download_bytes, total_latest_download_micros);
    let concurrent_latest_download_bytes_per_second = throughput_bytes_per_second(
        total_concurrent_downloaded_bytes,
        total_concurrent_latest_download_micros,
    );
    let concurrent_upload_bytes_per_second = throughput_bytes_per_second(
        total_concurrent_uploaded_bytes,
        total_concurrent_upload_micros,
    );
    let available_parallelism = available_parallelism_u64();
    let (metadata_backend, object_backend) =
        benchmark_backend_names.ok_or(BenchRuntimeError::MissingBenchmarkBackendNames)?;
    Ok(BenchReport {
        scenario,
        deployment_target,
        metadata_backend: metadata_backend.clone(),
        object_backend: object_backend.clone(),
        inventory_scope: inventory_scope(&metadata_backend, &object_backend),
        storage_dir: run_root,
        iterations,
        chunk_size_bytes: chunk_size_bytes_u64,
        concurrency,
        upload_max_in_flight_chunks: u64::try_from(upload_max_in_flight_chunks.get())?,
        base_bytes: base_bytes_u64,
        mutated_bytes: u64::try_from(mutated_bytes)?,
        available_parallelism,
        average_initial_upload_micros: checked_average_u64(
            total_initial_upload_micros,
            iterations_u64,
        )?,
        average_sparse_update_upload_micros: checked_average_u64(
            total_sparse_update_upload_micros,
            iterations_u64,
        )?,
        average_latest_download_micros: checked_average_u64(
            total_latest_download_micros,
            iterations_u64,
        )?,
        average_previous_download_micros: checked_average_u64(
            total_previous_download_micros,
            iterations_u64,
        )?,
        average_ranged_reconstruction_micros: checked_average_u64(
            total_ranged_reconstruction_micros,
            iterations_u64,
        )?,
        average_concurrent_latest_download_micros: checked_average_u64(
            total_concurrent_latest_download_micros,
            iterations_u64,
        )?,
        average_concurrent_upload_micros: checked_average_u64(
            total_concurrent_upload_micros,
            iterations_u64,
        )?,
        average_cross_repository_upload_micros: checked_average_u64(
            total_cross_repository_upload_micros,
            iterations_u64,
        )?,
        average_cached_latest_reconstruction_cold_micros: checked_average_u64(
            total_cached_latest_reconstruction_cold_micros,
            iterations_u64,
        )?,
        average_cached_latest_reconstruction_hot_micros: checked_average_u64(
            total_cached_latest_reconstruction_hot_micros,
            iterations_u64,
        )?,
        average_initial_upload_bytes_per_second: initial_upload_bytes_per_second,
        average_sparse_update_upload_bytes_per_second: sparse_update_upload_bytes_per_second,
        average_latest_download_bytes_per_second: latest_download_bytes_per_second,
        average_previous_download_bytes_per_second: throughput_bytes_per_second(
            measured_previous_download_bytes,
            total_previous_download_micros,
        ),
        average_concurrent_latest_download_bytes_per_second:
            concurrent_latest_download_bytes_per_second,
        average_concurrent_upload_bytes_per_second: concurrent_upload_bytes_per_second,
        average_cross_repository_upload_bytes_per_second: throughput_bytes_per_second(
            measured_cross_repository_upload_bytes,
            total_cross_repository_upload_micros,
        ),
        average_cached_latest_reconstruction_hit_bytes_per_second: throughput_bytes_per_second(
            total_cached_reconstruction_response_bytes,
            total_cached_latest_reconstruction_hot_micros,
        ),
        average_process_cpu_micros: checked_average_u64(total_process_cpu_micros, iterations_u64)?,
        average_process_cpu_cores_per_mille: checked_average_u64(
            total_process_cpu_cores_per_mille,
            iterations_u64,
        )?,
        average_process_host_utilization_per_mille: checked_average_u64(
            total_process_host_utilization_per_mille,
            iterations_u64,
        )?,
        concurrent_latest_download_scaling_per_mille: scaling_per_mille(
            concurrent_latest_download_bytes_per_second,
            latest_download_bytes_per_second,
            concurrency,
        ),
        concurrent_upload_scaling_per_mille: scaling_per_mille(
            concurrent_upload_bytes_per_second,
            sparse_update_upload_bytes_per_second,
            concurrency,
        ),
        total_uploaded_bytes,
        total_downloaded_bytes,
        total_cached_reconstruction_response_bytes,
        cache_hit_iterations,
        total_concurrent_downloaded_bytes,
        total_concurrent_uploaded_bytes,
        total_concurrent_newly_stored_bytes,
        total_cross_repository_newly_stored_bytes,
        total_newly_stored_bytes,
        total_initial_inserted_chunks,
        total_sparse_update_inserted_chunks,
        total_sparse_update_reused_chunks,
        total_concurrent_upload_inserted_chunks,
        total_concurrent_upload_reused_chunks,
        total_cross_repository_inserted_chunks,
        total_cross_repository_reused_chunks,
        iterations_detail: detail,
    })
}

/// Runs the zero-storage upload-ingest benchmark suite.
///
/// # Errors
///
/// Returns [`BenchRuntimeError`] when parameters are invalid or the ingest path fails.
pub async fn run_ingest_bench(config: BenchConfig) -> Result<IngestBenchReport, BenchRuntimeError> {
    let scenario = config.scenario;
    let iterations = config.iterations;
    let concurrency = config.concurrency;
    let upload_max_in_flight_chunks = config.upload_max_in_flight_chunks;
    let chunk_size_bytes = config.chunk_size_bytes;
    let base_bytes = config.base_bytes;
    let mutated_bytes = config.mutated_bytes;

    if !scenario.supports_ingest() {
        return Err(BenchRuntimeError::UnsupportedScenarioForMode);
    }
    if iterations == 0 {
        return Err(BenchRuntimeError::ZeroIterations);
    }
    if concurrency == 0 {
        return Err(BenchRuntimeError::ZeroConcurrency);
    }
    if upload_max_in_flight_chunks == 0 {
        return Err(BenchRuntimeError::ZeroUploadMaxInFlightChunks);
    }
    if chunk_size_bytes == 0 {
        return Err(BenchRuntimeError::ZeroChunkSize);
    }
    if mutated_bytes == 0 {
        return Err(BenchRuntimeError::ZeroMutatedBytes);
    }
    if mutated_bytes > base_bytes {
        return Err(BenchRuntimeError::MutatedBytesExceedBaseBytes);
    }

    let base = build_base_asset(base_bytes)?;
    let updated = build_sparse_update(&base, mutated_bytes)?;
    let chunk_size = NonZeroUsize::new(chunk_size_bytes).ok_or(BenchRuntimeError::ZeroChunkSize)?;
    let upload_max_in_flight_chunks = NonZeroUsize::new(upload_max_in_flight_chunks)
        .ok_or(BenchRuntimeError::ZeroUploadMaxInFlightChunks)?;
    let concurrent_upload_cases = build_concurrent_ingest_upload_cases(
        &updated,
        mutated_bytes,
        chunk_size.get(),
        concurrency,
    )?;
    let fixture = IngestBenchScenario {
        chunk_size,
        upload_max_in_flight_chunks,
        concurrent_upload_cases: &concurrent_upload_cases,
        base: Bytes::from(base),
        updated: Bytes::from(updated),
    };

    let mut detail = Vec::with_capacity(usize::try_from(iterations)?);
    let mut total_initial_upload_micros = 0_u64;
    let mut total_sparse_update_upload_micros = 0_u64;
    let mut total_concurrent_upload_micros = 0_u64;
    let mut total_uploaded_bytes = 0_u64;
    let mut total_concurrent_uploaded_bytes = 0_u64;
    let mut total_initial_inserted_chunks = 0_u64;
    let mut total_sparse_update_inserted_chunks = 0_u64;
    let mut total_concurrent_upload_inserted_chunks = 0_u64;
    let mut total_concurrent_upload_process_cpu_micros = 0_u64;
    let mut total_concurrent_upload_process_cpu_cores_per_mille = 0_u64;
    let mut total_concurrent_upload_process_host_utilization_per_mille = 0_u64;
    let mut total_process_cpu_micros = 0_u64;
    let mut total_process_cpu_cores_per_mille = 0_u64;
    let mut total_process_host_utilization_per_mille = 0_u64;

    for index in 0..iterations {
        let iteration_number = checked_add_u32(index, 1)?;
        let report = run_ingest_bench_iteration(iteration_number, &fixture, scenario).await?;
        total_initial_upload_micros =
            checked_add_u64(total_initial_upload_micros, report.initial_upload_micros)?;
        total_sparse_update_upload_micros = checked_add_u64(
            total_sparse_update_upload_micros,
            report.sparse_update_upload_micros,
        )?;
        total_concurrent_upload_micros = checked_add_u64(
            total_concurrent_upload_micros,
            report.concurrent_upload_micros,
        )?;
        total_uploaded_bytes = checked_add_u64(total_uploaded_bytes, report.uploaded_bytes)?;
        total_concurrent_uploaded_bytes = checked_add_u64(
            total_concurrent_uploaded_bytes,
            report.concurrent_uploaded_bytes,
        )?;
        total_initial_inserted_chunks = checked_add_u64(
            total_initial_inserted_chunks,
            report.initial_inserted_chunks,
        )?;
        total_sparse_update_inserted_chunks = checked_add_u64(
            total_sparse_update_inserted_chunks,
            report.sparse_update_inserted_chunks,
        )?;
        total_concurrent_upload_inserted_chunks = checked_add_u64(
            total_concurrent_upload_inserted_chunks,
            report.concurrent_upload_inserted_chunks,
        )?;
        total_concurrent_upload_process_cpu_micros = checked_add_u64(
            total_concurrent_upload_process_cpu_micros,
            report.concurrent_upload_process_cpu_micros,
        )?;
        total_concurrent_upload_process_cpu_cores_per_mille = checked_add_u64(
            total_concurrent_upload_process_cpu_cores_per_mille,
            report.concurrent_upload_process_cpu_cores_per_mille,
        )?;
        total_concurrent_upload_process_host_utilization_per_mille = checked_add_u64(
            total_concurrent_upload_process_host_utilization_per_mille,
            report.concurrent_upload_process_host_utilization_per_mille,
        )?;
        total_process_cpu_micros =
            checked_add_u64(total_process_cpu_micros, report.process_cpu_micros)?;
        total_process_cpu_cores_per_mille = checked_add_u64(
            total_process_cpu_cores_per_mille,
            report.process_cpu_cores_per_mille,
        )?;
        total_process_host_utilization_per_mille = checked_add_u64(
            total_process_host_utilization_per_mille,
            report.process_host_utilization_per_mille,
        )?;
        detail.push(report);
    }

    let iterations_u64 = u64::from(iterations);
    let base_bytes_u64 = u64::try_from(base_bytes)?;
    let initial_upload_bytes_per_second = throughput_bytes_per_second(
        checked_mul_u64(
            base_bytes_u64,
            measured_iteration_count(total_initial_upload_micros, iterations_u64),
        )?,
        total_initial_upload_micros,
    );
    let sparse_update_upload_bytes_per_second = throughput_bytes_per_second(
        checked_mul_u64(
            base_bytes_u64,
            measured_iteration_count(total_sparse_update_upload_micros, iterations_u64),
        )?,
        total_sparse_update_upload_micros,
    );
    let concurrent_upload_bytes_per_second = throughput_bytes_per_second(
        total_concurrent_uploaded_bytes,
        total_concurrent_upload_micros,
    );
    let available_parallelism = available_parallelism_u64();
    Ok(IngestBenchReport {
        scenario,
        iterations,
        chunk_size_bytes: u64::try_from(chunk_size_bytes)?,
        concurrency,
        upload_max_in_flight_chunks: u64::try_from(upload_max_in_flight_chunks.get())?,
        base_bytes: base_bytes_u64,
        mutated_bytes: u64::try_from(mutated_bytes)?,
        available_parallelism,
        average_initial_upload_micros: checked_average_u64(
            total_initial_upload_micros,
            iterations_u64,
        )?,
        average_sparse_update_upload_micros: checked_average_u64(
            total_sparse_update_upload_micros,
            iterations_u64,
        )?,
        average_concurrent_upload_micros: checked_average_u64(
            total_concurrent_upload_micros,
            iterations_u64,
        )?,
        average_initial_upload_bytes_per_second: initial_upload_bytes_per_second,
        average_sparse_update_upload_bytes_per_second: sparse_update_upload_bytes_per_second,
        average_concurrent_upload_bytes_per_second: concurrent_upload_bytes_per_second,
        average_concurrent_upload_process_cpu_micros: checked_average_u64(
            total_concurrent_upload_process_cpu_micros,
            iterations_u64,
        )?,
        average_concurrent_upload_process_cpu_cores_per_mille: checked_average_u64(
            total_concurrent_upload_process_cpu_cores_per_mille,
            iterations_u64,
        )?,
        average_concurrent_upload_process_host_utilization_per_mille: checked_average_u64(
            total_concurrent_upload_process_host_utilization_per_mille,
            iterations_u64,
        )?,
        average_process_cpu_micros: checked_average_u64(total_process_cpu_micros, iterations_u64)?,
        average_process_cpu_cores_per_mille: checked_average_u64(
            total_process_cpu_cores_per_mille,
            iterations_u64,
        )?,
        average_process_host_utilization_per_mille: checked_average_u64(
            total_process_host_utilization_per_mille,
            iterations_u64,
        )?,
        concurrent_upload_scaling_per_mille: scaling_per_mille(
            concurrent_upload_bytes_per_second,
            sparse_update_upload_bytes_per_second,
            concurrency,
        ),
        total_uploaded_bytes,
        total_concurrent_uploaded_bytes,
        total_initial_inserted_chunks,
        total_sparse_update_inserted_chunks,
        total_concurrent_upload_inserted_chunks,
        iterations_detail: detail,
    })
}

async fn allocate_bench_run_root(storage_dir: &Path) -> Result<PathBuf, BenchRuntimeError> {
    let mut index = 0_u32;
    loop {
        let candidate = storage_dir.join(format!("run-{index:04}"));
        match fs::metadata(&candidate).await {
            Ok(_metadata) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(candidate),
            Err(error) => return Err(BenchRuntimeError::Io(error)),
        }
        index = checked_add_u32(index, 1)?;
    }
}

fn iteration_namespace(run_namespace: &str, iteration: u32) -> String {
    format!("{run_namespace}-iteration-{iteration:04}")
}

fn namespaced_file_id(namespace: &str, file_id: &str) -> String {
    format!("{namespace}-{file_id}")
}

fn build_iteration_repository_scopes(
    namespace: &str,
) -> Result<(RepositoryScope, RepositoryScope), BenchRuntimeError> {
    let left_owner = format!("bench-left-{namespace}");
    let right_owner = format!("bench-right-{namespace}");
    let scope_left = RepositoryScope::new(
        RepositoryProvider::Generic,
        &left_owner,
        "assets",
        Some("main"),
    )?;
    let scope_right = RepositoryScope::new(
        RepositoryProvider::Generic,
        &right_owner,
        "assets",
        Some("main"),
    )?;
    Ok((scope_left, scope_right))
}

fn inventory_scope(metadata_backend: &str, object_backend: &str) -> BenchInventoryScope {
    match (metadata_backend, object_backend) {
        ("local", "local") => BenchInventoryScope::Isolated,
        ("postgres", "s3") => BenchInventoryScope::BackendGlobal,
        _ => BenchInventoryScope::Mixed,
    }
}

async fn run_bench_iteration(
    iteration: u32,
    storage_dir: PathBuf,
    run_namespace: &str,
    fixture: BenchFixture<'_>,
    scenario: BenchScenario,
    backend_setup: &BenchBackendSetup,
) -> Result<(BenchIterationReport, String, String), BenchRuntimeError> {
    let available_parallelism = available_parallelism_u64();
    let iteration_started = Instant::now();
    let process_cpu_started = capture_process_cpu_micros();
    let namespace = iteration_namespace(run_namespace, iteration);
    let asset_file_id = namespaced_file_id(&namespace, "asset.bin");
    let cross_repository_file_id = namespaced_file_id(&namespace, "cross-repo.bin");
    let (scope_left, scope_right) = build_iteration_repository_scopes(&namespace)?;
    let backend = backend_setup
        .create_backend(
            storage_dir.clone(),
            fixture.chunk_size,
            fixture.upload_max_in_flight_chunks,
            &namespace,
        )
        .await?;
    let metadata_backend = backend.metadata_backend_name().to_owned();
    let object_backend = backend.object_backend_name().to_owned();

    let initial = if matches!(scenario, BenchScenario::InitialUpload | BenchScenario::Full) {
        let operation_started = Instant::now();
        let response = backend
            .upload_file(&asset_file_id, fixture.base.clone(), None)
            .await?;
        let elapsed = duration_micros(operation_started.elapsed())?;
        (response, elapsed)
    } else {
        (
            backend
                .upload_file(&asset_file_id, fixture.base.clone(), None)
                .await?,
            0,
        )
    };

    let sparse_update = if matches!(
        scenario,
        BenchScenario::SparseUpdateUpload | BenchScenario::Full
    ) {
        let operation_started = Instant::now();
        let response = backend
            .upload_file(&asset_file_id, fixture.updated.clone(), None)
            .await?;
        let elapsed = duration_micros(operation_started.elapsed())?;
        (response, elapsed)
    } else {
        (
            backend
                .upload_file(&asset_file_id, fixture.updated.clone(), None)
                .await?,
            0,
        )
    };

    let (latest, latest_download_micros) = if matches!(
        scenario,
        BenchScenario::LatestDownload | BenchScenario::Full
    ) {
        let operation_started = Instant::now();
        let response = backend.download_file(&asset_file_id, None, None).await?;
        let elapsed = duration_micros(operation_started.elapsed())?;
        (response, elapsed)
    } else {
        (Vec::new(), 0)
    };

    let (previous, previous_download_micros) = if matches!(
        scenario,
        BenchScenario::PreviousDownload | BenchScenario::Full
    ) {
        let operation_started = Instant::now();
        let response = backend
            .download_file(&asset_file_id, Some(&initial.0.content_hash), None)
            .await?;
        let elapsed = duration_micros(operation_started.elapsed())?;
        (response, elapsed)
    } else {
        (Vec::new(), 0)
    };

    let ranged_reconstruction_micros = if matches!(
        scenario,
        BenchScenario::RangedReconstruction | BenchScenario::Full
    ) {
        let operation_started = Instant::now();
        let reconstruction = backend
            .reconstruction(
                &asset_file_id,
                None,
                Some(fixture.ranged_reconstruction),
                None,
            )
            .await?;
        let elapsed = duration_micros(operation_started.elapsed())?;
        if reconstruction.terms.is_empty() {
            return Err(BenchRuntimeError::EmptyRangedReconstruction);
        }
        elapsed
    } else {
        0
    };

    let (concurrent_latest_download_micros, concurrent_downloaded_bytes) = if matches!(
        scenario,
        BenchScenario::ConcurrentLatestDownload | BenchScenario::Full
    ) {
        run_concurrent_latest_downloads(
            &backend,
            &asset_file_id,
            fixture.updated.as_ref(),
            fixture.concurrency,
        )
        .await?
    } else {
        (0, 0)
    };
    let (
        concurrent_upload_micros,
        concurrent_uploaded_bytes,
        concurrent_newly_stored_bytes,
        concurrent_upload_inserted_chunks,
        concurrent_upload_reused_chunks,
    ) = if matches!(
        scenario,
        BenchScenario::ConcurrentUpload | BenchScenario::Full
    ) {
        run_concurrent_uploads(&backend, &namespace, fixture.concurrent_upload_cases).await?
    } else {
        (0, 0, 0, 0, 0)
    };
    let seeded_cross_repository = backend
        .upload_file(
            &cross_repository_file_id,
            fixture.cross_repository_base.clone(),
            Some(&scope_left),
        )
        .await?;
    let (cross_repository, cross_repository_upload_micros) = if matches!(
        scenario,
        BenchScenario::CrossRepositoryUpload | BenchScenario::Full
    ) {
        let operation_started = Instant::now();
        let response = backend
            .upload_file(
                &cross_repository_file_id,
                fixture.cross_repository_updated.clone(),
                Some(&scope_right),
            )
            .await?;
        let elapsed = duration_micros(operation_started.elapsed())?;
        (response, elapsed)
    } else {
        (
            backend
                .upload_file(
                    &cross_repository_file_id,
                    fixture.cross_repository_updated.clone(),
                    Some(&scope_right),
                )
                .await?,
            0,
        )
    };
    let cached_reconstruction = if matches!(
        scenario,
        BenchScenario::CachedLatestReconstruction | BenchScenario::Full
    ) {
        backend
            .benchmark_memory_reconstruction_cache(
                &asset_file_id,
                &sparse_update.0.content_hash,
                None,
            )
            .await?
    } else {
        ReconstructionCacheBenchReport {
            cold_load_micros: 0,
            hot_load_micros: 0,
            response_bytes: 0,
            cache_hit: false,
        }
    };

    if !latest.is_empty() && latest.as_slice() != fixture.updated.as_ref() {
        return Err(BenchRuntimeError::LatestDownloadMismatch);
    }
    if !previous.is_empty() && previous.as_slice() != fixture.base.as_ref() {
        return Err(BenchRuntimeError::PreviousDownloadMismatch);
    }
    let left_cross_repo = backend
        .download_file(&cross_repository_file_id, None, Some(&scope_left))
        .await?;
    let right_cross_repo = backend
        .download_file(&cross_repository_file_id, None, Some(&scope_right))
        .await?;
    if left_cross_repo.as_slice() != fixture.cross_repository_base.as_ref() {
        return Err(BenchRuntimeError::CrossRepositoryLeftDownloadMismatch);
    }
    if right_cross_repo.as_slice() != fixture.cross_repository_updated.as_ref() {
        return Err(BenchRuntimeError::CrossRepositoryRightDownloadMismatch);
    }
    if cross_repository.reused_chunks == 0 {
        return Err(BenchRuntimeError::CrossRepositoryUploadWithoutReusedChunks);
    }

    let stats = backend.stats().await?;
    let uploaded_bytes = match scenario {
        BenchScenario::Full => {
            let uploaded = checked_add_u64(
                checked_add_u64(initial.0.total_bytes, sparse_update.0.total_bytes)?,
                concurrent_uploaded_bytes,
            )?;
            checked_add_u64(
                uploaded,
                checked_add_u64(
                    seeded_cross_repository.total_bytes,
                    cross_repository.total_bytes,
                )?,
            )?
        }
        BenchScenario::InitialUpload => initial.0.total_bytes,
        BenchScenario::SparseUpdateUpload => sparse_update.0.total_bytes,
        BenchScenario::ConcurrentUpload => concurrent_uploaded_bytes,
        BenchScenario::CrossRepositoryUpload => cross_repository.total_bytes,
        BenchScenario::LatestDownload
        | BenchScenario::PreviousDownload
        | BenchScenario::RangedReconstruction
        | BenchScenario::ConcurrentLatestDownload
        | BenchScenario::CachedLatestReconstruction => 0,
    };
    let downloaded_bytes = match scenario {
        BenchScenario::Full => {
            let downloaded = checked_add_u64(
                checked_add_u64(u64::try_from(latest.len())?, u64::try_from(previous.len())?)?,
                concurrent_downloaded_bytes,
            )?;
            checked_add_u64(
                downloaded,
                checked_add_u64(
                    u64::try_from(left_cross_repo.len())?,
                    u64::try_from(right_cross_repo.len())?,
                )?,
            )?
        }
        BenchScenario::LatestDownload => u64::try_from(latest.len())?,
        BenchScenario::PreviousDownload => u64::try_from(previous.len())?,
        BenchScenario::ConcurrentLatestDownload => concurrent_downloaded_bytes,
        BenchScenario::InitialUpload
        | BenchScenario::SparseUpdateUpload
        | BenchScenario::RangedReconstruction
        | BenchScenario::ConcurrentUpload
        | BenchScenario::CrossRepositoryUpload
        | BenchScenario::CachedLatestReconstruction => 0,
    };
    let newly_stored_bytes = match scenario {
        BenchScenario::Full => {
            let newly_stored =
                checked_add_u64(initial.0.stored_bytes, sparse_update.0.stored_bytes)?;
            let newly_stored = checked_add_u64(newly_stored, concurrent_newly_stored_bytes)?;
            checked_add_u64(
                newly_stored,
                checked_add_u64(
                    seeded_cross_repository.stored_bytes,
                    cross_repository.stored_bytes,
                )?,
            )?
        }
        BenchScenario::InitialUpload => initial.0.stored_bytes,
        BenchScenario::SparseUpdateUpload => sparse_update.0.stored_bytes,
        BenchScenario::ConcurrentUpload => concurrent_newly_stored_bytes,
        BenchScenario::CrossRepositoryUpload => cross_repository.stored_bytes,
        BenchScenario::LatestDownload
        | BenchScenario::PreviousDownload
        | BenchScenario::RangedReconstruction
        | BenchScenario::ConcurrentLatestDownload
        | BenchScenario::CachedLatestReconstruction => 0,
    };
    let elapsed_micros = duration_micros(iteration_started.elapsed())?;
    let process_cpu_micros = capture_process_cpu_micros().saturating_sub(process_cpu_started);
    let process_cpu_cores_per_mille = ratio_per_mille(process_cpu_micros, elapsed_micros);
    let process_host_utilization_per_mille =
        host_utilization_per_mille(process_cpu_micros, elapsed_micros, available_parallelism);

    Ok((
        BenchIterationReport {
            iteration,
            storage_dir,
            initial_upload_micros: initial.1,
            sparse_update_upload_micros: sparse_update.1,
            latest_download_micros,
            previous_download_micros,
            ranged_reconstruction_micros,
            concurrent_latest_download_micros,
            concurrent_upload_micros,
            cross_repository_upload_micros,
            cached_latest_reconstruction_cold_micros: cached_reconstruction.cold_load_micros,
            cached_latest_reconstruction_hot_micros: cached_reconstruction.hot_load_micros,
            uploaded_bytes,
            downloaded_bytes,
            cached_reconstruction_response_bytes: cached_reconstruction.response_bytes,
            cached_latest_reconstruction_cache_hit: cached_reconstruction.cache_hit,
            concurrent_downloaded_bytes,
            concurrent_uploaded_bytes,
            concurrent_newly_stored_bytes,
            newly_stored_bytes,
            initial_inserted_chunks: match scenario {
                BenchScenario::Full | BenchScenario::InitialUpload => initial.0.inserted_chunks,
                BenchScenario::SparseUpdateUpload
                | BenchScenario::LatestDownload
                | BenchScenario::PreviousDownload
                | BenchScenario::RangedReconstruction
                | BenchScenario::ConcurrentLatestDownload
                | BenchScenario::ConcurrentUpload
                | BenchScenario::CrossRepositoryUpload
                | BenchScenario::CachedLatestReconstruction => 0,
            },
            sparse_update_inserted_chunks: match scenario {
                BenchScenario::Full | BenchScenario::SparseUpdateUpload => {
                    sparse_update.0.inserted_chunks
                }
                BenchScenario::InitialUpload
                | BenchScenario::LatestDownload
                | BenchScenario::PreviousDownload
                | BenchScenario::RangedReconstruction
                | BenchScenario::ConcurrentLatestDownload
                | BenchScenario::ConcurrentUpload
                | BenchScenario::CrossRepositoryUpload
                | BenchScenario::CachedLatestReconstruction => 0,
            },
            sparse_update_reused_chunks: match scenario {
                BenchScenario::Full | BenchScenario::SparseUpdateUpload => {
                    sparse_update.0.reused_chunks
                }
                BenchScenario::InitialUpload
                | BenchScenario::LatestDownload
                | BenchScenario::PreviousDownload
                | BenchScenario::RangedReconstruction
                | BenchScenario::ConcurrentLatestDownload
                | BenchScenario::ConcurrentUpload
                | BenchScenario::CrossRepositoryUpload
                | BenchScenario::CachedLatestReconstruction => 0,
            },
            concurrent_upload_inserted_chunks,
            concurrent_upload_reused_chunks,
            cross_repository_inserted_chunks: match scenario {
                BenchScenario::Full | BenchScenario::CrossRepositoryUpload => {
                    cross_repository.inserted_chunks
                }
                BenchScenario::InitialUpload
                | BenchScenario::SparseUpdateUpload
                | BenchScenario::LatestDownload
                | BenchScenario::PreviousDownload
                | BenchScenario::RangedReconstruction
                | BenchScenario::ConcurrentLatestDownload
                | BenchScenario::ConcurrentUpload
                | BenchScenario::CachedLatestReconstruction => 0,
            },
            cross_repository_reused_chunks: match scenario {
                BenchScenario::Full | BenchScenario::CrossRepositoryUpload => {
                    cross_repository.reused_chunks
                }
                BenchScenario::InitialUpload
                | BenchScenario::SparseUpdateUpload
                | BenchScenario::LatestDownload
                | BenchScenario::PreviousDownload
                | BenchScenario::RangedReconstruction
                | BenchScenario::ConcurrentLatestDownload
                | BenchScenario::ConcurrentUpload
                | BenchScenario::CachedLatestReconstruction => 0,
            },
            cross_repository_newly_stored_bytes: match scenario {
                BenchScenario::Full | BenchScenario::CrossRepositoryUpload => {
                    cross_repository.stored_bytes
                }
                BenchScenario::InitialUpload
                | BenchScenario::SparseUpdateUpload
                | BenchScenario::LatestDownload
                | BenchScenario::PreviousDownload
                | BenchScenario::RangedReconstruction
                | BenchScenario::ConcurrentLatestDownload
                | BenchScenario::ConcurrentUpload
                | BenchScenario::CachedLatestReconstruction => 0,
            },
            chunk_objects: stats.chunks,
            chunk_bytes: stats.chunk_bytes,
            visible_files: stats.files,
            process_cpu_micros,
            process_cpu_cores_per_mille,
            process_host_utilization_per_mille,
        },
        metadata_backend,
        object_backend,
    ))
}

async fn run_ingest_bench_iteration(
    iteration: u32,
    scenario: &IngestBenchScenario<'_>,
    focus: BenchScenario,
) -> Result<IngestBenchIterationReport, BenchRuntimeError> {
    let available_parallelism = available_parallelism_u64();
    let iteration_started = Instant::now();
    let process_cpu_started = capture_process_cpu_micros();
    let initial = if matches!(focus, BenchScenario::InitialUpload | BenchScenario::Full) {
        let operation_started = Instant::now();
        let report = ingest_without_storage_with_parallelism(
            scenario.chunk_size,
            scenario.upload_max_in_flight_chunks,
            "asset.bin",
            scenario.base.clone(),
            None,
        )
        .await?;
        let elapsed = duration_micros(operation_started.elapsed())?;
        (report, elapsed)
    } else {
        (
            ingest_without_storage_with_parallelism(
                scenario.chunk_size,
                scenario.upload_max_in_flight_chunks,
                "asset.bin",
                scenario.base.clone(),
                None,
            )
            .await?,
            0,
        )
    };

    let sparse_update = if matches!(
        focus,
        BenchScenario::SparseUpdateUpload | BenchScenario::Full
    ) {
        let operation_started = Instant::now();
        let report = ingest_without_storage_with_parallelism(
            scenario.chunk_size,
            scenario.upload_max_in_flight_chunks,
            "asset.bin",
            scenario.updated.clone(),
            None,
        )
        .await?;
        let elapsed = duration_micros(operation_started.elapsed())?;
        (report, elapsed)
    } else {
        (
            ingest_without_storage_with_parallelism(
                scenario.chunk_size,
                scenario.upload_max_in_flight_chunks,
                "asset.bin",
                scenario.updated.clone(),
                None,
            )
            .await?,
            0,
        )
    };

    let concurrent_upload =
        if matches!(focus, BenchScenario::ConcurrentUpload | BenchScenario::Full) {
            run_concurrent_ingest_uploads(
                scenario.chunk_size,
                scenario.upload_max_in_flight_chunks,
                scenario.concurrent_upload_cases,
                available_parallelism,
            )
            .await?
        } else {
            TimedConcurrentIngestUpload::default()
        };
    let uploaded_bytes = match focus {
        BenchScenario::Full => {
            let uploaded = checked_add_u64(initial.0.total_bytes, sparse_update.0.total_bytes)?;
            checked_add_u64(uploaded, concurrent_upload.uploaded_bytes)?
        }
        BenchScenario::InitialUpload => initial.0.total_bytes,
        BenchScenario::SparseUpdateUpload => sparse_update.0.total_bytes,
        BenchScenario::ConcurrentUpload => concurrent_upload.uploaded_bytes,
        BenchScenario::LatestDownload
        | BenchScenario::PreviousDownload
        | BenchScenario::RangedReconstruction
        | BenchScenario::ConcurrentLatestDownload
        | BenchScenario::CrossRepositoryUpload
        | BenchScenario::CachedLatestReconstruction => 0,
    };
    let elapsed_micros = duration_micros(iteration_started.elapsed())?;
    let process_cpu_micros = capture_process_cpu_micros().saturating_sub(process_cpu_started);
    let process_cpu_cores_per_mille = ratio_per_mille(process_cpu_micros, elapsed_micros);
    let process_host_utilization_per_mille =
        host_utilization_per_mille(process_cpu_micros, elapsed_micros, available_parallelism);

    Ok(IngestBenchIterationReport {
        iteration,
        initial_upload_micros: initial.1,
        sparse_update_upload_micros: sparse_update.1,
        concurrent_upload_micros: concurrent_upload.elapsed_micros,
        uploaded_bytes,
        concurrent_uploaded_bytes: concurrent_upload.uploaded_bytes,
        initial_inserted_chunks: match focus {
            BenchScenario::Full | BenchScenario::InitialUpload => initial.0.inserted_chunks,
            BenchScenario::SparseUpdateUpload
            | BenchScenario::LatestDownload
            | BenchScenario::PreviousDownload
            | BenchScenario::RangedReconstruction
            | BenchScenario::ConcurrentLatestDownload
            | BenchScenario::ConcurrentUpload
            | BenchScenario::CrossRepositoryUpload
            | BenchScenario::CachedLatestReconstruction => 0,
        },
        sparse_update_inserted_chunks: match focus {
            BenchScenario::Full | BenchScenario::SparseUpdateUpload => {
                sparse_update.0.inserted_chunks
            }
            BenchScenario::InitialUpload
            | BenchScenario::LatestDownload
            | BenchScenario::PreviousDownload
            | BenchScenario::RangedReconstruction
            | BenchScenario::ConcurrentLatestDownload
            | BenchScenario::ConcurrentUpload
            | BenchScenario::CrossRepositoryUpload
            | BenchScenario::CachedLatestReconstruction => 0,
        },
        concurrent_upload_inserted_chunks: concurrent_upload.inserted_chunks,
        concurrent_upload_process_cpu_micros: concurrent_upload.process_cpu_micros,
        concurrent_upload_process_cpu_cores_per_mille: concurrent_upload
            .process_cpu_cores_per_mille,
        concurrent_upload_process_host_utilization_per_mille: concurrent_upload
            .process_host_utilization_per_mille,
        process_cpu_micros,
        process_cpu_cores_per_mille,
        process_host_utilization_per_mille,
    })
}

#[derive(Debug, Clone)]
struct ConcurrentUploadCase {
    file_id: String,
    expected_bytes: Bytes,
}

#[derive(Debug, Clone)]
struct ConcurrentIngestUploadCase {
    file_id: String,
    body: Bytes,
}

#[derive(Debug, Clone, Copy, Default)]
struct TimedConcurrentIngestUpload {
    elapsed_micros: u64,
    uploaded_bytes: u64,
    inserted_chunks: u64,
    process_cpu_micros: u64,
    process_cpu_cores_per_mille: u64,
    process_host_utilization_per_mille: u64,
}

async fn run_concurrent_latest_downloads(
    backend: &BenchmarkBackend,
    file_id: &str,
    expected: &[u8],
    concurrency: u32,
) -> Result<(u64, u64), BenchRuntimeError> {
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for _worker in 0..concurrency {
        let backend = backend.clone();
        let file_id = file_id.to_owned();
        tasks.spawn(async move { backend.download_file(&file_id, None, None).await });
    }

    let mut downloaded_bytes = 0_u64;
    while let Some(joined) = tasks.join_next().await {
        let bytes = joined
            .map_err(BenchRuntimeError::from)?
            .map_err(BenchRuntimeError::from)?;
        if bytes.as_slice() != expected {
            return Err(BenchRuntimeError::ConcurrentLatestDownloadMismatch);
        }
        downloaded_bytes = checked_add_u64(downloaded_bytes, u64::try_from(bytes.len())?)?;
    }

    Ok((duration_micros(started.elapsed())?, downloaded_bytes))
}

async fn run_concurrent_uploads(
    backend: &BenchmarkBackend,
    namespace: &str,
    cases: &[ConcurrentUploadCase],
) -> Result<(u64, u64, u64, u64, u64), BenchRuntimeError> {
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for case in cases {
        let backend = backend.clone();
        let file_id = namespaced_file_id(namespace, &case.file_id);
        let expected_bytes = case.expected_bytes.clone();
        tasks.spawn(async move {
            let response = backend
                .upload_file(&file_id, expected_bytes.clone(), None)
                .await?;
            Ok::<_, ServerError>((file_id, expected_bytes, response))
        });
    }

    let mut uploaded_bytes = 0_u64;
    let mut newly_stored_bytes = 0_u64;
    let mut inserted_chunks = 0_u64;
    let mut reused_chunks = 0_u64;
    let mut completed = Vec::with_capacity(cases.len());
    while let Some(joined) = tasks.join_next().await {
        let (file_id, expected_bytes, response) = joined
            .map_err(BenchRuntimeError::from)?
            .map_err(BenchRuntimeError::from)?;
        uploaded_bytes = checked_add_u64(uploaded_bytes, response.total_bytes)?;
        newly_stored_bytes = checked_add_u64(newly_stored_bytes, response.stored_bytes)?;
        inserted_chunks = checked_add_u64(inserted_chunks, response.inserted_chunks)?;
        reused_chunks = checked_add_u64(reused_chunks, response.reused_chunks)?;
        completed.push((file_id, expected_bytes));
    }
    let upload_micros = duration_micros(started.elapsed())?;

    for (file_id, expected_bytes) in completed {
        let bytes = backend.download_file(&file_id, None, None).await?;
        if bytes.as_slice() != expected_bytes.as_ref() {
            return Err(BenchRuntimeError::ConcurrentUploadVerificationMismatch);
        }
    }

    Ok((
        upload_micros,
        uploaded_bytes,
        newly_stored_bytes,
        inserted_chunks,
        reused_chunks,
    ))
}

async fn run_concurrent_ingest_uploads(
    chunk_size: NonZeroUsize,
    upload_max_in_flight_chunks: NonZeroUsize,
    cases: &[ConcurrentIngestUploadCase],
    available_parallelism: u64,
) -> Result<TimedConcurrentIngestUpload, BenchRuntimeError> {
    let started = Instant::now();
    let process_cpu_started = capture_process_cpu_micros();
    let mut tasks = JoinSet::new();
    for case in cases {
        let file_id = case.file_id.clone();
        let body = case.body.clone();
        tasks.spawn(async move {
            ingest_without_storage_with_parallelism(
                chunk_size,
                upload_max_in_flight_chunks,
                &file_id,
                body,
                None,
            )
            .await
        });
    }

    let mut uploaded_bytes = 0_u64;
    let mut inserted_chunks = 0_u64;
    while let Some(joined) = tasks.join_next().await {
        let response = joined
            .map_err(BenchRuntimeError::from)?
            .map_err(BenchRuntimeError::from)?;
        uploaded_bytes = checked_add_u64(uploaded_bytes, response.total_bytes)?;
        inserted_chunks = checked_add_u64(inserted_chunks, response.inserted_chunks)?;
    }
    let elapsed_micros = duration_micros(started.elapsed())?;
    let process_cpu_micros = capture_process_cpu_micros().saturating_sub(process_cpu_started);

    Ok(TimedConcurrentIngestUpload {
        elapsed_micros,
        uploaded_bytes,
        inserted_chunks,
        process_cpu_micros,
        process_cpu_cores_per_mille: ratio_per_mille(process_cpu_micros, elapsed_micros),
        process_host_utilization_per_mille: host_utilization_per_mille(
            process_cpu_micros,
            elapsed_micros,
            available_parallelism,
        ),
    })
}

fn build_concurrent_upload_cases(
    base: &[u8],
    mutated_bytes: usize,
    chunk_size: usize,
    concurrency: u32,
) -> Result<Vec<ConcurrentUploadCase>, BenchRuntimeError> {
    let chunk_count = base.len().div_ceil(chunk_size);
    let mut cases = Vec::with_capacity(usize::try_from(concurrency)?);
    for worker_index in 0..concurrency {
        let worker_index_usize = usize::try_from(worker_index)?;
        let selected_chunk = worker_index_usize
            .checked_rem(chunk_count)
            .ok_or(BenchRuntimeError::ConcurrentUploadChunkSelectionFailed)?;
        let chunk_start = selected_chunk
            .checked_mul(chunk_size)
            .ok_or(BenchRuntimeError::ChunkStartOverflow)?;
        let chunk_end = chunk_start
            .checked_add(chunk_size)
            .map(|value| value.min(base.len()))
            .ok_or(BenchRuntimeError::ChunkEndOverflow)?;
        let window_length = mutated_bytes.min(
            chunk_end
                .checked_sub(chunk_start)
                .ok_or(BenchRuntimeError::ChunkWindowUnderflow)?,
        );
        let expected_bytes = build_worker_update(base, chunk_start, window_length, worker_index)?;
        cases.push(ConcurrentUploadCase {
            file_id: format!("concurrent-{worker_index:04}.bin"),
            expected_bytes,
        });
    }

    Ok(cases)
}

fn build_concurrent_ingest_upload_cases(
    base: &[u8],
    mutated_bytes: usize,
    chunk_size: usize,
    concurrency: u32,
) -> Result<Vec<ConcurrentIngestUploadCase>, BenchRuntimeError> {
    let upload_cases = build_concurrent_upload_cases(base, mutated_bytes, chunk_size, concurrency)?;
    let cases = upload_cases
        .into_iter()
        .map(|case| ConcurrentIngestUploadCase {
            file_id: case.file_id,
            body: case.expected_bytes,
        })
        .collect();
    Ok(cases)
}

fn build_worker_update(
    base: &[u8],
    start: usize,
    mutated_bytes: usize,
    worker_index: u32,
) -> Result<Bytes, BenchRuntimeError> {
    let mut updated = base.to_vec();
    let end = start
        .checked_add(mutated_bytes)
        .ok_or_else(|| BenchRuntimeError::WorkerMutationWindowOverflow)?;
    let window = updated
        .get_mut(start..end)
        .ok_or_else(|| BenchRuntimeError::WorkerMutationWindowOutOfBounds)?;
    let worker_seed = usize::try_from(worker_index)?;
    for (offset, byte) in window.iter_mut().enumerate() {
        let delta_source = worker_seed
            .checked_mul(17)
            .and_then(|value| value.checked_add(offset.saturating_mul(13)))
            .and_then(|value| value.checked_add(31))
            .ok_or(BenchRuntimeError::WorkerDeltaOverflow)?;
        let delta = u8::try_from(delta_source % 251)?;
        *byte = byte.wrapping_add(delta).wrapping_add(1);
    }

    Ok(Bytes::from(updated))
}

fn build_base_asset(length: usize) -> Result<Vec<u8>, BenchRuntimeError> {
    let mut bytes = Vec::with_capacity(length);
    for index in 0..length {
        let value = u8::try_from((index.saturating_mul(31).saturating_add(17)) % 251)?;
        bytes.push(value);
    }

    Ok(bytes)
}

fn build_sparse_update(base: &[u8], mutated_bytes: usize) -> Result<Vec<u8>, BenchRuntimeError> {
    let mut updated = base.to_vec();
    let remaining = base
        .len()
        .checked_sub(mutated_bytes)
        .ok_or(BenchRuntimeError::MutatedBytesExceedBaseBytes)?;
    let start = remaining
        .checked_div(2)
        .ok_or(BenchRuntimeError::BenchmarkDivisorZero)?;
    let end = start
        .checked_add(mutated_bytes)
        .ok_or(BenchRuntimeError::MutationWindowOverflow)?;
    let window = updated
        .get_mut(start..end)
        .ok_or(BenchRuntimeError::MutationWindowOutOfBounds)?;
    for (offset, byte) in window.iter_mut().enumerate() {
        let delta = u8::try_from((offset.saturating_mul(13).saturating_add(29)) % 251)?;
        *byte = byte.wrapping_add(delta).wrapping_add(1);
    }

    Ok(updated)
}

fn build_mutation_range(
    base_bytes: usize,
    mutated_bytes: usize,
) -> Result<ByteRange, BenchRuntimeError> {
    let remaining = base_bytes
        .checked_sub(mutated_bytes)
        .ok_or(BenchRuntimeError::MutatedBytesExceedBaseBytes)?;
    let start = remaining
        .checked_div(2)
        .ok_or(BenchRuntimeError::BenchmarkDivisorZero)?;
    let end = start
        .checked_add(mutated_bytes)
        .and_then(|value| value.checked_sub(1))
        .ok_or(BenchRuntimeError::MutationRangeOverflow)?;
    let start = u64::try_from(start)?;
    let end = u64::try_from(end)?;

    ByteRange::new(start, end).map_err(BenchRuntimeError::MutationRangeInvalid)
}

fn build_cross_repository_assets(
    chunk_size: usize,
) -> Result<(Vec<u8>, Vec<u8>), BenchRuntimeError> {
    let capacity = chunk_size
        .checked_mul(3)
        .ok_or_else(|| BenchRuntimeError::CrossRepositoryAssetOverflow)?;
    let mut base = Vec::with_capacity(capacity);
    base.extend(repeat_n(0x11, chunk_size));
    base.extend(repeat_n(0x22, chunk_size));
    base.extend(repeat_n(0x33, chunk_size));

    let mut updated = base.clone();
    let middle_start = chunk_size;
    let middle_end = chunk_size
        .checked_mul(2)
        .ok_or_else(|| BenchRuntimeError::CrossRepositoryAssetOverflow)?;
    let middle = updated
        .get_mut(middle_start..middle_end)
        .ok_or_else(|| BenchRuntimeError::CrossRepositoryMiddleChunkOutOfBounds)?;
    middle.fill(0x44);

    Ok((base, updated))
}

fn duration_micros(duration: Duration) -> Result<u64, BenchRuntimeError> {
    u64::try_from(duration.as_micros()).map_err(BenchRuntimeError::from)
}

fn checked_add_u64(left: u64, right: u64) -> Result<u64, BenchRuntimeError> {
    left.checked_add(right)
        .ok_or(BenchRuntimeError::BenchmarkCounterU64Overflow)
}

fn checked_add_u32(left: u32, right: u32) -> Result<u32, BenchRuntimeError> {
    left.checked_add(right)
        .ok_or(BenchRuntimeError::BenchmarkCounterU32Overflow)
}

fn checked_average_u64(total: u64, count: u64) -> Result<u64, BenchRuntimeError> {
    total
        .checked_div(count)
        .ok_or(BenchRuntimeError::BenchmarkDivisorZero)
}

fn checked_mul_u64(left: u64, right: u64) -> Result<u64, BenchRuntimeError> {
    left.checked_mul(right)
        .ok_or(BenchRuntimeError::BenchmarkCounterU64Overflow)
}

const fn measured_iteration_count(total_micros: u64, iterations: u64) -> u64 {
    if total_micros == 0 { 0 } else { iterations }
}

fn throughput_bytes_per_second(bytes: u64, micros: u64) -> u64 {
    if bytes == 0 || micros == 0 {
        return 0;
    }

    bytes
        .saturating_mul(1_000_000)
        .checked_div(micros)
        .unwrap_or(u64::MAX)
}

const SCHEDSTAT_PATH: &str = "/proc/self/schedstat";
const TASK_SCHEDSTAT_DIR_PATH: &str = "/proc/self/task";

fn available_parallelism_u64() -> u64 {
    thread::available_parallelism()
        .map(usize::from)
        .ok()
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(1)
}

fn capture_process_cpu_micros() -> u64 {
    let Ok(entries) = std_fs::read_dir(TASK_SCHEDSTAT_DIR_PATH) else {
        return read_schedstat_runtime_micros(Path::new(SCHEDSTAT_PATH));
    };

    let mut total_runtime_micros = 0_u64;
    for entry in entries {
        let Ok(entry) = entry else {
            continue;
        };
        let runtime_micros = read_schedstat_runtime_micros(&entry.path().join("schedstat"));
        total_runtime_micros = total_runtime_micros.saturating_add(runtime_micros);
    }

    if total_runtime_micros == 0 {
        read_schedstat_runtime_micros(Path::new(SCHEDSTAT_PATH))
    } else {
        total_runtime_micros
    }
}

fn read_schedstat_runtime_micros(path: &Path) -> u64 {
    let Ok(schedstat) = std_fs::read_to_string(path) else {
        return 0;
    };
    let Some(runtime_nanos) = schedstat.split_ascii_whitespace().next() else {
        return 0;
    };
    let Ok(runtime_nanos) = runtime_nanos.parse::<u64>() else {
        return 0;
    };

    runtime_nanos / 1_000
}

fn ratio_per_mille(numerator: u64, denominator: u64) -> u64 {
    if numerator == 0 || denominator == 0 {
        return 0;
    }

    let scaled = u128::from(numerator)
        .checked_mul(1_000)
        .and_then(|value| value.checked_div(u128::from(denominator)))
        .unwrap_or_else(|| u128::from(u64::MAX));
    u64::try_from(scaled).unwrap_or(u64::MAX)
}

fn host_utilization_per_mille(
    cpu_micros: u64,
    wall_micros: u64,
    available_parallelism: u64,
) -> u64 {
    if cpu_micros == 0 || wall_micros == 0 || available_parallelism == 0 {
        return 0;
    }

    let denominator = u128::from(wall_micros)
        .checked_mul(u128::from(available_parallelism))
        .unwrap_or_else(|| u128::from(u64::MAX));
    if denominator == 0 {
        return 0;
    }

    let scaled = u128::from(cpu_micros)
        .checked_mul(1_000)
        .and_then(|value| value.checked_div(denominator))
        .unwrap_or_else(|| u128::from(u64::MAX));
    u64::try_from(scaled).unwrap_or(u64::MAX)
}

fn scaling_per_mille(aggregate_throughput: u64, single_throughput: u64, concurrency: u32) -> u64 {
    if aggregate_throughput == 0 || single_throughput == 0 || concurrency == 0 {
        return 0;
    }

    aggregate_throughput
        .saturating_mul(1_000)
        .checked_div(single_throughput)
        .and_then(|value| value.checked_div(u64::from(concurrency)))
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{
        BenchConfig, BenchDeploymentTarget, BenchInventoryScope, BenchRuntimeError, BenchScenario,
        DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS, available_parallelism_u64, build_base_asset,
        build_concurrent_upload_cases, build_sparse_update, host_utilization_per_mille,
        ratio_per_mille, run_bench, run_ingest_bench,
    };

    #[test]
    fn sparse_update_changes_only_requested_window() {
        let base = build_base_asset(128);
        assert!(base.is_ok());
        let Ok(base) = base else {
            return;
        };

        let updated = build_sparse_update(&base, 16);
        assert!(updated.is_ok());
        let Ok(updated) = updated else {
            return;
        };

        let changed = base
            .iter()
            .zip(&updated)
            .filter(|(left, right)| left != right)
            .count();
        assert_eq!(changed, 16);
    }

    #[test]
    fn concurrent_upload_cases_mutate_deterministic_chunk_windows() {
        let base = build_base_asset(12);
        assert!(base.is_ok());
        let Ok(base) = base else {
            return;
        };

        let cases = build_concurrent_upload_cases(&base, 4, 4, 3);
        assert!(cases.is_ok());
        let Ok(cases) = cases else {
            return;
        };

        assert_eq!(cases.len(), 3);
        let first = cases.first().map(|case| &case.expected_bytes);
        let second = cases.get(1).map(|case| &case.expected_bytes);
        let third = cases.get(2).map(|case| &case.expected_bytes);
        assert!(first.is_some());
        assert!(second.is_some());
        assert!(third.is_some());
        let Some(first) = first else {
            return;
        };
        let Some(second) = second else {
            return;
        };
        let Some(third) = third else {
            return;
        };
        assert_ne!(first, &base);
        assert_ne!(second, &base);
        assert_ne!(third, &base);
        assert_ne!(first, second);
    }

    #[test]
    fn ratio_helpers_report_expected_cpu_usage() {
        assert_eq!(ratio_per_mille(0, 10), 0);
        assert_eq!(ratio_per_mille(500, 1_000), 500);
        assert_eq!(ratio_per_mille(2_000, 1_000), 2_000);
        assert_eq!(host_utilization_per_mille(2_000, 1_000, 4), 500);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bench_reports_sparse_update_and_concurrent_metrics() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };

        let report = run_bench(
            storage.path(),
            BenchConfig {
                deployment_target: BenchDeploymentTarget::IsolatedLocal,
                scenario: BenchScenario::Full,
                iterations: 1,
                concurrency: 2,
                upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
                chunk_size_bytes: 4,
                base_bytes: 12,
                mutated_bytes: 4,
            },
        )
        .await;
        assert!(report.is_ok());
        let Ok(report) = report else {
            return;
        };

        assert_eq!(report.iterations, 1);
        assert_eq!(
            report.deployment_target,
            BenchDeploymentTarget::IsolatedLocal
        );
        assert_eq!(report.metadata_backend, "local");
        assert_eq!(report.object_backend, "local");
        assert_eq!(report.inventory_scope, BenchInventoryScope::Isolated);
        assert_eq!(report.concurrency, 2);
        assert_eq!(report.available_parallelism, available_parallelism_u64());
        assert_eq!(report.iterations_detail.len(), 1);
        let iteration = report.iterations_detail.first();
        assert!(iteration.is_some());
        let Some(iteration) = iteration else {
            return;
        };
        assert_eq!(iteration.initial_inserted_chunks, 3);
        assert_eq!(iteration.sparse_update_inserted_chunks, 1);
        assert_eq!(iteration.sparse_update_reused_chunks, 2);
        assert_eq!(iteration.concurrent_upload_inserted_chunks, 2);
        assert_eq!(iteration.concurrent_upload_reused_chunks, 4);
        assert_eq!(iteration.concurrent_newly_stored_bytes, 8);
        assert_eq!(iteration.concurrent_uploaded_bytes, 24);
        assert_eq!(iteration.concurrent_downloaded_bytes, 24);
        assert_eq!(iteration.cross_repository_inserted_chunks, 1);
        assert_eq!(iteration.cross_repository_reused_chunks, 2);
        assert_eq!(iteration.cross_repository_newly_stored_bytes, 4);
        assert_eq!(iteration.newly_stored_bytes, 40);
        assert_eq!(report.total_sparse_update_reused_chunks, 2);
        assert_eq!(report.total_concurrent_upload_inserted_chunks, 2);
        assert_eq!(report.total_concurrent_upload_reused_chunks, 4);
        assert_eq!(report.total_concurrent_newly_stored_bytes, 8);
        assert_eq!(report.total_cross_repository_inserted_chunks, 1);
        assert_eq!(report.total_cross_repository_reused_chunks, 2);
        assert_eq!(report.total_cross_repository_newly_stored_bytes, 4);
        assert!(
            iteration.process_cpu_cores_per_mille >= iteration.process_host_utilization_per_mille
        );
        assert!(
            report.average_process_cpu_cores_per_mille
                >= report.average_process_host_utilization_per_mille
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bench_reuses_requested_storage_root_by_allocating_new_run_directories() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };

        let first = run_bench(
            storage.path(),
            BenchConfig {
                deployment_target: BenchDeploymentTarget::IsolatedLocal,
                scenario: BenchScenario::Full,
                iterations: 1,
                concurrency: 1,
                upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
                chunk_size_bytes: 4,
                base_bytes: 12,
                mutated_bytes: 4,
            },
        )
        .await;
        assert!(first.is_ok());
        let Ok(first) = first else {
            return;
        };
        let second = run_bench(
            storage.path(),
            BenchConfig {
                deployment_target: BenchDeploymentTarget::IsolatedLocal,
                scenario: BenchScenario::Full,
                iterations: 1,
                concurrency: 1,
                upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
                chunk_size_bytes: 4,
                base_bytes: 12,
                mutated_bytes: 4,
            },
        )
        .await;
        assert!(second.is_ok());
        let Ok(second) = second else {
            return;
        };

        assert_ne!(first.storage_dir, second.storage_dir);
        assert!(first.storage_dir.starts_with(storage.path()));
        assert!(second.storage_dir.starts_with(storage.path()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bench_rejects_mutation_window_larger_than_asset() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };

        let report = run_bench(
            storage.path(),
            BenchConfig {
                deployment_target: BenchDeploymentTarget::IsolatedLocal,
                scenario: BenchScenario::Full,
                iterations: 1,
                concurrency: 1,
                upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
                chunk_size_bytes: 4,
                base_bytes: 8,
                mutated_bytes: 16,
            },
        )
        .await;
        assert!(matches!(
            report,
            Err(BenchRuntimeError::MutatedBytesExceedBaseBytes)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bench_rejects_zero_concurrency() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };

        let report = run_bench(
            storage.path(),
            BenchConfig {
                deployment_target: BenchDeploymentTarget::IsolatedLocal,
                scenario: BenchScenario::Full,
                iterations: 1,
                concurrency: 0,
                upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
                chunk_size_bytes: 4,
                base_bytes: 8,
                mutated_bytes: 4,
            },
        )
        .await;
        assert!(matches!(report, Err(BenchRuntimeError::ZeroConcurrency)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_bench_reports_upload_metrics() {
        let report = run_ingest_bench(BenchConfig {
            deployment_target: BenchDeploymentTarget::IsolatedLocal,
            scenario: BenchScenario::Full,
            iterations: 1,
            concurrency: 2,
            upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
            chunk_size_bytes: 4,
            base_bytes: 12,
            mutated_bytes: 4,
        })
        .await;
        assert!(report.is_ok());
        let Ok(report) = report else {
            return;
        };

        assert_eq!(report.iterations, 1);
        assert_eq!(report.concurrency, 2);
        assert_eq!(report.available_parallelism, available_parallelism_u64());
        assert_eq!(report.total_initial_inserted_chunks, 3);
        assert_eq!(report.total_sparse_update_inserted_chunks, 3);
        assert_eq!(report.total_concurrent_upload_inserted_chunks, 6);
        assert_eq!(report.total_concurrent_uploaded_bytes, 24);
        assert!(
            report.average_process_cpu_cores_per_mille
                >= report.average_process_host_utilization_per_mille
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bench_can_focus_on_cross_repository_upload() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };

        let report = run_bench(
            storage.path(),
            BenchConfig {
                deployment_target: BenchDeploymentTarget::IsolatedLocal,
                scenario: BenchScenario::CrossRepositoryUpload,
                iterations: 1,
                concurrency: 2,
                upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
                chunk_size_bytes: 4,
                base_bytes: 12,
                mutated_bytes: 4,
            },
        )
        .await;
        assert!(report.is_ok());
        let Ok(report) = report else {
            return;
        };

        assert_eq!(report.scenario, BenchScenario::CrossRepositoryUpload);
        assert_eq!(report.average_initial_upload_micros, 0);
        assert_eq!(report.average_sparse_update_upload_micros, 0);
        assert_eq!(report.average_latest_download_micros, 0);
        assert_eq!(report.average_previous_download_micros, 0);
        assert_eq!(report.average_concurrent_upload_micros, 0);
        assert_eq!(report.total_uploaded_bytes, 12);
        assert_eq!(report.total_cross_repository_inserted_chunks, 1);
        assert_eq!(report.total_cross_repository_reused_chunks, 2);
        assert_eq!(report.total_cross_repository_newly_stored_bytes, 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_bench_rejects_unsupported_download_focus() {
        let report = run_ingest_bench(BenchConfig {
            deployment_target: BenchDeploymentTarget::IsolatedLocal,
            scenario: BenchScenario::LatestDownload,
            iterations: 1,
            concurrency: 2,
            upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
            chunk_size_bytes: 4,
            base_bytes: 12,
            mutated_bytes: 4,
        })
        .await;
        assert!(matches!(
            report,
            Err(BenchRuntimeError::UnsupportedScenarioForMode)
        ));
    }
}
