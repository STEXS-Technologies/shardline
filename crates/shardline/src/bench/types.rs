use std::{
    io::Error as IoError,
    num::{NonZeroUsize, TryFromIntError},
    path::PathBuf,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use shardline_protocol::{ByteRange, RangeError, TokenClaimsError};
use shardline_server::{BenchmarkBackend, ServerConfig, ServerConfigError, ServerError};
use thiserror::Error;

#[cfg(test)]
pub(crate) const DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS: usize = 64;

/// Latency measurements for a single benchmark iteration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatencyMetrics {
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
}

/// Byte-count measurements for a single benchmark iteration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ByteMetrics {
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
    /// New bytes written during the cross-repository upload.
    pub cross_repository_newly_stored_bytes: u64,
}

/// Chunk-count measurements for a single benchmark iteration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkMetrics {
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
}

/// Process timing measurements for a single benchmark iteration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimingMetrics {
    /// Total process CPU time consumed while executing this iteration workload.
    pub process_cpu_micros: u64,
    /// Average CPU cores consumed during this iteration, in per-mille cores.
    pub process_cpu_cores_per_mille: u64,
    /// Fraction of host CPU capacity consumed during this iteration, in per-mille.
    pub process_host_utilization_per_mille: u64,
}

/// Inventory snapshot at the end of a single benchmark iteration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InventoryMetrics {
    /// Chunk object count after the iteration completes.
    pub chunk_objects: u64,
    /// Chunk object bytes after the iteration completes.
    pub chunk_bytes: u64,
    /// Visible file-record count after the iteration completes.
    pub visible_files: u64,
}

/// One benchmark iteration report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchIterationReport {
    /// Iteration number starting at one.
    pub iteration: u32,
    /// Storage root used for this isolated iteration.
    pub storage_dir: PathBuf,
    /// Latency measurements for this iteration.
    pub latency: LatencyMetrics,
    /// Byte-count measurements for this iteration.
    pub bytes: ByteMetrics,
    /// Chunk-count measurements for this iteration.
    pub chunks: ChunkMetrics,
    /// Process timing measurements for this iteration.
    pub timing: TimingMetrics,
    /// Inventory snapshot at the end of this iteration.
    pub inventory: InventoryMetrics,
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
    /// Average latency measurements across iterations.
    pub latency: LatencyMetrics,
    /// Average throughput measurements across iterations.
    pub throughput: BenchThroughputMetrics,
    /// Average process timing measurements across iterations.
    pub timing: TimingMetrics,
    /// Inventory totals across all iterations.
    pub totals: BenchTotals,
    /// Per-iteration detail.
    pub iterations_detail: Vec<BenchIterationReport>,
}

/// Average throughput measurements in the aggregate report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchThroughputMetrics {
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
}

/// Totals across all iterations in the aggregate report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchTotals {
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
}

impl BenchReport {
    pub fn print_summary(&self) {
        println!("mode: e2e");
        println!("deployment_target: {}", self.deployment_target.as_str());
        println!("metadata_backend: {}", self.metadata_backend);
        println!("object_backend: {}", self.object_backend);
        println!("inventory_scope: {}", self.inventory_scope.as_str());
        println!("scenario: {}", self.scenario.as_str());
        if self.scenario == BenchScenario::Full {
            println!("scenario: sparse-update");
            println!("scenario: concurrent-latest-download");
            println!("scenario: concurrent-upload");
            println!("scenario: cross-repository-upload");
            println!("scenario: cached-latest-reconstruction");
        }
        println!("storage_dir: {}", self.storage_dir.display());
        println!("iterations: {}", self.iterations);
        println!("concurrency: {}", self.concurrency);
        println!(
            "upload_max_in_flight_chunks: {}",
            self.upload_max_in_flight_chunks
        );
        println!("chunk_size_bytes: {}", self.chunk_size_bytes);
        println!("base_bytes: {}", self.base_bytes);
        println!("mutated_bytes: {}", self.mutated_bytes);
        println!("available_parallelism: {}", self.available_parallelism);
        println!(
            "average_initial_upload_micros: {}",
            self.latency.initial_upload_micros
        );
        println!(
            "average_sparse_update_upload_micros: {}",
            self.latency.sparse_update_upload_micros
        );
        println!(
            "average_latest_download_micros: {}",
            self.latency.latest_download_micros
        );
        println!(
            "average_previous_download_micros: {}",
            self.latency.previous_download_micros
        );
        println!(
            "average_ranged_reconstruction_micros: {}",
            self.latency.ranged_reconstruction_micros
        );
        println!(
            "average_concurrent_latest_download_micros: {}",
            self.latency.concurrent_latest_download_micros
        );
        println!(
            "average_concurrent_upload_micros: {}",
            self.latency.concurrent_upload_micros
        );
        println!(
            "average_cross_repository_upload_micros: {}",
            self.latency.cross_repository_upload_micros
        );
        println!(
            "average_cached_latest_reconstruction_cold_micros: {}",
            self.latency.cached_latest_reconstruction_cold_micros
        );
        println!(
            "average_cached_latest_reconstruction_hot_micros: {}",
            self.latency.cached_latest_reconstruction_hot_micros
        );
        println!(
            "average_process_cpu_micros: {}",
            self.timing.process_cpu_micros
        );
        println!(
            "average_process_cpu_cores_per_mille: {}",
            self.timing.process_cpu_cores_per_mille
        );
        println!(
            "average_process_host_utilization_per_mille: {}",
            self.timing.process_host_utilization_per_mille
        );
        println!(
            "average_initial_upload_bytes_per_second: {}",
            self.throughput.average_initial_upload_bytes_per_second
        );
        println!(
            "average_sparse_update_upload_bytes_per_second: {}",
            self.throughput
                .average_sparse_update_upload_bytes_per_second
        );
        println!(
            "average_latest_download_bytes_per_second: {}",
            self.throughput.average_latest_download_bytes_per_second
        );
        println!(
            "average_previous_download_bytes_per_second: {}",
            self.throughput.average_previous_download_bytes_per_second
        );
        println!(
            "average_concurrent_latest_download_bytes_per_second: {}",
            self.throughput
                .average_concurrent_latest_download_bytes_per_second
        );
        println!(
            "average_concurrent_upload_bytes_per_second: {}",
            self.throughput.average_concurrent_upload_bytes_per_second
        );
        println!(
            "average_cross_repository_upload_bytes_per_second: {}",
            self.throughput
                .average_cross_repository_upload_bytes_per_second
        );
        println!(
            "average_cached_latest_reconstruction_hit_bytes_per_second: {}",
            self.throughput
                .average_cached_latest_reconstruction_hit_bytes_per_second
        );
        println!(
            "concurrent_latest_download_scaling_per_mille: {}",
            self.totals.concurrent_latest_download_scaling_per_mille
        );
        println!(
            "concurrent_upload_scaling_per_mille: {}",
            self.totals.concurrent_upload_scaling_per_mille
        );
        println!("total_uploaded_bytes: {}", self.totals.total_uploaded_bytes);
        println!(
            "total_downloaded_bytes: {}",
            self.totals.total_downloaded_bytes
        );
        println!(
            "total_cached_reconstruction_response_bytes: {}",
            self.totals.total_cached_reconstruction_response_bytes
        );
        println!("cache_hit_iterations: {}", self.totals.cache_hit_iterations);
        println!(
            "total_concurrent_downloaded_bytes: {}",
            self.totals.total_concurrent_downloaded_bytes
        );
        println!(
            "total_concurrent_uploaded_bytes: {}",
            self.totals.total_concurrent_uploaded_bytes
        );
        println!(
            "total_newly_stored_bytes: {}",
            self.totals.total_newly_stored_bytes
        );
        println!(
            "total_concurrent_newly_stored_bytes: {}",
            self.totals.total_concurrent_newly_stored_bytes
        );
        println!(
            "total_cross_repository_newly_stored_bytes: {}",
            self.totals.total_cross_repository_newly_stored_bytes
        );
        println!(
            "total_initial_inserted_chunks: {}",
            self.totals.total_initial_inserted_chunks
        );
        println!(
            "total_sparse_update_inserted_chunks: {}",
            self.totals.total_sparse_update_inserted_chunks
        );
        println!(
            "total_sparse_update_reused_chunks: {}",
            self.totals.total_sparse_update_reused_chunks
        );
        println!(
            "total_concurrent_upload_inserted_chunks: {}",
            self.totals.total_concurrent_upload_inserted_chunks
        );
        println!(
            "total_concurrent_upload_reused_chunks: {}",
            self.totals.total_concurrent_upload_reused_chunks
        );
        println!(
            "total_cross_repository_inserted_chunks: {}",
            self.totals.total_cross_repository_inserted_chunks
        );
        println!(
            "total_cross_repository_reused_chunks: {}",
            self.totals.total_cross_repository_reused_chunks
        );
        if let Some(last) = self.iterations_detail.last() {
            println!(
                "last_iteration_chunk_objects: {}",
                last.inventory.chunk_objects
            );
            println!("last_iteration_chunk_bytes: {}", last.inventory.chunk_bytes);
            println!(
                "last_iteration_visible_files: {}",
                last.inventory.visible_files
            );
        }
    }
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

impl IngestBenchReport {
    pub fn print_summary(&self) {
        println!("mode: ingest");
        println!("scenario: {}", self.scenario.as_str());
        if self.scenario == BenchScenario::Full {
            println!("scenario: sparse-update");
            println!("scenario: concurrent-upload");
        }
        println!("iterations: {}", self.iterations);
        println!("concurrency: {}", self.concurrency);
        println!(
            "upload_max_in_flight_chunks: {}",
            self.upload_max_in_flight_chunks
        );
        println!("chunk_size_bytes: {}", self.chunk_size_bytes);
        println!("base_bytes: {}", self.base_bytes);
        println!("mutated_bytes: {}", self.mutated_bytes);
        println!("available_parallelism: {}", self.available_parallelism);
        println!(
            "average_initial_upload_micros: {}",
            self.average_initial_upload_micros
        );
        println!(
            "average_sparse_update_upload_micros: {}",
            self.average_sparse_update_upload_micros
        );
        println!(
            "average_concurrent_upload_micros: {}",
            self.average_concurrent_upload_micros
        );
        println!(
            "average_initial_upload_bytes_per_second: {}",
            self.average_initial_upload_bytes_per_second
        );
        println!(
            "average_sparse_update_upload_bytes_per_second: {}",
            self.average_sparse_update_upload_bytes_per_second
        );
        println!(
            "average_concurrent_upload_bytes_per_second: {}",
            self.average_concurrent_upload_bytes_per_second
        );
        println!(
            "average_concurrent_upload_process_cpu_micros: {}",
            self.average_concurrent_upload_process_cpu_micros
        );
        println!(
            "average_concurrent_upload_process_cpu_cores_per_mille: {}",
            self.average_concurrent_upload_process_cpu_cores_per_mille
        );
        println!(
            "average_concurrent_upload_process_host_utilization_per_mille: {}",
            self.average_concurrent_upload_process_host_utilization_per_mille
        );
        println!(
            "average_process_cpu_micros: {}",
            self.average_process_cpu_micros
        );
        println!(
            "average_process_cpu_cores_per_mille: {}",
            self.average_process_cpu_cores_per_mille
        );
        println!(
            "average_process_host_utilization_per_mille: {}",
            self.average_process_host_utilization_per_mille
        );
        println!(
            "concurrent_upload_scaling_per_mille: {}",
            self.concurrent_upload_scaling_per_mille
        );
        println!("total_uploaded_bytes: {}", self.total_uploaded_bytes);
        println!(
            "total_concurrent_uploaded_bytes: {}",
            self.total_concurrent_uploaded_bytes
        );
        println!(
            "total_initial_inserted_chunks: {}",
            self.total_initial_inserted_chunks
        );
        println!(
            "total_sparse_update_inserted_chunks: {}",
            self.total_sparse_update_inserted_chunks
        );
        println!(
            "total_concurrent_upload_inserted_chunks: {}",
            self.total_concurrent_upload_inserted_chunks
        );
    }
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
    pub(crate) const fn supports_ingest(self) -> bool {
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
pub(crate) struct ConcurrentUploadCase {
    pub(crate) file_id: String,
    pub(crate) expected_bytes: Bytes,
}

#[derive(Debug, Clone)]
pub(crate) struct BenchFixture<'asset> {
    pub(crate) chunk_size: NonZeroUsize,
    pub(crate) upload_max_in_flight_chunks: NonZeroUsize,
    pub(crate) concurrency: u32,
    pub(crate) base: Bytes,
    pub(crate) updated: Bytes,
    pub(crate) ranged_reconstruction: ByteRange,
    pub(crate) concurrent_upload_cases: &'asset [ConcurrentUploadCase],
    pub(crate) cross_repository_base: Bytes,
    pub(crate) cross_repository_updated: Bytes,
}

#[derive(Debug, Clone)]
pub(crate) struct IngestBenchScenario<'asset> {
    pub(crate) chunk_size: NonZeroUsize,
    pub(crate) upload_max_in_flight_chunks: NonZeroUsize,
    pub(crate) concurrent_upload_cases: &'asset [ConcurrentIngestUploadCase],
    pub(crate) base: Bytes,
    pub(crate) updated: Bytes,
}

#[derive(Debug, Clone)]
pub(crate) struct ConcurrentIngestUploadCase {
    pub(crate) file_id: String,
    pub(crate) body: Bytes,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct TimedConcurrentIngestUpload {
    pub(crate) elapsed_micros: u64,
    pub(crate) uploaded_bytes: u64,
    pub(crate) inserted_chunks: u64,
    pub(crate) process_cpu_micros: u64,
    pub(crate) process_cpu_cores_per_mille: u64,
    pub(crate) process_host_utilization_per_mille: u64,
}

#[derive(Debug, Clone)]
pub(crate) enum BenchBackendSetup {
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
    BenchmarkTaskJoin(#[from] tokio::task::JoinError),
}

impl BenchBackendSetup {
    pub(crate) async fn create_backend(
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
