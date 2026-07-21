mod concurrent;
mod e2e;
mod ingest;
mod runner;
mod sparse;
mod types;

#[cfg(test)]
mod tests;

// Public re-exports for the crate's external API (used by lib.rs).
pub use runner::{run_bench, run_ingest_bench};
pub use types::{
    BenchConfig, BenchDeploymentTarget, BenchInventoryScope, BenchIterationReport, BenchReport,
    BenchRuntimeError, BenchScenario, ByteMetrics, ChunkMetrics, IngestBenchIterationReport,
    IngestBenchReport, InventoryMetrics, LatencyMetrics, TimingMetrics,
};

// Crate-internal re-exports for sibling submodules.
#[cfg(test)]
pub(crate) use types::DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS;
pub(crate) use types::{
    BenchBackendSetup, BenchFixture, ConcurrentIngestUploadCase, ConcurrentUploadCase,
    IngestBenchScenario, TimedConcurrentIngestUpload,
};

// Re-exports used by sibling submodules (e2e, ingest, concurrent).
pub(crate) use runner::{
    available_parallelism_u64, build_iteration_repository_scopes, capture_process_cpu_micros,
    checked_add_u64, duration_micros, host_utilization_per_mille, iteration_namespace,
    namespaced_file_id, ratio_per_mille,
};
// Re-exports used only in test code.
#[cfg(test)]
pub(crate) use runner::{
    checked_add_u32, checked_average_u64, checked_mul_u64, inventory_scope,
    measured_iteration_count, scaling_per_mille, throughput_bytes_per_second,
};
#[cfg(test)]
pub(crate) use sparse::{build_base_asset, build_concurrent_upload_cases, build_sparse_update};
