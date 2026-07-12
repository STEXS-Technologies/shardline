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

#[allow(unused_imports)]
pub(crate) use runner::{
    allocate_bench_run_root, available_parallelism_u64, build_iteration_repository_scopes,
    capture_process_cpu_micros, checked_add_u32, checked_add_u64, checked_average_u64,
    checked_mul_u64, duration_micros, host_utilization_per_mille, inventory_scope,
    iteration_namespace, measured_iteration_count, namespaced_file_id, ratio_per_mille,
    scaling_per_mille, throughput_bytes_per_second,
};
#[allow(unused_imports)]
pub(crate) use sparse::{
    build_base_asset, build_concurrent_ingest_upload_cases, build_concurrent_upload_cases,
    build_cross_repository_assets, build_mutation_range, build_sparse_update,
};
