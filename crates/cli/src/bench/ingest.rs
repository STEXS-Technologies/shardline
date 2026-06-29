use std::time::Instant;

use shardline_server::ingest_without_storage_with_parallelism;

use super::{
    BenchRuntimeError, BenchScenario, IngestBenchIterationReport, available_parallelism_u64,
    capture_process_cpu_micros, checked_add_u64, duration_micros, host_utilization_per_mille,
    ratio_per_mille,
};
use crate::bench::concurrent;

pub(crate) async fn run_ingest_bench_iteration(
    iteration: u32,
    scenario: &super::IngestBenchScenario<'_>,
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
            concurrent::run_concurrent_ingest_uploads(
                scenario.chunk_size,
                scenario.upload_max_in_flight_chunks,
                scenario.concurrent_upload_cases,
                available_parallelism,
            )
            .await?
        } else {
            super::TimedConcurrentIngestUpload::default()
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
