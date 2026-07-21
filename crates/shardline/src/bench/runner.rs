use std::{
    fs as std_fs,
    io::ErrorKind,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use bytes::Bytes;
use shardline_protocol::{RepositoryProvider, RepositoryScope};
use shardline_server::ServerConfig;
use tokio::fs;

use super::e2e::run_bench_iteration;
use super::ingest::run_ingest_bench_iteration;
use super::sparse::{
    build_base_asset, build_concurrent_ingest_upload_cases, build_concurrent_upload_cases,
    build_cross_repository_assets, build_mutation_range, build_sparse_update,
};
use super::types::*;

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

        total_initial_upload_micros = checked_add_u64(
            total_initial_upload_micros,
            report.latency.initial_upload_micros,
        )?;
        total_sparse_update_upload_micros = checked_add_u64(
            total_sparse_update_upload_micros,
            report.latency.sparse_update_upload_micros,
        )?;
        total_latest_download_micros = checked_add_u64(
            total_latest_download_micros,
            report.latency.latest_download_micros,
        )?;
        total_previous_download_micros = checked_add_u64(
            total_previous_download_micros,
            report.latency.previous_download_micros,
        )?;
        total_ranged_reconstruction_micros = checked_add_u64(
            total_ranged_reconstruction_micros,
            report.latency.ranged_reconstruction_micros,
        )?;
        total_concurrent_latest_download_micros = checked_add_u64(
            total_concurrent_latest_download_micros,
            report.latency.concurrent_latest_download_micros,
        )?;
        total_concurrent_upload_micros = checked_add_u64(
            total_concurrent_upload_micros,
            report.latency.concurrent_upload_micros,
        )?;
        total_cross_repository_upload_micros = checked_add_u64(
            total_cross_repository_upload_micros,
            report.latency.cross_repository_upload_micros,
        )?;
        total_cached_latest_reconstruction_cold_micros = checked_add_u64(
            total_cached_latest_reconstruction_cold_micros,
            report.latency.cached_latest_reconstruction_cold_micros,
        )?;
        total_cached_latest_reconstruction_hot_micros = checked_add_u64(
            total_cached_latest_reconstruction_hot_micros,
            report.latency.cached_latest_reconstruction_hot_micros,
        )?;
        total_uploaded_bytes = checked_add_u64(total_uploaded_bytes, report.bytes.uploaded_bytes)?;
        total_downloaded_bytes =
            checked_add_u64(total_downloaded_bytes, report.bytes.downloaded_bytes)?;
        total_cached_reconstruction_response_bytes = checked_add_u64(
            total_cached_reconstruction_response_bytes,
            report.bytes.cached_reconstruction_response_bytes,
        )?;
        total_concurrent_downloaded_bytes = checked_add_u64(
            total_concurrent_downloaded_bytes,
            report.bytes.concurrent_downloaded_bytes,
        )?;
        total_concurrent_uploaded_bytes = checked_add_u64(
            total_concurrent_uploaded_bytes,
            report.bytes.concurrent_uploaded_bytes,
        )?;
        total_concurrent_newly_stored_bytes = checked_add_u64(
            total_concurrent_newly_stored_bytes,
            report.bytes.concurrent_newly_stored_bytes,
        )?;
        total_cross_repository_newly_stored_bytes = checked_add_u64(
            total_cross_repository_newly_stored_bytes,
            report.bytes.cross_repository_newly_stored_bytes,
        )?;
        total_newly_stored_bytes =
            checked_add_u64(total_newly_stored_bytes, report.bytes.newly_stored_bytes)?;
        total_initial_inserted_chunks = checked_add_u64(
            total_initial_inserted_chunks,
            report.chunks.initial_inserted_chunks,
        )?;
        total_sparse_update_inserted_chunks = checked_add_u64(
            total_sparse_update_inserted_chunks,
            report.chunks.sparse_update_inserted_chunks,
        )?;
        total_sparse_update_reused_chunks = checked_add_u64(
            total_sparse_update_reused_chunks,
            report.chunks.sparse_update_reused_chunks,
        )?;
        total_concurrent_upload_inserted_chunks = checked_add_u64(
            total_concurrent_upload_inserted_chunks,
            report.chunks.concurrent_upload_inserted_chunks,
        )?;
        total_concurrent_upload_reused_chunks = checked_add_u64(
            total_concurrent_upload_reused_chunks,
            report.chunks.concurrent_upload_reused_chunks,
        )?;
        total_cross_repository_inserted_chunks = checked_add_u64(
            total_cross_repository_inserted_chunks,
            report.chunks.cross_repository_inserted_chunks,
        )?;
        total_cross_repository_reused_chunks = checked_add_u64(
            total_cross_repository_reused_chunks,
            report.chunks.cross_repository_reused_chunks,
        )?;
        total_process_cpu_micros =
            checked_add_u64(total_process_cpu_micros, report.timing.process_cpu_micros)?;
        total_process_cpu_cores_per_mille = checked_add_u64(
            total_process_cpu_cores_per_mille,
            report.timing.process_cpu_cores_per_mille,
        )?;
        total_process_host_utilization_per_mille = checked_add_u64(
            total_process_host_utilization_per_mille,
            report.timing.process_host_utilization_per_mille,
        )?;
        cache_hit_iterations = checked_add_u64(
            cache_hit_iterations,
            if report.bytes.cached_latest_reconstruction_cache_hit {
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
        latency: LatencyMetrics {
            initial_upload_micros: checked_average_u64(
                total_initial_upload_micros,
                iterations_u64,
            )?,
            sparse_update_upload_micros: checked_average_u64(
                total_sparse_update_upload_micros,
                iterations_u64,
            )?,
            latest_download_micros: checked_average_u64(
                total_latest_download_micros,
                iterations_u64,
            )?,
            previous_download_micros: checked_average_u64(
                total_previous_download_micros,
                iterations_u64,
            )?,
            ranged_reconstruction_micros: checked_average_u64(
                total_ranged_reconstruction_micros,
                iterations_u64,
            )?,
            concurrent_latest_download_micros: checked_average_u64(
                total_concurrent_latest_download_micros,
                iterations_u64,
            )?,
            concurrent_upload_micros: checked_average_u64(
                total_concurrent_upload_micros,
                iterations_u64,
            )?,
            cross_repository_upload_micros: checked_average_u64(
                total_cross_repository_upload_micros,
                iterations_u64,
            )?,
            cached_latest_reconstruction_cold_micros: checked_average_u64(
                total_cached_latest_reconstruction_cold_micros,
                iterations_u64,
            )?,
            cached_latest_reconstruction_hot_micros: checked_average_u64(
                total_cached_latest_reconstruction_hot_micros,
                iterations_u64,
            )?,
        },
        throughput: BenchThroughputMetrics {
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
        },
        timing: TimingMetrics {
            process_cpu_micros: checked_average_u64(total_process_cpu_micros, iterations_u64)?,
            process_cpu_cores_per_mille: checked_average_u64(
                total_process_cpu_cores_per_mille,
                iterations_u64,
            )?,
            process_host_utilization_per_mille: checked_average_u64(
                total_process_host_utilization_per_mille,
                iterations_u64,
            )?,
        },
        totals: BenchTotals {
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
        },
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

pub(crate) async fn allocate_bench_run_root(
    storage_dir: &Path,
) -> Result<PathBuf, BenchRuntimeError> {
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

pub(crate) fn iteration_namespace(run_namespace: &str, iteration: u32) -> String {
    format!("{run_namespace}-iteration-{iteration:04}")
}

pub(crate) fn namespaced_file_id(namespace: &str, file_id: &str) -> String {
    format!("{namespace}-{file_id}")
}

pub(crate) fn build_iteration_repository_scopes(
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

pub(crate) fn inventory_scope(metadata_backend: &str, object_backend: &str) -> BenchInventoryScope {
    match (metadata_backend, object_backend) {
        ("local", "local") => BenchInventoryScope::Isolated,
        ("postgres", "s3") => BenchInventoryScope::BackendGlobal,
        _ => BenchInventoryScope::Mixed,
    }
}

pub(crate) fn duration_micros(duration: Duration) -> Result<u64, BenchRuntimeError> {
    u64::try_from(duration.as_micros()).map_err(BenchRuntimeError::from)
}

pub(crate) fn checked_add_u64(left: u64, right: u64) -> Result<u64, BenchRuntimeError> {
    left.checked_add(right)
        .ok_or(BenchRuntimeError::BenchmarkCounterU64Overflow)
}

pub(crate) fn checked_add_u32(left: u32, right: u32) -> Result<u32, BenchRuntimeError> {
    left.checked_add(right)
        .ok_or(BenchRuntimeError::BenchmarkCounterU32Overflow)
}

pub(crate) fn checked_average_u64(total: u64, count: u64) -> Result<u64, BenchRuntimeError> {
    total
        .checked_div(count)
        .ok_or(BenchRuntimeError::BenchmarkDivisorZero)
}

pub(crate) fn checked_mul_u64(left: u64, right: u64) -> Result<u64, BenchRuntimeError> {
    left.checked_mul(right)
        .ok_or(BenchRuntimeError::BenchmarkCounterU64Overflow)
}

pub(crate) const fn measured_iteration_count(total_micros: u64, iterations: u64) -> u64 {
    if total_micros == 0 { 0 } else { iterations }
}

pub(crate) fn throughput_bytes_per_second(bytes: u64, micros: u64) -> u64 {
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

pub(crate) fn available_parallelism_u64() -> u64 {
    thread::available_parallelism()
        .map(usize::from)
        .ok()
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(1)
}

pub(crate) fn capture_process_cpu_micros() -> u64 {
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

pub(crate) fn ratio_per_mille(numerator: u64, denominator: u64) -> u64 {
    if numerator == 0 || denominator == 0 {
        return 0;
    }

    let scaled = u128::from(numerator)
        .checked_mul(1_000)
        .and_then(|value| value.checked_div(u128::from(denominator)))
        .unwrap_or_else(|| u128::from(u64::MAX));
    u64::try_from(scaled).unwrap_or(u64::MAX)
}

pub(crate) fn host_utilization_per_mille(
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

pub(crate) fn scaling_per_mille(
    aggregate_throughput: u64,
    single_throughput: u64,
    concurrency: u32,
) -> u64 {
    if aggregate_throughput == 0 || single_throughput == 0 || concurrency == 0 {
        return 0;
    }

    aggregate_throughput
        .saturating_mul(1_000)
        .checked_div(single_throughput)
        .and_then(|value| value.checked_div(u64::from(concurrency)))
        .unwrap_or(u64::MAX)
}
