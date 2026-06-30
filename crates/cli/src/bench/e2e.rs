use std::{path::PathBuf, time::Instant};

use shardline_server::ReconstructionCacheBenchReport;

use super::{
    BenchBackendSetup, BenchFixture, BenchIterationReport, BenchRuntimeError, BenchScenario,
    ByteMetrics, ChunkMetrics, InventoryMetrics, LatencyMetrics, TimingMetrics,
    available_parallelism_u64, build_iteration_repository_scopes, capture_process_cpu_micros,
    checked_add_u64, duration_micros, host_utilization_per_mille, iteration_namespace,
    namespaced_file_id, ratio_per_mille,
};
use crate::bench::concurrent;

pub(crate) async fn run_bench_iteration(
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
        concurrent::run_concurrent_latest_downloads(
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
        concurrent::run_concurrent_uploads(&backend, &namespace, fixture.concurrent_upload_cases)
            .await?
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
            latency: LatencyMetrics {
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
            },
            bytes: ByteMetrics {
                uploaded_bytes,
                downloaded_bytes,
                cached_reconstruction_response_bytes: cached_reconstruction.response_bytes,
                cached_latest_reconstruction_cache_hit: cached_reconstruction.cache_hit,
                concurrent_downloaded_bytes,
                concurrent_uploaded_bytes,
                concurrent_newly_stored_bytes,
                newly_stored_bytes,
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
            },
            chunks: ChunkMetrics {
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
            },
            timing: TimingMetrics {
                process_cpu_micros,
                process_cpu_cores_per_mille,
                process_host_utilization_per_mille,
            },
            inventory: InventoryMetrics {
                chunk_objects: stats.chunks,
                chunk_bytes: stats.chunk_bytes,
                visible_files: stats.files,
            },
        },
        metadata_backend,
        object_backend,
    ))
}
