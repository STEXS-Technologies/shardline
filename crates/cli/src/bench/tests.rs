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
    assert_eq!(iteration.chunks.initial_inserted_chunks, 3);
    assert_eq!(iteration.chunks.sparse_update_inserted_chunks, 1);
    assert_eq!(iteration.chunks.sparse_update_reused_chunks, 2);
    assert_eq!(iteration.chunks.concurrent_upload_inserted_chunks, 2);
    assert_eq!(iteration.chunks.concurrent_upload_reused_chunks, 4);
    assert_eq!(iteration.bytes.concurrent_newly_stored_bytes, 8);
    assert_eq!(iteration.bytes.concurrent_uploaded_bytes, 24);
    assert_eq!(iteration.bytes.concurrent_downloaded_bytes, 24);
    assert_eq!(iteration.chunks.cross_repository_inserted_chunks, 1);
    assert_eq!(iteration.chunks.cross_repository_reused_chunks, 2);
    assert_eq!(iteration.bytes.cross_repository_newly_stored_bytes, 4);
    assert_eq!(iteration.bytes.newly_stored_bytes, 40);
    assert_eq!(report.totals.total_sparse_update_reused_chunks, 2);
    assert_eq!(report.totals.total_concurrent_upload_inserted_chunks, 2);
    assert_eq!(report.totals.total_concurrent_upload_reused_chunks, 4);
    assert_eq!(report.totals.total_concurrent_newly_stored_bytes, 8);
    assert_eq!(report.totals.total_cross_repository_inserted_chunks, 1);
    assert_eq!(report.totals.total_cross_repository_reused_chunks, 2);
    assert_eq!(report.totals.total_cross_repository_newly_stored_bytes, 4);
    assert!(
        iteration.timing.process_cpu_cores_per_mille
            >= iteration.timing.process_host_utilization_per_mille
    );
    assert!(
        report.timing.process_cpu_cores_per_mille
            >= report.timing.process_host_utilization_per_mille
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
    assert_eq!(report.latency.initial_upload_micros, 0);
    assert_eq!(report.latency.sparse_update_upload_micros, 0);
    assert_eq!(report.latency.latest_download_micros, 0);
    assert_eq!(report.latency.previous_download_micros, 0);
    assert_eq!(report.latency.concurrent_upload_micros, 0);
    assert_eq!(report.totals.total_uploaded_bytes, 12);
    assert_eq!(report.totals.total_cross_repository_inserted_chunks, 1);
    assert_eq!(report.totals.total_cross_repository_reused_chunks, 2);
    assert_eq!(report.totals.total_cross_repository_newly_stored_bytes, 4);
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
