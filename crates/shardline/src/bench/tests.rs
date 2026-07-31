use super::{
    BenchConfig, BenchDeploymentTarget, BenchInventoryScope, BenchRuntimeError, BenchScenario,
    DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS, available_parallelism_u64, build_base_asset,
    build_concurrent_upload_cases, build_iteration_repository_scopes, build_sparse_update,
    checked_add_u32, checked_add_u64, checked_average_u64, checked_mul_u64, duration_micros,
    host_utilization_per_mille, inventory_scope, iteration_namespace, measured_iteration_count,
    namespaced_file_id, ratio_per_mille, run_bench, run_ingest_bench, scaling_per_mille,
    throughput_bytes_per_second,
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
            chunk_size_bytes: 1024,
            base_bytes: 8192,
            mutated_bytes: 1024,
        },
    )
    .await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    // Exercise print_summary while the report is fresh
    report.print_summary();

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
    // Chunk counts are derived from CDC boundaries over the deterministic
    // bench payloads (chunk_size=1024, min=128, max=2048) and are not the
    // fixed-size chunk arithmetic the assertions originally encoded.
    assert_eq!(iteration.chunks.initial_inserted_chunks, 4);
    assert_eq!(iteration.chunks.sparse_update_inserted_chunks, 2);
    assert_eq!(iteration.chunks.sparse_update_reused_chunks, 2);
    assert_eq!(iteration.chunks.concurrent_upload_inserted_chunks, 2);
    assert_eq!(iteration.chunks.concurrent_upload_reused_chunks, 6);
    assert_eq!(iteration.bytes.concurrent_newly_stored_bytes, 4096);
    assert_eq!(iteration.bytes.concurrent_uploaded_bytes, 16384);
    assert_eq!(iteration.bytes.concurrent_downloaded_bytes, 16384);
    assert_eq!(iteration.chunks.cross_repository_inserted_chunks, 1);
    assert_eq!(iteration.chunks.cross_repository_reused_chunks, 1);
    assert_eq!(iteration.bytes.cross_repository_newly_stored_bytes, 2048);
    assert_eq!(iteration.bytes.newly_stored_bytes, 21504);
    assert_eq!(report.totals.total_sparse_update_reused_chunks, 2);
    assert_eq!(report.totals.total_concurrent_upload_inserted_chunks, 2);
    assert_eq!(report.totals.total_concurrent_upload_reused_chunks, 6);
    assert_eq!(report.totals.total_concurrent_newly_stored_bytes, 4096);
    assert_eq!(report.totals.total_cross_repository_inserted_chunks, 1);
    assert_eq!(report.totals.total_cross_repository_reused_chunks, 1);
    assert_eq!(
        report.totals.total_cross_repository_newly_stored_bytes,
        2048
    );
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
            chunk_size_bytes: 1024,
            base_bytes: 8192,
            mutated_bytes: 1024,
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
            chunk_size_bytes: 1024,
            base_bytes: 8192,
            mutated_bytes: 1024,
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
            chunk_size_bytes: 1024,
            base_bytes: 4096,
            mutated_bytes: 8192,
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
            chunk_size_bytes: 1024,
            base_bytes: 4096,
            mutated_bytes: 1024,
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
        chunk_size_bytes: 1024,
        base_bytes: 8192,
        mutated_bytes: 1024,
    })
    .await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    // Exercise print_summary for IngestBenchReport
    report.print_summary();

    assert_eq!(report.iterations, 1);
    assert_eq!(report.concurrency, 2);
    assert_eq!(report.available_parallelism, available_parallelism_u64());
    // Chunk counts derive from CDC boundaries over the deterministic bench
    // payloads (chunk_size=1024, min=128, max=2048), not fixed-size arithmetic.
    assert_eq!(report.total_initial_inserted_chunks, 4);
    assert_eq!(report.total_sparse_update_inserted_chunks, 4);
    assert_eq!(report.total_concurrent_upload_inserted_chunks, 8);
    assert_eq!(report.total_concurrent_uploaded_bytes, 16384);
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
            chunk_size_bytes: 1024,
            base_bytes: 8192,
            mutated_bytes: 1024,
        },
    )
    .await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    // Exercise print_summary on a focused-scenario report
    report.print_summary();

    assert_eq!(report.scenario, BenchScenario::CrossRepositoryUpload);
    assert_eq!(report.latency.initial_upload_micros, 0);
    assert_eq!(report.latency.sparse_update_upload_micros, 0);
    assert_eq!(report.latency.latest_download_micros, 0);
    assert_eq!(report.latency.previous_download_micros, 0);
    assert_eq!(report.latency.concurrent_upload_micros, 0);
    // Chunk counts derive from CDC boundaries over the deterministic
    // cross-repository payloads (chunk_size=1024, min=128, max=2048).
    assert_eq!(report.totals.total_uploaded_bytes, 3072);
    assert_eq!(report.totals.total_cross_repository_inserted_chunks, 1);
    assert_eq!(report.totals.total_cross_repository_reused_chunks, 1);
    assert_eq!(
        report.totals.total_cross_repository_newly_stored_bytes,
        2048
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_bench_rejects_unsupported_download_focus() {
    let report = run_ingest_bench(BenchConfig {
        deployment_target: BenchDeploymentTarget::IsolatedLocal,
        scenario: BenchScenario::LatestDownload,
        iterations: 1,
        concurrency: 2,
        upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
        chunk_size_bytes: 1024,
        base_bytes: 8192,
        mutated_bytes: 1024,
    })
    .await;
    assert!(matches!(
        report,
        Err(BenchRuntimeError::UnsupportedScenarioForMode)
    ));
}

// ── inventory_scope ──────────────────────────────────────────────────────

#[test]
fn inventory_scope_isolated_local() {
    assert_eq!(
        inventory_scope("local", "local"),
        BenchInventoryScope::Isolated
    );
}

#[test]
fn inventory_scope_backend_global() {
    assert_eq!(
        inventory_scope("postgres", "s3"),
        BenchInventoryScope::BackendGlobal
    );
}

#[test]
fn inventory_scope_mixed() {
    assert_eq!(inventory_scope("local", "s3"), BenchInventoryScope::Mixed);
    assert_eq!(
        inventory_scope("postgres", "local"),
        BenchInventoryScope::Mixed
    );
}

// ── iteration_namespace / namespaced_file_id ─────────────────────────────

#[test]
fn iteration_namespace_formats_correctly() {
    assert_eq!(
        iteration_namespace("run-0000", 1),
        "run-0000-iteration-0001"
    );
    assert_eq!(iteration_namespace("bench", 42), "bench-iteration-0042");
}

#[test]
fn namespaced_file_id_formats_correctly() {
    assert_eq!(
        namespaced_file_id("run-0000-iteration-0001", "asset.bin"),
        "run-0000-iteration-0001-asset.bin"
    );
}

// ── build_iteration_repository_scopes ────────────────────────────────────

#[test]
fn build_iteration_repository_scopes_returns_distinct_scopes() {
    let result = build_iteration_repository_scopes("test-ns");
    assert!(result.is_ok());
    let (left, right) = result.unwrap();
    assert_ne!(left.owner(), right.owner());
    assert!(left.owner().contains("test-ns"));
    assert!(right.owner().contains("test-ns"));
}

// ── checked_add_u64 ──────────────────────────────────────────────────────

#[test]
fn checked_add_u64_ok() {
    assert_eq!(checked_add_u64(100, 200).unwrap(), 300);
}

#[test]
fn checked_add_u64_overflow_rejected() {
    let result = checked_add_u64(u64::MAX, 1);
    assert!(matches!(
        result,
        Err(BenchRuntimeError::BenchmarkCounterU64Overflow)
    ));
}

// ── checked_add_u32 ──────────────────────────────────────────────────────

#[test]
fn checked_add_u32_ok() {
    assert_eq!(checked_add_u32(10, 20).unwrap(), 30);
}

#[test]
fn checked_add_u32_overflow_rejected() {
    let result = checked_add_u32(u32::MAX, 1);
    assert!(matches!(
        result,
        Err(BenchRuntimeError::BenchmarkCounterU32Overflow)
    ));
}

// ── checked_average_u64 ──────────────────────────────────────────────────

#[test]
fn checked_average_u64_ok() {
    assert_eq!(checked_average_u64(100, 5).unwrap(), 20);
}

#[test]
fn checked_average_u64_zero_divisor_rejected() {
    let result = checked_average_u64(100, 0);
    assert!(matches!(
        result,
        Err(BenchRuntimeError::BenchmarkDivisorZero)
    ));
}

// ── checked_mul_u64 ──────────────────────────────────────────────────────

#[test]
fn checked_mul_u64_ok() {
    assert_eq!(checked_mul_u64(10, 20).unwrap(), 200);
}

#[test]
fn checked_mul_u64_overflow_rejected() {
    let result = checked_mul_u64(u64::MAX, 2);
    assert!(matches!(
        result,
        Err(BenchRuntimeError::BenchmarkCounterU64Overflow)
    ));
}

// ── measured_iteration_count ─────────────────────────────────────────────

#[test]
fn measured_iteration_count_zero_micros() {
    assert_eq!(measured_iteration_count(0, 10), 0);
}

#[test]
fn measured_iteration_count_nonzero_micros() {
    assert_eq!(measured_iteration_count(100, 5), 5);
}

// ── throughput_bytes_per_second ──────────────────────────────────────────

#[test]
fn throughput_zero_when_no_bytes_or_micros() {
    assert_eq!(throughput_bytes_per_second(0, 1000), 0);
    assert_eq!(throughput_bytes_per_second(1000, 0), 0);
}

#[test]
fn throughput_computes_correctly() {
    // 100 bytes in 1_000_000 micros = 100 bytes/sec
    assert_eq!(throughput_bytes_per_second(100, 1_000_000), 100);
}

#[test]
fn throughput_saturates_at_max() {
    // When the multiplication would overflow, it saturates to u64::MAX
    let result = throughput_bytes_per_second(u64::MAX, 1);
    assert_eq!(result, u64::MAX);
}

// ── duration_micros ──────────────────────────────────────────────────────

#[test]
fn duration_micros_converts_correctly() {
    use std::time::Duration;
    let result = duration_micros(Duration::from_secs(1));
    assert_eq!(result.unwrap(), 1_000_000);
}

#[test]
fn duration_micros_zero() {
    use std::time::Duration;
    let result = duration_micros(Duration::ZERO);
    assert_eq!(result.unwrap(), 0);
}

// ── scaling_per_mille ────────────────────────────────────────────────────

#[test]
fn scaling_per_mille_zero_when_no_throughput() {
    assert_eq!(scaling_per_mille(0, 100, 2), 0);
    assert_eq!(scaling_per_mille(100, 0, 2), 0);
    assert_eq!(scaling_per_mille(100, 100, 0), 0);
}

#[test]
fn scaling_per_mille_ideal_linear() {
    // 1000 per-mille = ideal linear scaling
    assert_eq!(scaling_per_mille(200, 100, 2), 1000);
}

#[test]
fn scaling_per_mille_sub_linear() {
    // 150 / (100 * 2) = 750 per-mille
    assert_eq!(scaling_per_mille(150, 100, 2), 750);
}

// ── BenchDeploymentTarget::as_str ─────────────────────────────────────

#[test]
fn bench_deployment_target_as_str_isolated_local() {
    assert_eq!(
        BenchDeploymentTarget::IsolatedLocal.as_str(),
        "isolated-local"
    );
}

#[test]
fn bench_deployment_target_as_str_configured() {
    assert_eq!(BenchDeploymentTarget::Configured.as_str(), "configured");
}

// ── BenchInventoryScope::as_str ───────────────────────────────────────

#[test]
fn bench_inventory_scope_as_str_isolated() {
    assert_eq!(BenchInventoryScope::Isolated.as_str(), "isolated");
}

#[test]
fn bench_inventory_scope_as_str_mixed() {
    assert_eq!(BenchInventoryScope::Mixed.as_str(), "mixed");
}

#[test]
fn bench_inventory_scope_as_str_backend_global() {
    assert_eq!(
        BenchInventoryScope::BackendGlobal.as_str(),
        "backend-global"
    );
}

// ── BenchScenario::supports_ingest ────────────────────────────────────

#[test]
fn bench_scenario_supports_ingest_full_and_upload() {
    assert!(BenchScenario::Full.supports_ingest());
    assert!(BenchScenario::InitialUpload.supports_ingest());
    assert!(BenchScenario::SparseUpdateUpload.supports_ingest());
    assert!(BenchScenario::ConcurrentUpload.supports_ingest());
}

#[test]
fn bench_scenario_rejects_ingest_for_download_scenarios() {
    assert!(!BenchScenario::LatestDownload.supports_ingest());
    assert!(!BenchScenario::PreviousDownload.supports_ingest());
    assert!(!BenchScenario::RangedReconstruction.supports_ingest());
    assert!(!BenchScenario::ConcurrentLatestDownload.supports_ingest());
    assert!(!BenchScenario::CrossRepositoryUpload.supports_ingest());
    assert!(!BenchScenario::CachedLatestReconstruction.supports_ingest());
}

// ── BenchBackendSetup::create_backend ─────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_backend_isolated_local_creates_backend() {
    use std::num::NonZeroUsize;
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("bench-root");

    let setup = super::BenchBackendSetup::IsolatedLocal;
    let result = setup
        .create_backend(
            root.clone(),
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(64).unwrap(),
            "test-ns",
        )
        .await;
    assert!(result.is_ok());
}

// ── Focused e2e scenarios ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_focused_initial_upload_scenario() {
    let storage = tempfile::tempdir().unwrap();
    let report = run_bench(
        storage.path(),
        BenchConfig {
            deployment_target: BenchDeploymentTarget::IsolatedLocal,
            scenario: BenchScenario::InitialUpload,
            iterations: 1,
            concurrency: 1,
            upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
            chunk_size_bytes: 1024,
            base_bytes: 8192,
            mutated_bytes: 1024,
        },
    )
    .await;
    assert!(report.is_ok());
    let report = report.unwrap();
    // Only initial upload should have non-zero latency
    assert!(report.latency.initial_upload_micros > 0);
    assert_eq!(report.latency.sparse_update_upload_micros, 0);
    assert_eq!(report.latency.latest_download_micros, 0);
    assert_eq!(report.latency.previous_download_micros, 0);
    assert_eq!(report.totals.total_uploaded_bytes, 8192);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_focused_sparse_update_upload_scenario() {
    let storage = tempfile::tempdir().unwrap();
    let report = run_bench(
        storage.path(),
        BenchConfig {
            deployment_target: BenchDeploymentTarget::IsolatedLocal,
            scenario: BenchScenario::SparseUpdateUpload,
            iterations: 1,
            concurrency: 1,
            upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
            chunk_size_bytes: 1024,
            base_bytes: 8192,
            mutated_bytes: 1024,
        },
    )
    .await;
    assert!(report.is_ok());
    let report = report.unwrap();
    assert!(report.latency.sparse_update_upload_micros > 0);
    assert_eq!(report.latency.initial_upload_micros, 0);
    assert_eq!(report.totals.total_uploaded_bytes, 8192);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_focused_latest_download_scenario() {
    let storage = tempfile::tempdir().unwrap();
    let report = run_bench(
        storage.path(),
        BenchConfig {
            deployment_target: BenchDeploymentTarget::IsolatedLocal,
            scenario: BenchScenario::LatestDownload,
            iterations: 1,
            concurrency: 1,
            upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
            chunk_size_bytes: 1024,
            base_bytes: 8192,
            mutated_bytes: 1024,
        },
    )
    .await;
    assert!(report.is_ok());
    let report = report.unwrap();
    assert!(report.latency.latest_download_micros > 0);
    assert_eq!(report.latency.initial_upload_micros, 0);
    assert_eq!(report.totals.total_downloaded_bytes, 8192);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_focused_ranged_reconstruction_scenario() {
    let storage = tempfile::tempdir().unwrap();
    let report = run_bench(
        storage.path(),
        BenchConfig {
            deployment_target: BenchDeploymentTarget::IsolatedLocal,
            scenario: BenchScenario::RangedReconstruction,
            iterations: 1,
            concurrency: 1,
            upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
            chunk_size_bytes: 1024,
            base_bytes: 8192,
            mutated_bytes: 1024,
        },
    )
    .await;
    assert!(report.is_ok());
    let report = report.unwrap();
    assert!(report.latency.ranged_reconstruction_micros > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_focused_concurrent_upload_scenario() {
    let storage = tempfile::tempdir().unwrap();
    let report = run_bench(
        storage.path(),
        BenchConfig {
            deployment_target: BenchDeploymentTarget::IsolatedLocal,
            scenario: BenchScenario::ConcurrentUpload,
            iterations: 1,
            concurrency: 2,
            upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
            chunk_size_bytes: 1024,
            base_bytes: 8192,
            mutated_bytes: 1024,
        },
    )
    .await;
    assert!(report.is_ok());
    let report = report.unwrap();
    assert!(report.latency.concurrent_upload_micros > 0);
    assert_eq!(report.latency.initial_upload_micros, 0);
    assert_eq!(report.latency.sparse_update_upload_micros, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_focused_concurrent_latest_download_scenario() {
    let storage = tempfile::tempdir().unwrap();
    let report = run_bench(
        storage.path(),
        BenchConfig {
            deployment_target: BenchDeploymentTarget::IsolatedLocal,
            scenario: BenchScenario::ConcurrentLatestDownload,
            iterations: 1,
            concurrency: 2,
            upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
            chunk_size_bytes: 1024,
            base_bytes: 8192,
            mutated_bytes: 1024,
        },
    )
    .await;
    assert!(report.is_ok());
    let report = report.unwrap();
    assert!(report.latency.concurrent_latest_download_micros > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_focused_cached_latest_reconstruction_scenario() {
    let storage = tempfile::tempdir().unwrap();
    let report = run_bench(
        storage.path(),
        BenchConfig {
            deployment_target: BenchDeploymentTarget::IsolatedLocal,
            scenario: BenchScenario::CachedLatestReconstruction,
            iterations: 1,
            concurrency: 1,
            upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
            chunk_size_bytes: 1024,
            base_bytes: 8192,
            mutated_bytes: 1024,
        },
    )
    .await;
    assert!(report.is_ok());
    let report = report.unwrap();
    // Either cold or hot load may be zero if the cache wasn't populated
    // but at least one should have data
    assert!(
        report.latency.cached_latest_reconstruction_cold_micros > 0
            || report.latency.cached_latest_reconstruction_hot_micros > 0
    );
}

// ── Focused ingest scenarios ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_bench_focused_initial_upload() {
    let report = run_ingest_bench(BenchConfig {
        deployment_target: BenchDeploymentTarget::IsolatedLocal,
        scenario: BenchScenario::InitialUpload,
        iterations: 1,
        concurrency: 2,
        upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
        chunk_size_bytes: 1024,
        base_bytes: 8192,
        mutated_bytes: 1024,
    })
    .await;
    assert!(report.is_ok());
    let report = report.unwrap();
    assert!(report.average_initial_upload_micros > 0);
    assert_eq!(report.average_sparse_update_upload_micros, 0);
    assert_eq!(report.total_uploaded_bytes, 8192);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_bench_focused_sparse_update() {
    let report = run_ingest_bench(BenchConfig {
        deployment_target: BenchDeploymentTarget::IsolatedLocal,
        scenario: BenchScenario::SparseUpdateUpload,
        iterations: 1,
        concurrency: 2,
        upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
        chunk_size_bytes: 1024,
        base_bytes: 8192,
        mutated_bytes: 1024,
    })
    .await;
    assert!(report.is_ok());
    let report = report.unwrap();
    assert!(report.average_sparse_update_upload_micros > 0);
    // Total should still be base bytes (reuse is bounded by CDC boundary
    // alignment of the deterministic payloads, not fixed-size chunk math)
    assert_eq!(report.total_uploaded_bytes, 8192);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_bench_focused_concurrent_upload() {
    let report = run_ingest_bench(BenchConfig {
        deployment_target: BenchDeploymentTarget::IsolatedLocal,
        scenario: BenchScenario::ConcurrentUpload,
        iterations: 1,
        concurrency: 2,
        upload_max_in_flight_chunks: DEFAULT_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
        chunk_size_bytes: 1024,
        base_bytes: 8192,
        mutated_bytes: 1024,
    })
    .await;
    assert!(report.is_ok());
    let report = report.unwrap();
    assert!(report.average_concurrent_upload_micros > 0);
    assert_eq!(report.average_initial_upload_micros, 0);
    assert_eq!(report.average_sparse_update_upload_micros, 0);
    assert_eq!(report.total_concurrent_uploaded_bytes, 16384);
}
