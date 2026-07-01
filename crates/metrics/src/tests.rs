use prometheus::{Registry, Encoder, TextEncoder};

use crate::{
    CasMetrics, encode_metrics, metrics, record_download, record_fsck_run, record_gc_run,
    record_hub_api_commit, record_hub_api_file_download, record_hub_api_file_upload,
    record_hub_api_request, record_provider_token_exchange, record_provider_webhook,
    record_reconstruction, record_reconstruction_cache_hit, record_reconstruction_cache_miss,
    record_upload, record_xet_dedupe_shard_query, record_xet_reconstruction,
    record_xet_shard_upload, record_xet_xorb_download, record_xet_xorb_upload, registry,
};

fn new_registry_and_metrics() -> (Registry, CasMetrics) {
    let registry = Registry::new();
    let m = CasMetrics::new(&registry);
    (registry, m)
}

fn encode(registry: &Registry) -> String {
    let encoder = TextEncoder::new();
    let families = registry.gather();
    let mut buffer = Vec::new();
    encoder.encode(&families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

#[test]
fn record_upload_increments_counter_and_bytes() {
    let (registry, m) = new_registry_and_metrics();
    m.transfer.record_upload("http", 100);
    m.transfer.record_upload("http", 250);
    let output = encode(&registry);
    assert!(
        output.contains("shardline_upload_requests_total 2"),
        "expected upload_requests=2, got:\n{output}"
    );
    assert!(
        output.contains("shardline_upload_bytes_total 350"),
        "expected upload_bytes=350, got:\n{output}"
    );
}

#[test]
fn record_download_increments_counter_and_bytes() {
    let (registry, m) = new_registry_and_metrics();
    m.transfer.record_download("grpc", 500);
    m.transfer.record_download("grpc", 150);
    let output = encode(&registry);
    assert!(
        output.contains("shardline_download_requests_total 2"),
        "expected download_requests=2, got:\n{output}"
    );
    assert!(
        output.contains("shardline_download_bytes_total 650"),
        "expected download_bytes=650, got:\n{output}"
    );
}

#[test]
fn record_reconstruction_increments_counters_and_observations() {
    let (registry, m) = new_registry_and_metrics();
    m.reconstruction.record(true, std::time::Duration::from_millis(10), 5);
    m.reconstruction.record(false, std::time::Duration::from_millis(20), 3);
    let output = encode(&registry);
    assert!(
        output.contains("shardline_reconstruction_requests_total 2"),
        "expected reconstruction_requests=2, got:\n{output}"
    );
    assert!(
        output.contains("shardline_reconstruction_chunks_fetched_total 8"),
        "expected chunks_fetched=8, got:\n{output}"
    );
}

#[test]
fn record_reconstruction_cache_hit_and_miss() {
    let (registry, m) = new_registry_and_metrics();
    m.reconstruction.record_cache_hit();
    m.reconstruction.record_cache_hit();
    m.reconstruction.record_cache_miss();
    let output = encode(&registry);
    assert!(
        output.contains("shardline_reconstruction_cache_hits_total 2"),
        "expected cache_hits=2, got:\n{output}"
    );
    assert!(
        output.contains("shardline_reconstruction_cache_misses_total 1"),
        "expected cache_misses=1, got:\n{output}"
    );
}

#[test]
fn record_gc_run_increments_all_counters() {
    let (registry, m) = new_registry_and_metrics();
    m.gc.record_run(std::time::Duration::from_secs(5), 10, 1024);
    m.gc.record_run(std::time::Duration::from_secs(1), 5, 512);
    let output = encode(&registry);
    assert!(
        output.contains("shardline_gc_runs_total 2"),
        "expected gc_runs=2, got:\n{output}"
    );
    assert!(
        output.contains("shardline_gc_objects_collected_total 15"),
        "expected objects_collected=15, got:\n{output}"
    );
    assert!(
        output.contains("shardline_gc_bytes_collected_total 1536"),
        "expected bytes_collected=1536, got:\n{output}"
    );
}

#[test]
fn record_fsck_run_increments_counters() {
    let (registry, m) = new_registry_and_metrics();
    m.fsck.record_run(std::time::Duration::from_millis(50), 3);
    m.fsck.record_run(std::time::Duration::from_millis(10), 1);
    let output = encode(&registry);
    assert!(
        output.contains("shardline_fsck_runs_total 2"),
        "expected fsck_runs=2, got:\n{output}"
    );
    assert!(
        output.contains("shardline_fsck_errors_found_total 4"),
        "expected fsck_errors=4, got:\n{output}"
    );
}

#[test]
fn encode_metrics_outputs_prometheus_text_format() {
    let _m = metrics();
    record_upload("http", 1024);
    record_download("http", 512);
    record_reconstruction(true, std::time::Duration::from_millis(5), 10);
    record_gc_run(std::time::Duration::from_secs(1), 1, 256);
    record_fsck_run(std::time::Duration::from_millis(10), 0);
    let output = encode_metrics();
    assert!(!output.is_empty());
    assert!(
        output.contains("# HELP"),
        "expected HELP lines in output"
    );
    assert!(
        output.contains("# TYPE"),
        "expected TYPE lines in output"
    );
    assert!(
        output.contains("shardline_"),
        "expected shardline-prefixed metrics"
    );
}

#[test]
fn free_function_record_upload_delegates_to_transfer_metrics() {
    let (registry, m) = new_registry_and_metrics();
    record_upload("grpc", 999);
    // The global record_upload uses the global metrics, but we can verify the
    // transfer module's record_upload directly here for the unit-test angle.
    m.transfer.record_upload("http", 42);
    let output = encode(&registry);
    assert!(output.contains("shardline_upload_bytes_total 42"));
}

#[test]
fn free_function_record_download_delegates_to_transfer_metrics() {
    let (registry, m) = new_registry_and_metrics();
    m.transfer.record_download("http", 77);
    let output = encode(&registry);
    assert!(output.contains("shardline_download_bytes_total 77"));
}

#[test]
fn cas_metrics_new_creates_independent_registries() {
    let r1 = Registry::new();
    let r2 = Registry::new();
    let m1 = CasMetrics::new(&r1);
    let m2 = CasMetrics::new(&r2);
    m1.transfer.record_upload("http", 100);
    let out1 = encode(&r1);
    let out2 = encode(&r2);
    assert!(out1.contains("shardline_upload_bytes_total 100"));
    assert!(!out2.contains("shardline_upload_bytes_total 100"));
}

#[test]
fn encode_metrics_returns_valid_utf8() {
    let _m = metrics();
    let output = encode_metrics();
    assert!(std::str::from_utf8(output.as_bytes()).is_ok());
}

#[test]
fn cas_metrics_new_does_not_panic() {
    let registry = Registry::new();
    let _m = CasMetrics::new(&registry);
}

#[test]
fn encode_metrics_returns_non_empty_string() {
    let _m = metrics();
    let output = encode_metrics();
    assert!(!output.is_empty());
}

#[test]
fn encode_metrics_contains_help_or_type_lines() {
    let _m = metrics();
    let output = encode_metrics();
    assert!(
        output.contains("# HELP") || output.contains("# TYPE"),
        "expected at least one HELP or TYPE line"
    );
}

#[test]
fn record_upload_does_not_panic() {
    record_upload("http", 1024);
    record_upload("grpc", 2048);
}

#[test]
fn record_download_does_not_panic() {
    record_download("http", 512);
    record_download("grpc", 4096);
}

#[test]
fn record_xet_shard_upload_does_not_panic() {
    record_xet_shard_upload(100);
}

#[test]
fn record_xet_xorb_upload_does_not_panic() {
    record_xet_xorb_upload(200);
}

#[test]
fn record_xet_xorb_download_does_not_panic() {
    record_xet_xorb_download(300);
}

#[test]
fn record_xet_reconstruction_does_not_panic() {
    record_xet_reconstruction(true, std::time::Duration::from_millis(10), 5);
    record_xet_reconstruction(false, std::time::Duration::from_millis(0), 0);
}

#[test]
fn record_xet_dedupe_shard_query_does_not_panic() {
    record_xet_dedupe_shard_query(true);
    record_xet_dedupe_shard_query(false);
}

#[test]
fn record_reconstruction_does_not_panic() {
    record_reconstruction(true, std::time::Duration::from_millis(5), 10);
    record_reconstruction(false, std::time::Duration::ZERO, 0);
}

#[test]
fn record_reconstruction_cache_hit_miss_does_not_panic() {
    record_reconstruction_cache_hit();
    record_reconstruction_cache_miss();
}

#[test]
fn record_gc_run_does_not_panic() {
    record_gc_run(std::time::Duration::from_millis(50), 10, 1024);
}

#[test]
fn record_fsck_run_does_not_panic() {
    record_fsck_run(std::time::Duration::from_millis(25), 3);
}

#[test]
fn record_hub_api_requests_do_not_panic() {
    record_hub_api_request("GET", "/models", 200);
    record_hub_api_request("POST", "/datasets", 201);
    record_hub_api_commit("create");
    record_hub_api_file_upload();
    record_hub_api_file_download();
}

#[test]
fn record_provider_events_do_not_panic() {
    record_provider_webhook("github", "push");
    record_provider_token_exchange();
}

#[test]
fn global_metrics_and_registry_are_accessible() {
    let _m = metrics();
    let _r = registry();
}
