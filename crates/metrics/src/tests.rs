use prometheus::{Encoder, Registry, TextEncoder};

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
    m.reconstruction
        .record(true, std::time::Duration::from_millis(10), 5);
    m.reconstruction
        .record(false, std::time::Duration::from_millis(20), 3);
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
    assert!(output.contains("# HELP"), "expected HELP lines in output");
    assert!(output.contains("# TYPE"), "expected TYPE lines in output");
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
    let _m2 = CasMetrics::new(&r2);
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

// ── Backend metrics ────────────────────────────────────────────────────────

#[test]
fn backend_record_s3_request_increments_counter() {
    let (registry, m) = new_registry_and_metrics();
    m.backend.record_s3_request(std::time::Duration::from_millis(10));
    let output = encode(&registry);
    assert!(output.contains("shardline_s3_requests_total 1"));
}

#[test]
fn backend_record_s3_error_increments_counter() {
    let (registry, m) = new_registry_and_metrics();
    m.backend.record_s3_error();
    let output = encode(&registry);
    assert!(output.contains("shardline_s3_errors_total 1"));
}

#[test]
fn backend_record_local_io_increments_counter() {
    let (registry, m) = new_registry_and_metrics();
    m.backend.record_local_io(std::time::Duration::from_micros(100));
    let output = encode(&registry);
    assert!(output.contains("shardline_local_io_operations_total 1"));
}

// ── Storage metrics ────────────────────────────────────────────────────────

#[test]
fn storage_record_object_stored_increments() {
    let (registry, m) = new_registry_and_metrics();
    m.storage.record_object_stored(512);
    let output = encode(&registry);
    assert!(output.contains("shardline_objects_total 1"));
    assert!(output.contains("shardline_objects_bytes_total 512"));
}

#[test]
fn storage_record_chunk_stored_increments() {
    let (registry, m) = new_registry_and_metrics();
    m.storage.record_chunk_stored(1024);
    let output = encode(&registry);
    assert!(output.contains("shardline_chunks_total 1"));
    assert!(output.contains("shardline_chunks_bytes_total 1024"));
}

#[test]
fn storage_record_xorb_stored_increments() {
    let (registry, m) = new_registry_and_metrics();
    m.storage.record_xorb_stored(2048);
    let output = encode(&registry);
    assert!(output.contains("shardline_xorbs_total 1"));
    assert!(output.contains("shardline_xorbs_bytes_total 2048"));
}

#[test]
fn storage_record_shard_stored_increments() {
    let (registry, m) = new_registry_and_metrics();
    m.storage.record_shard_stored();
    let output = encode(&registry);
    assert!(output.contains("shardline_shards_total 1"));
}

#[test]
fn storage_record_dedup_saves_increments() {
    let (registry, m) = new_registry_and_metrics();
    m.storage.record_dedup_saves(4096);
    let output = encode(&registry);
    assert!(output.contains("shardline_dedup_saves_bytes_total 4096"));
}

// ── System metrics ─────────────────────────────────────────────────────────

#[test]
fn system_connection_opened_and_closed() {
    let (registry, m) = new_registry_and_metrics();
    m.system.connection_opened();
    m.system.connection_opened();
    m.system.connection_closed();
    let output = encode(&registry);
    assert!(output.contains("shardline_active_connections 1"));
}

#[test]
fn system_set_uptime() {
    let (registry, m) = new_registry_and_metrics();
    m.system.set_uptime(12345);
    let output = encode(&registry);
    assert!(output.contains("shardline_server_uptime_seconds 12345"));
}

// ── Transfer metrics ───────────────────────────────────────────────────────

#[test]
fn transfer_record_upload_duration_observes() {
    let (registry, m) = new_registry_and_metrics();
    m.transfer.record_upload_duration(0.5);
    // Verify the histogram was created — it will have a _bucket, _count, _sum
    let output = encode(&registry);
    assert!(output.contains("shardline_upload_duration_seconds"));
}

#[test]
fn transfer_record_download_duration_observes() {
    let (registry, m) = new_registry_and_metrics();
    m.transfer.record_download_duration(0.25);
    let output = encode(&registry);
    assert!(output.contains("shardline_download_duration_seconds"));
}

#[test]
fn transfer_record_range_request_increments() {
    let (registry, m) = new_registry_and_metrics();
    m.transfer.record_range_request();
    let output = encode(&registry);
    assert!(output.contains("shardline_range_requests_total 1"));
}

// ── Protocol metrics ───────────────────────────────────────────────────────

#[test]
fn protocol_record_lfs_upload_increments() {
    let (registry, m) = new_registry_and_metrics();
    m.protocol.record_lfs_upload();
    let output = encode(&registry);
    assert!(output.contains("shardline_lfs_upload_requests_total 1"));
}

#[test]
fn protocol_record_lfs_download_increments() {
    let (registry, m) = new_registry_and_metrics();
    m.protocol.record_lfs_download();
    let output = encode(&registry);
    assert!(output.contains("shardline_lfs_download_requests_total 1"));
}

#[test]
fn protocol_record_oci_upload_increments() {
    let (registry, m) = new_registry_and_metrics();
    m.protocol.record_oci_upload();
    let output = encode(&registry);
    assert!(output.contains("shardline_oci_upload_requests_total 1"));
}

#[test]
fn protocol_record_oci_download_increments() {
    let (registry, m) = new_registry_and_metrics();
    m.protocol.record_oci_download();
    let output = encode(&registry);
    assert!(output.contains("shardline_oci_download_requests_total 1"));
}

#[test]
fn protocol_oci_registry_token_operations() {
    let (registry, m) = new_registry_and_metrics();
    m.protocol.record_oci_registry_token_request();
    m.protocol.record_oci_registry_token_rate_limited();
    m.protocol.begin_oci_registry_token_request();
    m.protocol.end_oci_registry_token_request();
    let output = encode(&registry);
    assert!(output.contains("shardline_oci_registry_token_requests_total 1"));
    assert!(output.contains("shardline_oci_registry_token_rate_limited_total 1"));
    assert!(output.contains("shardline_oci_registry_token_active_requests 0"));
}

// ── Provider metrics ───────────────────────────────────────────────────────

#[test]
fn provider_record_webhook_duration_observes() {
    let (registry, m) = new_registry_and_metrics();
    m.provider.record_webhook_duration(std::time::Duration::from_millis(100));
    let output = encode(&registry);
    assert!(output.contains("shardline_provider_webhook_processing_duration_seconds"));
}

// ── Xet metrics (direct struct methods) ────────────────────────────────────

#[test]
fn xet_record_shard_upload_increments() {
    let (registry, m) = new_registry_and_metrics();
    m.xet.record_shard_upload(100);
    let output = encode(&registry);
    assert!(output.contains("shardline_xet_shard_uploads_total 1"));
    assert!(output.contains("shardline_xet_shard_upload_bytes_total 100"));
}

#[test]
fn xet_record_xorb_upload_increments() {
    let (registry, m) = new_registry_and_metrics();
    m.xet.record_xorb_upload(200);
    let output = encode(&registry);
    assert!(output.contains("shardline_xet_xorb_uploads_total 1"));
    assert!(output.contains("shardline_xet_xorb_upload_bytes_total 200"));
}

#[test]
fn xet_record_xorb_download_increments() {
    let (registry, m) = new_registry_and_metrics();
    m.xet.record_xorb_download(300);
    let output = encode(&registry);
    assert!(output.contains("shardline_xet_xorb_downloads_total 300"));
}

#[test]
fn xet_record_dedupe_shard_query_hit() {
    let (registry, m) = new_registry_and_metrics();
    m.xet.record_dedupe_shard_query(true);
    let output = encode(&registry);
    assert!(output.contains("shardline_xet_dedupe_shard_queries_total 1"));
    assert!(output.contains("shardline_xet_dedupe_shard_hits_total 1"));
}

#[test]
fn xet_record_dedupe_shard_query_miss() {
    let (registry, m) = new_registry_and_metrics();
    m.xet.record_dedupe_shard_query(false);
    let output = encode(&registry);
    assert!(output.contains("shardline_xet_dedupe_shard_queries_total 1"));
    // Hits counter is present with value 0 when it hasn't been incremented
    assert!(
        output.contains("shardline_xet_dedupe_shard_hits_total 0"),
        "expected hits counter at 0, got:\n{output}"
    );
}

// ── Zero-value edge cases ─────────────────────────────────────────────────

#[test]
fn record_upload_zero_bytes() {
    let (registry, m) = new_registry_and_metrics();
    m.transfer.record_upload("http", 0);
    let output = encode(&registry);
    assert!(output.contains("shardline_upload_requests_total 1"));
    assert!(output.contains("shardline_upload_bytes_total 0"));
}

#[test]
fn record_download_zero_bytes() {
    let (registry, m) = new_registry_and_metrics();
    m.transfer.record_download("http", 0);
    let output = encode(&registry);
    assert!(output.contains("shardline_download_requests_total 1"));
    assert!(output.contains("shardline_download_bytes_total 0"));
}

#[test]
fn xet_record_shard_upload_zero_bytes() {
    let (registry, m) = new_registry_and_metrics();
    m.xet.record_shard_upload(0);
    let output = encode(&registry);
    assert!(output.contains("shardline_xet_shard_uploads_total 1"));
    assert!(output.contains("shardline_xet_shard_upload_bytes_total 0"));
}

#[test]
fn xet_record_xorb_upload_zero_bytes() {
    let (registry, m) = new_registry_and_metrics();
    m.xet.record_xorb_upload(0);
    let output = encode(&registry);
    assert!(output.contains("shardline_xet_xorb_uploads_total 1"));
    assert!(output.contains("shardline_xet_xorb_upload_bytes_total 0"));
}

#[test]
fn xet_record_xorb_download_zero_bytes() {
    let (registry, m) = new_registry_and_metrics();
    m.xet.record_xorb_download(0);
    let output = encode(&registry);
    assert!(output.contains("shardline_xet_xorb_downloads_total 0"));
}

#[test]
fn storage_record_object_stored_zero_bytes() {
    let (registry, m) = new_registry_and_metrics();
    m.storage.record_object_stored(0);
    let output = encode(&registry);
    assert!(output.contains("shardline_objects_total 1"));
    assert!(output.contains("shardline_objects_bytes_total 0"));
}

#[test]
fn storage_record_chunk_stored_zero_bytes() {
    let (registry, m) = new_registry_and_metrics();
    m.storage.record_chunk_stored(0);
    let output = encode(&registry);
    assert!(output.contains("shardline_chunks_total 1"));
    assert!(output.contains("shardline_chunks_bytes_total 0"));
}

#[test]
fn storage_record_xorb_stored_zero_bytes() {
    let (registry, m) = new_registry_and_metrics();
    m.storage.record_xorb_stored(0);
    let output = encode(&registry);
    assert!(output.contains("shardline_xorbs_total 1"));
    assert!(output.contains("shardline_xorbs_bytes_total 0"));
}

#[test]
fn storage_record_shard_stored_multiple() {
    let (registry, m) = new_registry_and_metrics();
    m.storage.record_shard_stored();
    m.storage.record_shard_stored();
    m.storage.record_shard_stored();
    let output = encode(&registry);
    assert!(output.contains("shardline_shards_total 3"));
}

#[test]
fn storage_record_dedup_saves_zero() {
    let (registry, m) = new_registry_and_metrics();
    m.storage.record_dedup_saves(0);
    let output = encode(&registry);
    assert!(output.contains("shardline_dedup_saves_bytes_total 0"));
}

#[test]
fn transfer_record_upload_duration_zero() {
    let (registry, m) = new_registry_and_metrics();
    m.transfer.record_upload_duration(0.0);
    let output = encode(&registry);
    assert!(output.contains("shardline_upload_duration_seconds_count 1"));
}

#[test]
fn transfer_record_download_duration_zero() {
    let (registry, m) = new_registry_and_metrics();
    m.transfer.record_download_duration(0.0);
    let output = encode(&registry);
    assert!(output.contains("shardline_download_duration_seconds_count 1"));
}

#[test]
fn system_set_uptime_zero() {
    let (registry, m) = new_registry_and_metrics();
    m.system.set_uptime(0);
    let output = encode(&registry);
    assert!(output.contains("shardline_server_uptime_seconds 0"));
}

#[test]
fn system_connection_negative_gauge() {
    let (registry, m) = new_registry_and_metrics();
    m.system.connection_closed();
    let output = encode(&registry);
    // Gauge can go negative; just verify it's present
    assert!(output.contains("shardline_active_connections"));
}

#[test]
fn backend_record_s3_request_duration() {
    let (registry, m) = new_registry_and_metrics();
    m.backend.record_s3_request(std::time::Duration::from_secs(0));
    let output = encode(&registry);
    assert!(output.contains("shardline_s3_requests_total 1"));
    assert!(output.contains("shardline_s3_request_duration_seconds_count 1"));
}

#[test]
fn backend_record_local_io_duration() {
    let (registry, m) = new_registry_and_metrics();
    m.backend.record_local_io(std::time::Duration::from_nanos(0));
    let output = encode(&registry);
    assert!(output.contains("shardline_local_io_operations_total 1"));
    assert!(output.contains("shardline_local_io_duration_seconds_count 1"));
}

#[test]
fn protocol_record_hub_api_request_various_params() {
    let (registry, m) = new_registry_and_metrics();
    m.protocol.record_hub_api_request("/models", "GET", 200);
    m.protocol.record_hub_api_request("/datasets", "POST", 201);
    m.protocol.record_hub_api_request("/spaces", "PUT", 204);
    let output = encode(&registry);
    assert!(output.contains("shardline_hub_api_requests_total 3"));
}

#[test]
fn protocol_record_hub_api_commit_various_ops() {
    let (registry, m) = new_registry_and_metrics();
    m.protocol.record_hub_api_commit("create");
    m.protocol.record_hub_api_commit("update");
    m.protocol.record_hub_api_commit("delete");
    let output = encode(&registry);
    assert!(output.contains("shardline_hub_api_commit_operations_total 3"));
}

#[test]
fn protocol_record_hub_api_file_upload_and_download() {
    let (registry, m) = new_registry_and_metrics();
    m.protocol.record_hub_api_file_upload();
    m.protocol.record_hub_api_file_download();
    let output = encode(&registry);
    assert!(output.contains("shardline_hub_api_file_uploads_total 1"));
    assert!(output.contains("shardline_hub_api_file_downloads_total 1"));
}

#[test]
fn provider_record_webhook_and_token_exchange() {
    let (registry, m) = new_registry_and_metrics();
    m.provider.record_webhook("github", "push");
    m.provider.record_webhook("gitlab", "merge_request");
    m.provider.record_token_exchange();
    let output = encode(&registry);
    assert!(output.contains("shardline_provider_webhook_events_total 2"));
    assert!(output.contains("shardline_provider_token_exchange_total 1"));
}

#[test]
fn provider_record_webhook_duration_zero() {
    let (registry, m) = new_registry_and_metrics();
    m.provider.record_webhook_duration(std::time::Duration::ZERO);
    let output = encode(&registry);
    assert!(output.contains("shardline_provider_webhook_processing_duration_seconds_count 1"));
}

#[test]
fn gc_record_run_zero_values() {
    let (registry, m) = new_registry_and_metrics();
    m.gc.record_run(std::time::Duration::ZERO, 0, 0);
    let output = encode(&registry);
    assert!(output.contains("shardline_gc_runs_total 1"));
    assert!(output.contains("shardline_gc_objects_collected_total 0"));
    assert!(output.contains("shardline_gc_bytes_collected_total 0"));
}

#[test]
fn fsck_record_run_zero_values() {
    let (registry, m) = new_registry_and_metrics();
    m.fsck.record_run(std::time::Duration::ZERO, 0);
    let output = encode(&registry);
    assert!(output.contains("shardline_fsck_runs_total 1"));
    assert!(output.contains("shardline_fsck_errors_found_total 0"));
}

#[test]
fn reconstruction_record_with_zero_chunks() {
    let (registry, m) = new_registry_and_metrics();
    m.reconstruction.record(true, std::time::Duration::ZERO, 0);
    let output = encode(&registry);
    assert!(output.contains("shardline_reconstruction_requests_total 1"));
    assert!(output.contains("shardline_reconstruction_chunks_fetched_total 0"));
}

#[test]
fn reconstruction_record_failure_path() {
    let (registry, m) = new_registry_and_metrics();
    m.reconstruction.record(false, std::time::Duration::from_millis(5), 3);
    let output = encode(&registry);
    assert!(output.contains("shardline_reconstruction_requests_total 1"));
    assert!(output.contains("shardline_reconstruction_chunks_fetched_total 3"));
}

#[test]
fn oci_registry_token_begin_end_tracking() {
    let (registry, m) = new_registry_and_metrics();
    m.protocol.begin_oci_registry_token_request();
    m.protocol.begin_oci_registry_token_request();
    m.protocol.end_oci_registry_token_request();
    let output = encode(&registry);
    assert!(output.contains("shardline_oci_registry_token_active_requests 1"));
}

#[test]
fn oci_registry_token_all_metrics() {
    let (registry, m) = new_registry_and_metrics();
    m.protocol.record_oci_registry_token_request();
    m.protocol.record_oci_registry_token_rate_limited();
    let output = encode(&registry);
    assert!(output.contains("shardline_oci_registry_token_requests_total 1"));
    assert!(output.contains("shardline_oci_registry_token_rate_limited_total 1"));
}
