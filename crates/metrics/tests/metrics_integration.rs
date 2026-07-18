//! Integration tests for `shardline-metrics`.
//!
//! These tests exercise the free-function convenience API and the global static
//! registry — call paths that the per-module unit tests do not cover (those use
//! fresh [`Registry`] instances per test).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use,
    clippy::format_push_string,
    clippy::panic
)]

use std::time::Duration;

use shardline_metrics::{
    encode_metrics, metrics, record_download, record_fsck_run, record_gc_run,
    record_hub_api_commit, record_hub_api_file_download, record_hub_api_file_upload,
    record_hub_api_request, record_provider_token_exchange, record_provider_webhook,
    record_reconstruction, record_reconstruction_cache_hit, record_reconstruction_cache_miss,
    record_upload, record_xet_dedupe_shard_query, record_xet_reconstruction,
    record_xet_shard_upload, record_xet_xorb_download, record_xet_xorb_upload, registry,
};

// ---------------------------------------------------------------------------
// record_upload / record_download
// ---------------------------------------------------------------------------

#[test]
fn record_upload_accepts_various_protocols_and_sizes() {
    // These should never panic regardless of input.
    record_upload("https", 0);
    record_upload("grpc", 1);
    record_upload("", u64::MAX);
    record_upload("local", 42);
}

#[test]
fn record_download_accepts_various_protocols_and_sizes() {
    record_download("https", 0);
    record_download("s3", 9_007_199_254_740_991);
    record_download("", 1);
}

// ---------------------------------------------------------------------------
// Xet metrics
// ---------------------------------------------------------------------------

#[test]
fn record_xet_shard_upload_accepts_zero_and_large() {
    record_xet_shard_upload(0);
    record_xet_shard_upload(u64::MAX);
}

#[test]
fn record_xet_xorb_upload_accepts_various_sizes() {
    record_xet_xorb_upload(0);
    record_xet_xorb_upload(1);
    record_xet_xorb_upload(10_000_000);
}

#[test]
fn record_xet_xorb_download_accepts_various_sizes() {
    record_xet_xorb_download(0);
    record_xet_xorb_download(u64::MAX);
}

#[test]
fn record_xet_reconstruction_accepts_ok_and_fail() {
    record_xet_reconstruction(true, Duration::from_secs(1), 5);
    record_xet_reconstruction(false, Duration::from_nanos(0), 0);
    record_xet_reconstruction(true, Duration::MAX, u64::MAX);
}

#[test]
fn record_xet_dedupe_shard_query_accepts_hit_and_miss() {
    record_xet_dedupe_shard_query(true);
    record_xet_dedupe_shard_query(false);
}

// ---------------------------------------------------------------------------
// Reconstruction metrics
// ---------------------------------------------------------------------------

#[test]
fn record_reconstruction_accepts_all_variants() {
    record_reconstruction(true, Duration::from_millis(50), 3);
    record_reconstruction(false, Duration::from_secs(10), u64::MAX);
}

#[test]
fn record_reconstruction_cache_hit_and_miss_do_not_panic() {
    record_reconstruction_cache_hit();
    record_reconstruction_cache_miss();
}

// ---------------------------------------------------------------------------
// GC / fsck metrics
// ---------------------------------------------------------------------------

#[test]
fn record_gc_run_accepts_various_inputs() {
    record_gc_run(Duration::from_secs(30), 100, 1024);
    record_gc_run(Duration::ZERO, 0, 0);
    record_gc_run(Duration::from_nanos(1), u64::MAX, u64::MAX);
}

#[test]
fn record_fsck_run_accepts_various_inputs() {
    record_fsck_run(Duration::from_secs(5), 3);
    record_fsck_run(Duration::ZERO, 0);
    record_fsck_run(Duration::MAX, u64::MAX);
}

// ---------------------------------------------------------------------------
// Hub API metrics
// ---------------------------------------------------------------------------

#[test]
fn record_hub_api_request_accepts_various_args() {
    record_hub_api_request("/api/v1/repos", "POST", 201);
    record_hub_api_request("", "", 0);
    record_hub_api_request("/health", "GET", 200);
}

#[test]
fn record_hub_api_commit_accepts_various_operations() {
    record_hub_api_commit("create");
    record_hub_api_commit("delete");
    record_hub_api_commit("");
}

#[test]
fn record_hub_api_file_upload_and_download_do_not_panic() {
    record_hub_api_file_upload();
    record_hub_api_file_download();
}

// ---------------------------------------------------------------------------
// Provider metrics
// ---------------------------------------------------------------------------

#[test]
fn record_provider_webhook_accepts_various_providers_and_events() {
    record_provider_webhook("github", "push");
    record_provider_webhook("gitlab", "merge_request");
    record_provider_webhook("", "");
}

#[test]
fn record_provider_token_exchange_do_not_panic() {
    record_provider_token_exchange();
}

// ---------------------------------------------------------------------------
// encode_metrics output
// ---------------------------------------------------------------------------

#[test]
fn encode_metrics_contains_expected_metric_names() {
    // Call a representative sample so the static initializer runs and metrics
    // are guaranteed to exist in the output.
    record_upload("test", 1);
    record_download("test", 1);
    record_xet_shard_upload(1);
    record_xet_xorb_download(1);
    record_hub_api_request("/test", "GET", 200);
    record_gc_run(Duration::from_secs(1), 1, 1);
    record_fsck_run(Duration::from_secs(1), 1);

    let output = encode_metrics();

    assert!(!output.is_empty(), "encode_metrics returned empty string");

    // Prometheus exposition format: each line starts with a metric name.
    assert!(
        output.contains("shardline_upload_bytes_total"),
        "missing upload_bytes_total metric\n---\n{output}"
    );
    assert!(
        output.contains("shardline_download_bytes_total"),
        "missing download_bytes_total metric"
    );
    assert!(
        output.contains("shardline_xet_shard_uploads_total"),
        "missing xet_shard_uploads_total metric"
    );
    assert!(
        output.contains("shardline_hub_api_requests_total"),
        "missing hub_api_requests_total metric"
    );
    assert!(
        output.contains("shardline_gc_runs_total"),
        "missing gc_runs_total metric"
    );
    assert!(
        output.contains("shardline_fsck_runs_total"),
        "missing fsck_runs_total metric"
    );

    // Every line should be valid Prometheus text — lines start with a metric
    // name, end with a value, or are comments / blank lines.
    for line in output.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        // Non-comment lines contain a value at the end (after last space).
        assert!(
            trimmed.contains(' '),
            "unexpected line without space: {trimmed:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// metrics() / registry()
// ---------------------------------------------------------------------------

#[test]
fn metrics_returns_valid_reference() {
    let m = metrics();
    // Access each sub-metric group to prove the reference is live.
    let _ = &m.storage;
    let _ = &m.transfer;
    let _ = &m.xet;
    let _ = &m.protocol;
    let _ = &m.reconstruction;
    let _ = &m.gc;
    let _ = &m.fsck;
    let _ = &m.backend;
    let _ = &m.provider;
    let _ = &m.system;
}

#[test]
fn registry_returns_valid_registry() {
    // Ensure METRICS lazy initialization by triggering a recording function.
    record_upload("test", 1);
    let gathered = registry().gather();
    assert!(!gathered.is_empty(), "registry returned zero metric families");
}

// ---------------------------------------------------------------------------
// Concurrent access
// ---------------------------------------------------------------------------

#[test]
fn concurrent_recording_does_not_panic() {
    let mut handles = Vec::new();

    for i in 0..10_u64 {
        handles.push(std::thread::spawn(move || {
            record_upload("concurrent", i);
            record_download("concurrent", i);
            record_xet_shard_upload(i);
            record_xet_dedupe_shard_query(i % 2 == 0);
            record_reconstruction(true, Duration::from_millis(i), i);
            record_hub_api_request("/test", "GET", 200);
            record_gc_run(Duration::from_secs(i), i, i * 2);
        }));
    }

    for (idx, handle) in handles.into_iter().enumerate() {
        handle
            .join()
            .unwrap_or_else(|_| panic!("concurrent thread {idx} panicked"));
    }
}

#[test]
fn concurrent_mixed_record_and_encode_does_not_panic() {
    let record_handle = std::thread::spawn(|| {
        for i in 0..50_u64 {
            record_upload("mixed", i);
            record_download("mixed", i);
        }
    });

    let encode_handle = std::thread::spawn(|| {
        for _ in 0..10 {
            let _output = encode_metrics();
        }
    });

    record_handle.join().expect("record thread panicked");
    encode_handle.join().expect("encode thread panicked");
}

// ---------------------------------------------------------------------------
// Edge cases for high-cardinality label-like arguments
// ---------------------------------------------------------------------------

#[test]
fn record_upload_with_long_protocol_name() {
    let long_protocol = "a".repeat(10_000);
    record_upload(&long_protocol, 1);
}

#[test]
fn record_download_with_long_protocol_name() {
    let long_protocol = "b".repeat(10_000);
    record_download(&long_protocol, 1);
}
