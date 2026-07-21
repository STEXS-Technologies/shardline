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
    record_download("max-bytes", u64::MAX);
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
    assert!(
        !gathered.is_empty(),
        "registry returned zero metric families"
    );
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
// Zero-value edge cases (area 2: record functions with 0 bytes / 0 count)
// ---------------------------------------------------------------------------

#[test]
fn record_functions_all_zero_values_do_not_panic() {
    record_upload("zero", 0);
    record_download("zero", 0);
    record_xet_shard_upload(0);
    record_xet_xorb_upload(0);
    record_xet_xorb_download(0);
    record_xet_reconstruction(true, Duration::ZERO, 0);
    record_xet_dedupe_shard_query(true);
    record_reconstruction(true, Duration::ZERO, 0);
    record_reconstruction_cache_hit();
    record_reconstruction_cache_miss();
    record_gc_run(Duration::ZERO, 0, 0);
    record_fsck_run(Duration::ZERO, 0);
    record_hub_api_request("", "", 0);
    record_hub_api_commit("");
    record_hub_api_file_upload();
    record_hub_api_file_download();
    record_provider_webhook("", "");
    record_provider_token_exchange();
}

// ---------------------------------------------------------------------------
// Very-large-value edge cases (area 3: record functions with u64::MAX)
// ---------------------------------------------------------------------------

#[test]
fn record_functions_all_max_values_do_not_panic() {
    record_upload("max", u64::MAX);
    record_download("max", u64::MAX);
    record_xet_shard_upload(u64::MAX);
    record_xet_xorb_upload(u64::MAX);
    record_xet_xorb_download(u64::MAX);
    record_xet_reconstruction(true, Duration::MAX, u64::MAX);
    record_xet_dedupe_shard_query(true);
    record_reconstruction(true, Duration::MAX, u64::MAX);
    record_reconstruction_cache_hit();
    record_reconstruction_cache_miss();
    record_gc_run(Duration::MAX, u64::MAX, u64::MAX);
    record_fsck_run(Duration::MAX, u64::MAX);
    record_hub_api_request("/max", "POST", 999);
    record_hub_api_commit("max_operation");
    record_hub_api_file_upload();
    record_hub_api_file_download();
    record_provider_webhook("max-provider", "max-event");
    record_provider_token_exchange();
}

// ---------------------------------------------------------------------------
// encode_metrics – comprehensive format and content checks (area 4)
// ---------------------------------------------------------------------------

#[test]
fn encode_metrics_contains_all_metric_families() {
    // Trigger every public record_* function to ensure the corresponding
    // counter families have at least one sample.
    record_upload("full", 1);
    record_download("full", 1);
    record_xet_shard_upload(1);
    record_xet_xorb_upload(1);
    record_xet_xorb_download(1);
    record_xet_reconstruction(true, Duration::from_secs(1), 1);
    record_xet_dedupe_shard_query(true);
    record_reconstruction(true, Duration::from_secs(1), 1);
    record_reconstruction_cache_hit();
    record_reconstruction_cache_miss();
    record_gc_run(Duration::from_secs(1), 1, 1);
    record_fsck_run(Duration::from_secs(1), 1);
    record_hub_api_request("/full", "GET", 200);
    record_hub_api_commit("full");
    record_hub_api_file_upload();
    record_hub_api_file_download();
    record_provider_webhook("full", "full");
    record_provider_token_exchange();

    let output = encode_metrics();
    assert!(!output.is_empty(), "encode_metrics returned empty string");

    // Every metric family name that results from the free functions above.
    let families = &[
        // transfer
        "shardline_upload_requests_total",
        "shardline_upload_bytes_total",
        "shardline_download_requests_total",
        "shardline_download_bytes_total",
        // xet
        "shardline_xet_shard_uploads_total",
        "shardline_xet_shard_upload_bytes_total",
        "shardline_xet_xorb_uploads_total",
        "shardline_xet_xorb_upload_bytes_total",
        "shardline_xet_xorb_downloads_total",
        "shardline_xet_reconstruction_requests_total",
        "shardline_xet_dedupe_shard_queries_total",
        "shardline_xet_dedupe_shard_hits_total",
        // reconstruction
        "shardline_reconstruction_requests_total",
        "shardline_reconstruction_chunks_fetched_total",
        "shardline_reconstruction_cache_hits_total",
        "shardline_reconstruction_cache_misses_total",
        // gc
        "shardline_gc_runs_total",
        "shardline_gc_objects_collected_total",
        "shardline_gc_bytes_collected_total",
        // fsck
        "shardline_fsck_runs_total",
        "shardline_fsck_errors_found_total",
        // hub api
        "shardline_hub_api_requests_total",
        "shardline_hub_api_commit_operations_total",
        "shardline_hub_api_file_uploads_total",
        "shardline_hub_api_file_downloads_total",
        // provider
        "shardline_provider_webhook_events_total",
        "shardline_provider_token_exchange_total",
    ];

    for family in families {
        assert!(
            output.contains(family),
            "encode_metrics output missing {family}\n---\n{output}"
        );
    }

    // All metric families should have accompanying HELP and TYPE lines.
    assert!(
        output.contains("# HELP"),
        "output contains no HELP lines\n---\n{output}"
    );
    assert!(
        output.contains("# TYPE"),
        "output contains no TYPE lines\n---\n{output}"
    );

    // Every comment line should be well-formed: "# HELP <name> <doc>"
    // or "# TYPE <name> <type>".
    for line in output.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("# HELP ") || trimmed.starts_with("# TYPE ") {
            assert!(
                trimmed.splitn(4, ' ').count() >= 3,
                "malformed comment line: {trimmed:?}"
            );
        }
    }
}

#[test]
fn encode_metrics_lines_are_well_formed() {
    // Record a couple of distinct values so lines exist.
    record_upload("format-check", 42);
    record_download("format-check", 100);

    let output = encode_metrics();
    assert!(!output.is_empty(), "encode_metrics returned empty string");

    for line in output.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        // Non-comment lines must have at least a metric name and a value
        // separated by whitespace.
        assert!(
            trimmed.contains(' '),
            "unexpected line without space: {trimmed:?}"
        );

        // The last token should be a parseable numeric value (int or float).
        let value_part = trimmed
            .split(' ')
            .next_back()
            .expect("line should have at least one token");
        let _: f64 = value_part
            .parse()
            .unwrap_or_else(|_| panic!("last token is not numeric: {value_part:?}"));
    }
}

// ---------------------------------------------------------------------------
// metrics() structure verification after recording (area 5)
// ---------------------------------------------------------------------------

#[test]
fn metrics_sub_fields_accessible_after_recording() {
    // Prove that after calling record functions, the metrics() global
    // reference still yields valid sub-structs with accessible counters.
    record_upload("struct-check", 1);
    record_download("struct-check", 2);
    record_xet_shard_upload(3);
    record_gc_run(Duration::from_secs(1), 4, 5);
    record_fsck_run(Duration::from_secs(1), 6);

    let m = metrics();

    // Transfer counters are accessible after recording.
    let _ = m.transfer.upload_requests.get();
    let _ = m.transfer.upload_bytes.get();
    let _ = m.transfer.download_requests.get();
    let _ = m.transfer.download_bytes.get();

    // Xet counters are accessible.
    let _ = m.xet.shard_uploads.get();
    let _ = m.xet.shard_upload_bytes.get();
    let _ = m.xet.xorb_uploads.get();
    let _ = m.xet.xorb_upload_bytes.get();
    let _ = m.xet.xorb_downloads.get();
    let _ = m.xet.reconstruction_requests.get();
    let _ = m.xet.dedupe_shard_queries.get();
    let _ = m.xet.dedupe_shard_hits.get();

    // Histograms are accessible.
    let _ = &m.xet.reconstruction_duration;
    let _ = &m.xet.reconstruction_chunks;

    // GC counters are accessible.
    let _ = m.gc.runs.get();
    let _ = m.gc.objects_collected.get();
    let _ = m.gc.bytes_collected.get();

    // Fsck counters are accessible.
    let _ = m.fsck.runs.get();
    let _ = m.fsck.errors_found.get();

    // Reconstruction counters are accessible.
    let _ = m.reconstruction.requests.get();
    let _ = m.reconstruction.chunks_fetched.get();
    let _ = m.reconstruction.cache_hits.get();
    let _ = m.reconstruction.cache_misses.get();

    // Protocol counters are accessible.
    let _ = m.protocol.hub_api_requests.get();
    let _ = m.protocol.hub_api_commits.get();
    let _ = m.protocol.hub_api_file_uploads.get();
    let _ = m.protocol.hub_api_file_downloads.get();
    let _ = m.protocol.lfs_uploads.get();
    let _ = m.protocol.lfs_downloads.get();
    let _ = m.protocol.oci_uploads.get();
    let _ = m.protocol.oci_downloads.get();

    // Provider counters are accessible.
    let _ = m.provider.webhook_events.get();
    let _ = m.provider.token_exchanges.get();

    // System gauges are accessible.
    let _ = m.system.active_connections.get();
    let _ = m.system.server_uptime.get();

    // Backend counters are accessible.
    let _ = m.backend.s3_requests.get();
    let _ = m.backend.s3_errors.get();
    let _ = m.backend.local_io_operations.get();

    // Storage counters/gauges are accessible.
    let _ = m.storage.objects_total.get();
    let _ = m.storage.objects_bytes_total.get();
    let _ = m.storage.chunks_total.get();
    let _ = m.storage.chunks_bytes_total.get();
    let _ = m.storage.xorbs_total.get();
    let _ = m.storage.xorbs_bytes_total.get();
    let _ = m.storage.shards_total.get();
    let _ = m.storage.dedup_saves_bytes_total.get();

    // The encoded output should contain the metric names for the
    // groups we recorded.
    let output = encode_metrics();
    assert!(output.contains("shardline_upload_bytes_total"));
    assert!(output.contains("shardline_download_bytes_total"));
    assert!(output.contains("shardline_xet_shard_uploads_total"));
    assert!(output.contains("shardline_gc_runs_total"));
    assert!(output.contains("shardline_fsck_runs_total"));
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
