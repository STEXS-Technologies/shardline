#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string
    )
)]

pub mod storage;
pub mod transfer;
pub mod xet;
pub mod protocol;
pub mod reconstruction;
pub mod gc;
pub mod fsck;
pub mod backend;
pub mod provider;
pub mod system;
pub mod middleware;

use std::sync::LazyLock;

use prometheus::{Registry, TextEncoder};

use storage::StorageMetrics;
use transfer::TransferMetrics;
use xet::XetMetrics;
use protocol::ProtocolMetrics;
use reconstruction::ReconstructionMetrics;
use gc::GcMetrics;
use fsck::FsckMetrics;
use backend::StorageBackendMetrics;
use provider::ProviderMetrics;
use system::SystemMetrics;

/// Central metrics registry for the entire Shardline CAS backend.
pub struct CasMetrics {
    pub storage: StorageMetrics,
    pub transfer: TransferMetrics,
    pub xet: XetMetrics,
    pub protocol: ProtocolMetrics,
    pub reconstruction: ReconstructionMetrics,
    pub gc: GcMetrics,
    pub fsck: FsckMetrics,
    pub backend: StorageBackendMetrics,
    pub provider: ProviderMetrics,
    pub system: SystemMetrics,
}

static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);
static METRICS: LazyLock<CasMetrics> = LazyLock::new(|| CasMetrics::new(&REGISTRY));

impl CasMetrics {
    /// # Panics
    ///
    /// Panics if prometheus metric registration fails (should not happen with static names).
    #[must_use]
    pub fn new(registry: &Registry) -> Self {
        Self {
            storage: StorageMetrics::new(registry),
            transfer: TransferMetrics::new(registry),
            xet: XetMetrics::new(registry),
            protocol: ProtocolMetrics::new(registry),
            reconstruction: ReconstructionMetrics::new(registry),
            gc: GcMetrics::new(registry),
            fsck: FsckMetrics::new(registry),
            backend: StorageBackendMetrics::new(registry),
            provider: ProviderMetrics::new(registry),
            system: SystemMetrics::new(registry),
        }
    }
}

/// Returns a reference to the global metrics instance.
#[must_use]
pub fn metrics() -> &'static CasMetrics {
    &METRICS
}

/// Returns the Prometheus registry for scraping.
#[must_use]
pub fn registry() -> &'static Registry {
    &REGISTRY
}

/// Renders all metrics in Prometheus exposition format.
#[must_use]
pub fn encode_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    encoder.encode_to_string(&metric_families).unwrap_or_default()
}

// ── Convenience free functions ──────────────────────────────────────────

pub fn record_upload(protocol: &str, bytes: u64) {
    metrics().transfer.record_upload(protocol, bytes);
}

pub fn record_download(protocol: &str, bytes: u64) {
    metrics().transfer.record_download(protocol, bytes);
}

pub fn record_xet_shard_upload(bytes: u64) {
    metrics().xet.record_shard_upload(bytes);
}

pub fn record_xet_xorb_upload(bytes: u64) {
    metrics().xet.record_xorb_upload(bytes);
}

pub fn record_xet_xorb_download(bytes: u64) {
    metrics().xet.record_xorb_download(bytes);
}

pub fn record_xet_reconstruction(ok: bool, duration: std::time::Duration, chunks: u64) {
    metrics().xet.record_reconstruction(ok, duration, chunks);
}

pub fn record_xet_dedupe_shard_query(hit: bool) {
    metrics().xet.record_dedupe_shard_query(hit);
}

pub fn record_reconstruction(ok: bool, duration: std::time::Duration, chunks: u64) {
    metrics().reconstruction.record(ok, duration, chunks);
}

pub fn record_reconstruction_cache_hit() {
    metrics().reconstruction.record_cache_hit();
}

pub fn record_reconstruction_cache_miss() {
    metrics().reconstruction.record_cache_miss();
}

pub fn record_gc_run(duration: std::time::Duration, objects_collected: u64, bytes_collected: u64) {
    metrics().gc.record_run(duration, objects_collected, bytes_collected);
}

pub fn record_fsck_run(duration: std::time::Duration, errors_found: u64) {
    metrics().fsck.record_run(duration, errors_found);
}

pub fn record_hub_api_request(endpoint: &str, method: &str, status: u16) {
    metrics().protocol.record_hub_api_request(endpoint, method, status);
}

pub fn record_hub_api_commit(operation: &str) {
    metrics().protocol.record_hub_api_commit(operation);
}

pub fn record_hub_api_file_upload() {
    metrics().protocol.record_hub_api_file_upload();
}

pub fn record_hub_api_file_download() {
    metrics().protocol.record_hub_api_file_download();
}

pub fn record_provider_webhook(provider: &str, event_type: &str) {
    metrics().provider.record_webhook(provider, event_type);
}

pub fn record_provider_token_exchange() {
    metrics().provider.record_token_exchange();
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Registry;

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
}
