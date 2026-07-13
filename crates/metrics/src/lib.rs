#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string
    )
)]

pub mod backend;
pub mod fsck;
pub mod gc;
pub mod middleware;
pub mod protocol;
pub mod provider;
pub mod reconstruction;
pub mod storage;
pub mod system;
pub mod transfer;
pub mod xet;

#[cfg(test)]
mod tests;

use std::sync::LazyLock;

use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Registry, TextEncoder};

use backend::StorageBackendMetrics;
use fsck::FsckMetrics;
use gc::GcMetrics;
use protocol::ProtocolMetrics;
use provider::ProviderMetrics;
use reconstruction::ReconstructionMetrics;
use storage::StorageMetrics;
use system::SystemMetrics;
use transfer::TransferMetrics;
use xet::XetMetrics;

// ── Infallible metric constructors ────────────────────────────────────────

/// Creates an `IntCounter`, aborting if the name is invalid.
#[must_use]
fn must_counter(name: &str, help: &str) -> IntCounter {
    IntCounter::new(name, help).unwrap_or_else(|_| std::process::abort())
}

/// Creates an `IntGauge`, aborting if the name is invalid.
#[must_use]
fn must_gauge(name: &str, help: &str) -> IntGauge {
    IntGauge::new(name, help).unwrap_or_else(|_| std::process::abort())
}

/// Creates a `Histogram` from options, aborting if the metric name is invalid.
#[must_use]
fn must_histogram(opts: HistogramOpts) -> Histogram {
    Histogram::with_opts(opts).unwrap_or_else(|_| std::process::abort())
}

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
    encoder
        .encode_to_string(&metric_families)
        .unwrap_or_default()
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
    metrics()
        .gc
        .record_run(duration, objects_collected, bytes_collected);
}

pub fn record_fsck_run(duration: std::time::Duration, errors_found: u64) {
    metrics().fsck.record_run(duration, errors_found);
}

pub fn record_hub_api_request(endpoint: &str, method: &str, status: u16) {
    metrics()
        .protocol
        .record_hub_api_request(endpoint, method, status);
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
