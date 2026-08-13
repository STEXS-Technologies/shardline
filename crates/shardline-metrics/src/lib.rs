//! Metrics collection for shardline.
//!
//! All server components report through this crate's shared Prometheus
//! registry. The global instance is created lazily and registered exactly
//! once, so recording is always safe to call.
//!
//! # Quick start
//!
//! ```
//! use shardline_metrics::metrics;
//!
//! // The global metrics instance is always available and never fails.
//! let m = metrics();
//! m.system.set_uptime(1_700_000_000);
//!
//! // Render everything in Prometheus exposition format for scraping.
//! let text = shardline_metrics::encode_metrics();
//! assert!(text.contains("shardline_server_uptime_seconds"));
//! ```
//!
//! # Error handling convention
//!
//! All metrics use prometheus counters/gauges/histograms registered in a shared
//! registry. `registry.register(...)` returns an error only when the same metric
//! name is registered twice — which cannot happen in normal operation because
//! each metric is registered exactly once at module init via `lazy_static!`.
//!
//! Errors are intentionally discarded with `.ok()`: metrics must be best-effort
//! and must never cause a crash or abort program flow. Duplicate registration
//! (should it occur) is benign and produces no incorrect behavior.

#![deny(unsafe_code)]
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
pub mod recorders;
pub mod storage;
pub mod system;
pub mod transfer;
pub mod xet;

pub use recorders::*;

#[cfg(test)]
mod tests;
