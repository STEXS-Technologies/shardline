/// Default retention for processed webhook delivery claims before repair prunes them.
pub const DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS: u64 = 2_592_000;

pub(crate) const WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS: u64 = 300;

pub(crate) mod types;
pub(crate) mod reachability;
pub(crate) mod classification;
pub(crate) mod orchestrator;

#[cfg(test)]
mod tests;

// Public API re-exports
pub use types::{LifecycleRepairOptions, LifecycleRepairReport};
pub use orchestrator::{run_lifecycle_repair, run_local_lifecycle_repair};
