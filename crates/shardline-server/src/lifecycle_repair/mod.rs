/// Default retention for processed webhook delivery claims before repair prunes them.
pub const DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS: u64 = 2_592_000;

pub(crate) const WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS: u64 = 300;

pub(crate) mod classification;
mod fault_injection;
pub(crate) mod orchestrator;
pub(crate) mod reachability;
pub(crate) mod types;

#[cfg(test)]
mod tests;

// Public API re-exports
pub use orchestrator::{run_lifecycle_repair, run_local_lifecycle_repair};
pub use types::{LifecycleRepairBoundary, LifecycleRepairOptions, LifecycleRepairReport};
