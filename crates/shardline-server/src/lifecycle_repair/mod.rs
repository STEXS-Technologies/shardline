/// Default retention for processed webhook delivery claims before repair prunes them.
mod constants;
pub use constants::DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS;

pub(crate) use constants::WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS;

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
