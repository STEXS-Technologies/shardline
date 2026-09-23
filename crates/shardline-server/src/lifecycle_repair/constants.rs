/// Default retention for processed webhook delivery claims before repair prunes them.
pub const DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS: u64 = 2_592_000;

pub(crate) const WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS: u64 = 300;
