use std::collections::HashSet;

use super::DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS;

/// Lifecycle-repair execution options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifecycleRepairOptions {
    /// Retention applied to processed webhook-delivery claims before they become repairable.
    pub webhook_retention_seconds: u64,
}

impl Default for LifecycleRepairOptions {
    fn default() -> Self {
        Self {
            webhook_retention_seconds: DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS,
        }
    }
}

/// Lifecycle-repair report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleRepairReport {
    /// Number of file and file-version records scanned to derive reachability.
    pub scanned_records: u64,
    /// Number of distinct object keys found reachable from current live metadata.
    pub referenced_objects: u64,
    /// Number of quarantine candidates inspected.
    pub scanned_quarantine_candidates: u64,
    /// Number of quarantine candidates removed because the object was already missing.
    pub removed_missing_quarantine_candidates: u64,
    /// Number of quarantine candidates removed because the object was reachable again.
    pub removed_reachable_quarantine_candidates: u64,
    /// Number of quarantine candidates removed because an active hold protected the object.
    pub removed_held_quarantine_candidates: u64,
    /// Number of retention holds inspected.
    pub scanned_retention_holds: u64,
    /// Number of expired retention holds removed.
    pub removed_expired_retention_holds: u64,
    /// Number of retention holds removed because the protected object was already missing.
    pub removed_missing_retention_holds: u64,
    /// Number of webhook delivery claims inspected.
    pub scanned_webhook_deliveries: u64,
    /// Number of webhook delivery claims removed because they were older than the configured retention.
    pub removed_stale_webhook_deliveries: u64,
    /// Number of webhook delivery claims removed because they were far in the future.
    pub removed_future_webhook_deliveries: u64,
}

#[derive(Debug, Default)]
pub(crate) struct RepairReachability {
    pub referenced_object_keys: HashSet<String>,
    pub live_dedupe_chunk_hashes: HashSet<String>,
    pub scanned_records: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuarantineRepairAction {
    Keep,
    DeleteMissing,
    DeleteReachable,
    DeleteHeld,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetentionHoldRepairAction {
    Keep,
    DeleteExpired,
    DeleteMissing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WebhookDeliveryRepairAction {
    Keep,
    DeleteStale,
    DeleteFuture,
}
