use super::types::{QuarantineRepairAction, WebhookDeliveryRepairAction};

pub(crate) const fn classify_quarantine_repair_action(
    object_exists: bool,
    is_reachable: bool,
    is_held: bool,
) -> QuarantineRepairAction {
    if !object_exists {
        return QuarantineRepairAction::DeleteMissing;
    }
    if is_reachable {
        return QuarantineRepairAction::DeleteReachable;
    }
    if is_held {
        return QuarantineRepairAction::DeleteHeld;
    }
    QuarantineRepairAction::Keep
}

pub(crate) const fn classify_webhook_delivery_repair_action(
    processed_at_unix_seconds: u64,
    stale_cutoff_unix_seconds: u64,
    max_processed_at_unix_seconds: u64,
) -> WebhookDeliveryRepairAction {
    if processed_at_unix_seconds > max_processed_at_unix_seconds {
        return WebhookDeliveryRepairAction::DeleteFuture;
    }
    if processed_at_unix_seconds <= stale_cutoff_unix_seconds {
        return WebhookDeliveryRepairAction::DeleteStale;
    }
    WebhookDeliveryRepairAction::Keep
}
