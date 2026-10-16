//! Shared operator view of pending attempts; inspection grants no execution authority.
use crate::ServerError;
use serde::Serialize;

/// A bounded page for CLI or administrative inspection.
#[derive(Serialize)]
pub struct PendingDeleteInspectionPage {
    /// Whether this backend and account support authoritative inspection.
    pub supported: bool,
    /// Why an empty page may be inconclusive.
    pub inspection_notice: Option<&'static str>,
    /// Pending objects with their original attempt and recovery guidance.
    pub pending_deletes: Vec<serde_json::Value>,
    /// Attempt epoch to pass to the next read with the same configuration.
    pub next_after: Option<String>,
}

/// Formats an already-read page without settling markers or issuing storage requests.
/// Supply at most `limit + 1` rows from `PostgresIndexStore::pending_remote_deletes`.
/// `None` means unavailable inspection, rather than an empty authoritative result.
///
/// # Errors
/// Returns invalid page bounds or an oversized input page.
pub fn pending_delete_inspection_page(
    pending: Option<Vec<(String, u64)>>,
    limit: usize,
) -> Result<PendingDeleteInspectionPage, ServerError> {
    if !(1..=100).contains(&limit)
        || pending
            .as_ref()
            .is_some_and(|rows| rows.len() > limit.saturating_add(1))
    {
        return Err(ServerError::InvalidPath);
    }
    let supported = pending.is_some();
    let inspection_notice = (!supported).then_some(
        "Pending-delete inspection is unavailable for this backend or database account. An empty list does not prove that no deletes are pending. Check the original GC command and logs before retrying uncertain work.",
    );
    let mut pending = pending.unwrap_or_default();
    let has_more = pending.len() > limit;
    pending.truncate(limit);
    let next_after = has_more
        .then(|| pending.last().map(|(_, epoch)| epoch.to_string()))
        .flatten();
    let pending_deletes = pending.into_iter().map(|(key, epoch)| serde_json::json!({
        "object_key": key, "attempt": epoch.to_string(), "state": "pending",
        "reason": "delete has not been proved settled",
        "next_step": "Check whether the original GC is still running and read its logs. After it stops, run shardline repair with the same configuration to recover saved replies, then inspect shardline history --pending-deletes again. A backlog may need several repair runs. Missing or invalid replies remain blocked; object absence alone cannot clear them.",
    })).collect();
    Ok(PendingDeleteInspectionPage {
        supported,
        inspection_notice,
        pending_deletes,
        next_after,
    })
}
