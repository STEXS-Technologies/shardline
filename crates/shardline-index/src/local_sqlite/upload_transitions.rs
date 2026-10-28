//! Upload state and evidence share one durability boundary per supplied batch.

use rusqlite::{TransactionBehavior, params};
use shardline_protocol::unix_now_seconds_lossy;
use shardline_reliability::LifecycleEvent;

use super::{
    LocalIndexStore, LocalIndexStoreError, helpers, index_store::load_verified_sqlite_intent,
    u64_to_i64, upload_recovery_guard,
};
use crate::UploadIntentState;

pub(super) fn commit(
    store: &LocalIndexStore,
    intent_id: &str,
    transitions: &[(UploadIntentState, LifecycleEvent)],
) -> Result<bool, LocalIndexStoreError> {
    let mut connection = store.open_connection()?;
    // Acquire the writer before reading; concurrent duplicates must never
    // upgrade an obsolete WAL snapshot or publish from an unchecked head.
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    for (next, event) in transitions {
        let Some(current) = load_verified_sqlite_intent(&transaction, intent_id)? else {
            return Ok(false);
        };
        if current.state() == *next {
            continue;
        }
        if !current.state().can_transition_to(*next) {
            return Ok(false);
        }
        event.validate_for_transition(intent_id, current.state(), *next)?;
        let marked =
            upload_recovery_guard::enter(&transaction, intent_id, &event.operation.repository)?;
        let now = u64_to_i64(unix_now_seconds_lossy())?;
        let rows = transaction.execute(
            "UPDATE shardline_upload_intents SET state = ?1, updated_at_unix_seconds = ?2
             WHERE intent_id = ?3 AND state = ?4",
            params![next.as_str(), now, intent_id, current.state().as_str()],
        )?;
        if rows == 0 {
            return Ok(false);
        }
        helpers::persist_reliability_event_at(&transaction, event, now)?;
        let stored: String = transaction.query_row(
            "SELECT event_json FROM shardline_reliability_events
             WHERE operation_kind = ?1 AND operation_id = ?2 AND sequence = ?3",
            params![
                event.operation.kind.as_str(),
                event.operation.operation_id,
                u64_to_i64(event.sequence)?
            ],
            |row| row.get(0),
        )?;
        if stored != serde_json::to_string(event)? {
            return Err(LocalIndexStoreError::ReliabilityEventConflict(
                event.operation.operation_id.clone(),
            ));
        }
        if marked {
            upload_recovery_guard::leave(&transaction, intent_id)?;
        }
    }
    transaction.commit()?;
    Ok(true)
}

#[cfg(test)]
mod tests;
