//! Keep completion events and their final authoritative row in one transaction.

use shardline_reliability::{
    LifecycleEvent, OperationKind, ReliabilityError, verify_upload_lifecycle_head,
};
use sqlx::Row;

use super::{
    PostgresIndexStore, PostgresMetadataStoreError, insert_reliability_event,
    load_postgres_latest_evidence_event, upload_recovery_guard,
};
use crate::UploadIntentState;

pub(super) async fn commit(
    store: &PostgresIndexStore,
    intent_id: &str,
    transitions: &[(UploadIntentState, LifecycleEvent)],
) -> Result<bool, PostgresMetadataStoreError> {
    let mut transaction = store.pool.begin().await?;
    // Serialize duplicate completions before loading the evidence head. Both
    // reads use the same transaction and no competing writer can advance it.
    let Some(row) = sqlx::query(
        "SELECT object_key, object_hash, object_length, created_at, updated_at, state FROM shardline_upload_intents
         WHERE intent_id = $1 FOR UPDATE",
    )
    .bind(intent_id)
    .fetch_optional(&mut *transaction)
    .await?
    else {
        return Ok(false);
    };
    let key: String = row.try_get("object_key")?;
    let hash: String = row.try_get("object_hash")?;
    // Preserve the validation performed by the public intent reader even when
    // the caller invokes the transactional batch directly.
    let _length = super::i64_to_u64(row.try_get("object_length")?)?;
    let created: chrono::DateTime<chrono::Utc> = row.try_get("created_at")?;
    let updated: chrono::DateTime<chrono::Utc> = row.try_get("updated_at")?;
    let _created = super::i64_to_u64(created.timestamp())?;
    let _updated = super::i64_to_u64(updated.timestamp())?;
    let text: String = row.try_get("state")?;
    let mut current = UploadIntentState::parse(&text)
        .ok_or(PostgresMetadataStoreError::InvalidUploadIntentState(text))?;
    let head: LifecycleEvent = serde_json::from_value(
        load_postgres_latest_evidence_event(&mut *transaction, OperationKind::Upload, intent_id)
            .await?
            .ok_or(PostgresMetadataStoreError::Reliability(
                ReliabilityError::OperationMismatch,
            ))?,
    )?;
    verify_upload_lifecycle_head(
        &head,
        &head.operation.tenant,
        &head.operation.repository,
        intent_id,
        &key,
        &hash,
        current,
    )?;
    for (next, event) in transitions {
        if current == *next {
            continue;
        }
        if !current.can_transition_to(*next) {
            return Ok(false);
        }
        event.validate_for_transition(intent_id, current, *next)?;
        let previous =
            upload_recovery_guard::enter(&mut transaction, intent_id, &event.operation.repository)
                .await?;
        let rows = sqlx::query(
            "UPDATE shardline_upload_intents SET state = $1, updated_at = now()
             WHERE intent_id = $2 AND state = $3",
        )
        .bind(next.as_str())
        .bind(intent_id)
        .bind(current.as_str())
        .execute(&mut *transaction)
        .await?;
        if rows.rows_affected() != 1 {
            return Ok(false);
        }
        insert_reliability_event(transaction.as_mut(), event).await?;
        upload_recovery_guard::leave(&mut transaction, previous.as_deref()).await?;
        current = *next;
    }
    transaction.commit().await?;
    Ok(true)
}

#[cfg(test)]
mod tests;
