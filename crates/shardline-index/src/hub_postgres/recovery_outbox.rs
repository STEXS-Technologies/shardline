use super::PostgresIndexStore;
use crate::hub::HubRefChange;
use crate::hub_recovery::{
    HubRecoveryError, HubRecoveryMode, HubRecoveryOutboxItem, HubRecoveryOutboxStore,
    MAX_HUB_REF_EVENT_BYTES, build_hub_ref_batch_event, decode_hub_ref_event,
    validate_mode_transition,
};
use sqlx::{Postgres, Row, Transaction};

/// Appends a complete ref batch using the publication transaction's repository
/// lock, sequence allocation, and commit boundary.
pub(super) async fn append_ref_batch(
    tx: &mut Transaction<'_, Postgres>,
    repository_id: &str,
    changes: &[HubRefChange],
) -> Result<(), sqlx::Error> {
    if changes.is_empty() {
        return Ok(());
    }
    let config = sqlx::query(
        "SELECT mode, deployment_id FROM shardline_hub_recovery_config WHERE singleton_id = 1 FOR SHARE",
    )
    .fetch_one(&mut **tx)
    .await?;
    let mode: String = config.try_get("mode")?;
    if mode == HubRecoveryMode::Off.as_str() {
        return Ok(());
    }
    if mode != HubRecoveryMode::Shadow.as_str() {
        return Err(sqlx::Error::Protocol("invalid Hub recovery mode".into()));
    }
    let deployment_id: Option<String> = config.try_get("deployment_id")?;
    let deployment_id = deployment_id
        .ok_or_else(|| sqlx::Error::Protocol("SHADOW mode has no deployment ID".into()))?;
    let sequence: i64 = sqlx::query_scalar(
        "INSERT INTO shardline_hub_recovery_sequences (repo_id, last_sequence, last_archived_sequence)
         VALUES ($1, 1, 0)
         ON CONFLICT (repo_id) DO UPDATE SET last_sequence = shardline_hub_recovery_sequences.last_sequence + 1
         RETURNING last_sequence",
    )
    .bind(repository_id)
    .fetch_one(&mut **tx)
    .await?;
    let sequence = u64::try_from(sequence)
        .map_err(|_conversion_error| sqlx::Error::Protocol("Hub sequence was negative".into()))?;
    let transaction_id = format!(
        "hub-ref:{}:{sequence}",
        hex::encode(repository_id.as_bytes())
    );
    let event = build_hub_ref_batch_event(
        &deployment_id,
        repository_id,
        sequence,
        &transaction_id,
        changes,
    )
    .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
    let event_json =
        serde_json::to_string(&event).map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
    if event_json.len() > MAX_HUB_REF_EVENT_BYTES {
        return Err(sqlx::Error::Protocol(
            "Hub outbox event exceeds byte limit".into(),
        ));
    }
    sqlx::query(
        "INSERT INTO shardline_hub_recovery_outbox
         (repo_id, domain_sequence, transaction_id, event_json, created_at_unix_seconds)
         VALUES ($1, $2, $3, $4::jsonb, EXTRACT(EPOCH FROM now())::bigint)",
    )
    .bind(repository_id)
    .bind(
        i64::try_from(sequence)
            .map_err(|_conversion_error| sqlx::Error::Protocol("Hub sequence overflow".into()))?,
    )
    .bind(transaction_id)
    .bind(event_json)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[async_trait::async_trait]
impl HubRecoveryOutboxStore for PostgresIndexStore {
    async fn set_hub_recovery_mode(
        &self,
        mode: HubRecoveryMode,
        deployment_id: Option<&str>,
    ) -> Result<(), HubRecoveryError> {
        let deployment_id = match (mode, deployment_id) {
            (HubRecoveryMode::Off, None) => None,
            (HubRecoveryMode::Shadow, Some(value))
                if !value.is_empty()
                    && value.len() <= 256
                    && !value.chars().any(char::is_control) =>
            {
                Some(value)
            }
            _ => {
                return Err(HubRecoveryError::Outbox(
                    "shadow mode requires a valid stable deployment ID; OFF takes no ID".into(),
                ));
            }
        };
        let deployment_id = deployment_id.map(ToOwned::to_owned);
        let result = async {
            let mut tx = self.pool().begin().await?;
            sqlx::query(
                "INSERT INTO shardline_hub_recovery_config (singleton_id, mode, deployment_id)
                 VALUES (1, 'off', NULL) ON CONFLICT (singleton_id) DO NOTHING",
            )
            .execute(&mut *tx)
            .await?;
            let config = sqlx::query(
                "SELECT mode, deployment_id FROM shardline_hub_recovery_config WHERE singleton_id = 1 FOR UPDATE",
            )
            .fetch_one(&mut *tx)
            .await?;
            let current_mode: String = config.try_get("mode")?;
            let current_deployment: Option<String> = config.try_get("deployment_id")?;
            let has_history: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM shardline_hub_recovery_outbox)",
            )
            .fetch_one(&mut *tx)
            .await?;
            validate_mode_transition(
                &current_mode,
                current_deployment.as_deref(),
                has_history,
                mode,
                deployment_id.as_deref(),
            )
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
            sqlx::query(
                "UPDATE shardline_hub_recovery_config SET mode = $1, deployment_id = $2 WHERE singleton_id = 1",
            )
            .bind(mode.as_str())
            .bind(deployment_id)
            .execute(&mut *tx)
            .await?;
            tx.commit().await
        }
        .await;
        result.map_err(|error: sqlx::Error| HubRecoveryError::Outbox(error.to_string()))?;
        Ok(())
    }

    async fn next_pending_hub_ref_event(
        &self,
        repository_id: &str,
    ) -> Result<Option<HubRecoveryOutboxItem>, HubRecoveryError> {
        let result = async {
            let state = sqlx::query(
                "SELECT last_sequence, last_archived_sequence FROM shardline_hub_recovery_sequences WHERE repo_id = $1",
            )
            .bind(repository_id)
            .fetch_optional(self.pool())
            .await?;
            let Some(state) = state else {
                return Ok(None);
            };
            let last_sequence: i64 = state.try_get("last_sequence")?;
            let archived_sequence: i64 = state.try_get("last_archived_sequence")?;
            if archived_sequence >= last_sequence {
                return Ok(None);
            }
            let expected = archived_sequence
                .checked_add(1)
                .ok_or_else(|| sqlx::Error::Protocol("Hub sequence overflow".into()))?;
            let row = sqlx::query(
                "SELECT transaction_id, event_json::text AS event_json FROM shardline_hub_recovery_outbox
                 WHERE repo_id = $1 AND domain_sequence = $2 AND archived_at_unix_seconds IS NULL",
            )
            .bind(repository_id)
            .bind(expected)
            .fetch_optional(self.pool())
            .await?
            .ok_or_else(|| sqlx::Error::Protocol("Hub outbox sequence gap".into()))?;
            let transaction_id: String = row.try_get("transaction_id")?;
            let event_json: String = row.try_get("event_json")?;
            if event_json.len() > MAX_HUB_REF_EVENT_BYTES {
                return Err(sqlx::Error::Protocol("Hub outbox event exceeds byte limit".into()));
            }
            decode_hub_ref_event(repository_id, expected, transaction_id, &event_json)
                .map(Some)
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))
        }
        .await;
        result.map_err(|error: sqlx::Error| HubRecoveryError::Outbox(error.to_string()))
    }

    async fn mark_hub_ref_event_archived(
        &self,
        item: &HubRecoveryOutboxItem,
        archive: &crate::HubRecoveryArchiveObject,
    ) -> Result<(), HubRecoveryError> {
        item.verify_identity()?;
        if archive.event != item.event
            || archive.checksum_blake3.len() != 64
            || !archive
                .checksum_blake3
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(HubRecoveryError::IntegrityMismatch);
        }
        let result = async {
            let mut tx = self.pool().begin().await?;
            let archived: Option<i64> = sqlx::query_scalar(
                "SELECT last_archived_sequence + 1 FROM shardline_hub_recovery_sequences
                 WHERE repo_id = $1 AND last_sequence >= $2 FOR UPDATE",
            )
            .bind(&item.repository_id)
            .bind(i64::try_from(item.domain_sequence).map_err(|_conversion_error| sqlx::Error::Protocol("Hub sequence overflow".into()))?)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(expected) = archived else {
                return Err(sqlx::Error::Protocol("Hub outbox sequence gap".into()));
            };
            if u64::try_from(expected).ok() != Some(item.domain_sequence) {
                return Err(sqlx::Error::Protocol("Hub outbox sequence gap".into()));
            }
            let changed = sqlx::query(
                "UPDATE shardline_hub_recovery_outbox SET archived_at_unix_seconds = EXTRACT(EPOCH FROM now())::bigint, archive_digest = $1
                 WHERE repo_id = $2 AND domain_sequence = $3 AND transaction_id = $4 AND archived_at_unix_seconds IS NULL",
            )
            .bind(&archive.checksum_blake3)
            .bind(&item.repository_id)
            .bind(expected)
            .bind(&item.transaction_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if changed != 1 {
                return Err(sqlx::Error::Protocol("Hub outbox row changed before archive mark".into()));
            }
            sqlx::query(
                "UPDATE shardline_hub_recovery_sequences SET last_archived_sequence = $1
                 WHERE repo_id = $2 AND last_archived_sequence = $3",
            )
            .bind(expected)
            .bind(&item.repository_id)
            .bind(expected.checked_sub(1).ok_or_else(|| {
                sqlx::Error::Protocol("Hub sequence underflow".into())
            })?)
            .execute(&mut *tx)
            .await?;
            tx.commit().await
        }
        .await;
        result.map_err(|error: sqlx::Error| HubRecoveryError::Outbox(error.to_string()))?;
        Ok(())
    }
}
