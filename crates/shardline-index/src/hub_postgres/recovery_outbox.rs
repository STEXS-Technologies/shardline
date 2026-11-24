use super::PostgresIndexStore;
use super::verify_hub_ref_evidence_batch;
use crate::hub::{HubRef, HubRefChange};
use crate::hub_recovery::{
    HubRecoveryError, HubRecoveryMode, HubRecoveryOutboxItem, HubRecoveryOutboxStore,
    MAX_HUB_REF_EVENT_BYTES, MAX_PENDING_REPOSITORY_PAGE, build_hub_ref_batch_event,
    decode_hub_ref_event, make_checkpoint_source, validate_mode_transition,
    validate_requested_mode,
};
use futures_util::TryStreamExt;
use shardline_reliability::HubRefBatchManifestReferenceV1;
use sqlx::{Postgres, Row, Transaction};

/// Appends a complete ref batch using the publication transaction's repository
/// lock, sequence allocation, and commit boundary.
pub(super) async fn append_ref_batch(
    tx: &mut Transaction<'_, Postgres>,
    repository_id: &str,
    changes: &[HubRefChange],
    manifest_refs: &[HubRefBatchManifestReferenceV1],
) -> Result<(), sqlx::Error> {
    if changes.is_empty() && manifest_refs.is_empty() {
        return Ok(());
    }
    let config = sqlx::query(
        "SELECT mode, deployment_id FROM shardline_hub_recovery_config WHERE singleton_id = 1 FOR SHARE",
    )
    .fetch_optional(&mut **tx)
    .await?;
    let Some(config) = config else {
        // The production migration inserts the singleton OFF row. Isolated
        // schema fixtures copy table definitions without rows, so absence has
        // the same safe semantics as the read-only mode API: OFF. Manifest
        // references still require an authoritative SHADOW config row.
        if manifest_refs.is_empty() {
            return Ok(());
        }
        return Err(sqlx::Error::Protocol(
            "manifest references require authoritative SHADOW mode".into(),
        ));
    };
    let mode: String = config.try_get("mode")?;
    if mode == HubRecoveryMode::Off.as_str() && manifest_refs.is_empty() {
        return Ok(());
    }
    if mode == HubRecoveryMode::Off.as_str() {
        return Err(sqlx::Error::Protocol(
            "manifest references require authoritative SHADOW mode".into(),
        ));
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
    let transaction_id = crate::hub_recovery::hub_ref_transaction_id(repository_id, sequence);
    let event = build_hub_ref_batch_event(
        &deployment_id,
        repository_id,
        sequence,
        &transaction_id,
        changes,
        manifest_refs,
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
    async fn hub_recovery_mode(
        &self,
    ) -> Result<(HubRecoveryMode, Option<String>), HubRecoveryError> {
        let row = sqlx::query(
            "SELECT mode, deployment_id FROM shardline_hub_recovery_config WHERE singleton_id = 1",
        )
        .fetch_optional(self.pool())
        .await
        .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
        match row {
            None => Ok((HubRecoveryMode::Off, None)),
            Some(row) => {
                let mode: String = row
                    .try_get("mode")
                    .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
                let deployment: Option<String> = row
                    .try_get("deployment_id")
                    .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
                match (mode.as_str(), deployment) {
                    ("off", None) => Ok((HubRecoveryMode::Off, None)),
                    ("shadow", Some(deployment)) => Ok((HubRecoveryMode::Shadow, Some(deployment))),
                    _ => Err(HubRecoveryError::Outbox(
                        "stored Hub recovery mode is invalid".into(),
                    )),
                }
            }
        }
    }

    async fn pending_hub_ref_repositories_after(
        &self,
        cursor: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<String>, HubRecoveryError> {
        if limit.get() > crate::hub_recovery::MAX_PENDING_REPOSITORY_PAGE {
            return Err(HubRecoveryError::PageLimitExceeded);
        }
        let limit = i64::try_from(limit.get())
            .map_err(|_conversion_error| HubRecoveryError::PageLimitExceeded)?;
        sqlx::query_scalar(
            "SELECT repo_id FROM shardline_hub_recovery_sequences
             WHERE last_sequence > last_archived_sequence AND ($1::text IS NULL OR repo_id > $1)
             ORDER BY repo_id LIMIT $2",
        )
        .bind(cursor)
        .bind(limit)
        .fetch_all(self.pool())
        .await
        .map_err(|error| HubRecoveryError::Outbox(error.to_string()))
    }

    async fn hub_repositories_after(
        &self,
        cursor: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<String>, HubRecoveryError> {
        if limit.get() > MAX_PENDING_REPOSITORY_PAGE {
            return Err(HubRecoveryError::PageLimitExceeded);
        }
        let limit = i64::try_from(limit.get())
            .map_err(|_conversion_error| HubRecoveryError::PageLimitExceeded)?;
        sqlx::query_scalar(
            "SELECT repo_id FROM shardline_hub_repos
             WHERE ($1::text IS NULL OR repo_id > $1) ORDER BY repo_id LIMIT $2",
        )
        .bind(cursor)
        .bind(limit)
        .fetch_all(self.pool())
        .await
        .map_err(|error| HubRecoveryError::Outbox(error.to_string()))
    }

    async fn hub_ref_checkpoint_source(
        &self,
        repository_id: &str,
    ) -> Result<crate::HubRefCheckpointSource, HubRecoveryError> {
        let mut tx = self
            .pool()
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM shardline_hub_repos WHERE repo_id = $1)",
        )
        .bind(repository_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
        if !exists {
            return Err(HubRecoveryError::Outbox(
                "Hub repository does not exist".into(),
            ));
        }
        let config = sqlx::query(
            "SELECT mode, deployment_id FROM shardline_hub_recovery_config WHERE singleton_id = 1",
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
        let deployment_id = match config {
            None => None,
            Some(row) => {
                let mode: String = row
                    .try_get("mode")
                    .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
                let deployment: Option<String> = row
                    .try_get("deployment_id")
                    .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
                match (mode.as_str(), deployment) {
                    ("off", None) => None,
                    ("shadow", Some(deployment)) => Some(deployment),
                    _ => return Err(HubRecoveryError::InvalidCheckpointSource),
                }
            }
        };
        let limit = shardline_reliability::MAX_HUB_REF_STATE_CHECKPOINT_REFS
            .checked_add(1)
            .and_then(|limit| i64::try_from(limit).ok())
            .ok_or(HubRecoveryError::InvalidCheckpointSource)?;
        let mut refs = Vec::new();
        {
            let mut rows = sqlx::query(
                "SELECT repo_id, ref_name, sha FROM shardline_hub_refs
                 WHERE repo_id = $1 ORDER BY ref_name LIMIT $2",
            )
            .bind(repository_id)
            .bind(limit)
            .fetch(&mut *tx);
            while let Some(row) = rows
                .try_next()
                .await
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?
            {
                refs.push(HubRef {
                    repo_id: row
                        .try_get("repo_id")
                        .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?,
                    ref_name: row
                        .try_get("ref_name")
                        .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?,
                    sha: row
                        .try_get("sha")
                        .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?,
                });
            }
        }
        if refs.len() > shardline_reliability::MAX_HUB_REF_STATE_CHECKPOINT_REFS {
            return Err(HubRecoveryError::InvalidCheckpointSource);
        }
        verify_hub_ref_evidence_batch(&mut tx, &refs)
            .await
            .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
        let sequence = sqlx::query(
            "SELECT last_sequence, last_archived_sequence
             FROM shardline_hub_recovery_sequences WHERE repo_id = $1",
        )
        .bind(repository_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
        let (committed, archived) = if let Some(row) = sequence {
            (
                row.try_get::<i64, _>("last_sequence")
                    .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?,
                row.try_get::<i64, _>("last_archived_sequence")
                    .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?,
            )
        } else {
            (0, 0)
        };
        let digest = if archived == 0 {
            None
        } else {
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT archive_digest FROM shardline_hub_recovery_outbox
                 WHERE repo_id = $1 AND domain_sequence = $2
                   AND archived_at_unix_seconds IS NOT NULL",
            )
            .bind(repository_id)
            .bind(archived)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?
            .flatten()
        };
        let source = make_checkpoint_source(
            repository_id.to_owned(),
            deployment_id,
            refs,
            committed,
            archived,
            digest,
        )?;
        tx.commit()
            .await
            .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
        Ok(source)
    }

    async fn set_hub_recovery_mode(
        &self,
        mode: HubRecoveryMode,
        deployment_id: Option<&str>,
    ) -> Result<(), HubRecoveryError> {
        let deployment_id = validate_requested_mode(mode, deployment_id)?;
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
            let previous_archive_digest = if expected == 1 {
                None
            } else {
                let previous_sequence = expected
                    .checked_sub(1)
                    .ok_or_else(|| sqlx::Error::Protocol("Hub sequence underflow".into()))?;
                let digest = sqlx::query_scalar::<_, Option<String>>(
                    "SELECT archive_digest FROM shardline_hub_recovery_outbox
                     WHERE repo_id = $1 AND domain_sequence = $2
                       AND archived_at_unix_seconds IS NOT NULL",
                )
                .bind(repository_id)
                .bind(previous_sequence)
                .fetch_optional(self.pool())
                .await?
                .flatten()
                .ok_or_else(|| sqlx::Error::Protocol("Hub archive predecessor is missing".into()))?;
                Some(digest)
            };
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
            decode_hub_ref_event(repository_id, expected, transaction_id, &event_json, previous_archive_digest)
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
