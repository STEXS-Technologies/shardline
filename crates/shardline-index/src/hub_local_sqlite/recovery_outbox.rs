use super::{LocalIndexStore, open_hub_connection};
use crate::hub::{HubRef, HubRefChange};
use crate::hub_recovery::{
    MAX_HUB_REF_EVENT_BYTES, MAX_PENDING_REPOSITORY_PAGE, build_hub_ref_batch_event,
    decode_hub_ref_event, make_checkpoint_source, validate_mode_transition,
    validate_requested_mode,
};
use crate::{HubRecoveryError, HubRecoveryMode, HubRecoveryOutboxItem, HubRecoveryOutboxStore};
use rusqlite::OptionalExtension;
use shardline_reliability::HubRefBatchManifestReferenceV1;

async fn run_outbox_blocking<T, F>(work: F) -> Result<T, HubRecoveryError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, HubRecoveryError> + Send + 'static,
{
    crate::local_sqlite::run_blocking_store(move || {
        work().map_err(|error| {
            crate::LocalIndexStoreError::Sqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(
                error,
            )))
        })
    })
    .await
    .map_err(|error| HubRecoveryError::Outbox(error.to_string()))
}

/// Appends the complete SQL ref mutation batch to the same transaction that
/// publishes it. OFF performs only the existing publication path.
pub(super) fn append_ref_batch(
    tx: &rusqlite::Transaction<'_>,
    repository_id: &str,
    changes: &[HubRefChange],
    manifest_refs: &[HubRefBatchManifestReferenceV1],
) -> Result<(), crate::LocalIndexStoreError> {
    if changes.is_empty() && manifest_refs.is_empty() {
        return Ok(());
    }
    let config: (String, Option<String>) = tx.query_row(
        "SELECT mode, deployment_id FROM shardline_hub_recovery_config WHERE singleton_id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if config.0 == HubRecoveryMode::Off.as_str() && manifest_refs.is_empty() {
        return Ok(());
    }
    if config.0 == HubRecoveryMode::Off.as_str() {
        return Err(crate::LocalIndexStoreError::Sqlite(
            rusqlite::Error::InvalidParameterName(
                "manifest references require authoritative SHADOW mode".to_owned(),
            ),
        ));
    }
    if config.0 != HubRecoveryMode::Shadow.as_str() {
        return Err(crate::LocalIndexStoreError::Sqlite(
            rusqlite::Error::InvalidParameterName("invalid Hub recovery mode".to_owned()),
        ));
    }
    let deployment_id = config.1.ok_or_else(|| {
        crate::LocalIndexStoreError::Sqlite(rusqlite::Error::InvalidParameterName(
            "SHADOW mode has no deployment ID".to_owned(),
        ))
    })?;
    let sequence: i64 = tx.query_row(
        "INSERT INTO shardline_hub_recovery_sequences (repo_id, last_sequence, last_archived_sequence)
         VALUES (?1, 1, 0)
         ON CONFLICT (repo_id) DO UPDATE SET last_sequence = last_sequence + 1
         RETURNING last_sequence",
        [repository_id],
        |row| row.get(0),
    )?;
    let domain_sequence = u64::try_from(sequence).map_err(|_conversion_error| {
        crate::LocalIndexStoreError::IntegerOutOfRange("Hub sequence".to_owned())
    })?;
    // The scoped sequence is the stable transaction identity: retrying a
    // rolled-back SQL transaction reuses the same identity and sequence.
    let transaction_id =
        crate::hub_recovery::hub_ref_transaction_id(repository_id, domain_sequence);
    let event = build_hub_ref_batch_event(
        &deployment_id,
        repository_id,
        domain_sequence,
        &transaction_id,
        changes,
        manifest_refs,
    )
    .map_err(|error| {
        crate::LocalIndexStoreError::Sqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(
            error,
        )))
    })?;
    let event_json = serde_json::to_string(&event)?;
    if event_json.len() > MAX_HUB_REF_EVENT_BYTES {
        return Err(crate::LocalIndexStoreError::MetadataTooLarge {
            observed_bytes: u64::try_from(event_json.len()).unwrap_or(u64::MAX),
            maximum_bytes: u64::try_from(MAX_HUB_REF_EVENT_BYTES).unwrap_or(u64::MAX),
        });
    }
    let now = i64::try_from(shardline_protocol::unix_now_seconds_lossy()).map_err(
        |_conversion_error| crate::LocalIndexStoreError::IntegerOutOfRange("timestamp".to_owned()),
    )?;
    tx.execute(
        "INSERT INTO shardline_hub_recovery_outbox
         (repo_id, domain_sequence, transaction_id, event_json, created_at_unix_seconds)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![repository_id, sequence, transaction_id, event_json, now],
    )?;
    Ok(())
}

#[async_trait::async_trait]
impl HubRecoveryOutboxStore for LocalIndexStore {
    async fn hub_recovery_mode(
        &self,
    ) -> Result<(HubRecoveryMode, Option<String>), HubRecoveryError> {
        let root = self.root().to_owned();
        run_outbox_blocking(move || {
            let conn = open_hub_connection(&root).map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let row: Option<(String, Option<String>)> = conn.query_row(
                "SELECT mode, deployment_id FROM shardline_hub_recovery_config WHERE singleton_id = 1",
                [], |row| Ok((row.get(0)?, row.get(1)?)),
            ).optional().map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            match row {
                None => Ok((HubRecoveryMode::Off, None)),
                Some((mode, deployment)) if mode == "off" && deployment.is_none() => Ok((HubRecoveryMode::Off, None)),
                Some((mode, deployment)) if mode == "shadow" && deployment.is_some() => Ok((HubRecoveryMode::Shadow, deployment)),
                _ => Err(HubRecoveryError::Outbox("stored Hub recovery mode is invalid".into())),
            }
        }).await
    }

    async fn pending_hub_ref_repositories_after(
        &self,
        cursor: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<String>, HubRecoveryError> {
        if limit.get() > crate::hub_recovery::MAX_PENDING_REPOSITORY_PAGE {
            return Err(HubRecoveryError::PageLimitExceeded);
        }
        let root = self.root().to_owned();
        let cursor = cursor.map(ToOwned::to_owned);
        let limit = i64::try_from(limit.get())
            .map_err(|_conversion_error| HubRecoveryError::PageLimitExceeded)?;
        run_outbox_blocking(move || {
            let conn = open_hub_connection(&root)
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let mut statement = conn
                .prepare(
                    "SELECT s.repo_id FROM shardline_hub_recovery_sequences s
                 WHERE s.last_sequence > s.last_archived_sequence AND (?1 IS NULL OR s.repo_id > ?1)
                 ORDER BY s.repo_id LIMIT ?2",
                )
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let rows = statement
                .query_map(rusqlite::params![cursor, limit], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))
        })
        .await
    }

    async fn hub_repositories_after(
        &self,
        cursor: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<String>, HubRecoveryError> {
        if limit.get() > MAX_PENDING_REPOSITORY_PAGE {
            return Err(HubRecoveryError::PageLimitExceeded);
        }
        let root = self.root().to_owned();
        let cursor = cursor.map(ToOwned::to_owned);
        let limit = i64::try_from(limit.get())
            .map_err(|_conversion_error| HubRecoveryError::PageLimitExceeded)?;
        run_outbox_blocking(move || {
            let conn = open_hub_connection(&root)
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let mut statement = conn
                .prepare(
                    "SELECT repo_id FROM shardline_hub_repos
                     WHERE (?1 IS NULL OR repo_id > ?1) ORDER BY repo_id LIMIT ?2",
                )
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let rows = statement
                .query_map(rusqlite::params![cursor, limit], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))
        })
        .await
    }

    async fn hub_ref_checkpoint_source(
        &self,
        repository_id: &str,
    ) -> Result<crate::HubRefCheckpointSource, HubRecoveryError> {
        let root = self.root().to_owned();
        let repository_id = repository_id.to_owned();
        run_outbox_blocking(move || {
            let mut connection = open_hub_connection(&root)
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let tx = connection
                .transaction()
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let exists: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM shardline_hub_repos WHERE repo_id = ?1)",
                    [&repository_id],
                    |row| row.get(0),
                )
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            if !exists {
                return Err(HubRecoveryError::Outbox(
                    "Hub repository does not exist".into(),
                ));
            }
            let ref_limit = shardline_reliability::MAX_HUB_REF_STATE_CHECKPOINT_REFS
                .checked_add(1)
                .and_then(|limit| i64::try_from(limit).ok())
                .ok_or(HubRecoveryError::InvalidCheckpointSource)?;
            let config: Option<(String, Option<String>)> = tx
                .query_row(
                    "SELECT mode, deployment_id FROM shardline_hub_recovery_config WHERE singleton_id = 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let deployment_id = match config {
                None => None,
                Some((mode, None)) if mode == "off" => None,
                Some((mode, Some(deployment))) if mode == "shadow" => Some(deployment),
                _ => return Err(HubRecoveryError::InvalidCheckpointSource),
            };
            let refs = {
                let mut statement = tx
                    .prepare(
                        "SELECT repo_id, ref_name, sha FROM shardline_hub_refs
                         WHERE repo_id = ?1 ORDER BY ref_name LIMIT ?2",
                    )
                    .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
                let rows = statement
                    .query_map(
                        rusqlite::params![
                            &repository_id,
                            ref_limit,
                        ],
                        |row| {
                            Ok(HubRef {
                                repo_id: row.get(0)?,
                                ref_name: row.get(1)?,
                                sha: row.get(2)?,
                            })
                        },
                    )
                    .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?
            };
            if refs.len() > shardline_reliability::MAX_HUB_REF_STATE_CHECKPOINT_REFS {
                return Err(HubRecoveryError::InvalidCheckpointSource);
            }
            let evidence_refs = refs
                .iter()
                .map(|reference| {
                    (
                        reference.repo_id.clone(),
                        reference.ref_name.clone(),
                        Some(reference.sha.clone()),
                    )
                })
                .collect::<Vec<_>>();
            crate::hub_local_sqlite::verify_hub_ref_evidence_batch(&tx, &evidence_refs)
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let sequence = tx
                .query_row(
                    "SELECT last_sequence, last_archived_sequence
                     FROM shardline_hub_recovery_sequences WHERE repo_id = ?1",
                    [&repository_id],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?
                .unwrap_or((0, 0));
            let digest = if sequence.1 == 0 {
                None
            } else {
                tx.query_row(
                    "SELECT archive_digest FROM shardline_hub_recovery_outbox
                     WHERE repo_id = ?1 AND domain_sequence = ?2
                       AND archived_at_unix_seconds IS NOT NULL",
                    rusqlite::params![&repository_id, sequence.1],
                    |row| row.get::<_, Option<String>>(0),
                )
                .optional()
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?
                .flatten()
            };
            let source = make_checkpoint_source(
                repository_id,
                deployment_id,
                refs,
                sequence.0,
                sequence.1,
                digest,
            )?;
            tx.commit()
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            Ok(source)
        })
        .await
    }

    async fn set_hub_recovery_mode(
        &self,
        mode: HubRecoveryMode,
        deployment_id: Option<&str>,
    ) -> Result<(), HubRecoveryError> {
        let deployment_id = validate_requested_mode(mode, deployment_id)?;
        let root = self.root().to_owned();
        run_outbox_blocking(move || {
            let mut connection = open_hub_connection(&root)
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let tx = connection
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let (current_mode, current_deployment): (String, Option<String>) = tx
                .query_row(
                    "SELECT mode, deployment_id FROM shardline_hub_recovery_config WHERE singleton_id = 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let has_history: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM shardline_hub_recovery_outbox)",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            validate_mode_transition(
                &current_mode,
                current_deployment.as_deref(),
                has_history,
                mode,
                deployment_id.as_deref(),
            )?;
            tx
                .execute(
                    "UPDATE shardline_hub_recovery_config
                     SET mode = ?1, deployment_id = ?2 WHERE singleton_id = 1",
                    rusqlite::params![mode.as_str(), deployment_id],
                )
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            tx.commit()
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            Ok(())
        })
        .await
    }

    async fn next_pending_hub_ref_event(
        &self,
        repository_id: &str,
    ) -> Result<Option<HubRecoveryOutboxItem>, HubRecoveryError> {
        let root = self.root().to_owned();
        let repository_id = repository_id.to_owned();
        run_outbox_blocking(move || {
            let connection = open_hub_connection(&root)
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let state = connection
                .query_row(
                    "SELECT last_sequence, last_archived_sequence
                 FROM shardline_hub_recovery_sequences WHERE repo_id = ?1",
                    [&repository_id],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let Some((last_sequence, archived_sequence)) = state else {
                return Ok(None);
            };
            if archived_sequence >= last_sequence {
                return Ok(None);
            }
            let expected = archived_sequence
                .checked_add(1)
                .ok_or(HubRecoveryError::SequenceGap)?;
            let previous_archive_digest = if expected == 1 {
                None
            } else {
                let previous_sequence = expected
                    .checked_sub(1)
                    .ok_or(HubRecoveryError::SequenceGap)?;
                connection
                    .query_row(
                        "SELECT archive_digest FROM shardline_hub_recovery_outbox
                         WHERE repo_id = ?1 AND domain_sequence = ?2
                           AND archived_at_unix_seconds IS NOT NULL",
                        rusqlite::params![&repository_id, previous_sequence],
                        |row| row.get::<_, Option<String>>(0),
                    )
                    .optional()
                    .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?
                    .flatten()
                    .ok_or(HubRecoveryError::SequenceGap)
                    .map(Some)?
            };
            let row = connection
                .query_row(
                    "SELECT transaction_id, event_json FROM shardline_hub_recovery_outbox
                 WHERE repo_id = ?1 AND domain_sequence = ?2
                   AND archived_at_unix_seconds IS NULL",
                    rusqlite::params![repository_id, expected],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?
                .ok_or(HubRecoveryError::SequenceGap)?;
            decode_hub_ref_event(
                &repository_id,
                expected,
                row.0,
                &row.1,
                previous_archive_digest,
            )
            .map(Some)
        })
        .await
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
        let root = self.root().to_owned();
        let item = item.clone();
        let archive_checksum = archive.checksum_blake3.clone();
        run_outbox_blocking(move || {
            let mut connection = open_hub_connection(&root)
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let tx = connection
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let expected: i64 = tx
                .query_row(
                    "SELECT last_archived_sequence + 1 FROM shardline_hub_recovery_sequences
                 WHERE repo_id = ?1 AND last_sequence >= ?2",
                    rusqlite::params![
                        item.repository_id,
                        i64::try_from(item.domain_sequence)
                            .map_err(|_conversion_error| HubRecoveryError::SequenceGap)?
                    ],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?
                .ok_or(HubRecoveryError::SequenceGap)?;
            if u64::try_from(expected).ok() != Some(item.domain_sequence) {
                return Err(HubRecoveryError::SequenceGap);
            }
            let now = i64::try_from(shardline_protocol::unix_now_seconds_lossy())
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            let sequence = i64::try_from(item.domain_sequence)
                .map_err(|_conversion_error| HubRecoveryError::SequenceGap)?;
            let outbox_changed = tx
                .execute(
                    "UPDATE shardline_hub_recovery_outbox
                 SET archived_at_unix_seconds = ?1, archive_digest = ?2
                 WHERE repo_id = ?3 AND domain_sequence = ?4 AND transaction_id = ?5
                   AND archived_at_unix_seconds IS NULL",
                    rusqlite::params![
                        now,
                        archive_checksum,
                        item.repository_id,
                        sequence,
                        item.transaction_id
                    ],
                )
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            if outbox_changed != 1 {
                return Err(HubRecoveryError::SequenceGap);
            }
            let sequence_changed = tx
                .execute(
                    "UPDATE shardline_hub_recovery_sequences
                 SET last_archived_sequence = ?1
                 WHERE repo_id = ?2 AND last_archived_sequence = ?3",
                    rusqlite::params![sequence, item.repository_id, sequence.saturating_sub(1)],
                )
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            if sequence_changed != 1 {
                return Err(HubRecoveryError::SequenceGap);
            }
            tx.commit()
                .map_err(|error| HubRecoveryError::Outbox(error.to_string()))?;
            Ok(())
        })
        .await
    }
}
