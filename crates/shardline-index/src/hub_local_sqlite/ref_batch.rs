//! Atomic repository ref publication using the existing SQLite evidence chain.
use super::*;
use crate::hub::{
    HubRefBatchOutcome, HubRefChange, HubRevision, PreparedHubRevisionBatch,
    prepared_revision_matches_first_change, valid_ref_batch,
};

pub(super) fn apply(
    store: &LocalIndexStore,
    repo_id: &str,
    changes: &[HubRefChange],
) -> Result<HubRefBatchOutcome, LocalIndexStoreError> {
    apply_inner(store, repo_id, changes, &[], None)
}

pub(super) fn apply_with_manifests(
    store: &LocalIndexStore,
    repo_id: &str,
    changes: &[HubRefChange],
    manifests: &[shardline_reliability::HubRefBatchManifestReferenceV1],
) -> Result<HubRefBatchOutcome, LocalIndexStoreError> {
    apply_inner(store, repo_id, changes, manifests, None)
}

pub(super) fn apply_with_prepared_revisions(
    store: &LocalIndexStore,
    repo_id: &str,
    changes: &[HubRefChange],
    revisions: &[HubRevision],
    manifests: &[shardline_reliability::HubRefBatchManifestReferenceV1],
) -> Result<HubRefBatchOutcome, LocalIndexStoreError> {
    apply_inner(store, repo_id, changes, manifests, Some(revisions))
}

fn apply_inner(
    store: &LocalIndexStore,
    repo_id: &str,
    changes: &[HubRefChange],
    manifests: &[shardline_reliability::HubRefBatchManifestReferenceV1],
    prepared_revisions: Option<&[HubRevision]>,
) -> Result<HubRefBatchOutcome, LocalIndexStoreError> {
    if !valid_ref_batch(changes) {
        return Ok(HubRefBatchOutcome::Conflict);
    }
    let prepared_batch = prepared_revisions
        .map(|revisions| {
            PreparedHubRevisionBatch::new(repo_id, changes, revisions, manifests).ok_or_else(|| {
                rusqlite::Error::InvalidParameterName(
                    "prepared revisions and manifests do not match the changed ref heads"
                        .to_owned(),
                )
            })
        })
        .transpose()?;
    retry_sqlite_busy(|| {
        let mut connection = open_hub_connection(store.root())?;
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM shardline_hub_repos WHERE repo_id = ?1)",
            params![repo_id],
            |row| row.get(0),
        )?;
        if !exists {
            return Ok(HubRefBatchOutcome::Conflict);
        }
        let mut recovery_changes = Vec::with_capacity(changes.len());
        for change in changes {
            let name = canonical_ref_name(&change.ref_name);
            let current: Option<String> = tx
                .query_row(
                    "SELECT sha FROM shardline_hub_refs WHERE repo_id = ?1 AND ref_name = ?2",
                    params![repo_id, name],
                    |row| row.get(0),
                )
                .optional()?;
            if current != change.expected_sha {
                return Ok(HubRefBatchOutcome::Conflict);
            }
            if current != change.new_sha {
                recovery_changes.push(change.clone());
            }
            let before = hub_ref_snapshot(repo_id, name, current.clone())?;
            let after = hub_ref_snapshot(repo_id, name, change.new_sha.clone())?;
            let (stored, missing) = current_hub_ref_evidence(&tx, repo_id, name, current)?;
            let evidence = verify_and_append_snapshot_transition(stored, before, after)?.0;
            if let Some(new_sha) = &change.new_sha {
                let now = u64_to_i64(unix_now_seconds_lossy())?;
                if let Some(prepared_batch) = &prepared_batch {
                    if change.expected_sha != change.new_sha {
                        let prepared = prepared_batch.revision(new_sha).ok_or_else(|| {
                            rusqlite::Error::InvalidParameterName(
                                "prepared revision is missing".to_owned(),
                            )
                        })?;
                        let existing = tx
                            .query_row(
                                "SELECT repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds
                                 FROM shardline_hub_revisions WHERE repo_id = ?1 AND sha = ?2",
                                params![repo_id, new_sha],
                                super::decode_revision,
                            )
                            .optional()?;
                        if let Some(existing) = existing {
                            if !existing.matches_prepared(prepared) {
                                return Err(rusqlite::Error::InvalidParameterName(
                                    "prepared revision conflicts with persisted metadata"
                                        .to_owned(),
                                )
                                .into());
                            }
                        } else {
                            let first_change =
                                prepared_batch.first_change(new_sha).ok_or_else(|| {
                                    rusqlite::Error::InvalidParameterName(
                                        "prepared revision has no changed ref".to_owned(),
                                    )
                                })?;
                            if !prepared_revision_matches_first_change(
                                repo_id,
                                first_change,
                                prepared,
                            ) {
                                return Err(rusqlite::Error::InvalidParameterName(
                                    "prepared revision does not match its first changed ref"
                                        .to_owned(),
                                )
                                .into());
                            }
                            tx.execute(
                                "INSERT INTO shardline_hub_revisions (repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds)
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                                params![
                                    repo_id,
                                    canonical_ref_name(&prepared.ref_name),
                                    prepared.sha,
                                    prepared.parent_sha,
                                    prepared.message,
                                    u64_to_i64(prepared.created_at_unix_seconds)?,
                                ],
                            )?;
                        }
                    }
                } else {
                    tx.execute(
                        "INSERT INTO shardline_hub_revisions (repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(repo_id, sha) DO NOTHING",
                        params![repo_id, name, new_sha, change.expected_sha, change.message, now],
                    )?;
                }
                tx.execute(
                    "INSERT INTO shardline_hub_refs (repo_id, ref_name, sha) VALUES (?1, ?2, ?3)
                     ON CONFLICT(repo_id, ref_name) DO UPDATE SET sha = excluded.sha",
                    params![repo_id, name, new_sha],
                )?;
                if name == "main" {
                    tx.execute("UPDATE shardline_hub_repos SET default_branch = ?1, updated_at_unix_seconds = ?2 WHERE repo_id = ?3",
                        params![new_sha, now, repo_id])?;
                }
            } else {
                tx.execute(
                    "DELETE FROM shardline_hub_refs WHERE repo_id = ?1 AND ref_name = ?2",
                    params![repo_id, name],
                )?;
            }
            if missing {
                for event in evidence.events() {
                    persist_reliability_event(&tx, event)?;
                }
            } else if let Some(event) = evidence.events().last() {
                persist_reliability_event(&tx, event)?;
            }
        }
        super::recovery_outbox::append_ref_batch(&tx, repo_id, &recovery_changes, manifests)?;
        tx.commit()?;
        Ok(HubRefBatchOutcome::Applied)
    })
}
