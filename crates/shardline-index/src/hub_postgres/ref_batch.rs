//! Atomic repository ref publication serialized with existing mutable writers.
use super::*;
use crate::hub::{
    HubRefBatchOutcome, HubRefChange, HubRevision, PreparedHubRevisionBatch,
    prepared_revision_matches_first_change, valid_ref_batch,
};

pub(super) fn apply(
    store: &PostgresIndexStore,
    repo_id: &str,
    changes: &[HubRefChange],
) -> Result<HubRefBatchOutcome, PostgresMetadataStoreError> {
    apply_inner(store, repo_id, changes, &[], None)
}

pub(super) fn apply_with_manifests(
    store: &PostgresIndexStore,
    repo_id: &str,
    changes: &[HubRefChange],
    manifests: &[shardline_reliability::HubRefBatchManifestReferenceV1],
) -> Result<HubRefBatchOutcome, PostgresMetadataStoreError> {
    apply_inner(store, repo_id, changes, manifests, None)
}

pub(super) fn apply_with_prepared_revisions(
    store: &PostgresIndexStore,
    repo_id: &str,
    changes: &[HubRefChange],
    revisions: &[HubRevision],
    manifests: &[shardline_reliability::HubRefBatchManifestReferenceV1],
) -> Result<HubRefBatchOutcome, PostgresMetadataStoreError> {
    apply_inner(store, repo_id, changes, manifests, Some(revisions))
}

fn apply_inner(
    store: &PostgresIndexStore,
    repo_id: &str,
    changes: &[HubRefChange],
    manifests: &[shardline_reliability::HubRefBatchManifestReferenceV1],
    prepared_revisions: Option<&[HubRevision]>,
) -> Result<HubRefBatchOutcome, PostgresMetadataStoreError> {
    if !valid_ref_batch(changes) {
        return Ok(HubRefBatchOutcome::Conflict);
    }
    let prepared_batch = prepared_revisions
        .map(|revisions| {
            PreparedHubRevisionBatch::new(repo_id, changes, revisions, manifests).ok_or_else(|| {
                PostgresMetadataStoreError::ReliabilityEventConflict(
                    "prepared revisions and manifests do not match changed ref heads".into(),
                )
            })
        })
        .transpose()?;
    block_on_async(async {
        let mut tx = store.pool().begin().await?;
        let repository: Option<String> = sqlx::query_scalar(
            "SELECT repo_id FROM shardline_hub_repos WHERE repo_id = $1 FOR UPDATE",
        )
        .bind(repo_id)
        .fetch_optional(&mut *tx)
        .await?;
        if repository.is_none() {
            return Ok(HubRefBatchOutcome::Conflict);
        }
        let mut recovery_changes = Vec::with_capacity(changes.len());
        for change in changes {
            let name = canonical_ref_name(&change.ref_name);
            let current: Option<String> = sqlx::query_scalar(
                "SELECT sha FROM shardline_hub_refs WHERE repo_id = $1 AND ref_name = $2",
            )
            .bind(repo_id)
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?;
            if current != change.expected_sha {
                return Ok(HubRefBatchOutcome::Conflict);
            }
            if current != change.new_sha {
                recovery_changes.push(change.clone());
            }
            let before = HubRefSnapshot::new(repo_id, name, current.clone())?;
            let after = HubRefSnapshot::new(repo_id, name, change.new_sha.clone())?;
            let evidence = verify_and_append_snapshot_transition(
                current_hub_ref_evidence(&mut tx, repo_id, name, current).await?,
                before,
                after,
            )?
            .0;
            if let Some(new_sha) = &change.new_sha {
                if let Some(prepared_batch) = &prepared_batch {
                    if change.expected_sha != change.new_sha {
                        let prepared = prepared_batch.revision(new_sha).ok_or_else(|| {
                            PostgresMetadataStoreError::ReliabilityEventConflict(
                                "prepared revision is missing".into(),
                            )
                        })?;
                        let existing = sqlx::query(
                            "SELECT repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds
                             FROM shardline_hub_revisions WHERE repo_id = $1 AND sha = $2",
                        )
                        .bind(repo_id)
                        .bind(new_sha)
                        .fetch_optional(&mut *tx)
                        .await?;
                        if let Some(existing) = existing {
                            let existing = decode_revision(&existing)?;
                            if !existing.matches_prepared(prepared) {
                                return Err(PostgresMetadataStoreError::ReliabilityEventConflict(
                                    "prepared revision conflicts with persisted metadata".into(),
                                ));
                            }
                        } else {
                            let first_change =
                                prepared_batch.first_change(new_sha).ok_or_else(|| {
                                    PostgresMetadataStoreError::ReliabilityEventConflict(
                                        "prepared revision has no changed ref".into(),
                                    )
                                })?;
                            if !prepared_revision_matches_first_change(
                                repo_id,
                                first_change,
                                prepared,
                            ) {
                                return Err(PostgresMetadataStoreError::ReliabilityEventConflict(
                                    "prepared revision does not match its first changed ref".into(),
                                ));
                            }
                            sqlx::query(
                                "INSERT INTO shardline_hub_revisions (repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds)
                                 VALUES ($1, $2, $3, $4, $5, $6)",
                            )
                            .bind(repo_id)
                            .bind(canonical_ref_name(&prepared.ref_name))
                            .bind(&prepared.sha)
                            .bind(&prepared.parent_sha)
                            .bind(&prepared.message)
                            .bind(u64_to_i64(prepared.created_at_unix_seconds)?)
                            .execute(&mut *tx)
                            .await?;
                        }
                    }
                } else {
                    sqlx::query(
                        "INSERT INTO shardline_hub_revisions (repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds)
                         VALUES ($1,$2,$3,$4,$5,EXTRACT(EPOCH FROM now())::bigint) ON CONFLICT(repo_id,sha) DO NOTHING",
                    ).bind(repo_id).bind(name).bind(new_sha).bind(&change.expected_sha).bind(&change.message).execute(&mut *tx).await?;
                }
                sqlx::query(
                    "INSERT INTO shardline_hub_refs (repo_id, ref_name, sha) VALUES ($1,$2,$3)
                     ON CONFLICT(repo_id,ref_name) DO UPDATE SET sha = EXCLUDED.sha",
                )
                .bind(repo_id)
                .bind(name)
                .bind(new_sha)
                .execute(&mut *tx)
                .await?;
                if name == "main" {
                    sqlx::query("UPDATE shardline_hub_repos SET default_branch = $1, updated_at_unix_seconds = EXTRACT(EPOCH FROM now())::bigint WHERE repo_id = $2")
                        .bind(new_sha).bind(repo_id).execute(&mut *tx).await?;
                }
            } else {
                sqlx::query("DELETE FROM shardline_hub_refs WHERE repo_id = $1 AND ref_name = $2")
                    .bind(repo_id)
                    .bind(name)
                    .execute(&mut *tx)
                    .await?;
            }
            persist_hub_ref_evidence(&mut tx, &evidence).await?;
        }
        super::recovery_outbox::append_ref_batch(&mut tx, repo_id, &recovery_changes, manifests)
            .await?;
        tx.commit().await?;
        Ok(HubRefBatchOutcome::Applied)
    })
}
