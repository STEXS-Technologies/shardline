//! Atomic repository ref publication serialized with existing mutable writers.
use super::*;
use crate::hub::{HubRefBatchOutcome, HubRefChange, valid_ref_batch};

pub(super) fn apply(
    store: &PostgresIndexStore,
    repo_id: &str,
    changes: &[HubRefChange],
) -> Result<HubRefBatchOutcome, PostgresMetadataStoreError> {
    if !valid_ref_batch(changes) {
        return Ok(HubRefBatchOutcome::Conflict);
    }
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
            let before = HubRefSnapshot::new(repo_id, name, current.clone())?;
            let after = HubRefSnapshot::new(repo_id, name, change.new_sha.clone())?;
            let evidence = verify_and_append_snapshot_transition(
                current_hub_ref_evidence(&mut tx, repo_id, name, current).await?,
                before,
                after,
            )?
            .0;
            if let Some(new_sha) = &change.new_sha {
                sqlx::query(
                    "INSERT INTO shardline_hub_revisions (repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds)
                     VALUES ($1,$2,$3,$4,$5,EXTRACT(EPOCH FROM now())::bigint) ON CONFLICT(repo_id,sha) DO NOTHING",
                ).bind(repo_id).bind(name).bind(new_sha).bind(&change.expected_sha).bind(&change.message).execute(&mut *tx).await?;
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
        tx.commit().await?;
        Ok(HubRefBatchOutcome::Applied)
    })
}
