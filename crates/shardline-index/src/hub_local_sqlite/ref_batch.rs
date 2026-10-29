//! Atomic repository ref publication using the existing SQLite evidence chain.
use super::*;
use crate::hub::{HubRefBatchOutcome, HubRefChange, valid_ref_batch};

pub(super) fn apply(
    store: &LocalIndexStore,
    repo_id: &str,
    changes: &[HubRefChange],
) -> Result<HubRefBatchOutcome, LocalIndexStoreError> {
    if !valid_ref_batch(changes) {
        return Ok(HubRefBatchOutcome::Conflict);
    }
    retry_sqlite_busy(|| {
        let mut connection = open_hub_connection_rw(store.root())?;
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM shardline_hub_repos WHERE repo_id = ?1)",
            params![repo_id],
            |row| row.get(0),
        )?;
        if !exists {
            return Ok(HubRefBatchOutcome::Conflict);
        }
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
            let before = hub_ref_snapshot(repo_id, name, current.clone())?;
            let after = hub_ref_snapshot(repo_id, name, change.new_sha.clone())?;
            let (stored, missing) = current_hub_ref_evidence(&tx, repo_id, name, current)?;
            let evidence = verify_and_append_snapshot_transition(stored, before, after)?.0;
            if let Some(new_sha) = &change.new_sha {
                let now = u64_to_i64(unix_now_seconds_lossy())?;
                tx.execute(
                    "INSERT INTO shardline_hub_revisions (repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(repo_id, sha) DO NOTHING",
                    params![repo_id, name, new_sha, change.expected_sha, change.message, now],
                )?;
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
                    persist_hub_ref_evidence(&tx, event)?;
                }
            } else if let Some(event) = evidence.events().last() {
                persist_hub_ref_evidence(&tx, event)?;
            }
        }
        tx.commit()?;
        Ok(HubRefBatchOutcome::Applied)
    })
}
