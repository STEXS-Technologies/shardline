//! Shared-policy publication and owner policy administration.
#[cfg(test)]
mod tests;
use rusqlite::{OptionalExtension, params};
use shardline_protocol::unix_now_seconds_lossy;
use shardline_reliability::HubRefEvidenceLog;

use super::{
    LocalIndexStore, LocalIndexStoreError, open_hub_connection_rw, retry_sqlite_busy, u64_to_i64,
};
use crate::{
    hub::{HubCreationActor, HubCreationPolicy, HubRepo, HubRepoType},
    local_sqlite::{hub_ref_snapshot, persist_hub_ref_evidence},
};

pub(super) fn create(
    store: &LocalIndexStore,
    repo_type: HubRepoType,
    name: &str,
    private: bool,
    authorized: bool,
    actor: Option<&HubCreationActor>,
) -> Result<HubRepo, LocalIndexStoreError> {
    let initial_sha = crate::hub::EMPTY_HUB_REVISION;
    let now = unix_now_seconds_lossy();
    retry_sqlite_busy(|| {
        let mut conn = open_hub_connection_rw(store.root())?;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO shardline_hub_repos (repo_id, repo_type, private, default_branch, created_at_unix_seconds, updated_at_unix_seconds, creation_authorized, creation_issuer, creation_subject)
             VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6, ?7, ?8)",
            params![name, super::repo_type_to_str(repo_type), private, initial_sha, u64_to_i64(now)?, authorized,
                actor.map(|a| a.issuer.as_str()), actor.map(|a| a.subject.as_str())],
        )?;
        tx.execute(
            "INSERT INTO shardline_hub_revisions (repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds)
             VALUES (?1, 'main', ?2, NULL, NULL, ?3)",
            params![name, initial_sha, u64_to_i64(now)?],
        )?;
        tx.execute(
            "INSERT INTO shardline_hub_refs (repo_id, ref_name, sha) VALUES (?1, 'main', ?2)",
            params![name, initial_sha],
        )?;
        let evidence = HubRefEvidenceLog::baseline(hub_ref_snapshot(
            name,
            "main",
            Some(initial_sha.to_owned()),
        )?)?;
        for event in evidence.events() {
            persist_hub_ref_evidence(&tx, event)?;
        }
        tx.commit()?;
        Ok(())
    })?;
    Ok(HubRepo {
        repo_id: name.to_owned(),
        repo_type,
        private,
        default_branch: initial_sha.to_owned(),
        created_at_unix_seconds: now,
        updated_at_unix_seconds: now,
    })
}

impl LocalIndexStore {
    /// Replaces or clears an owner-managed namespace creation policy atomically.
    /// Clearing restores legacy behavior; it does not remove repositories.
    /// # Errors
    /// Returns invalid-policy or database errors without partial changes.
    pub fn set_hub_creation_policy(
        &self,
        namespace: &str,
        policy: Option<&HubCreationPolicy>,
    ) -> Result<(), LocalIndexStoreError> {
        let defaults = HubCreationPolicy::default();
        let effective = policy.unwrap_or(&defaults);
        effective.validate(namespace)?;
        retry_sqlite_busy(|| {
            let mut conn = open_hub_connection_rw(self.root())?;
            let tx = conn.transaction()?;
            tx.execute(
                "INSERT INTO shardline_hub_creation_policies(namespace,configured,enabled,max_repositories,max_creations,window_seconds)
                 VALUES (?1,?2,?3,?4,?5,?6) ON CONFLICT(namespace) DO UPDATE SET configured=excluded.configured,
                 enabled=excluded.enabled,max_repositories=excluded.max_repositories,max_creations=excluded.max_creations,window_seconds=excluded.window_seconds",
                params![namespace,policy.is_some(),effective.enabled,effective.max_repositories.map(u64_to_i64).transpose()?,
                    effective.max_creations.map(u64_to_i64).transpose()?,i64::from(effective.window_seconds)],
            )?;
            tx.execute(
                "DELETE FROM shardline_hub_creation_denials WHERE namespace=?1",
                [namespace],
            )?;
            for actor in &effective.denied_creators {
                tx.execute("INSERT OR IGNORE INTO shardline_hub_creation_denials(namespace,issuer,subject) VALUES (?1,?2,?3)", params![namespace,actor.issuer,actor.subject])?;
            }
            tx.commit()?;
            Ok(())
        })
    }

    /// Reads the configured shared creation policy, excluding implicit defaults.
    /// # Errors
    /// Returns an error when policy metadata cannot be read.
    pub fn hub_creation_policy(
        &self,
        namespace: &str,
    ) -> Result<Option<HubCreationPolicy>, LocalIndexStoreError> {
        HubCreationPolicy::default().validate(namespace)?;
        let mut conn = open_hub_connection_rw(self.root())?;
        let tx = conn.transaction()?;
        let policy = tx.query_row(
            "SELECT enabled,max_repositories,max_creations,window_seconds FROM shardline_hub_creation_policies WHERE namespace=?1 AND configured=1",
            [namespace], |row| Ok((row.get::<_,bool>(0)?,row.get::<_,Option<i64>>(1)?,row.get::<_,Option<i64>>(2)?,row.get::<_,u32>(3)?)),
        ).optional()?;
        let Some((enabled, max_repositories, max_creations, window_seconds)) = policy else {
            return Ok(None);
        };
        let mut query = tx.prepare("SELECT issuer,subject FROM shardline_hub_creation_denials WHERE namespace=?1 ORDER BY issuer,subject")?;
        let denied_creators = query
            .query_map([namespace], |row| {
                Ok(HubCreationActor {
                    issuer: row.get(0)?,
                    subject: row.get(1)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(HubCreationPolicy {
            enabled,
            max_repositories: max_repositories.map(super::i64_to_u64).transpose()?,
            max_creations: max_creations.map(super::i64_to_u64).transpose()?,
            window_seconds,
            denied_creators,
        }))
    }
}
