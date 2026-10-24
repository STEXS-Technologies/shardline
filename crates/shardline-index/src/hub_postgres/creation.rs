//! Shared-policy publication and owner policy administration.
use super::{
    PostgresIndexStore, PostgresMetadataStoreError, block_on_async, i64_to_u64,
    persist_hub_ref_evidence, u64_to_i64,
};
use crate::hub::{HubCreationActor, HubCreationPolicy, HubRepo, HubRepoType};
use shardline_reliability::{HubRefEvidenceLog, HubRefSnapshot};
use sqlx::Row;

#[cfg(test)]
mod tests;

pub(super) fn create(
    store: &PostgresIndexStore,
    repo_type: HubRepoType,
    name: &str,
    private: bool,
    authorized: bool,
    actor: Option<&HubCreationActor>,
) -> Result<HubRepo, PostgresMetadataStoreError> {
    block_on_async(async {
        let initial_sha = crate::hub::EMPTY_HUB_REVISION;
        let mut tx = store.pool().begin().await?;
        let row = sqlx::query(
            "INSERT INTO shardline_hub_repos(repo_id,repo_type,private,default_branch,created_at_unix_seconds,updated_at_unix_seconds,creation_authorized,creation_issuer,creation_subject)
             VALUES ($1,$2,$3,$4,EXTRACT(EPOCH FROM now())::bigint,EXTRACT(EPOCH FROM now())::bigint,$5,$6,$7)
             RETURNING created_at_unix_seconds,updated_at_unix_seconds",
        ).bind(name).bind(super::repo_type_to_str(repo_type)).bind(private).bind(initial_sha).bind(authorized)
            .bind(actor.map(|a| a.issuer.as_str())).bind(actor.map(|a| a.subject.as_str()))
            .fetch_one(&mut *tx).await?;
        sqlx::query(
            "INSERT INTO shardline_hub_revisions(repo_id,ref_name,sha,parent_sha,message,created_at_unix_seconds)
             VALUES ($1,'main',$2,NULL,NULL,EXTRACT(EPOCH FROM now())::bigint) ON CONFLICT(repo_id,sha) DO NOTHING",
        ).bind(name).bind(initial_sha).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO shardline_hub_refs(repo_id,ref_name,sha) VALUES ($1,'main',$2) ON CONFLICT(repo_id,ref_name) DO NOTHING")
            .bind(name).bind(initial_sha).execute(&mut *tx).await?;
        let evidence = HubRefEvidenceLog::baseline(HubRefSnapshot::new(
            name,
            "main",
            Some(initial_sha.to_owned()),
        )?)?;
        persist_hub_ref_evidence(&mut tx, &evidence).await?;
        tx.commit().await?;
        Ok(HubRepo {
            repo_id: name.to_owned(),
            repo_type,
            private,
            default_branch: initial_sha.to_owned(),
            created_at_unix_seconds: i64_to_u64(row.try_get("created_at_unix_seconds")?)?,
            updated_at_unix_seconds: i64_to_u64(row.try_get("updated_at_unix_seconds")?)?,
        })
    })
}

impl PostgresIndexStore {
    /// Replaces or clears an owner-managed namespace creation policy atomically.
    /// # Errors
    /// Returns validation or storage errors without partial policy publication.
    pub fn set_hub_creation_policy(
        &self,
        namespace: &str,
        policy: Option<&HubCreationPolicy>,
    ) -> Result<(), PostgresMetadataStoreError> {
        let defaults = HubCreationPolicy::default();
        let effective = policy.unwrap_or(&defaults);
        effective.validate(namespace)?;
        block_on_async(async {
            let mut tx = self.pool().begin().await?;
            // Match the trigger's order before taking any policy/actor row locks.
            sqlx::query("SELECT pg_advisory_xact_lock(1397506896,hashtext($1))")
                .bind(namespace)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                "INSERT INTO shardline_hub_creation_policies(namespace,configured,enabled,max_repositories,max_creations,window_seconds)
                 VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(namespace) DO UPDATE SET configured=excluded.configured,
                 enabled=excluded.enabled,max_repositories=excluded.max_repositories,max_creations=excluded.max_creations,window_seconds=excluded.window_seconds",
            ).bind(namespace).bind(policy.is_some()).bind(effective.enabled)
                .bind(effective.max_repositories.map(u64_to_i64).transpose()?)
                .bind(effective.max_creations.map(u64_to_i64).transpose()?).bind(i64::from(effective.window_seconds))
                .execute(&mut *tx).await?;
            sqlx::query("DELETE FROM shardline_hub_creation_denials WHERE namespace=$1")
                .bind(namespace)
                .execute(&mut *tx)
                .await?;
            for actor in &effective.denied_creators {
                sqlx::query("INSERT INTO shardline_hub_creation_denials(namespace,issuer,subject) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING")
                    .bind(namespace).bind(&actor.issuer).bind(&actor.subject).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            Ok(())
        })
    }

    /// Reads a consistent configured policy, excluding implicit defaults.
    /// # Errors
    /// Returns validation or storage errors when policy metadata cannot be read.
    pub fn hub_creation_policy(
        &self,
        namespace: &str,
    ) -> Result<Option<HubCreationPolicy>, PostgresMetadataStoreError> {
        HubCreationPolicy::default().validate(namespace)?;
        block_on_async(async {
            let mut tx = self.pool().begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock(1397506896,hashtext($1))")
                .bind(namespace)
                .execute(&mut *tx)
                .await?;
            let row = sqlx::query("SELECT enabled,max_repositories,max_creations,window_seconds FROM shardline_hub_creation_policies WHERE namespace=$1 AND configured")
                .bind(namespace).fetch_optional(&mut *tx).await?;
            let Some(row) = row else { return Ok(None) };
            let rows = sqlx::query("SELECT issuer,subject FROM shardline_hub_creation_denials WHERE namespace=$1 ORDER BY issuer,subject")
                .bind(namespace).fetch_all(&mut *tx).await?;
            let denied_creators = rows
                .iter()
                .map(|r| {
                    Ok(HubCreationActor {
                        issuer: r.try_get("issuer")?,
                        subject: r.try_get("subject")?,
                    })
                })
                .collect::<Result<Vec<_>, sqlx::Error>>()?;
            let policy = HubCreationPolicy {
                enabled: row.try_get("enabled")?,
                max_repositories: row
                    .try_get::<Option<i64>, _>("max_repositories")?
                    .map(i64_to_u64)
                    .transpose()?,
                max_creations: row
                    .try_get::<Option<i64>, _>("max_creations")?
                    .map(i64_to_u64)
                    .transpose()?,
                window_seconds: u32::try_from(row.try_get::<i64, _>("window_seconds")?).map_err(
                    |error| PostgresMetadataStoreError::IntegerOutOfRange(error.to_string()),
                )?,
                denied_creators,
            };
            tx.commit().await?;
            Ok(Some(policy))
        })
    }
}
