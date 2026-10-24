use shardline_protocol::RepositoryScope;
use sqlx::query_scalar;

use super::{PostgresIndexStore, PostgresMetadataStoreError, block_on_async};
use crate::hub::{HubObjectKind, HubObjectLocation};

impl PostgresIndexStore {
    /// Checks authoritative LFS-pointer membership in one repository commit.
    /// # Errors
    /// Returns an error when metadata cannot be read.
    pub async fn hub_commit_contains_lfs(
        &self,
        repo_id: &str,
        commit: &str,
        sha: &str,
        size: u64,
    ) -> Result<bool, PostgresMetadataStoreError> {
        let size = super::u64_to_i64(size)?;
        Ok(query_scalar(
            "SELECT EXISTS(SELECT 1 FROM shardline_hub_file_entries f JOIN shardline_hub_revisions r ON r.sha=f.commit_sha
             WHERE r.repo_id=$1 AND r.sha=$2 AND f.sha=$3 AND f.size=$4 AND f.is_lfs=TRUE)",
        ).bind(repo_id).bind(commit).bind(sha).bind(size).fetch_one(self.pool()).await?)
    }
}

pub(super) fn lookup(
    store: &PostgresIndexStore,
    scope: &RepositoryScope,
    kind: HubObjectKind,
    object_id: &str,
) -> Result<Option<HubObjectLocation>, PostgresMetadataStoreError> {
    block_on_async(async {
        let revision: Option<String> = query_scalar(
            "SELECT storage_revision FROM shardline_hub_object_locations WHERE repository_key=$1 AND object_kind=$2 AND object_id=$3",
        ).bind(crate::XetFileIdentity::repository_key_for_scope(scope)).bind(kind.as_str()).bind(object_id)
            .fetch_optional(store.pool()).await?;
        Ok(revision.map(|revision| HubObjectLocation {
            storage_revision: (!revision.is_empty()).then_some(revision),
        }))
    })
}

pub(super) fn remember(
    store: &PostgresIndexStore,
    scope: &RepositoryScope,
    kind: HubObjectKind,
    object_id: &str,
) -> Result<(), PostgresMetadataStoreError> {
    block_on_async(async {
        sqlx::query(
            "INSERT INTO shardline_hub_object_locations(repository_key,object_kind,object_id,storage_revision)
             VALUES ($1,$2,$3,$4) ON CONFLICT(repository_key,object_kind,object_id)
             DO UPDATE SET storage_revision=EXCLUDED.storage_revision
             WHERE shardline_hub_object_locations.storage_revision<>EXCLUDED.storage_revision",
        ).bind(crate::XetFileIdentity::repository_key_for_scope(scope)).bind(kind.as_str()).bind(object_id)
            .bind(scope.revision().unwrap_or_default()).execute(store.pool()).await?;
        Ok(())
    })
}
