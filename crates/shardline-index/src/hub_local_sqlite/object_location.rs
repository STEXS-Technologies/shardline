use rusqlite::{OptionalExtension, params};
use shardline_protocol::RepositoryScope;

use super::{
    LocalIndexStore, LocalIndexStoreError, open_hub_connection, open_hub_connection_rw,
    retry_sqlite_busy,
};
use crate::hub::{HubObjectKind, HubObjectLocation};

impl LocalIndexStore {
    /// Checks authoritative LFS-pointer membership in one repository commit.
    /// # Errors
    /// Returns an error when metadata cannot be read.
    pub fn hub_commit_contains_lfs(
        &self,
        repo_id: &str,
        commit: &str,
        sha: &str,
        size: u64,
    ) -> Result<bool, LocalIndexStoreError> {
        let connection = open_hub_connection(self.root())?;
        let size = super::u64_to_i64(size)?;
        Ok(connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM shardline_hub_file_entries f JOIN shardline_hub_revisions r ON r.sha=f.commit_sha
             WHERE r.repo_id=?1 AND r.sha=?2 AND f.sha=?3 AND f.size=?4 AND f.is_lfs=1)",
            params![repo_id,commit,sha,size], |row| row.get(0),
        )?)
    }
}

pub(super) fn lookup(
    store: &LocalIndexStore,
    scope: &RepositoryScope,
    kind: HubObjectKind,
    object_id: &str,
) -> Result<Option<HubObjectLocation>, LocalIndexStoreError> {
    let connection = open_hub_connection(store.root())?;
    let revision: Option<String> = connection.query_row(
        "SELECT storage_revision FROM shardline_hub_object_locations WHERE repository_key=?1 AND object_kind=?2 AND object_id=?3",
        params![crate::XetFileIdentity::repository_key_for_scope(scope), kind.as_str(), object_id],
        |row| row.get(0),
    ).optional()?;
    Ok(revision.map(|revision| HubObjectLocation {
        storage_revision: (!revision.is_empty()).then_some(revision),
    }))
}

pub(super) fn remember(
    store: &LocalIndexStore,
    scope: &RepositoryScope,
    kind: HubObjectKind,
    object_id: &str,
) -> Result<(), LocalIndexStoreError> {
    retry_sqlite_busy(|| {
        let connection = open_hub_connection_rw(store.root())?;
        connection.execute(
            "INSERT INTO shardline_hub_object_locations(repository_key,object_kind,object_id,storage_revision)
             VALUES (?1,?2,?3,?4) ON CONFLICT(repository_key,object_kind,object_id)
             DO UPDATE SET storage_revision=excluded.storage_revision
             WHERE storage_revision<>excluded.storage_revision",
            params![crate::XetFileIdentity::repository_key_for_scope(scope), kind.as_str(), object_id, scope.revision().unwrap_or_default()],
        )?;
        Ok(())
    })
}
