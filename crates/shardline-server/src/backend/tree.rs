//! Revision and tree metadata policy shared by the concrete index stores.
use std::num::NonZeroUsize;

use shardline_index::{RepoKey, RevisionRecord, TreeEntry, TreeKey, TreeStore};
use shardline_protocol::{RepositoryScope, unix_now_seconds_lossy};

use super::{RegisterPathOutcome, ServerBackend};
use crate::{ServerError, validation::validate_content_hash};

impl ServerBackend {
    pub(crate) async fn resolve_tree_path(
        &self,
        key: &TreeKey,
        path: &str,
    ) -> Result<Option<TreeEntry>, ServerError> {
        let result = match self {
            Self::Local(backend) => backend.index_store().tree_entry(key, path).await?,
            Self::Postgres(backend) => backend.index_store().tree_entry(key, path).await?,
        };
        Ok(result)
    }

    pub(crate) async fn scan_tree_raw(
        &self,
        key: &TreeKey,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TreeEntry>, ServerError> {
        let result = match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .scan_tree(key, prefix, cursor, limit)
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .scan_tree(key, prefix, cursor, limit)
                    .await?
            }
        };
        Ok(result)
    }

    /// Validates the referenced file record, then atomically checks repository
    /// capacity and publishes the revision and path mapping. Storage transactions
    /// serialize registration with revision deletion/pruning across connections.
    /// Existing revision creation time is preserved while update time refreshes.
    /// An existing revision remains usable at the revision cap; the tree-entry
    /// cap retains the existing rule that refreshes at full capacity are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::UnregisteredFile`] when no record exists in the revision
    /// scope, [`ServerError::InvalidContentHash`] for a malformed `file_id`,
    /// [`ServerError::TooManyRevisions`] at the per-repo revision or tree-entry cap,
    /// or the adapter error when persistence fails.
    pub(crate) async fn register_tree_path(
        &self,
        key: &TreeKey,
        path: &str,
        file_id: &str,
        scope: Option<&RepositoryScope>,
        max_revisions_per_repo: NonZeroUsize,
        max_tree_entries_per_repo: NonZeroUsize,
    ) -> Result<RegisterPathOutcome, ServerError> {
        let _guard = match self {
            Self::Local(backend) => Some(backend.lock_metadata_writes().await),
            Self::Postgres(_) => None,
        };
        validate_content_hash(file_id)?;
        let record = match self {
            Self::Local(backend) => backend.read_record(file_id, None, scope).await,
            Self::Postgres(backend) => backend.read_record(file_id, None, scope).await,
        };
        let record = match record {
            Ok(record) => record,
            Err(ServerError::NotFound) => {
                return Err(ServerError::UnregisteredFile(key.revision.clone()));
            }
            Err(error) => return Err(error),
        };
        let now = unix_now_seconds_lossy();
        let entry = TreeEntry {
            provider: key.provider.clone(),
            owner: key.owner.clone(),
            repo: key.repo.clone(),
            revision: key.revision.clone(),
            path: path.to_owned(),
            file_id: file_id.to_owned(),
            size_bytes: record.total_bytes,
            updated_at_unix_seconds: now,
        };
        let outcome = match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .register_tree_entry(
                        &entry,
                        max_revisions_per_repo.get(),
                        max_tree_entries_per_repo.get(),
                    )
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .register_tree_entry(
                        &entry,
                        max_revisions_per_repo.get(),
                        max_tree_entries_per_repo.get(),
                    )
                    .await?
            }
        };
        let outcome = match outcome {
            shardline_index::TreeRegistrationOutcome::Registered(outcome) => outcome,
            shardline_index::TreeRegistrationOutcome::LimitExceeded => {
                return Err(ServerError::TooManyRevisions);
            }
        };
        Ok(RegisterPathOutcome {
            entry,
            created: outcome.created,
        })
    }

    pub(crate) async fn delete_tree_path(
        &self,
        key: &TreeKey,
        path: &str,
        recursive: bool,
    ) -> Result<u64, ServerError> {
        let result = match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .delete_tree_entries(key, path, recursive)
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .delete_tree_entries(key, path, recursive)
                    .await?
            }
        };
        Ok(result)
    }

    pub(crate) async fn list_revisions(
        &self,
        key: &RepoKey,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RevisionRecord>, ServerError> {
        let result = match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .list_revisions(key, cursor, limit)
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .list_revisions(key, cursor, limit)
                    .await?
            }
        };
        Ok(result)
    }

    pub(crate) async fn create_revision(
        &self,
        rev: &RevisionRecord,
        max_revisions: usize,
    ) -> Result<shardline_index::RevisionCreationOutcome, ServerError> {
        let result = match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .create_revision_bounded(rev, max_revisions)
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .create_revision_bounded(rev, max_revisions)
                    .await?
            }
        };
        Ok(result)
    }

    pub(crate) async fn delete_revision(
        &self,
        key: &RepoKey,
        rev: &str,
    ) -> Result<bool, ServerError> {
        let result = match self {
            Self::Local(backend) => backend.index_store().delete_revision(key, rev).await?,
            Self::Postgres(backend) => backend.index_store().delete_revision(key, rev).await?,
        };
        Ok(result > 0)
    }
}

#[cfg(test)]
mod tests;
