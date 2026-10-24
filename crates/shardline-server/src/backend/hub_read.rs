//! Authorize an immutable Hub file before selecting its native storage scope.

use shardline_protocol::RepositoryScope;

use crate::{ServerBackend, ServerError};

impl ServerBackend {
    pub(crate) async fn hub_native_read_scope(
        &self,
        scope: &RepositoryScope,
        file_id: &str,
    ) -> Result<RepositoryScope, ServerError> {
        crate::validation::validate_content_hash(file_id)?;
        let commit = scope.revision().ok_or(ServerError::NotFound)?.to_owned();
        let repo_id = format!("{}/{}", scope.owner(), scope.name());
        let record = match self {
            Self::Local(backend) => {
                let records = backend.record_store().clone();
                let root = backend.metadata_root.join("hub");
                let scope = scope.clone();
                let file_id = file_id.to_owned();
                crate::admission::blocking_work(&backend.stream_work_pool, move || {
                    let (sha, record) = records
                        .repository_xet_file(&scope, &file_id)?
                        .ok_or(ServerError::NotFound)?;
                    let hub = shardline_index::LocalIndexStore::open(root);
                    if !hub.hub_commit_contains_lfs(&repo_id, &commit, &sha, record.total_bytes)? {
                        return Err(ServerError::NotFound);
                    }
                    Ok::<_, ServerError>(record)
                })
                .await?
            }
            Self::Postgres(backend) => {
                let (sha, record) = backend
                    .record_store()
                    .repository_xet_file(scope, file_id)
                    .await?
                    .ok_or(ServerError::NotFound)?;
                if !backend
                    .index_store()
                    .hub_commit_contains_lfs(&repo_id, &commit, &sha, record.total_bytes)
                    .await?
                {
                    return Err(ServerError::NotFound);
                }
                record
            }
        };
        record.validate_reconstruction_plan()?;
        record.repository_scope.ok_or(ServerError::NotFound)
    }
}
