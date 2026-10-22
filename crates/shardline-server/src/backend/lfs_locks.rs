use crate::{ServerBackend, ServerError};
use shardline_index::{CreateLfsLockOutcome, LfsLockRecord, LfsLockStore, UnlockLfsOutcome};

impl ServerBackend {
    pub(crate) async fn create_lfs_lock(
        &self,
        scope: &str,
        lock: &LfsLockRecord,
    ) -> Result<CreateLfsLockOutcome, ServerError> {
        match self {
            Self::Local(backend) => Ok(backend.record_store().create_lfs_lock(scope, lock).await?),
            Self::Postgres(backend) => {
                Ok(backend.record_store().create_lfs_lock(scope, lock).await?)
            }
        }
    }

    pub(crate) async fn list_lfs_locks(
        &self,
        scope: &str,
        path: Option<&str>,
        id: Option<&str>,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<LfsLockRecord>, ServerError> {
        match self {
            Self::Local(backend) => Ok(backend
                .record_store()
                .list_lfs_locks(scope, path, id, after, limit)
                .await?),
            Self::Postgres(backend) => Ok(backend
                .record_store()
                .list_lfs_locks(scope, path, id, after, limit)
                .await?),
        }
    }

    pub(crate) async fn unlock_lfs_lock(
        &self,
        scope: &str,
        id: &str,
        owner: &str,
        force: bool,
    ) -> Result<UnlockLfsOutcome, ServerError> {
        match self {
            Self::Local(backend) => Ok(backend
                .record_store()
                .unlock_lfs_lock(scope, id, owner, force)
                .await?),
            Self::Postgres(backend) => Ok(backend
                .record_store()
                .unlock_lfs_lock(scope, id, owner, force)
                .await?),
        }
    }
}
