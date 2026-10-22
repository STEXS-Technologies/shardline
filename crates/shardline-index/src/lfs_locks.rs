//! Durable Git LFS file locks shared by all nodes using a metadata database.

/// Repository-scoped file lock and its authenticated owner identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LfsLockRecord {
    /// Opaque immutable identifier.
    pub id: String,
    /// Repository-relative file path.
    pub path: String,
    /// Private issuer-qualified identity, never returned by HTTP.
    pub owner_key: String,
    /// Public owner name.
    pub owner_name: String,
    /// RFC 3339 creation timestamp.
    pub locked_at: String,
}

/// Atomic creation result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateLfsLockOutcome {
    /// Committed new lock.
    Created(LfsLockRecord),
    /// Existing owner, without changing the lock.
    Conflict(LfsLockRecord),
}

/// Atomic ownership check and deletion result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnlockLfsOutcome {
    /// Removed lock.
    Removed(LfsLockRecord),
    /// No such lock in the repository.
    Missing,
    /// Another owner and no forced unlock requested.
    Forbidden,
}

/// Transactional LFS locking for SQLite and PostgreSQL deployments.
#[async_trait::async_trait]
pub trait LfsLockStore: Send + Sync {
    /// Storage error.
    type Error: Send + Sync;
    /// Creates an exclusive lock without overwriting an existing owner.
    async fn create_lfs_lock(
        &self,
        scope: &str,
        lock: &LfsLockRecord,
    ) -> Result<CreateLfsLockOutcome, Self::Error>;
    /// Lists a bounded page, in immutable-ID order.
    async fn list_lfs_locks(
        &self,
        scope: &str,
        path: Option<&str>,
        id: Option<&str>,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<LfsLockRecord>, Self::Error>;
    /// Checks ownership and removes the lock in one transaction.
    async fn unlock_lfs_lock(
        &self,
        scope: &str,
        id: &str,
        owner: &str,
        force: bool,
    ) -> Result<UnlockLfsOutcome, Self::Error>;
}
