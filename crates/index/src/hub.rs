use std::sync::Arc;

pub use super::hub_local_sqlite::ensure_hub_tables;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubRepoType {
    Model,
    Dataset,
    Space,
}

impl HubRepoType {
    /// Returns the string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::Dataset => "dataset",
            Self::Space => "space",
        }
    }

    /// Parses from a string.
    #[must_use]
    pub fn parse_str(s: &str) -> Option<Self> {
        match s {
            "model" | "models" => Some(Self::Model),
            "dataset" | "datasets" => Some(Self::Dataset),
            "space" | "spaces" => Some(Self::Space),
            _ => None,
        }
    }

    /// Converts from the `shardline_hub_api::models::RepoType` enum.
    ///
    /// This is a bridge function. The Hub API crate defines its own `RepoType`
    /// and this crate defines `HubRepoType`. This converts between them.
    #[must_use]
    pub fn from_api_repo_type(rt: &str) -> Option<Self> {
        Self::parse_str(rt)
    }
}

/// A Hub repository record.
#[derive(Debug, Clone)]
pub struct HubRepo {
    pub repo_id: String,
    pub repo_type: HubRepoType,
    pub private: bool,
    pub default_branch: String,
    pub created_at_unix_seconds: u64,
    pub updated_at_unix_seconds: u64,
}

impl HubRepo {
    /// Generates a deterministic SHA for a commit.
    ///
    /// # Errors
    ///
    /// Returns an error if formatting the hash fails.
    pub fn compute_commit_sha(
        parent_sha: &str,
        message: &str,
        files_hash: &str,
    ) -> Result<String, std::fmt::Error> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        parent_sha.hash(&mut hasher);
        message.hash(&mut hasher);
        files_hash.hash(&mut hasher);
        Ok(format!("{:016x}", hasher.finish()))
    }
}

/// A Hub revision record.
#[derive(Debug, Clone)]
pub struct HubRevision {
    pub repo_id: String,
    pub ref_name: String,
    pub sha: String,
    pub parent_sha: Option<String>,
    pub message: Option<String>,
    pub created_at_unix_seconds: u64,
}

/// A Hub file entry within a commit tree.
#[derive(Debug, Clone)]
pub struct HubFileEntry {
    pub path: String,
    pub size: u64,
    pub sha: String,
    pub is_lfs: bool,
    /// Inline file content (only set for non-LFS files ≤1 MiB).
    pub inline_content: Option<Vec<u8>>,
}

/// A registered webhook for a Hub repository.
#[derive(Debug, Clone)]
pub struct HubWebhook {
    pub id: String,
    pub repo_id: String,
    pub url: String,
    pub events: Vec<String>,
    pub secret: Option<String>,
    pub active: bool,
    pub created_at_unix_seconds: u64,
}

/// Hub API persistence contract.
///
/// Both [`LocalIndexStore`](crate::LocalIndexStore) and
/// [`PostgresIndexStore`](crate::PostgresIndexStore) implement this trait to
/// provide durable storage for Hub API metadata.
pub trait HubStore: Send + Sync {
    /// Adapter-specific error type.
    type Error: std::fmt::Display + Send + Sync + 'static;

    /// Creates a new repository. Returns `Err` if the repo already exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the repository already exists or the storage backend
    /// operation fails.
    fn create_repo(
        &self,
        repo_type: HubRepoType,
        name: &str,
        private: bool,
    ) -> Result<HubRepo, Self::Error>;

    /// Returns a repository by ID, or `None` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn get_repo(&self, repo_id: &str) -> Result<Option<HubRepo>, Self::Error>;

    /// Lists all repositories.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn list_repos(&self) -> Result<Vec<HubRepo>, Self::Error>;

    /// Searches repositories by name prefix and optional type filter.
    ///
    /// Returns up to `limit` repositories whose `repo_id` starts with `name_prefix`,
    /// optionally filtered by `repo_type`. Results are ordered by `repo_id`.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn search_repos(
        &self,
        repo_type: Option<HubRepoType>,
        name_prefix: &str,
        limit: usize,
    ) -> Result<Vec<HubRepo>, Self::Error>;

    /// Creates a new revision (commit) in a repository.
    ///
    /// If `parent_sha` is provided, implements optimistic concurrency: returns
    /// `Err` if the current HEAD does not match.
    ///
    /// # Errors
    ///
    /// Returns an error on optimistic concurrency conflict or when the storage
    /// backend operation fails.
    fn create_revision(
        &self,
        repo_id: &str,
        parent_sha: Option<&str>,
        new_sha: &str,
        ref_name: &str,
        message: &str,
    ) -> Result<HubRevision, Self::Error>;

    /// Lists all revisions for a repository.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn list_revisions(&self, repo_id: &str) -> Result<Vec<HubRevision>, Self::Error>;

    /// Resolves a revision string ("main", a SHA, or a ref name) to a SHA.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn resolve_revision(
        &self,
        repo_id: &str,
        revision: &str,
    ) -> Result<Option<String>, Self::Error>;

    /// Stores file entries for a given commit SHA.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn store_files(&self, commit_sha: &str, files: &[HubFileEntry]) -> Result<(), Self::Error>;

    /// Returns all file entries at a given commit SHA.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn get_files(&self, commit_sha: &str) -> Result<Vec<HubFileEntry>, Self::Error>;

    /// Stores an LFS object.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn put_lfs_object(&self, oid: &str, data: &[u8]) -> Result<(), Self::Error>;

    /// Returns an LFS object by OID.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn get_lfs_object(&self, oid: &str) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Returns whether an LFS object exists.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn has_lfs_object(&self, oid: &str) -> Result<bool, Self::Error>;

    /// Creates a webhook for a repository.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn create_webhook(
        &self,
        repo_id: &str,
        url: &str,
        events: &[String],
        secret: Option<&str>,
    ) -> Result<crate::hub::HubWebhook, Self::Error>;

    /// Lists all webhooks for a repository.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn list_webhooks(&self, repo_id: &str) -> Result<Vec<crate::hub::HubWebhook>, Self::Error>;

    /// Deletes a repository and all associated data (revisions, file entries, webhooks).
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn delete_repo(&self, repo_id: &str) -> Result<(), Self::Error>;

    /// Deletes a webhook by ID.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn delete_webhook(&self, repo_id: &str, webhook_id: &str) -> Result<(), Self::Error>;

    /// Returns webhooks subscribed to a given event for a repository.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn webhooks_for_event(
        &self,
        repo_id: &str,
        event: &str,
    ) -> Result<Vec<crate::hub::HubWebhook>, Self::Error>;
}

/// A type-erased wrapper that boxes errors for use as `dyn HubStore`.
pub struct BoxedHubStore {
    inner: Arc<dyn ErasedHubStore>,
}

impl Clone for BoxedHubStore {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

trait ErasedHubStore: Send + Sync {
    fn create_repo(
        &self,
        repo_type: HubRepoType,
        name: &str,
        private: bool,
    ) -> Result<HubRepo, Box<dyn std::error::Error + Send + Sync>>;

    fn get_repo(
        &self,
        repo_id: &str,
    ) -> Result<Option<HubRepo>, Box<dyn std::error::Error + Send + Sync>>;

    fn list_repos(&self) -> Result<Vec<HubRepo>, Box<dyn std::error::Error + Send + Sync>>;

    fn search_repos(
        &self,
        repo_type: Option<HubRepoType>,
        name_prefix: &str,
        limit: usize,
    ) -> Result<Vec<HubRepo>, Box<dyn std::error::Error + Send + Sync>>;

    fn create_revision(
        &self,
        repo_id: &str,
        parent_sha: Option<&str>,
        new_sha: &str,
        ref_name: &str,
        message: &str,
    ) -> Result<HubRevision, Box<dyn std::error::Error + Send + Sync>>;

    fn list_revisions(
        &self,
        repo_id: &str,
    ) -> Result<Vec<HubRevision>, Box<dyn std::error::Error + Send + Sync>>;

    fn resolve_revision(
        &self,
        repo_id: &str,
        revision: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>>;

    fn store_files(
        &self,
        commit_sha: &str,
        files: &[HubFileEntry],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn get_files(
        &self,
        commit_sha: &str,
    ) -> Result<Vec<HubFileEntry>, Box<dyn std::error::Error + Send + Sync>>;

    fn put_lfs_object(
        &self,
        oid: &str,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn get_lfs_object(
        &self,
        oid: &str,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>;

    fn has_lfs_object(&self, oid: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>;

    fn create_webhook(
        &self,
        repo_id: &str,
        url: &str,
        events: &[String],
        secret: Option<&str>,
    ) -> Result<HubWebhook, Box<dyn std::error::Error + Send + Sync>>;

    fn list_webhooks(
        &self,
        repo_id: &str,
    ) -> Result<Vec<HubWebhook>, Box<dyn std::error::Error + Send + Sync>>;

    fn delete_repo(
        &self,
        repo_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn delete_webhook(
        &self,
        repo_id: &str,
        webhook_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn webhooks_for_event(
        &self,
        repo_id: &str,
        event: &str,
    ) -> Result<Vec<HubWebhook>, Box<dyn std::error::Error + Send + Sync>>;
}

impl<T: HubStore> ErasedHubStore for T {
    fn create_repo(
        &self,
        repo_type: HubRepoType,
        name: &str,
        private: bool,
    ) -> Result<HubRepo, Box<dyn std::error::Error + Send + Sync>> {
        T::create_repo(self, repo_type, name, private)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn get_repo(
        &self,
        repo_id: &str,
    ) -> Result<Option<HubRepo>, Box<dyn std::error::Error + Send + Sync>> {
        T::get_repo(self, repo_id).map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn list_repos(&self) -> Result<Vec<HubRepo>, Box<dyn std::error::Error + Send + Sync>> {
        T::list_repos(self).map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn search_repos(
        &self,
        repo_type: Option<HubRepoType>,
        name_prefix: &str,
        limit: usize,
    ) -> Result<Vec<HubRepo>, Box<dyn std::error::Error + Send + Sync>> {
        T::search_repos(self, repo_type, name_prefix, limit)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn create_revision(
        &self,
        repo_id: &str,
        parent_sha: Option<&str>,
        new_sha: &str,
        ref_name: &str,
        message: &str,
    ) -> Result<HubRevision, Box<dyn std::error::Error + Send + Sync>> {
        T::create_revision(self, repo_id, parent_sha, new_sha, ref_name, message)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn list_revisions(
        &self,
        repo_id: &str,
    ) -> Result<Vec<HubRevision>, Box<dyn std::error::Error + Send + Sync>> {
        T::list_revisions(self, repo_id)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn resolve_revision(
        &self,
        repo_id: &str,
        revision: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        T::resolve_revision(self, repo_id, revision)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn store_files(
        &self,
        commit_sha: &str,
        files: &[HubFileEntry],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        T::store_files(self, commit_sha, files)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn get_files(
        &self,
        commit_sha: &str,
    ) -> Result<Vec<HubFileEntry>, Box<dyn std::error::Error + Send + Sync>> {
        T::get_files(self, commit_sha)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn put_lfs_object(
        &self,
        oid: &str,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        T::put_lfs_object(self, oid, data)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn get_lfs_object(
        &self,
        oid: &str,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        T::get_lfs_object(self, oid)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn has_lfs_object(&self, oid: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        T::has_lfs_object(self, oid)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn create_webhook(
        &self,
        repo_id: &str,
        url: &str,
        events: &[String],
        secret: Option<&str>,
    ) -> Result<HubWebhook, Box<dyn std::error::Error + Send + Sync>> {
        T::create_webhook(self, repo_id, url, events, secret)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn list_webhooks(
        &self,
        repo_id: &str,
    ) -> Result<Vec<HubWebhook>, Box<dyn std::error::Error + Send + Sync>> {
        T::list_webhooks(self, repo_id)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn delete_repo(
        &self,
        repo_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        T::delete_repo(self, repo_id)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn delete_webhook(
        &self,
        repo_id: &str,
        webhook_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        T::delete_webhook(self, repo_id, webhook_id)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn webhooks_for_event(
        &self,
        repo_id: &str,
        event: &str,
    ) -> Result<Vec<crate::hub::HubWebhook>, Box<dyn std::error::Error + Send + Sync>> {
        T::webhooks_for_event(self, repo_id, event)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }
}

impl BoxedHubStore {
    /// Creates a new `BoxedHubStore` from any `HubStore` implementation.
    pub fn new(
        store: impl HubStore<Error = Box<dyn std::error::Error + Send + Sync>> + 'static,
    ) -> Self {
        Self {
            inner: Arc::new(store),
        }
    }

    /// Creates a new `BoxedHubStore` from a value implementing `HubStore` with any error type.
    pub fn from_store<T: HubStore + 'static>(store: T) -> Self
    where
        T::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let erased: Arc<dyn ErasedHubStore> = Arc::new(ErasedAdapter(store));
        Self { inner: erased }
    }

    /// Creates a repository.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn create_repo(
        &self,
        repo_type: HubRepoType,
        name: &str,
        private: bool,
    ) -> Result<HubRepo, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.create_repo(repo_type, name, private)
    }

    /// Returns a repository by ID.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn get_repo(
        &self,
        repo_id: &str,
    ) -> Result<Option<HubRepo>, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.get_repo(repo_id)
    }

    /// Lists all repositories.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn list_repos(&self) -> Result<Vec<HubRepo>, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.list_repos()
    }

    /// Searches repositories by name prefix and optional type filter.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn search_repos(
        &self,
        repo_type: Option<HubRepoType>,
        name_prefix: &str,
        limit: usize,
    ) -> Result<Vec<HubRepo>, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.search_repos(repo_type, name_prefix, limit)
    }

    /// Creates a new revision.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn create_revision(
        &self,
        repo_id: &str,
        parent_sha: Option<&str>,
        new_sha: &str,
        ref_name: &str,
        message: &str,
    ) -> Result<HubRevision, Box<dyn std::error::Error + Send + Sync>> {
        self.inner
            .create_revision(repo_id, parent_sha, new_sha, ref_name, message)
    }

    /// Lists all revisions.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn list_revisions(
        &self,
        repo_id: &str,
    ) -> Result<Vec<HubRevision>, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.list_revisions(repo_id)
    }

    /// Resolves a revision.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn resolve_revision(
        &self,
        repo_id: &str,
        revision: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.resolve_revision(repo_id, revision)
    }

    /// Stores file entries.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn store_files(
        &self,
        commit_sha: &str,
        files: &[HubFileEntry],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.store_files(commit_sha, files)
    }

    /// Returns file entries.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn get_files(
        &self,
        commit_sha: &str,
    ) -> Result<Vec<HubFileEntry>, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.get_files(commit_sha)
    }

    /// Stores an LFS object.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn put_lfs_object(
        &self,
        oid: &str,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.put_lfs_object(oid, data)
    }

    /// Returns an LFS object.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn get_lfs_object(
        &self,
        oid: &str,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.get_lfs_object(oid)
    }

    /// Returns whether an LFS object exists.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn has_lfs_object(
        &self,
        oid: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.has_lfs_object(oid)
    }

    /// Creates a webhook for a repository.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn create_webhook(
        &self,
        repo_id: &str,
        url: &str,
        events: &[String],
        secret: Option<&str>,
    ) -> Result<HubWebhook, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.create_webhook(repo_id, url, events, secret)
    }

    /// Lists all webhooks for a repository.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn list_webhooks(
        &self,
        repo_id: &str,
    ) -> Result<Vec<HubWebhook>, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.list_webhooks(repo_id)
    }

    /// Deletes a repository and all associated data.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn delete_repo(
        &self,
        repo_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.delete_repo(repo_id)
    }

    /// Deletes a webhook by ID.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn delete_webhook(
        &self,
        repo_id: &str,
        webhook_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.delete_webhook(repo_id, webhook_id)
    }

    /// Returns webhooks subscribed to a given event for a repository.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn webhooks_for_event(
        &self,
        repo_id: &str,
        event: &str,
    ) -> Result<Vec<HubWebhook>, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.webhooks_for_event(repo_id, event)
    }
}

/// Adapter to bridge any `HubStore` to `ErasedHubStore` via `Into<Box<dyn Error>>`.
struct ErasedAdapter<T>(T);

impl<T: HubStore> ErasedHubStore for ErasedAdapter<T>
where
    T::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    fn create_repo(
        &self,
        repo_type: HubRepoType,
        name: &str,
        private: bool,
    ) -> Result<HubRepo, Box<dyn std::error::Error + Send + Sync>> {
        T::create_repo(&self.0, repo_type, name, private).map_err(Into::into)
    }

    fn get_repo(
        &self,
        repo_id: &str,
    ) -> Result<Option<HubRepo>, Box<dyn std::error::Error + Send + Sync>> {
        T::get_repo(&self.0, repo_id).map_err(Into::into)
    }

    fn list_repos(&self) -> Result<Vec<HubRepo>, Box<dyn std::error::Error + Send + Sync>> {
        T::list_repos(&self.0).map_err(Into::into)
    }

    fn search_repos(
        &self,
        repo_type: Option<HubRepoType>,
        name_prefix: &str,
        limit: usize,
    ) -> Result<Vec<HubRepo>, Box<dyn std::error::Error + Send + Sync>> {
        T::search_repos(&self.0, repo_type, name_prefix, limit).map_err(Into::into)
    }

    fn create_revision(
        &self,
        repo_id: &str,
        parent_sha: Option<&str>,
        new_sha: &str,
        ref_name: &str,
        message: &str,
    ) -> Result<HubRevision, Box<dyn std::error::Error + Send + Sync>> {
        T::create_revision(&self.0, repo_id, parent_sha, new_sha, ref_name, message)
            .map_err(Into::into)
    }

    fn list_revisions(
        &self,
        repo_id: &str,
    ) -> Result<Vec<HubRevision>, Box<dyn std::error::Error + Send + Sync>> {
        T::list_revisions(&self.0, repo_id).map_err(Into::into)
    }

    fn resolve_revision(
        &self,
        repo_id: &str,
        revision: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        T::resolve_revision(&self.0, repo_id, revision).map_err(Into::into)
    }

    fn store_files(
        &self,
        commit_sha: &str,
        files: &[HubFileEntry],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        T::store_files(&self.0, commit_sha, files).map_err(Into::into)
    }

    fn get_files(
        &self,
        commit_sha: &str,
    ) -> Result<Vec<HubFileEntry>, Box<dyn std::error::Error + Send + Sync>> {
        T::get_files(&self.0, commit_sha).map_err(Into::into)
    }

    fn put_lfs_object(
        &self,
        oid: &str,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        T::put_lfs_object(&self.0, oid, data).map_err(Into::into)
    }

    fn get_lfs_object(
        &self,
        oid: &str,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        T::get_lfs_object(&self.0, oid).map_err(Into::into)
    }

    fn has_lfs_object(&self, oid: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        T::has_lfs_object(&self.0, oid).map_err(Into::into)
    }

    fn create_webhook(
        &self,
        repo_id: &str,
        url: &str,
        events: &[String],
        secret: Option<&str>,
    ) -> Result<HubWebhook, Box<dyn std::error::Error + Send + Sync>> {
        T::create_webhook(&self.0, repo_id, url, events, secret).map_err(Into::into)
    }

    fn list_webhooks(
        &self,
        repo_id: &str,
    ) -> Result<Vec<HubWebhook>, Box<dyn std::error::Error + Send + Sync>> {
        T::list_webhooks(&self.0, repo_id).map_err(Into::into)
    }

    fn delete_repo(
        &self,
        repo_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        T::delete_repo(&self.0, repo_id).map_err(Into::into)
    }

    fn delete_webhook(
        &self,
        repo_id: &str,
        webhook_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        T::delete_webhook(&self.0, repo_id, webhook_id).map_err(Into::into)
    }

    fn webhooks_for_event(
        &self,
        repo_id: &str,
        event: &str,
    ) -> Result<Vec<HubWebhook>, Box<dyn std::error::Error + Send + Sync>> {
        T::webhooks_for_event(&self.0, repo_id, event).map_err(Into::into)
    }
}
