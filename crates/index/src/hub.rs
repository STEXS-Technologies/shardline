use shardline_protocol::SecretString;
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
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// An active Git-compatible reference pointing at a revision.
///
/// Revisions are immutable history records. References are kept separately so a
/// branch or tag can be removed without deleting the commit data it previously
/// pointed to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubRef {
    pub repo_id: String,
    pub ref_name: String,
    pub sha: String,
}

/// Converts the Git spelling of a branch reference to the Hub storage spelling.
///
/// Tags deliberately keep their full `refs/tags/...` name, while branches use
/// their short name so Hub API revisions such as `main` and Git Smart HTTP
/// `refs/heads/main` address the same ref.
#[must_use]
pub fn canonical_ref_name(ref_name: &str) -> &str {
    ref_name
        .strip_prefix("refs/heads/")
        .filter(|name| !name.is_empty())
        .unwrap_or(ref_name)
}

/// A Hub file entry within a commit tree.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub secret: Option<SecretString>,
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

    /// Lists the active branch and tag references for a repository.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    fn list_refs(&self, repo_id: &str) -> Result<Vec<HubRef>, Self::Error>;

    /// Deletes an active branch or tag only if it still points to `expected_sha`.
    ///
    /// The compare-and-delete semantics prevent a stale Git receive-pack request
    /// from deleting a ref that has advanced concurrently.
    ///
    /// # Errors
    ///
    /// Returns an error when the ref is protected, missing, has moved, or the
    /// storage backend operation fails.
    fn delete_ref(
        &self,
        repo_id: &str,
        ref_name: &str,
        expected_sha: &str,
    ) -> Result<(), Self::Error>;

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

    fn list_refs(
        &self,
        repo_id: &str,
    ) -> Result<Vec<HubRef>, Box<dyn std::error::Error + Send + Sync>>;

    fn delete_ref(
        &self,
        repo_id: &str,
        ref_name: &str,
        expected_sha: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

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

    fn delete_repo(&self, repo_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

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

    fn list_refs(
        &self,
        repo_id: &str,
    ) -> Result<Vec<HubRef>, Box<dyn std::error::Error + Send + Sync>> {
        T::list_refs(self, repo_id).map_err(|e| Box::new(std::io::Error::other(e.to_string())) as _)
    }

    fn delete_ref(
        &self,
        repo_id: &str,
        ref_name: &str,
        expected_sha: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        T::delete_ref(self, repo_id, ref_name, expected_sha)
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

    fn delete_repo(&self, repo_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    /// Lists active branches and tags.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage backend operation fails.
    pub fn list_refs(
        &self,
        repo_id: &str,
    ) -> Result<Vec<HubRef>, Box<dyn std::error::Error + Send + Sync>> {
        self.inner.list_refs(repo_id)
    }

    /// Atomically removes a branch or tag when it still points to `expected_sha`.
    ///
    /// # Errors
    ///
    /// Returns an error when the ref is protected, missing, has moved, or the
    /// storage backend operation fails.
    pub fn delete_ref(
        &self,
        repo_id: &str,
        ref_name: &str,
        expected_sha: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.delete_ref(repo_id, ref_name, expected_sha)
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

    fn list_refs(
        &self,
        repo_id: &str,
    ) -> Result<Vec<HubRef>, Box<dyn std::error::Error + Send + Sync>> {
        T::list_refs(&self.0, repo_id).map_err(Into::into)
    }

    fn delete_ref(
        &self,
        repo_id: &str,
        ref_name: &str,
        expected_sha: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        T::delete_ref(&self.0, repo_id, ref_name, expected_sha).map_err(Into::into)
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

    fn delete_repo(&self, repo_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    // ---------------------------------------------------------------------------
    // HubRepo::compute_commit_sha
    // ---------------------------------------------------------------------------

    #[test]
    fn compute_commit_sha_same_inputs_same_output() {
        let sha1 = HubRepo::compute_commit_sha("abc", "commit msg", "def").unwrap();
        let sha2 = HubRepo::compute_commit_sha("abc", "commit msg", "def").unwrap();
        assert_eq!(sha1, sha2);
    }

    #[test]
    fn compute_commit_sha_different_parent_produces_different_sha() {
        let sha1 = HubRepo::compute_commit_sha("abc", "commit msg", "def").unwrap();
        let sha2 = HubRepo::compute_commit_sha("xyz", "commit msg", "def").unwrap();
        assert_ne!(sha1, sha2);
    }

    #[test]
    fn compute_commit_sha_different_message_produces_different_sha() {
        let sha1 = HubRepo::compute_commit_sha("abc", "msg1", "def").unwrap();
        let sha2 = HubRepo::compute_commit_sha("abc", "msg2", "def").unwrap();
        assert_ne!(sha1, sha2);
    }

    #[test]
    fn compute_commit_sha_different_files_produces_different_sha() {
        let sha1 = HubRepo::compute_commit_sha("abc", "msg", "aaa").unwrap();
        let sha2 = HubRepo::compute_commit_sha("abc", "msg", "bbb").unwrap();
        assert_ne!(sha1, sha2);
    }

    // ---------------------------------------------------------------------------
    // HubRepoType::as_str
    // ---------------------------------------------------------------------------

    #[test]
    fn hub_repo_type_as_str() {
        assert_eq!(HubRepoType::Model.as_str(), "model");
        assert_eq!(HubRepoType::Dataset.as_str(), "dataset");
        assert_eq!(HubRepoType::Space.as_str(), "space");
    }

    // ---------------------------------------------------------------------------
    // HubRepoType::parse_str
    // ---------------------------------------------------------------------------

    #[test]
    fn parse_str_valid_singular() {
        assert_eq!(HubRepoType::parse_str("model"), Some(HubRepoType::Model));
        assert_eq!(
            HubRepoType::parse_str("dataset"),
            Some(HubRepoType::Dataset)
        );
        assert_eq!(HubRepoType::parse_str("space"), Some(HubRepoType::Space));
    }

    #[test]
    fn parse_str_valid_plural() {
        assert_eq!(HubRepoType::parse_str("models"), Some(HubRepoType::Model));
        assert_eq!(
            HubRepoType::parse_str("datasets"),
            Some(HubRepoType::Dataset)
        );
        assert_eq!(HubRepoType::parse_str("spaces"), Some(HubRepoType::Space));
    }

    #[test]
    fn parse_str_invalid() {
        assert_eq!(HubRepoType::parse_str("invalid"), None);
        assert_eq!(HubRepoType::parse_str(""), None);
        assert_eq!(HubRepoType::parse_str("unknown"), None);
    }

    // ---------------------------------------------------------------------------
    // HubRepo fields
    // ---------------------------------------------------------------------------

    #[test]
    fn hub_repo_fields() {
        let repo = HubRepo {
            repo_id: "owner/name".to_owned(),
            repo_type: HubRepoType::Model,
            private: false,
            default_branch: "main".to_owned(),
            created_at_unix_seconds: 1000,
            updated_at_unix_seconds: 2000,
        };
        assert_eq!(repo.repo_id, "owner/name");
        assert_eq!(repo.repo_type, HubRepoType::Model);
        assert!(!repo.private);
        assert_eq!(repo.default_branch, "main");
        assert_eq!(repo.created_at_unix_seconds, 1000);
        assert_eq!(repo.updated_at_unix_seconds, 2000);
    }

    #[test]
    fn hub_repo_private_flag() {
        let public = HubRepo {
            repo_id: "a/b".to_owned(),
            repo_type: HubRepoType::Dataset,
            private: false,
            default_branch: String::new(),
            created_at_unix_seconds: 0,
            updated_at_unix_seconds: 0,
        };
        assert!(!public.private);

        let private = HubRepo {
            repo_id: "a/b".to_owned(),
            repo_type: HubRepoType::Dataset,
            private: true,
            default_branch: String::new(),
            created_at_unix_seconds: 0,
            updated_at_unix_seconds: 0,
        };
        assert!(private.private);
    }

    // ---------------------------------------------------------------------------
    // HubRepoType::from_api_repo_type
    // ---------------------------------------------------------------------------

    #[test]
    fn from_api_repo_type() {
        assert_eq!(
            HubRepoType::from_api_repo_type("model"),
            Some(HubRepoType::Model)
        );
        assert_eq!(
            HubRepoType::from_api_repo_type("dataset"),
            Some(HubRepoType::Dataset)
        );
        assert_eq!(HubRepoType::from_api_repo_type("invalid"), None);
    }

    // ---------------------------------------------------------------------------
    // canonical_ref_name
    // ---------------------------------------------------------------------------

    #[test]
    fn canonical_ref_name_preserves_short_branch_name() {
        assert_eq!(canonical_ref_name("main"), "main");
        assert_eq!(canonical_ref_name("feature"), "feature");
    }

    #[test]
    fn canonical_ref_name_strips_refs_heads_prefix() {
        assert_eq!(canonical_ref_name("refs/heads/main"), "main");
        assert_eq!(
            canonical_ref_name("refs/heads/feature/branch"),
            "feature/branch"
        );
    }

    #[test]
    fn canonical_ref_name_preserves_tag_refs() {
        assert_eq!(canonical_ref_name("refs/tags/v1.0"), "refs/tags/v1.0");
    }

    #[test]
    fn canonical_ref_name_handles_empty_after_strip() {
        // "refs/heads/" with nothing after strips to "", but filter rejects it
        assert_eq!(canonical_ref_name("refs/heads/"), "refs/heads/");
    }

    #[test]
    fn canonical_ref_name_preserves_other_ref_prefixes() {
        assert_eq!(
            canonical_ref_name("refs/notes/commits"),
            "refs/notes/commits"
        );
    }

    // ---------------------------------------------------------------------------
    // In-memory HubStore for exercising all BoxedHubStore delegation paths
    // ---------------------------------------------------------------------------

    struct MemoryHubStore {
        repos: std::sync::Mutex<HashMap<String, HubRepo>>,
        revisions: std::sync::Mutex<HashMap<String, Vec<HubRevision>>>,
        refs: std::sync::Mutex<HashMap<String, HashMap<String, String>>>,
        files: std::sync::Mutex<HashMap<String, Vec<HubFileEntry>>>,
        lfs: std::sync::Mutex<HashMap<String, Vec<u8>>>,
        webhooks: std::sync::Mutex<HashMap<String, Vec<HubWebhook>>>,
    }

    impl MemoryHubStore {
        fn new() -> Self {
            Self {
                repos: std::sync::Mutex::new(HashMap::new()),
                revisions: std::sync::Mutex::new(HashMap::new()),
                refs: std::sync::Mutex::new(HashMap::new()),
                files: std::sync::Mutex::new(HashMap::new()),
                lfs: std::sync::Mutex::new(HashMap::new()),
                webhooks: std::sync::Mutex::new(HashMap::new()),
            }
        }
    }

    impl HubStore for MemoryHubStore {
        type Error = Box<dyn std::error::Error + Send + Sync>;

        fn create_repo(
            &self,
            repo_type: HubRepoType,
            name: &str,
            private: bool,
        ) -> Result<HubRepo, Self::Error> {
            let mut repos = self.repos.lock().unwrap();
            if repos.contains_key(name) {
                return Err("repo already exists".into());
            }
            let repo = HubRepo {
                repo_id: name.to_owned(),
                repo_type,
                private,
                default_branch: "main".to_owned(),
                created_at_unix_seconds: 100,
                updated_at_unix_seconds: 100,
            };
            repos.insert(name.to_owned(), repo.clone());
            self.refs.lock().unwrap().insert(
                name.to_owned(),
                HashMap::from([(
                    "main".to_owned(),
                    "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3".to_owned(),
                )]),
            );
            Ok(repo)
        }

        fn get_repo(&self, repo_id: &str) -> Result<Option<HubRepo>, Self::Error> {
            Ok(self.repos.lock().unwrap().get(repo_id).cloned())
        }

        fn list_repos(&self) -> Result<Vec<HubRepo>, Self::Error> {
            let mut repos: Vec<_> = self.repos.lock().unwrap().values().cloned().collect();
            repos.sort_by(|a, b| a.repo_id.cmp(&b.repo_id));
            Ok(repos)
        }

        fn search_repos(
            &self,
            repo_type: Option<HubRepoType>,
            name_prefix: &str,
            limit: usize,
        ) -> Result<Vec<HubRepo>, Self::Error> {
            let repos = self.repos.lock().unwrap();
            let mut matched: Vec<_> = repos
                .values()
                .filter(|r| r.repo_id.starts_with(name_prefix))
                .filter(|r| match repo_type {
                    Some(t) => r.repo_type == t,
                    None => true,
                })
                .cloned()
                .collect();
            matched.sort_by(|a, b| a.repo_id.cmp(&b.repo_id));
            matched.truncate(limit);
            Ok(matched)
        }

        fn create_revision(
            &self,
            repo_id: &str,
            parent_sha: Option<&str>,
            new_sha: &str,
            ref_name: &str,
            message: &str,
        ) -> Result<HubRevision, Self::Error> {
            let ref_name = canonical_ref_name(ref_name);
            let mut repos = self.repos.lock().unwrap();
            if !repos.contains_key(repo_id) {
                return Err("repo not found".into());
            }
            // Drop the repos lock before acquiring revisions lock to avoid deadlock
            drop(repos);

            let current_ref = self
                .refs
                .lock()
                .unwrap()
                .get(repo_id)
                .and_then(|refs| refs.get(ref_name))
                .cloned();

            // Optimistic concurrency: compare against the current target ref.
            if let Some(parent_sha) = parent_sha {
                if let Some(current_sha) = current_ref {
                    if current_sha != parent_sha {
                        return Err("parent sha conflict".into());
                    }
                } else if !self
                    .revisions
                    .lock()
                    .unwrap()
                    .get(repo_id)
                    .is_some_and(|revisions| revisions.iter().any(|r| r.sha == parent_sha))
                {
                    return Err("parent sha conflict".into());
                }
            }

            let mut revisions = self.revisions.lock().unwrap();
            let repo_revisions = revisions.entry(repo_id.to_owned()).or_default();

            let revision = HubRevision {
                repo_id: repo_id.to_owned(),
                ref_name: ref_name.to_owned(),
                sha: new_sha.to_owned(),
                parent_sha: parent_sha.map(ToOwned::to_owned),
                message: Some(message.to_owned()),
                created_at_unix_seconds: 200,
            };
            repo_revisions.push(revision.clone());
            drop(revisions);
            self.refs
                .lock()
                .unwrap()
                .entry(repo_id.to_owned())
                .or_default()
                .insert(ref_name.to_owned(), new_sha.to_owned());
            Ok(revision)
        }

        fn list_refs(&self, repo_id: &str) -> Result<Vec<HubRef>, Self::Error> {
            let mut refs = self
                .refs
                .lock()
                .unwrap()
                .get(repo_id)
                .into_iter()
                .flat_map(|refs| refs.iter())
                .map(|(ref_name, sha)| HubRef {
                    repo_id: repo_id.to_owned(),
                    ref_name: ref_name.clone(),
                    sha: sha.clone(),
                })
                .collect::<Vec<_>>();
            refs.sort_by(|a, b| a.ref_name.cmp(&b.ref_name));
            Ok(refs)
        }

        fn delete_ref(
            &self,
            repo_id: &str,
            ref_name: &str,
            expected_sha: &str,
        ) -> Result<(), Self::Error> {
            let ref_name = canonical_ref_name(ref_name);
            if ref_name == "main" || ref_name == "HEAD" {
                return Err("default branch cannot be deleted".into());
            }
            let mut refs = self.refs.lock().unwrap();
            let repo_refs = refs.get_mut(repo_id).ok_or("repo not found")?;
            match repo_refs.get(ref_name) {
                Some(current) if current == expected_sha => {}
                Some(_) => return Err("ref sha conflict".into()),
                None => return Err("ref not found".into()),
            }
            repo_refs.remove(ref_name);
            Ok(())
        }

        fn list_revisions(&self, repo_id: &str) -> Result<Vec<HubRevision>, Self::Error> {
            Ok(self
                .revisions
                .lock()
                .unwrap()
                .get(repo_id)
                .cloned()
                .unwrap_or_default())
        }

        fn resolve_revision(
            &self,
            repo_id: &str,
            revision: &str,
        ) -> Result<Option<String>, Self::Error> {
            // Try exact SHA first, then the active ref mapping.
            let revisions = self.revisions.lock().unwrap();
            let repo_revisions = match revisions.get(repo_id) {
                Some(r) => r,
                None => return Ok(None),
            };

            // Check exact SHA
            if let Some(r) = repo_revisions.iter().find(|r| r.sha == revision) {
                return Ok(Some(r.sha.clone()));
            }
            Ok(self
                .refs
                .lock()
                .unwrap()
                .get(repo_id)
                .and_then(|refs| refs.get(canonical_ref_name(revision)))
                .cloned())
        }

        fn store_files(&self, commit_sha: &str, files: &[HubFileEntry]) -> Result<(), Self::Error> {
            self.files
                .lock()
                .unwrap()
                .insert(commit_sha.to_owned(), files.to_vec());
            Ok(())
        }

        fn get_files(&self, commit_sha: &str) -> Result<Vec<HubFileEntry>, Self::Error> {
            Ok(self
                .files
                .lock()
                .unwrap()
                .get(commit_sha)
                .cloned()
                .unwrap_or_default())
        }

        fn put_lfs_object(&self, oid: &str, data: &[u8]) -> Result<(), Self::Error> {
            self.lfs
                .lock()
                .unwrap()
                .insert(oid.to_owned(), data.to_vec());
            Ok(())
        }

        fn get_lfs_object(&self, oid: &str) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(self.lfs.lock().unwrap().get(oid).cloned())
        }

        fn has_lfs_object(&self, oid: &str) -> Result<bool, Self::Error> {
            Ok(self.lfs.lock().unwrap().contains_key(oid))
        }

        fn create_webhook(
            &self,
            repo_id: &str,
            url: &str,
            events: &[String],
            secret: Option<&str>,
        ) -> Result<HubWebhook, Self::Error> {
            let mut webhooks = self.webhooks.lock().unwrap();
            let repo_webhooks = webhooks.entry(repo_id.to_owned()).or_default();
            let id = format!("wh-{}", repo_webhooks.len() + 1);
            let wh = HubWebhook {
                id,
                repo_id: repo_id.to_owned(),
                url: url.to_owned(),
                events: events.to_vec(),
                secret: secret.map(SecretString::from_secret),
                active: true,
                created_at_unix_seconds: 300,
            };
            repo_webhooks.push(wh.clone());
            Ok(wh)
        }

        fn list_webhooks(&self, repo_id: &str) -> Result<Vec<HubWebhook>, Self::Error> {
            Ok(self
                .webhooks
                .lock()
                .unwrap()
                .get(repo_id)
                .cloned()
                .unwrap_or_default())
        }

        fn delete_repo(&self, repo_id: &str) -> Result<(), Self::Error> {
            let mut repos = self.repos.lock().unwrap();
            repos.remove(repo_id);
            drop(repos);
            self.revisions.lock().unwrap().remove(repo_id);
            self.refs.lock().unwrap().remove(repo_id);
            self.files.lock().unwrap().remove(repo_id);
            self.webhooks.lock().unwrap().remove(repo_id);
            Ok(())
        }

        fn delete_webhook(&self, repo_id: &str, webhook_id: &str) -> Result<(), Self::Error> {
            let mut webhooks = self.webhooks.lock().unwrap();
            if let Some(repo_webhooks) = webhooks.get_mut(repo_id) {
                repo_webhooks.retain(|wh| wh.id != webhook_id);
            }
            Ok(())
        }

        fn webhooks_for_event(
            &self,
            repo_id: &str,
            event: &str,
        ) -> Result<Vec<HubWebhook>, Self::Error> {
            Ok(self
                .webhooks
                .lock()
                .unwrap()
                .get(repo_id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|wh| wh.events.iter().any(|e| e == event))
                .collect())
        }
    }

    // ── BoxedHubStore with MemoryHubStore ──────────────────────────────────

    #[test]
    fn boxed_hub_store_create_and_get_repo() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        let repo = store
            .create_repo(HubRepoType::Model, "test/model", false)
            .unwrap();
        assert_eq!(repo.repo_id, "test/model");
        assert_eq!(repo.repo_type, HubRepoType::Model);
        assert!(!repo.private);

        let loaded = store.get_repo("test/model").unwrap();
        assert_eq!(loaded, Some(repo));

        let missing = store.get_repo("nonexistent").unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn boxed_hub_store_create_repo_duplicate_fails() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        store
            .create_repo(HubRepoType::Dataset, "dup/repo", true)
            .unwrap();
        let err = store.create_repo(HubRepoType::Dataset, "dup/repo", false);
        assert!(err.is_err());
    }

    #[test]
    fn boxed_hub_store_list_repos_is_empty_initially() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        let repos = store.list_repos().unwrap();
        assert!(repos.is_empty());
    }

    #[test]
    fn boxed_hub_store_list_repos_returns_all() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        store
            .create_repo(HubRepoType::Model, "a/model", false)
            .unwrap();
        store
            .create_repo(HubRepoType::Dataset, "b/dataset", true)
            .unwrap();
        let repos = store.list_repos().unwrap();
        assert_eq!(repos.len(), 2);
    }

    #[test]
    fn boxed_hub_store_search_repos_by_prefix() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        store
            .create_repo(HubRepoType::Model, "team/project-a", false)
            .unwrap();
        store
            .create_repo(HubRepoType::Model, "team/project-b", false)
            .unwrap();
        store
            .create_repo(HubRepoType::Dataset, "other/data", false)
            .unwrap();

        let results = store.search_repos(None, "team/", 10).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.repo_id.starts_with("team/")));
    }

    #[test]
    fn boxed_hub_store_search_repos_filters_by_type() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        store
            .create_repo(HubRepoType::Model, "a/model", false)
            .unwrap();
        store
            .create_repo(HubRepoType::Dataset, "a/data", true)
            .unwrap();

        let models = store
            .search_repos(Some(HubRepoType::Model), "a/", 10)
            .unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].repo_type, HubRepoType::Model);
    }

    #[test]
    fn boxed_hub_store_search_repos_respects_limit() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        store
            .create_repo(HubRepoType::Model, "x/one", false)
            .unwrap();
        store
            .create_repo(HubRepoType::Model, "x/two", false)
            .unwrap();
        store
            .create_repo(HubRepoType::Model, "x/three", false)
            .unwrap();

        let limited = store.search_repos(None, "x/", 2).unwrap();
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn boxed_hub_store_revision_lifecycle() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        store
            .create_repo(HubRepoType::Model, "rev/repo", false)
            .unwrap();

        // First revision with no parent
        let rev1 = store
            .create_revision("rev/repo", None, "sha001", "main", "initial")
            .unwrap();
        assert_eq!(rev1.sha, "sha001");
        assert!(rev1.parent_sha.is_none());

        let revs = store.list_revisions("rev/repo").unwrap();
        assert_eq!(revs.len(), 1);

        // Resolve by SHA
        let resolved = store.resolve_revision("rev/repo", "sha001").unwrap();
        assert_eq!(resolved, Some("sha001".to_owned()));

        // Resolve by ref name
        let resolved_by_ref = store.resolve_revision("rev/repo", "main").unwrap();
        assert_eq!(resolved_by_ref, Some("sha001".to_owned()));

        // Resolve nonexistent
        let missing = store.resolve_revision("rev/repo", "nosuch").unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn boxed_hub_store_optimistic_concurrency() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        store
            .create_repo(HubRepoType::Model, "oc/repo", false)
            .unwrap();

        // Create first commit
        store
            .create_revision("oc/repo", None, "abc", "main", "first")
            .unwrap();

        // Valid parent succeeds
        store
            .create_revision("oc/repo", Some("abc"), "def", "main", "second")
            .unwrap();

        // Stale parent rejected
        let err = store.create_revision("oc/repo", Some("abc"), "xxx", "main", "stale");
        assert!(err.is_err());
    }

    #[test]
    fn boxed_hub_store_create_revision_nonexistent_repo_fails() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        let err = store.create_revision("no-such-repo", None, "sha", "main", "msg");
        assert!(err.is_err());
    }

    #[test]
    fn boxed_hub_store_file_entries() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        let files = vec![HubFileEntry {
            path: "readme.md".to_owned(),
            size: 100,
            sha: "aaa".to_owned(),
            is_lfs: false,
            inline_content: None,
        }];

        store.store_files("commit-sha-1", &files).unwrap();
        let loaded = store.get_files("commit-sha-1").unwrap();
        assert_eq!(loaded, files);

        let empty = store.get_files("no-such-commit").unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn boxed_hub_store_lfs_objects() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        let data = b"lfs-content-123";

        assert!(!store.has_lfs_object("oid-1").unwrap());

        store.put_lfs_object("oid-1", data).unwrap();
        assert!(store.has_lfs_object("oid-1").unwrap());

        let loaded = store.get_lfs_object("oid-1").unwrap();
        assert_eq!(loaded, Some(data.to_vec()));

        let missing = store.get_lfs_object("no-such-oid").unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn boxed_hub_store_lfs_put_overwrites() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        store.put_lfs_object("oid-overwrite", b"v1").unwrap();
        store.put_lfs_object("oid-overwrite", b"v2").unwrap();
        let loaded = store.get_lfs_object("oid-overwrite").unwrap();
        assert_eq!(loaded, Some(b"v2".to_vec()));
    }

    #[test]
    fn boxed_hub_store_webhook_crud() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        store
            .create_repo(HubRepoType::Model, "wh/repo", false)
            .unwrap();

        let wh = store
            .create_webhook(
                "wh/repo",
                "https://example.com/hook",
                &["push".to_owned(), "pull_request".to_owned()],
                Some("secret123"),
            )
            .unwrap();
        assert!(wh.active);
        assert_eq!(wh.url, "https://example.com/hook");

        let webhooks = store.list_webhooks("wh/repo").unwrap();
        assert_eq!(webhooks.len(), 1);

        // webhooks_for_event
        let push_hooks = store.webhooks_for_event("wh/repo", "push").unwrap();
        assert_eq!(push_hooks.len(), 1);
        let tag_hooks = store.webhooks_for_event("wh/repo", "tag").unwrap();
        assert!(tag_hooks.is_empty());

        // Delete webhook
        store.delete_webhook("wh/repo", &wh.id).unwrap();
        let webhooks_after = store.list_webhooks("wh/repo").unwrap();
        assert!(webhooks_after.is_empty());
    }

    #[test]
    fn boxed_hub_store_delete_repo_cascades() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        store
            .create_repo(HubRepoType::Space, "del/repo", false)
            .unwrap();
        store
            .create_revision("del/repo", None, "s1", "main", "msg")
            .unwrap();
        store
            .create_webhook("del/repo", "https://hook", &["push".to_owned()], None)
            .unwrap();

        store.delete_repo("del/repo").unwrap();

        assert!(store.get_repo("del/repo").unwrap().is_none());
        assert!(store.list_revisions("del/repo").unwrap().is_empty());
        assert!(store.list_webhooks("del/repo").unwrap().is_empty());
    }

    #[test]
    fn boxed_hub_store_delete_webhook_nonexistent_is_idempotent() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        store
            .create_repo(HubRepoType::Dataset, "idem/repo", false)
            .unwrap();
        // Deleting a non-existent webhook should not error
        store.delete_webhook("idem/repo", "no-such-id").unwrap();
    }

    #[test]
    fn boxed_hub_store_resolve_revision_nonexistent_repo_returns_none() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        let result = store.resolve_revision("no-repo", "main").unwrap();
        assert!(result.is_none());
    }

    // ── BoxedHubStore construction + blanket impl ─────────────────────────

    #[test]
    fn boxed_hub_store_new_constructs() {
        let store = BoxedHubStore::new(MemoryHubStore::new());
        let _ = store;
    }

    #[test]
    fn boxed_hub_store_from_store_constructs() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        let _ = store;
    }

    #[test]
    fn boxed_hub_store_clone_works() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        let cloned = store.clone();
        let _ = (store, cloned);
    }

    /// Exercises the blanket `impl<T: HubStore> ErasedHubStore for T` path
    /// (used by `BoxedHubStore::new`) for every method at least once.
    #[test]
    fn boxed_hub_store_blanket_impl_exercise() {
        // Using `new` forces the blanket impl (not ErasedAdapter).
        let store = BoxedHubStore::new(MemoryHubStore::new());

        // create / get / list / search
        let repo = store
            .create_repo(HubRepoType::Model, "blanket/repo", true)
            .unwrap();
        assert!(repo.private);
        assert_eq!(store.get_repo("blanket/repo").unwrap(), Some(repo));

        let all = store.list_repos().unwrap();
        assert_eq!(all.len(), 1);

        let found = store.search_repos(None, "blanket", 10).unwrap();
        assert!(!found.is_empty());

        let none_found = store
            .search_repos(Some(HubRepoType::Dataset), "blanket", 10)
            .unwrap();
        assert!(none_found.is_empty());

        // revisions
        let rev = store
            .create_revision("blanket/repo", None, "sha1", "main", "first")
            .unwrap();
        assert_eq!(rev.sha, "sha1");
        let revs = store.list_revisions("blanket/repo").unwrap();
        assert_eq!(revs.len(), 1);

        let resolved = store.resolve_revision("blanket/repo", "main").unwrap();
        assert_eq!(resolved, Some("sha1".to_owned()));

        // files
        let files = vec![HubFileEntry {
            path: "f.txt".into(),
            size: 10,
            sha: "f1".into(),
            is_lfs: false,
            inline_content: None,
        }];
        store.store_files("c1", &files).unwrap();
        let loaded = store.get_files("c1").unwrap();
        assert_eq!(loaded.len(), 1);

        // LFS
        store.put_lfs_object("l1", b"data").unwrap();
        assert!(store.has_lfs_object("l1").unwrap());
        assert_eq!(store.get_lfs_object("l1").unwrap(), Some(b"data".to_vec()));

        // webhooks
        let wh = store
            .create_webhook("blanket/repo", "https://hook", &["push".into()], None)
            .unwrap();
        assert!(wh.active);
        let hooks = store.list_webhooks("blanket/repo").unwrap();
        assert_eq!(hooks.len(), 1);
        let event_hooks = store.webhooks_for_event("blanket/repo", "push").unwrap();
        assert_eq!(event_hooks.len(), 1);

        // delete webhook
        store.delete_webhook("blanket/repo", &wh.id).unwrap();
        assert!(store.list_webhooks("blanket/repo").unwrap().is_empty());

        // delete repo
        store.delete_repo("blanket/repo").unwrap();
        assert!(store.get_repo("blanket/repo").unwrap().is_none());
    }

    // ── HubRepoType extra edge cases ───────────────────────────────────────

    #[test]
    fn parse_str_case_sensitive() {
        assert_eq!(HubRepoType::parse_str("Model"), None);
        assert_eq!(HubRepoType::parse_str("MODEL"), None);
    }

    #[test]
    fn from_api_repo_type_unknown() {
        assert_eq!(HubRepoType::from_api_repo_type("unknown"), None);
        assert_eq!(HubRepoType::from_api_repo_type("Spaces"), None);
        assert_eq!(HubRepoType::from_api_repo_type(""), None);
    }

    // ── HubRepo::compute_commit_sha edge cases ─────────────────────────────

    #[test]
    fn compute_commit_sha_empty_inputs() {
        // Empty strings should still produce deterministic output
        let sha = HubRepo::compute_commit_sha("", "", "").unwrap();
        assert_eq!(sha.len(), 16);
        // Same inputs = same output
        assert_eq!(HubRepo::compute_commit_sha("", "", "").unwrap(), sha);
    }

    #[test]
    fn compute_commit_sha_unicode_inputs() {
        let sha = HubRepo::compute_commit_sha("abc", "commit: 你好世界", "def").unwrap();
        assert_eq!(sha.len(), 16);
    }

    // ── BoxedHubStore search_repos with empty prefix ───────────────────────

    #[test]
    fn boxed_hub_store_search_repos_empty_prefix() {
        let store = BoxedHubStore::from_store(MemoryHubStore::new());
        store
            .create_repo(HubRepoType::Model, "aaa/model", false)
            .unwrap();
        store
            .create_repo(HubRepoType::Dataset, "bbb/data", true)
            .unwrap();

        // Empty prefix should match everything (up to limit)
        let all = store.search_repos(None, "", 10).unwrap();
        assert_eq!(all.len(), 2);

        // With type filter
        let models = store
            .search_repos(Some(HubRepoType::Model), "", 10)
            .unwrap();
        assert_eq!(models.len(), 1);
    }

    // ── HubRevision and HubFileEntry field access ──────────────────────────

    #[test]
    fn hub_revision_fields() {
        let rev = HubRevision {
            repo_id: "owner/repo".to_owned(),
            ref_name: "main".to_owned(),
            sha: "abc123".to_owned(),
            parent_sha: Some("parent0".to_owned()),
            message: Some("my commit".to_owned()),
            created_at_unix_seconds: 1000,
        };
        assert_eq!(rev.sha, "abc123");
        assert_eq!(rev.parent_sha.as_deref(), Some("parent0"));
        assert_eq!(rev.message.as_deref(), Some("my commit"));
    }

    #[test]
    fn hub_file_entry_fields() {
        let entry = HubFileEntry {
            path: "src/lib.rs".to_owned(),
            size: 1024,
            sha: "filehash".to_owned(),
            is_lfs: true,
            inline_content: Some(vec![1, 2, 3]),
        };
        assert_eq!(entry.path, "src/lib.rs");
        assert!(entry.is_lfs);
        assert_eq!(entry.inline_content.as_deref(), Some(&[1, 2, 3][..]));
    }

    #[test]
    fn hub_webhook_fields() {
        let wh = HubWebhook {
            id: "wh-1".to_owned(),
            repo_id: "repo".to_owned(),
            url: "https://hook.example.com".to_owned(),
            events: vec!["push".to_owned()],
            secret: Some(SecretString::from_secret("s3kr3t")),
            active: true,
            created_at_unix_seconds: 500,
        };
        assert_eq!(wh.secret.as_ref().map(SecretString::expose_secret), Some("s3kr3t"));
        assert!(wh.active);
    }
}
