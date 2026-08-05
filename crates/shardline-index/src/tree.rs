use shardline_protocol::RepositoryScope;

/// A (provider, owner, repo) repository identity without a revision.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepoKey {
    /// Stable provider name.
    pub provider: String,
    /// Repository owner or namespace.
    pub owner: String,
    /// Repository name.
    pub repo: String,
}

impl RepoKey {
    /// Creates a repository identity.
    #[must_use]
    pub fn new(provider: &str, owner: &str, repo: &str) -> Self {
        Self {
            provider: provider.to_owned(),
            owner: owner.to_owned(),
            repo: repo.to_owned(),
        }
    }

    /// Builds a repository identity from a token scope, dropping any revision.
    #[must_use]
    pub fn from_scope(scope: &RepositoryScope) -> Self {
        Self::new(scope.provider().as_str(), scope.owner(), scope.name())
    }
}

/// A per-(provider, owner, repo, revision) tree namespace identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TreeKey {
    /// Stable provider name.
    pub provider: String,
    /// Repository owner or namespace.
    pub owner: String,
    /// Repository name.
    pub repo: String,
    /// Revision name (for example `main` or `feature`).
    pub revision: String,
}

impl TreeKey {
    /// Creates a tree namespace identity.
    #[must_use]
    pub fn new(provider: &str, owner: &str, repo: &str, revision: &str) -> Self {
        Self {
            provider: provider.to_owned(),
            owner: owner.to_owned(),
            repo: repo.to_owned(),
            revision: revision.to_owned(),
        }
    }

    /// Builds a tree namespace identity from a token scope that carries a revision.
    ///
    /// Returns `None` when the scope has no revision.
    #[must_use]
    pub fn from_scope(scope: &RepositoryScope) -> Option<Self> {
        let revision = scope.revision()?;
        Some(Self::new(
            scope.provider().as_str(),
            scope.owner(),
            scope.name(),
            revision,
        ))
    }
}

/// A path -> `file_id` mapping row within a revision tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    /// Stable provider name.
    pub provider: String,
    /// Repository owner or namespace.
    pub owner: String,
    /// Repository name.
    pub repo: String,
    /// Revision name.
    pub revision: String,
    /// Canonical normalized path (no leading/trailing slash).
    pub path: String,
    /// The 64-lowercase-hex `file_id` the path resolves to.
    pub file_id: String,
    /// Snapshot of the file record's `total_bytes` at registration time.
    pub size_bytes: u64,
    /// Unix seconds when the mapping row was last updated.
    pub updated_at_unix_seconds: u64,
}

/// Outcome of an upsert that reports whether a prior mapping existed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TreeEntryOutcome {
    /// True when no prior mapping existed at this path (a new insert).
    pub created: bool,
}

/// A revision registry row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevisionRecord {
    /// Stable provider name.
    pub provider: String,
    /// Repository owner or namespace.
    pub owner: String,
    /// Repository name.
    pub repo: String,
    /// Revision name.
    pub revision: String,
    /// Unix seconds when the revision was first created.
    pub created_at_unix_seconds: u64,
    /// Unix seconds when the revision was last touched.
    pub updated_at_unix_seconds: u64,
}

/// Tree metadata storage contract.
///
/// Implementations store per-revision path -> `file_id` mappings and a revision
/// registry. Tree entries are intentionally **not** a reachability source for
/// GC; deleting a mapping never touches the referenced file record or CAS
/// objects.
///
/// # Notes
///
/// The listing scan is ordered by raw path within a revision so callers can
/// paginate with an opaque keyset cursor over the raw path. A derived directory
/// that straddles a page boundary may be emitted on more than one page; clients
/// deduplicate by `path`.
#[async_trait::async_trait]
pub trait TreeStore: Send + Sync {
    /// Adapter-specific error type.
    type Error: Send + Sync;

    /// Inserts or replaces a path mapping, reporting whether it was newly created.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when persistence fails.
    async fn upsert_tree_entry(&self, entry: &TreeEntry) -> Result<TreeEntryOutcome, Self::Error>;

    /// Loads the mapping for exactly one path in a revision.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the lookup fails.
    async fn tree_entry(&self, key: &TreeKey, path: &str)
    -> Result<Option<TreeEntry>, Self::Error>;

    /// Deletes one path mapping (or the path and every descendant when
    /// `recursive`), returning the number of rows removed.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when deletion fails.
    async fn delete_tree_entries(
        &self,
        key: &TreeKey,
        path: &str,
        recursive: bool,
    ) -> Result<u64, Self::Error>;

    /// Scans raw path rows for a revision under `prefix`, ordered by path,
    /// resuming after `cursor` (keyset on the raw path), returning at most
    /// `limit` raw rows.
    ///
    /// `prefix` is the canonical directory path without a trailing slash (empty
    /// for the repository root). Raw rows whose path equals `prefix` or starts
    /// with `prefix + "/"` are included.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the scan fails.
    async fn scan_tree(
        &self,
        key: &TreeKey,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TreeEntry>, Self::Error>;

    /// Inserts or refreshes a revision registry row, returning `true` when the
    /// revision was newly created and `false` when it already existed.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when persistence fails.
    async fn upsert_revision(&self, rev: &RevisionRecord) -> Result<bool, Self::Error>;

    /// Loads one revision registry row.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the lookup fails.
    async fn revision(
        &self,
        key: &RepoKey,
        rev: &str,
    ) -> Result<Option<RevisionRecord>, Self::Error>;

    /// Lists every revision for a repository ordered by revision name.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the inventory lookup fails.
    async fn list_revisions(&self, key: &RepoKey) -> Result<Vec<RevisionRecord>, Self::Error>;

    /// Deletes a revision registry row **and** every tree entry for that
    /// revision, returning the number of revision rows removed (0 or 1).
    ///
    /// File records and CAS objects are untouched.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when deletion fails.
    async fn delete_revision(&self, key: &RepoKey, rev: &str) -> Result<u64, Self::Error>;
}
