use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::error::HubApiError;
use crate::models::{RepoResponse, RepoType, RevisionResponse};

/// In-memory repository metadata store.
#[derive(Debug, Clone, Default)]
pub struct RepoStore {
    inner: Arc<RwLock<RepoStoreInner>>,
}

#[derive(Debug, Default)]
struct RepoStoreInner {
    /// repo_id -> RepoEntry
    repos: HashMap<String, RepoEntry>,
    /// (repo_id, rev_sha) -> RevisionEntry
    revisions: HashMap<(String, String), RevisionEntry>,
}

#[derive(Debug, Clone)]
pub(crate) struct RepoEntry {
    pub repo_id: String,
    pub repo_type: RepoType,
    pub private: bool,
    /// repo_id -> default revision sha
    pub default_branch: String,
}

#[derive(Debug, Clone)]
pub(crate) struct RevisionEntry {
    pub ref_name: String,
    pub sha: String,
}

impl RepoStore {
    /// Creates a new empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a repository.
    ///
    /// # Errors
    ///
    /// Returns [`HubApiError::Conflict`] if the repository already exists.
    pub async fn create_repo(
        &self,
        repo_type: RepoType,
        name: &str,
        private: bool,
    ) -> Result<RepoResponse, HubApiError> {
        let repo_id = name.to_owned();
        let mut inner = self.inner.write().await;
        if inner.repos.contains_key(&repo_id) {
            return Err(HubApiError::Conflict);
        }
        let initial_sha = empty_tree_sha();
        let entry = RepoEntry {
            repo_id: repo_id.clone(),
            repo_type,
            private,
            default_branch: initial_sha.clone(),
        };
        inner.repos.insert(repo_id.clone(), entry);
        inner.revisions.insert(
            (repo_id.clone(), initial_sha.clone()),
            RevisionEntry {
                ref_name: "main".to_owned(),
                sha: initial_sha,
            },
        );
        Ok(RepoResponse {
            id: repo_id.clone(),
            repo_type,
            private,
            url: format!("/{repo_type_path}/{repo_id}", repo_type_path = repo_type.as_path_str()),
            default_branch: Some("main".to_owned()),
        })
    }

    /// Returns a repository by ID.
    ///
    /// # Errors
    ///
    /// Returns [`HubApiError::RepoNotFound`] if the repository does not exist.
    pub(crate) async fn get_repo(&self, repo_id: &str) -> Result<RepoEntry, HubApiError> {
        let inner = self.inner.read().await;
        inner
            .repos
            .get(repo_id)
            .cloned()
            .ok_or(HubApiError::RepoNotFound)
    }

    /// Lists revisions for a repository.
    pub async fn list_revisions(&self, repo_id: &str) -> Result<Vec<RevisionResponse>, HubApiError> {
        let inner = self.inner.read().await;
        let repo = inner.repos.get(repo_id).ok_or(HubApiError::RepoNotFound)?;
        let mut revisions = Vec::new();
        for ((id, _sha), entry) in &inner.revisions {
            if id == repo_id {
                revisions.push(RevisionResponse {
                    ref_name: entry.ref_name.clone(),
                    sha: entry.sha.clone(),
                });
            }
        }
        if revisions.is_empty() {
            revisions.push(RevisionResponse {
                ref_name: "main".to_owned(),
                sha: repo.default_branch.clone(),
            });
        }
        Ok(revisions)
    }

    /// Creates a new revision in a repository.
    ///
    /// # Errors
    ///
    /// Returns [`HubApiError::RepoNotFound`] if the repository does not exist.
    /// Returns [`HubApiError::OptimisticConcurrency`] if `parent_sha` does not match the current HEAD.
    pub async fn create_revision(
        &self,
        repo_id: &str,
        parent_sha: Option<&str>,
        new_sha: &str,
        ref_name: &str,
    ) -> Result<RevisionResponse, HubApiError> {
        let mut inner = self.inner.write().await;
        let repo = inner
            .repos
            .get(repo_id)
            .ok_or(HubApiError::RepoNotFound)?;

        if let Some(parent) = parent_sha {
            if repo.default_branch != parent {
                return Err(HubApiError::OptimisticConcurrency);
            }
        }

        let entry = RepoEntry {
            default_branch: new_sha.to_owned(),
            ..repo.clone()
        };
        inner.repos.insert(repo_id.to_owned(), entry);
        inner.revisions.insert(
            (repo_id.to_owned(), new_sha.to_owned()),
            RevisionEntry {
                ref_name: ref_name.to_owned(),
                sha: new_sha.to_owned(),
            },
        );
        Ok(RevisionResponse {
            ref_name: ref_name.to_owned(),
            sha: new_sha.to_owned(),
        })
    }

    /// Resolves a revision string to a SHA.
    ///
    /// # Errors
    ///
    /// Returns [`HubApiError::RevisionNotFound`] if the revision cannot be resolved.
    pub async fn resolve_revision(
        &self,
        repo_id: &str,
        revision: &str,
    ) -> Result<String, HubApiError> {
        let inner = self.inner.read().await;
        let repo = inner.repos.get(repo_id).ok_or(HubApiError::RepoNotFound)?;

        // If revision is "main" or empty, return default branch
        if revision.is_empty() || revision == "main" {
            return Ok(repo.default_branch.clone());
        }

        // Check if it's a direct SHA
        if inner.revisions.contains_key(&(repo_id.to_owned(), revision.to_owned())) {
            return Ok(revision.to_owned());
        }

        // Check ref names
        for ((id, sha), entry) in &inner.revisions {
            if id == repo_id && entry.ref_name == revision {
                return Ok(sha.clone());
            }
        }

        Err(HubApiError::RevisionNotFound)
    }

    /// Generates a deterministic SHA-256 for a commit.
    pub(crate) fn compute_commit_sha(parent_sha: &str, message: &str, files_hash: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        parent_sha.hash(&mut hasher);
        message.hash(&mut hasher);
        files_hash.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }
}

/// Returns the SHA of an empty tree (consistent initial state).
fn empty_tree_sha() -> String {
    // Git empty tree SHA
    "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3".to_owned()
}
