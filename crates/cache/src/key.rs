use shardline_protocol::{RepositoryProvider, RepositoryScope};

/// Repository scope material embedded into a cache key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepositoryScopeCacheKey {
    provider: String,
    owner: String,
    repo: String,
    revision: Option<String>,
}

impl RepositoryScopeCacheKey {
    /// Creates a repository scope cache key from a protocol scope.
    #[must_use]
    pub fn from_scope(scope: &RepositoryScope) -> Self {
        Self {
            provider: provider_token(scope.provider()).to_owned(),
            owner: scope.owner().to_owned(),
            repo: scope.name().to_owned(),
            revision: scope.revision().map(ToOwned::to_owned),
        }
    }

    /// Returns the repository provider.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Returns the repository owner or namespace.
    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Returns the repository name.
    #[must_use]
    pub fn repo(&self) -> &str {
        &self.repo
    }

    /// Returns the optional scoped revision.
    #[must_use]
    pub fn revision(&self) -> Option<&str> {
        self.revision.as_deref()
    }
}

/// Stable cache key for one file reconstruction response.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReconstructionCacheKey {
    file_id: String,
    content_hash: Option<String>,
    repository_scope: Option<RepositoryScopeCacheKey>,
}

impl ReconstructionCacheKey {
    /// Creates a cache key for the latest visible reconstruction of one file.
    #[must_use]
    pub fn latest(file_id: &str, repository_scope: Option<&RepositoryScope>) -> Self {
        Self {
            file_id: file_id.to_owned(),
            content_hash: None,
            repository_scope: repository_scope.map(RepositoryScopeCacheKey::from_scope),
        }
    }

    /// Creates a cache key for one immutable file version.
    #[must_use]
    pub fn version(
        file_id: &str,
        content_hash: &str,
        repository_scope: Option<&RepositoryScope>,
    ) -> Self {
        Self {
            file_id: file_id.to_owned(),
            content_hash: Some(content_hash.to_owned()),
            repository_scope: repository_scope.map(RepositoryScopeCacheKey::from_scope),
        }
    }

    /// Returns the file identifier.
    #[must_use]
    pub fn file_id(&self) -> &str {
        &self.file_id
    }

    /// Returns the optional immutable content hash.
    #[must_use]
    pub fn content_hash(&self) -> Option<&str> {
        self.content_hash.as_deref()
    }

    /// Returns the optional repository scope.
    #[must_use]
    pub const fn repository_scope(&self) -> Option<&RepositoryScopeCacheKey> {
        self.repository_scope.as_ref()
    }
}

const fn provider_token(provider: RepositoryProvider) -> &'static str {
    match provider {
        RepositoryProvider::GitHub => "github",
        RepositoryProvider::Gitea => "gitea",
        RepositoryProvider::GitLab => "gitlab",
        RepositoryProvider::Generic => "generic",
    }
}

#[cfg(test)]
mod tests {
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    use super::ReconstructionCacheKey;

    #[test]
    fn cache_key_keeps_version_and_scope_components() {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };

        let latest = ReconstructionCacheKey::latest("asset.bin", Some(&scope));
        let version = ReconstructionCacheKey::version("asset.bin", "deadbeef", Some(&scope));

        assert_eq!(latest.file_id(), "asset.bin");
        assert_eq!(latest.content_hash(), None);
        assert_eq!(version.content_hash(), Some("deadbeef"));
        let latest_scope = latest.repository_scope();
        assert!(latest_scope.is_some());
        let Some(latest_scope) = latest_scope else {
            return;
        };
        assert_eq!(latest_scope.owner(), "team");
        assert_eq!(latest_scope.repo(), "assets");
        assert_eq!(latest_scope.provider(), "github");
        assert_eq!(latest_scope.revision(), Some("main"));
    }
}
