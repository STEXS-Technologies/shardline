use std::sync::Arc;

use shardline_protocol::{RepositoryProvider, RepositoryScope};

/// Repository scope material embedded into a cache key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepositoryScopeCacheKey {
    provider: &'static str,
    owner: Arc<str>,
    repo: Arc<str>,
    revision: Option<Arc<str>>,
}

impl RepositoryScopeCacheKey {
    /// Creates a repository scope cache key from a protocol scope.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_cache::RepositoryScopeCacheKey;
    /// use shardline_protocol::{RepositoryProvider, RepositoryScope};
    ///
    /// let scope =
    ///     RepositoryScope::new(RepositoryProvider::GitLab, "group", "project", None)?;
    /// let key = RepositoryScopeCacheKey::from_scope(&scope);
    /// assert_eq!(key.provider(), "gitlab");
    /// assert_eq!(key.owner(), "group");
    /// assert_eq!(key.repo(), "project");
    /// assert_eq!(key.revision(), None);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn from_scope(scope: &RepositoryScope) -> Self {
        Self {
            provider: scope.provider().as_str(),
            owner: Arc::from(scope.owner()),
            repo: Arc::from(scope.name()),
            revision: scope.revision().map(Arc::from),
        }
    }

    /// Returns the repository provider.
    #[must_use]
    pub const fn provider(&self) -> &str {
        self.provider
    }

    /// Returns the repository provider as the canonical protocol enum.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_cache::RepositoryScopeCacheKey;
    /// use shardline_protocol::{RepositoryProvider, RepositoryScope};
    ///
    /// let scope =
    ///     RepositoryScope::new(RepositoryProvider::GitHub, "acme", "assets", None)?;
    /// let key = RepositoryScopeCacheKey::from_scope(&scope);
    /// assert_eq!(key.provider_kind(), RepositoryProvider::GitHub);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// Note: the stored key keeps a `&'static str` token so the `Hash`-derived cache
    /// semantics and the string wire representation stay unchanged
    /// (`RepositoryProvider` does not implement `Hash`, so it cannot be stored
    /// directly without reworking the derived `Hash`). This accessor reverse
    /// maps the token back to the canonical enum.
    #[must_use]
    pub fn provider_kind(self) -> RepositoryProvider {
        match self.provider {
            "github" => RepositoryProvider::GitHub,
            "gitea" => RepositoryProvider::Gitea,
            "gitlab" => RepositoryProvider::GitLab,
            "codeberg" => RepositoryProvider::Codeberg,
            // Unreachable by construction: `provider` is only ever populated
            // from `RepositoryProvider::as_str()`. Kept as
            // a lint-safe fallback (no unwrap/panic) to keep the match exhaustive.
            _other => RepositoryProvider::Generic,
        }
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
/// Clones share immutable identity strings.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReconstructionCacheKey {
    file_id: Arc<str>,
    content_hash: Option<Arc<str>>,
    repository_scope: Option<RepositoryScopeCacheKey>,
}

impl ReconstructionCacheKey {
    /// Creates a cache key for the latest visible reconstruction of one file.
    ///
    /// The key has no content hash, so it always resolves to whatever is
    /// currently the latest reconstruction for `file_id`.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_cache::ReconstructionCacheKey;
    /// use shardline_protocol::{RepositoryProvider, RepositoryScope};
    ///
    /// let scope =
    ///     RepositoryScope::new(RepositoryProvider::GitHub, "acme", "assets", Some("main"))?;
    ///
    /// // A repository-scoped "latest" key.
    /// let key = ReconstructionCacheKey::latest("assets/logo.png", Some(&scope));
    /// assert_eq!(key.file_id(), "assets/logo.png");
    /// assert_eq!(key.content_hash(), None);
    /// if let Some(scope_key) = key.repository_scope() {
    ///     assert_eq!(scope_key.repo(), "assets");
    ///     assert_eq!(scope_key.revision(), Some("main"));
    /// }
    ///
    /// // A global (unscoped) key.
    /// let global = ReconstructionCacheKey::latest("assets/logo.png", None);
    /// assert!(global.repository_scope().is_none());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn latest(file_id: &str, repository_scope: Option<&RepositoryScope>) -> Self {
        Self {
            file_id: Arc::from(file_id),
            content_hash: None,
            repository_scope: repository_scope.map(RepositoryScopeCacheKey::from_scope),
        }
    }

    /// Creates a cache key for one immutable file version.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_cache::ReconstructionCacheKey;
    /// use shardline_protocol::{RepositoryProvider, RepositoryScope};
    ///
    /// let scope =
    ///     RepositoryScope::new(RepositoryProvider::GitHub, "acme", "assets", Some("main"))?;
    ///
    /// let key = ReconstructionCacheKey::version("assets/logo.png", "content-456", Some(&scope));
    /// assert_eq!(key.file_id(), "assets/logo.png");
    /// assert_eq!(key.content_hash(), Some("content-456"));
    /// if let Some(scope_key) = key.repository_scope() {
    ///     assert_eq!(scope_key.owner(), "acme");
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn version(
        file_id: &str,
        content_hash: &str,
        repository_scope: Option<&RepositoryScope>,
    ) -> Self {
        Self {
            file_id: Arc::from(file_id),
            content_hash: Some(Arc::from(content_hash)),
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    use super::{ReconstructionCacheKey, RepositoryScopeCacheKey};

    const PROVIDERS: [(RepositoryProvider, &str); 5] = [
        (RepositoryProvider::GitHub, "github"),
        (RepositoryProvider::GitLab, "gitlab"),
        (RepositoryProvider::Gitea, "gitea"),
        (RepositoryProvider::Codeberg, "codeberg"),
        (RepositoryProvider::Generic, "generic"),
    ];

    #[test]
    fn cache_key_clones_share_immutable_identity_and_preserve_lookup() {
        use std::{collections::HashMap, sync::Arc};

        for content_hash in [None, Some("content-hash")] {
            let key = content_hash.map_or_else(
                || ReconstructionCacheKey::latest("file.bin", None),
                |hash| ReconstructionCacheKey::version("file.bin", hash, None),
            );
            let cloned = key.clone();
            assert!(Arc::ptr_eq(&key.file_id, &cloned.file_id));
            if let (Some(original), Some(copy)) = (&key.content_hash, &cloned.content_hash) {
                assert!(Arc::ptr_eq(original, copy));
            }
            let mut entries = HashMap::new();
            entries.insert(key, "payload");
            assert_eq!(entries.get(&cloned), Some(&"payload"));
            assert_eq!(cloned.content_hash(), content_hash);
        }
    }

    #[test]
    fn cache_key_constructors_preserve_distinct_file_version_and_scope_identity() {
        use std::collections::HashSet;

        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", Some("main"))
                .unwrap();
        let long_id = "a".repeat(1000);
        let mut keys = HashSet::new();
        for scope in [None, Some(&repository)] {
            let expected_scope = scope.map(RepositoryScopeCacheKey::from_scope);
            for file_id in ["file.bin", "", "file with spaces!@#.bin", &long_id] {
                for hash in [None, Some("hash123"), Some(""), Some("hash/with/slashes?")] {
                    let key = hash.map_or_else(
                        || ReconstructionCacheKey::latest(file_id, scope),
                        |hash| ReconstructionCacheKey::version(file_id, hash, scope),
                    );
                    assert_eq!(key.file_id(), file_id);
                    assert_eq!(key.content_hash(), hash);
                    assert_eq!(key.repository_scope(), expected_scope.as_ref());
                    assert!(keys.insert(key), "distinct identities must remain distinct");
                }
            }
        }
    }

    #[test]
    fn repository_scope_cache_keys_preserve_every_identity_component() {
        use std::collections::HashSet;

        let mut keys = HashSet::new();
        for (provider, token) in PROVIDERS {
            for revision in [None, Some("main")] {
                for (owner, repo) in [("owner", "repo"), ("other", "repo"), ("owner", "other")] {
                    let scope = RepositoryScope::new(provider, owner, repo, revision).unwrap();
                    let key = RepositoryScopeCacheKey::from_scope(&scope);
                    assert_eq!(key.provider(), token);
                    assert_eq!(key.owner(), owner);
                    assert_eq!(key.repo(), repo);
                    assert_eq!(key.revision(), revision);
                    assert_eq!(key.clone().provider_kind(), provider);
                    assert!(keys.insert(key), "distinct scopes must remain distinct");
                }
            }
        }
    }
}
