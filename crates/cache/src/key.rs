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
    #[must_use]
    pub fn from_scope(scope: &RepositoryScope) -> Self {
        Self {
            provider: provider_token(scope.provider()),
            owner: Arc::from(scope.owner()),
            repo: Arc::from(scope.name()),
            revision: scope.revision().map(Arc::from),
        }
    }

    /// Returns the repository provider.
    #[must_use]
    pub fn provider(&self) -> &str {
        self.provider
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
        RepositoryProvider::Codeberg => "codeberg",
        RepositoryProvider::Generic => "generic",
    }
}

#[cfg(test)]
mod tests {
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    use super::{ReconstructionCacheKey, RepositoryScopeCacheKey, provider_token};

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

    // ── None scope ───────────────────────────────────────────────────────

    #[test]
    fn cache_key_without_scope() {
        let latest = ReconstructionCacheKey::latest("file.bin", None);
        assert_eq!(latest.file_id(), "file.bin");
        assert_eq!(latest.content_hash(), None);
        assert!(latest.repository_scope().is_none());
    }

    #[test]
    fn cache_key_version_without_scope() {
        let version = ReconstructionCacheKey::version("file.bin", "abc123", None);
        assert_eq!(version.file_id(), "file.bin");
        assert_eq!(version.content_hash(), Some("abc123"));
        assert!(version.repository_scope().is_none());
    }

    // ── All provider tokens ──────────────────────────────────────────────

    #[test]
    fn provider_token_all_variants() {
        assert_eq!(provider_token(RepositoryProvider::GitHub), "github");
        assert_eq!(provider_token(RepositoryProvider::Gitea), "gitea");
        assert_eq!(provider_token(RepositoryProvider::GitLab), "gitlab");
        assert_eq!(provider_token(RepositoryProvider::Codeberg), "codeberg");
        assert_eq!(provider_token(RepositoryProvider::Generic), "generic");
    }

    // ── Scope with no revision ───────────────────────────────────────────

    #[test]
    fn cache_key_scope_without_revision() {
        let scope = RepositoryScope::new(RepositoryProvider::GitLab, "group", "project", None);
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let latest = ReconstructionCacheKey::latest("doc.pdf", Some(&scope));
        let scope_key = latest.repository_scope().unwrap();
        assert_eq!(scope_key.provider(), "gitlab");
        assert_eq!(scope_key.owner(), "group");
        assert_eq!(scope_key.repo(), "project");
        assert_eq!(scope_key.revision(), None);
    }

    // ── RepositoryScopeCacheKey from_scope ───────────────────────────────

    #[test]
    fn repository_scope_cache_key_from_scope() {
        let scope = RepositoryScope::new(RepositoryProvider::Codeberg, "user", "repo", Some("v1"));
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let scope_key = RepositoryScopeCacheKey::from_scope(&scope);
        assert_eq!(scope_key.provider(), "codeberg");
        assert_eq!(scope_key.owner(), "user");
        assert_eq!(scope_key.repo(), "repo");
        assert_eq!(scope_key.revision(), Some("v1"));
    }

    // ── latest with all provider combinations ────────────────────────────

    #[test]
    fn cache_key_latest_with_each_provider() {
        let providers = [
            (RepositoryProvider::GitHub, "github"),
            (RepositoryProvider::GitLab, "gitlab"),
            (RepositoryProvider::Gitea, "gitea"),
            (RepositoryProvider::Codeberg, "codeberg"),
            (RepositoryProvider::Generic, "generic"),
        ];
        for (provider, expected_token) in &providers {
            let scope = RepositoryScope::new(*provider, "owner", "repo", Some("branch"));
            assert!(scope.is_ok(), "scope creation failed for {expected_token}");
            let Ok(scope) = scope else {
                continue;
            };
            let key = ReconstructionCacheKey::latest("file.bin", Some(&scope));
            let scope_key = key.repository_scope().unwrap();
            assert_eq!(scope_key.provider(), *expected_token);
            assert_eq!(scope_key.owner(), "owner");
            assert_eq!(scope_key.repo(), "repo");
            assert_eq!(scope_key.revision(), Some("branch"));
            assert_eq!(key.file_id(), "file.bin");
            assert_eq!(key.content_hash(), None);
        }
    }

    // ── version with all provider combinations ───────────────────────────

    #[test]
    fn cache_key_version_with_each_provider() {
        let providers = [
            (RepositoryProvider::GitHub, "github"),
            (RepositoryProvider::GitLab, "gitlab"),
            (RepositoryProvider::Gitea, "gitea"),
            (RepositoryProvider::Codeberg, "codeberg"),
            (RepositoryProvider::Generic, "generic"),
        ];
        for (provider, expected_token) in &providers {
            let scope = RepositoryScope::new(*provider, "org", "project", None);
            assert!(scope.is_ok(), "scope creation failed for {expected_token}");
            let Ok(scope) = scope else {
                continue;
            };
            let key = ReconstructionCacheKey::version("ver.bin", "hash123", Some(&scope));
            let scope_key = key.repository_scope().unwrap();
            assert_eq!(scope_key.provider(), *expected_token);
            assert_eq!(scope_key.owner(), "org");
            assert_eq!(scope_key.repo(), "project");
            assert_eq!(scope_key.revision(), None);
            assert_eq!(key.file_id(), "ver.bin");
            assert_eq!(key.content_hash(), Some("hash123"));
        }
    }

    // ── empty values in constructors ─────────────────────────────────────

    #[test]
    fn cache_key_latest_with_empty_file_id() {
        let key = ReconstructionCacheKey::latest("", None);
        assert_eq!(key.file_id(), "");
        assert_eq!(key.content_hash(), None);
        assert!(key.repository_scope().is_none());
    }

    #[test]
    fn cache_key_version_with_empty_content_hash() {
        let key = ReconstructionCacheKey::version("f", "", None);
        assert_eq!(key.file_id(), "f");
        assert_eq!(key.content_hash(), Some(""));
        assert!(key.repository_scope().is_none());
    }

    #[test]
    fn cache_key_version_with_each_provider_no_revision() {
        let providers = [
            RepositoryProvider::GitHub,
            RepositoryProvider::GitLab,
            RepositoryProvider::Gitea,
            RepositoryProvider::Codeberg,
            RepositoryProvider::Generic,
        ];
        for provider in &providers {
            let scope = RepositoryScope::new(*provider, "ns", "name", None);
            assert!(scope.is_ok(), "scope creation failed for {provider:?}");
            let Ok(scope) = scope else {
                continue;
            };
            let key = ReconstructionCacheKey::version("v.bin", "abc", Some(&scope));
            let scope_key = key.repository_scope().unwrap();
            assert_eq!(scope_key.revision(), None, "revision should be None for {provider:?}");
        }
    }

    #[test]
    fn repository_scope_cache_key_from_scope_with_all_providers() {
        for provider in &[
            RepositoryProvider::GitHub,
            RepositoryProvider::GitLab,
            RepositoryProvider::Gitea,
            RepositoryProvider::Codeberg,
            RepositoryProvider::Generic,
        ] {
            let scope = RepositoryScope::new(*provider, "owner", "repo", Some("main"));
            assert!(scope.is_ok(), "scope creation failed for {provider:?}");
            let Ok(scope) = scope else {
                continue;
            };
            let scope_key = RepositoryScopeCacheKey::from_scope(&scope);
            assert_eq!(scope_key.owner(), "owner");
            assert_eq!(scope_key.repo(), "repo");
            assert_eq!(scope_key.revision(), Some("main"));
        }
    }

    #[test]
    fn repository_scope_cache_key_from_scope_without_revision() {
        for provider in &[
            RepositoryProvider::GitHub,
            RepositoryProvider::GitLab,
            RepositoryProvider::Gitea,
            RepositoryProvider::Codeberg,
            RepositoryProvider::Generic,
        ] {
            let scope = RepositoryScope::new(*provider, "own", "proj", None);
            assert!(scope.is_ok(), "scope creation failed for {provider:?}");
            let Ok(scope) = scope else {
                continue;
            };
            let scope_key = RepositoryScopeCacheKey::from_scope(&scope);
            assert_eq!(scope_key.owner(), "own");
            assert_eq!(scope_key.repo(), "proj");
            assert_eq!(scope_key.revision(), None);
        }
    }

    #[test]
    fn cache_key_latest_with_special_chars_in_file_id() {
        let key = ReconstructionCacheKey::latest("file with spaces!@#.bin", None);
        assert_eq!(key.file_id(), "file with spaces!@#.bin");
        assert_eq!(key.content_hash(), None);
    }

    #[test]
    fn cache_key_version_with_special_chars_in_hash() {
        let key = ReconstructionCacheKey::version("f", "hash/with/slashes?", None);
        assert_eq!(key.content_hash(), Some("hash/with/slashes?"));
    }

    #[test]
    fn cache_key_latest_long_file_id() {
        let long_id = "a".repeat(1000);
        let key = ReconstructionCacheKey::latest(&long_id, None);
        assert_eq!(key.file_id().len(), 1000);
    }

    #[test]
    fn cache_key_repository_scope_accessors() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "org", "repo", Some("dev"));
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let scope_key = RepositoryScopeCacheKey::from_scope(&scope);
        assert_eq!(scope_key.provider(), "github");
        assert_eq!(scope_key.owner(), "org");
        assert_eq!(scope_key.repo(), "repo");
        assert_eq!(scope_key.revision(), Some("dev"));
    }
}
