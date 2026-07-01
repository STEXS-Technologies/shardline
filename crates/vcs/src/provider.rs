use shardline_protocol::RepositoryProvider;

/// Supported version-control provider families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProviderKind {
    /// GitHub repositories.
    GitHub,
    /// Gitea repositories.
    Gitea,
    /// GitLab repositories.
    GitLab,
    /// Codeberg (Gitea-based) repositories.
    Codeberg,
    /// A provider implemented through the generic integration boundary.
    Generic,
}

impl ProviderKind {
    /// Returns the corresponding repository-scope provider.
    #[must_use]
    pub const fn repository_provider(self) -> RepositoryProvider {
        match self {
            Self::GitHub => RepositoryProvider::GitHub,
            Self::Gitea => RepositoryProvider::Gitea,
            Self::GitLab => RepositoryProvider::GitLab,
            Self::Codeberg => RepositoryProvider::Codeberg,
            Self::Generic => RepositoryProvider::Generic,
        }
    }

    /// Returns the stable lowercase provider name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.repository_provider().as_str()
    }
}
