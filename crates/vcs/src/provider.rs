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

#[cfg(test)]
mod tests {
    use super::ProviderKind;
    use shardline_protocol::RepositoryProvider;

    #[test]
    fn provider_kind_repository_provider_maps_correctly() {
        assert_eq!(ProviderKind::GitHub.repository_provider(), RepositoryProvider::GitHub);
        assert_eq!(ProviderKind::Gitea.repository_provider(), RepositoryProvider::Gitea);
        assert_eq!(ProviderKind::GitLab.repository_provider(), RepositoryProvider::GitLab);
        assert_eq!(ProviderKind::Codeberg.repository_provider(), RepositoryProvider::Codeberg);
        assert_eq!(ProviderKind::Generic.repository_provider(), RepositoryProvider::Generic);
    }

    #[test]
    fn provider_kind_as_str_returns_expected() {
        assert_eq!(ProviderKind::GitHub.as_str(), "github");
        assert_eq!(ProviderKind::Gitea.as_str(), "gitea");
        assert_eq!(ProviderKind::GitLab.as_str(), "gitlab");
        assert_eq!(ProviderKind::Codeberg.as_str(), "codeberg");
        assert_eq!(ProviderKind::Generic.as_str(), "generic");
    }

    #[test]
    fn provider_kind_variants_are_distinct() {
        assert_ne!(ProviderKind::GitHub, ProviderKind::Gitea);
        assert_ne!(ProviderKind::Gitea, ProviderKind::GitLab);
        assert_ne!(ProviderKind::GitLab, ProviderKind::Codeberg);
        assert_ne!(ProviderKind::Codeberg, ProviderKind::Generic);
    }
}
