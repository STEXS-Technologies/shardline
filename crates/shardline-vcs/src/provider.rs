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

impl From<ProviderKind> for RepositoryProvider {
    fn from(kind: ProviderKind) -> Self {
        kind.repository_provider()
    }
}

impl From<RepositoryProvider> for ProviderKind {
    fn from(provider: RepositoryProvider) -> Self {
        match provider {
            RepositoryProvider::GitHub => Self::GitHub,
            RepositoryProvider::Gitea => Self::Gitea,
            RepositoryProvider::GitLab => Self::GitLab,
            RepositoryProvider::Codeberg => Self::Codeberg,
            RepositoryProvider::Generic => Self::Generic,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ProviderKind;
    use shardline_protocol::RepositoryProvider;

    #[test]
    fn provider_kind_repository_provider_maps_correctly() {
        assert_eq!(
            ProviderKind::GitHub.repository_provider(),
            RepositoryProvider::GitHub
        );
        assert_eq!(
            ProviderKind::Gitea.repository_provider(),
            RepositoryProvider::Gitea
        );
        assert_eq!(
            ProviderKind::GitLab.repository_provider(),
            RepositoryProvider::GitLab
        );
        assert_eq!(
            ProviderKind::Codeberg.repository_provider(),
            RepositoryProvider::Codeberg
        );
        assert_eq!(
            ProviderKind::Generic.repository_provider(),
            RepositoryProvider::Generic
        );
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

    #[test]
    fn provider_kind_converts_to_repository_provider_all_variants() {
        assert_eq!(
            RepositoryProvider::from(ProviderKind::GitHub),
            RepositoryProvider::GitHub
        );
        assert_eq!(
            RepositoryProvider::from(ProviderKind::Gitea),
            RepositoryProvider::Gitea
        );
        assert_eq!(
            RepositoryProvider::from(ProviderKind::GitLab),
            RepositoryProvider::GitLab
        );
        assert_eq!(
            RepositoryProvider::from(ProviderKind::Codeberg),
            RepositoryProvider::Codeberg
        );
        assert_eq!(
            RepositoryProvider::from(ProviderKind::Generic),
            RepositoryProvider::Generic
        );
    }

    #[test]
    fn repository_provider_converts_to_provider_kind_all_variants() {
        assert_eq!(
            ProviderKind::from(RepositoryProvider::GitHub),
            ProviderKind::GitHub
        );
        assert_eq!(
            ProviderKind::from(RepositoryProvider::Gitea),
            ProviderKind::Gitea
        );
        assert_eq!(
            ProviderKind::from(RepositoryProvider::GitLab),
            ProviderKind::GitLab
        );
        assert_eq!(
            ProviderKind::from(RepositoryProvider::Codeberg),
            ProviderKind::Codeberg
        );
        assert_eq!(
            ProviderKind::from(RepositoryProvider::Generic),
            ProviderKind::Generic
        );
    }

    #[test]
    fn provider_kind_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<ProviderKind>();
        assert_sync::<ProviderKind>();
    }

    #[test]
    fn provider_kind_debug_format_matches_variant_name() {
        assert_eq!(format!("{:?}", ProviderKind::GitHub), "GitHub");
        assert_eq!(format!("{:?}", ProviderKind::Gitea), "Gitea");
        assert_eq!(format!("{:?}", ProviderKind::GitLab), "GitLab");
        assert_eq!(format!("{:?}", ProviderKind::Codeberg), "Codeberg");
        assert_eq!(format!("{:?}", ProviderKind::Generic), "Generic");
    }

    #[test]
    fn provider_kind_clone_yields_equal_value() {
        let original = ProviderKind::GitLab;
        let cloned = original;
        assert_eq!(original, cloned);
    }

    #[test]
    fn provider_kind_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash(value: ProviderKind) -> u64 {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        }

        assert_eq!(hash(ProviderKind::GitHub), hash(ProviderKind::GitHub));
        assert_ne!(hash(ProviderKind::GitHub), hash(ProviderKind::Gitea));
    }
}
