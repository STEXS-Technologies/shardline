/// Typed namespace for a mutable resource protected by a distributed fence.
///
/// The database strings are part of the rolling-upgrade compatibility contract.
/// Keeping them behind this enum prevents a spelling difference from silently
/// creating an independent advisory-lock and fencing namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResourceLockDomain {
    /// One OCI repository within a storage scope.
    OciRepository,
    /// One source-control provider repository.
    ProviderRepository,
}

impl ResourceLockDomain {
    /// Stable database representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OciRepository => "oci-repository",
            Self::ProviderRepository => "provider-repository",
        }
    }
}

/// Canonical identity of a mutable resource protected by a distributed fence.
///
/// Construction is protocol-specific so callers cannot pair an OCI resource
/// with the provider lock domain (or invent a new domain with a raw string).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourceLockKey {
    domain: ResourceLockDomain,
    resource: String,
}

impl ResourceLockKey {
    /// Builds the rolling-upgrade-compatible identity for an OCI repository.
    #[must_use]
    pub fn oci_repository(scope_namespace: &str, repository: &str) -> Self {
        Self {
            domain: ResourceLockDomain::OciRepository,
            resource: format!("{scope_namespace}:{repository}"),
        }
    }

    /// Builds the rolling-upgrade-compatible identity for a provider repository.
    #[must_use]
    pub fn provider_repository(provider: &str, owner: &str, repository: &str) -> Self {
        Self {
            domain: ResourceLockDomain::ProviderRepository,
            resource: format!("{provider}:{owner}/{repository}"),
        }
    }

    /// Typed resource namespace.
    #[must_use]
    pub const fn domain(&self) -> ResourceLockDomain {
        self.domain
    }

    /// Stable database representation of the resource identity.
    #[must_use]
    pub fn resource(&self) -> &str {
        &self.resource
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn valid_oci_components_have_unique_canonical_keys(
            scope_a in "[a-z0-9]{1,32}",
            repository_a in "[a-z0-9]{1,16}(/[a-z0-9]{1,16}){0,3}",
            scope_b in "[a-z0-9]{1,32}",
            repository_b in "[a-z0-9]{1,16}(/[a-z0-9]{1,16}){0,3}",
        ) {
            let first = ResourceLockKey::oci_repository(&scope_a, &repository_a);
            let second = ResourceLockKey::oci_repository(&scope_b, &repository_b);
            prop_assert_eq!(
                first == second,
                scope_a == scope_b && repository_a == repository_b
            );
        }

        #[test]
        fn valid_provider_components_have_unique_canonical_keys(
            provider_a in "[a-z]{1,12}",
            owner_a in "[A-Za-z0-9_-]{1,24}",
            repository_a in "[A-Za-z0-9_.-]{1,24}",
            provider_b in "[a-z]{1,12}",
            owner_b in "[A-Za-z0-9_-]{1,24}",
            repository_b in "[A-Za-z0-9_.-]{1,24}",
        ) {
            let first = ResourceLockKey::provider_repository(
                &provider_a,
                &owner_a,
                &repository_a,
            );
            let second = ResourceLockKey::provider_repository(
                &provider_b,
                &owner_b,
                &repository_b,
            );
            prop_assert_eq!(
                first == second,
                provider_a == provider_b
                    && owner_a == owner_b
                    && repository_a == repository_b
            );
        }
    }

    #[test]
    fn lock_domains_cannot_alias() {
        let values = [
            ResourceLockDomain::OciRepository,
            ResourceLockDomain::ProviderRepository,
        ];
        let encoded = values
            .into_iter()
            .map(ResourceLockDomain::as_str)
            .collect::<BTreeSet<_>>();
        assert_eq!(encoded.len(), values.len());
    }

    #[test]
    fn encodings_preserve_the_existing_database_contract() {
        let oci = ResourceLockKey::oci_repository("global", "team/assets");
        assert_eq!(oci.domain().as_str(), "oci-repository");
        assert_eq!(oci.resource(), "global:team/assets");

        let provider = ResourceLockKey::provider_repository("github", "team", "assets");
        assert_eq!(provider.domain().as_str(), "provider-repository");
        assert_eq!(provider.resource(), "github:team/assets");
    }
}
