use thiserror::Error;

use crate::ProviderKind;

const MAX_REFERENCE_COMPONENT_BYTES: usize = 512;

/// Repository identity within a provider.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepositoryRef {
    provider: ProviderKind,
    owner: String,
    name: String,
}

impl RepositoryRef {
    /// Creates a repository reference.
    ///
    /// # Errors
    ///
    /// Returns [`VcsReferenceError`] when the owner or repository name is empty,
    /// too large, or contains control characters.
    pub fn new(provider: ProviderKind, owner: &str, name: &str) -> Result<Self, VcsReferenceError> {
        validate_component(owner)?;
        validate_component(name)?;

        Ok(Self {
            provider,
            owner: owner.to_owned(),
            name: name.to_owned(),
        })
    }

    /// Returns the provider kind.
    #[must_use]
    pub const fn provider(&self) -> ProviderKind {
        self.provider
    }

    /// Returns the repository owner or namespace.
    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Returns the repository name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Git revision, branch, or tag reference.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RevisionRef(String);

impl RevisionRef {
    /// Creates a revision reference.
    ///
    /// # Errors
    ///
    /// Returns [`VcsReferenceError`] when the revision is empty, too large, or
    /// contains control characters.
    pub fn new(value: &str) -> Result<Self, VcsReferenceError> {
        validate_component(value)?;

        Ok(Self(value.to_owned()))
    }

    /// Returns the revision reference as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Requested repository access level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RepositoryAccess {
    /// Read access.
    Read,
    /// Write access.
    Write,
}

/// Repository or revision reference validation failure.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum VcsReferenceError {
    /// A required reference component was empty.
    #[error("reference component must not be empty")]
    Empty,
    /// A reference component contained a control character.
    #[error("reference component must not contain control characters")]
    ControlCharacter,
    /// A reference component exceeded the supported metadata bound.
    #[error("reference component exceeded supported length")]
    TooLong,
}

fn validate_component(value: &str) -> Result<(), VcsReferenceError> {
    if value.trim().is_empty() {
        return Err(VcsReferenceError::Empty);
    }

    if value.len() > MAX_REFERENCE_COMPONENT_BYTES {
        return Err(VcsReferenceError::TooLong);
    }

    if value.chars().any(char::is_control) {
        return Err(VcsReferenceError::ControlCharacter);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_REFERENCE_COMPONENT_BYTES, RepositoryAccess, RepositoryRef, RevisionRef,
        VcsReferenceError,
    };
    use crate::ProviderKind;

    #[test]
    fn repository_ref_rejects_empty_owner() {
        let reference = RepositoryRef::new(ProviderKind::GitHub, "", "assets");

        assert_eq!(reference, Err(VcsReferenceError::Empty));
    }

    #[test]
    fn repository_ref_rejects_empty_name() {
        let reference = RepositoryRef::new(ProviderKind::Gitea, "team", " ");

        assert_eq!(reference, Err(VcsReferenceError::Empty));
    }

    #[test]
    fn repository_ref_rejects_control_characters() {
        let reference = RepositoryRef::new(ProviderKind::Generic, "team\n", "assets");

        assert_eq!(reference, Err(VcsReferenceError::ControlCharacter));
    }

    #[test]
    fn repository_ref_rejects_oversized_components() {
        let oversized = "a".repeat(MAX_REFERENCE_COMPONENT_BYTES + 1);
        let reference = RepositoryRef::new(ProviderKind::GitLab, &oversized, "assets");

        assert_eq!(reference, Err(VcsReferenceError::TooLong));
    }

    #[test]
    fn repository_ref_keeps_provider_owner_and_name() {
        let reference = RepositoryRef::new(ProviderKind::GitHub, "team", "assets");

        assert!(reference.is_ok());
        if let Ok(value) = reference {
            assert_eq!(value.provider(), ProviderKind::GitHub);
            assert_eq!(value.owner(), "team");
            assert_eq!(value.name(), "assets");
        }
    }

    #[test]
    fn revision_ref_rejects_empty_values() {
        let revision = RevisionRef::new("\t");

        assert_eq!(revision, Err(VcsReferenceError::Empty));
    }

    #[test]
    fn revision_ref_rejects_control_characters() {
        let revision = RevisionRef::new("main\n");

        assert_eq!(revision, Err(VcsReferenceError::ControlCharacter));
    }

    #[test]
    fn revision_ref_rejects_oversized_values() {
        let oversized = "r".repeat(MAX_REFERENCE_COMPONENT_BYTES + 1);
        let revision = RevisionRef::new(&oversized);

        assert_eq!(revision, Err(VcsReferenceError::TooLong));
    }

    #[test]
    fn revision_ref_keeps_value() {
        let revision = RevisionRef::new("refs/heads/main");

        assert!(revision.is_ok());
        if let Ok(value) = revision {
            assert_eq!(value.as_str(), "refs/heads/main");
        }
    }

    #[test]
    fn repository_access_variants_are_distinct() {
        assert_ne!(RepositoryAccess::Read, RepositoryAccess::Write);
    }
}
