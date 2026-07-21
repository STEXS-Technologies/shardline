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

    #[test]
    fn vcs_reference_error_display_all_variants() {
        let cases: &[(VcsReferenceError, &str)] = &[
            (VcsReferenceError::Empty, "empty"),
            (VcsReferenceError::ControlCharacter, "control"),
            (VcsReferenceError::TooLong, "length"),
        ];
        for (error, substring) in cases {
            let msg = error.to_string();
            assert!(!msg.is_empty(), "empty display for {error:?}");
            assert!(
                msg.contains(substring),
                "expected '{substring}' in '{msg}' from {error:?}"
            );
        }
    }

    #[test]
    fn validate_component_rejects_tabs() {
        let result = super::validate_component("\t\t");
        assert_eq!(result, Err(VcsReferenceError::Empty));
    }

    #[test]
    fn repository_ref_rejects_control_character_in_name() {
        let result = RepositoryRef::new(ProviderKind::GitHub, "owner", "name\x00");
        assert_eq!(result, Err(VcsReferenceError::ControlCharacter));
    }

    #[test]
    fn repository_ref_rejects_whitespace_only_owner() {
        let result = RepositoryRef::new(ProviderKind::GitHub, "  ", "assets");
        assert_eq!(result, Err(VcsReferenceError::Empty));
    }

    #[test]
    fn repository_ref_rejects_whitespace_only_name() {
        let result = RepositoryRef::new(ProviderKind::GitHub, "owner", " \n ");
        assert_eq!(result, Err(VcsReferenceError::Empty));
    }

    #[test]
    fn repository_ref_rejects_control_character_in_owner() {
        let result = RepositoryRef::new(ProviderKind::GitHub, "own\ter", "assets");
        assert_eq!(result, Err(VcsReferenceError::ControlCharacter));
    }

    #[test]
    fn repository_ref_exact_max_length_owner_succeeds() {
        let max = "a".repeat(super::MAX_REFERENCE_COMPONENT_BYTES);
        let result = RepositoryRef::new(ProviderKind::GitHub, &max, "name");
        assert!(result.is_ok());
    }

    #[test]
    fn repository_ref_exact_max_length_name_succeeds() {
        let max = "b".repeat(super::MAX_REFERENCE_COMPONENT_BYTES);
        let result = RepositoryRef::new(ProviderKind::GitHub, "owner", &max);
        assert!(result.is_ok());
    }

    #[test]
    fn revision_ref_rejects_null_character() {
        let result = RevisionRef::new("refs/heads/\x00main");
        assert_eq!(result, Err(VcsReferenceError::ControlCharacter));
    }

    #[test]
    fn revision_ref_rejects_newline_characters() {
        let result = RevisionRef::new("refs/heads/main\r");
        assert_eq!(result, Err(VcsReferenceError::ControlCharacter));
    }

    #[test]
    fn revision_ref_rejects_whitespace_only() {
        let result = RevisionRef::new("   ");
        assert_eq!(result, Err(VcsReferenceError::Empty));
    }

    #[test]
    fn revision_ref_exact_max_length_succeeds() {
        let max = "v".repeat(super::MAX_REFERENCE_COMPONENT_BYTES);
        let result = RevisionRef::new(&max);
        assert!(result.is_ok());
    }

    #[test]
    fn repository_ref_clone_and_eq() {
        let a = RepositoryRef::new(ProviderKind::GitHub, "team", "assets").unwrap();
        let b = RepositoryRef::new(ProviderKind::GitHub, "team", "assets").unwrap();
        let c = RepositoryRef::new(ProviderKind::GitLab, "team", "assets").unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn repository_ref_clone_independent() {
        let a = RepositoryRef::new(ProviderKind::GitHub, "team", "assets").unwrap();
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn revision_ref_clone_independent() {
        let a = RevisionRef::new("refs/heads/main").unwrap();
        let b = a.clone();
        assert_eq!(a.as_str(), b.as_str());
    }

    #[test]
    fn revision_ref_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash(value: &RevisionRef) -> u64 {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        }

        assert_eq!(
            hash(&RevisionRef::new("main").unwrap()),
            hash(&RevisionRef::new("main").unwrap())
        );
        assert_ne!(
            hash(&RevisionRef::new("main").unwrap()),
            hash(&RevisionRef::new("develop").unwrap())
        );
    }

    #[test]
    fn repository_ref_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash(value: &RepositoryRef) -> u64 {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        }

        assert_eq!(
            hash(&RepositoryRef::new(ProviderKind::GitHub, "a", "b").unwrap()),
            hash(&RepositoryRef::new(ProviderKind::GitHub, "a", "b").unwrap())
        );
    }

    #[test]
    fn validate_component_rejects_all_control_characters() {
        for c in '\x00'..='\x1f' {
            // skip whitespace that trims to empty
            if c.is_whitespace() {
                continue;
            }
            let s = format!("valid{c}");
            let result = super::validate_component(&s);
            assert_eq!(
                result,
                Err(VcsReferenceError::ControlCharacter),
                "failed for U+{:04X}",
                c as u32
            );
        }
    }

    #[test]
    fn repository_ref_debug_format() {
        let r = RepositoryRef::new(ProviderKind::GitHub, "owner", "repo").unwrap();
        let debug = format!("{r:?}");
        assert!(debug.contains("GitHub") || debug.contains("owner") || debug.contains("repo"));
    }

    #[test]
    fn revision_ref_debug_format() {
        let r = RevisionRef::new("refs/heads/main").unwrap();
        let debug = format!("{r:?}");
        assert!(debug.contains("refs/heads/main"));
    }

    #[test]
    fn repository_access_debug_format() {
        assert_eq!(format!("{:?}", RepositoryAccess::Read), "Read");
        assert_eq!(format!("{:?}", RepositoryAccess::Write), "Write");
    }

    #[test]
    fn vcs_reference_error_derived_traits() {
        let err = VcsReferenceError::Empty;
        assert_eq!(err, VcsReferenceError::Empty);
        let copied = err;
        assert_eq!(err, copied);
    }
}
