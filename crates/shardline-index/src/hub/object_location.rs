//! Derived physical locations; these never confer repository authorization.

/// Kind of immutable Hub object whose original storage namespace is cached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubObjectKind {
    /// Inline or basic LFS object.
    File,
    /// Git pack or public commit identity archive.
    GitArchive,
}

impl HubObjectKind {
    /// Stable representation used in the derived location index.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::GitArchive => "git-archive",
        }
    }
}

/// The original credential revision used to address an immutable object.
///
/// Provider, owner and repository are supplied by the authorized lookup, never
/// by this value. A damaged index therefore cannot redirect a read to another
/// repository's namespace or to an arbitrary object-store key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubObjectLocation {
    /// `None` is the repository-wide credential namespace.
    pub storage_revision: Option<String>,
}
