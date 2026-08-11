//! Shared record-kind discriminator used by both the Postgres and local SQLite
//! metadata backends.
//!
//! Both backends historically declared their own `pub(crate)` enum
//! (`PostgresRecordKind` and `LocalRecordKind`) with identical `Latest`/`Version`
//! variants and identical string-match arms. This module unifies them into a
//! single type so the two backends share one source of truth for the persisted
//! string values (`"latest"` / `"version"`).
//!
//! The unified type lives here (a tiny shared module) rather than under either
//! backend so that neither `postgres` nor `local_sqlite` owns the other's
//! parsing rules. Each backend maps [`RecordKindParseError`] to its own
//! `InvalidRecordKind` error variant when reading from storage.

use std::fmt;
use std::str::FromStr;

/// Distinguishes the "latest" record for a file id from a pinned immutable
/// "version" record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum RecordKind {
    /// The latest (unversioned) record for a file id.
    Latest,
    /// A specific immutable version record for a file id.
    Version,
}

impl RecordKind {
    /// Returns the persisted string representation (`"latest"` / `"version"`).
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::Version => "version",
        }
    }
}

impl fmt::Display for RecordKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for RecordKind {
    type Err = RecordKindParseError;

    /// Parses a [`RecordKind`] from its persisted string representation.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "latest" => Ok(Self::Latest),
            "version" => Ok(Self::Version),
            _ => Err(RecordKindParseError::InvalidRecordKind),
        }
    }
}

/// Failure parsing a [`RecordKind`] from a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordKindParseError {
    /// The string was neither `"latest"` nor `"version"`.
    InvalidRecordKind,
}

impl fmt::Display for RecordKindParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid record kind")
    }
}

impl std::error::Error for RecordKindParseError {}
