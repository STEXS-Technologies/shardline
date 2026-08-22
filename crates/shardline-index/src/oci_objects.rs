use crate::OciTagEntry;

use std::{fmt, str::FromStr};

/// Immutable OCI object category recorded by the logical deletion index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OciObjectKind {
    /// Content-addressed layer or configuration bytes.
    Blob,
    /// Content-addressed manifest or index document.
    Manifest,
}

impl OciObjectKind {
    /// Stable database representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Blob => "blob",
            Self::Manifest => "manifest",
        }
    }
}

/// An invalid durable OCI object-kind representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OciObjectKindParseError;

impl fmt::Display for OciObjectKindParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OCI object kind must be blob or manifest")
    }
}

impl std::error::Error for OciObjectKindParseError {}

impl FromStr for OciObjectKind {
    type Err = OciObjectKindParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "blob" => Ok(Self::Blob),
            "manifest" => Ok(Self::Manifest),
            _ => Err(OciObjectKindParseError),
        }
    }
}

/// Repository-qualified identity of an immutable OCI object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OciObjectKey {
    /// Repository-scope storage namespace.
    pub scope_namespace: String,
    /// OCI repository name.
    pub repository: String,
    /// Blob or manifest namespace.
    pub kind: OciObjectKind,
    /// Lowercase SHA-256 digest without the `sha256:` prefix.
    pub digest_hex: String,
}

/// One durable OCI logical-deletion generation eligible for later GC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OciObjectTombstone {
    /// Repository-qualified immutable object identity.
    pub key: OciObjectKey,
    /// Wall-clock time at which this tombstone generation was written.
    pub deleted_at_unix_seconds: u64,
}

/// Durable OCI logical-deletion contract.
///
/// Object-store bytes are immutable and deliberately retained after deletion.
/// The metadata row is the visibility boundary, which prevents a process that
/// lost its database fence from physically deleting content republished by a
/// newer owner.
#[async_trait::async_trait]
pub trait OciObjectStore: Send + Sync {
    /// Adapter-specific error type.
    type Error: Send + Sync;

    /// Returns whether the object is logically deleted.
    async fn oci_object_is_deleted(&self, key: &OciObjectKey) -> Result<bool, Self::Error>;

    /// Publishes an object and its optional tag updates in one metadata commit.
    async fn publish_oci_object(
        &self,
        key: &OciObjectKey,
        tags: &[OciTagEntry],
    ) -> Result<(), Self::Error>;

    /// Logically deletes an object and, for manifests, removes all tags that
    /// still point to its digest in the same metadata commit.
    async fn delete_oci_object(&self, key: &OciObjectKey) -> Result<(), Self::Error>;

    /// Lists every durable OCI logical-deletion generation.
    async fn list_oci_object_tombstones(&self) -> Result<Vec<OciObjectTombstone>, Self::Error>;

    /// Removes the tombstone only when its deletion timestamp still matches
    /// the inspected generation.
    ///
    /// Returns `false` when a publisher or a newer delete changed the row.
    async fn delete_oci_object_tombstone_if_unchanged(
        &self,
        tombstone: &OciObjectTombstone,
    ) -> Result<bool, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_kind_database_representation_round_trips() {
        for kind in [OciObjectKind::Blob, OciObjectKind::Manifest] {
            assert_eq!(kind.as_str().parse(), Ok(kind));
        }
        assert_eq!(
            "layer".parse::<OciObjectKind>(),
            Err(OciObjectKindParseError)
        );
    }
}
