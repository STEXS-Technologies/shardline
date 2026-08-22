use crate::OciTagEntry;

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
}
