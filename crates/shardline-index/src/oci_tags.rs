/// One mutable OCI tag pointer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OciTagEntry {
    /// Repository-scope storage namespace.
    pub scope_namespace: String,
    /// OCI repository name.
    pub repository: String,
    /// OCI tag.
    pub tag: String,
    /// Lowercase SHA-256 manifest digest without the `sha256:` prefix.
    pub digest_hex: String,
}

/// Durable OCI tag storage contract.
///
/// The metadata database is the linearization point for mutable tag pointers;
/// manifest and blob bytes remain immutable objects in the object store.
#[async_trait::async_trait]
pub trait OciTagStore: Send + Sync {
    /// Adapter-specific error type.
    type Error: Send + Sync;

    /// Atomically creates or replaces a tag pointer.
    async fn upsert_oci_tag(&self, entry: &OciTagEntry) -> Result<(), Self::Error>;

    /// Inserts a tag only when it does not already exist.
    ///
    /// Used to import legacy object-store tag pointers without overwriting a
    /// concurrent database-authoritative mutation.
    async fn insert_oci_tag_if_absent(&self, entry: &OciTagEntry) -> Result<bool, Self::Error>;

    /// Loads one tag pointer.
    async fn oci_tag(
        &self,
        scope_namespace: &str,
        repository: &str,
        tag: &str,
    ) -> Result<Option<OciTagEntry>, Self::Error>;

    /// Lists a repository's tags in raw tag order, resuming after `cursor`.
    async fn list_oci_tags(
        &self,
        scope_namespace: &str,
        repository: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<OciTagEntry>, Self::Error>;

    /// Lists tags that currently point to one manifest digest.
    async fn list_oci_tags_by_digest(
        &self,
        scope_namespace: &str,
        repository: &str,
        digest_hex: &str,
    ) -> Result<Vec<OciTagEntry>, Self::Error>;

    /// Deletes a tag only if it still points to `digest_hex`.
    ///
    /// This conditional delete prevents manifest deletion from removing a tag
    /// concurrently retargeted by another replica.
    async fn delete_oci_tag_if_digest(
        &self,
        scope_namespace: &str,
        repository: &str,
        tag: &str,
        digest_hex: &str,
    ) -> Result<bool, Self::Error>;
}
