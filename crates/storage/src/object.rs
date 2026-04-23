use bytes::Bytes;
use shardline_protocol::ShardlineHash;

use crate::ObjectKey;

/// Object bytes presented to an object adapter.
#[derive(Debug, Clone)]
pub enum ObjectBody<'bytes> {
    /// Borrowed object bytes.
    Borrowed(&'bytes [u8]),
    /// Owned or shared object bytes.
    Shared(Bytes),
}

impl<'bytes> ObjectBody<'bytes> {
    /// Creates a borrowed object body without copying bytes.
    #[must_use]
    pub const fn from_slice(bytes: &'bytes [u8]) -> Self {
        Self::Borrowed(bytes)
    }

    /// Creates a shared object body without copying bytes.
    #[must_use]
    pub const fn from_bytes(bytes: Bytes) -> Self {
        Self::Shared(bytes)
    }

    /// Creates a shared object body from an owned buffer without copying bytes.
    #[must_use]
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        Self::Shared(Bytes::from(bytes))
    }

    /// Returns the borrowed bytes.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Shared(bytes) => bytes.as_ref(),
        }
    }

    /// Returns the body as owned or shared bytes.
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        match self {
            Self::Borrowed(bytes) => Bytes::copy_from_slice(bytes),
            Self::Shared(bytes) => bytes,
        }
    }
}

/// Integrity metadata expected for an object write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectIntegrity {
    hash: ShardlineHash,
    length: u64,
}

impl ObjectIntegrity {
    /// Creates object integrity metadata.
    #[must_use]
    pub const fn new(hash: ShardlineHash, length: u64) -> Self {
        Self { hash, length }
    }

    /// Returns the expected content hash.
    #[must_use]
    pub const fn hash(&self) -> ShardlineHash {
        self.hash
    }

    /// Returns the expected object length.
    #[must_use]
    pub const fn length(&self) -> u64 {
        self.length
    }
}

/// Result of an idempotent object write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    /// The object was inserted.
    Inserted,
    /// The object already existed with matching content.
    AlreadyExists,
}

/// Stored object metadata exposed by an adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    key: ObjectKey,
    length: u64,
    checksum: Option<ShardlineHash>,
}

impl ObjectMetadata {
    /// Creates stored object metadata.
    #[must_use]
    pub const fn new(key: ObjectKey, length: u64, checksum: Option<ShardlineHash>) -> Self {
        Self {
            key,
            length,
            checksum,
        }
    }

    /// Returns the validated object key.
    #[must_use]
    pub const fn key(&self) -> &ObjectKey {
        &self.key
    }

    /// Returns the stored object length.
    #[must_use]
    pub const fn length(&self) -> u64 {
        self.length
    }

    /// Returns the adapter-provided checksum when one is available.
    #[must_use]
    pub const fn checksum(&self) -> Option<ShardlineHash> {
        self.checksum
    }
}

/// Result of an idempotent object delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    /// The object was deleted.
    Deleted,
    /// The object was already absent.
    NotFound,
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use shardline_protocol::ShardlineHash;

    use super::{DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectMetadata};
    use crate::ObjectKey;

    #[test]
    fn object_body_borrows_input_bytes() {
        let bytes = [1, 2, 3];
        let body = ObjectBody::from_slice(&bytes);

        assert_eq!(body.as_slice(), bytes);
    }

    #[test]
    fn object_body_keeps_shared_bytes_without_copy() {
        let shared = Bytes::from_static(b"payload");
        let body = ObjectBody::from_bytes(shared.clone());

        assert_eq!(body.as_slice(), shared.as_ref());
        assert_eq!(body.into_bytes(), shared);
    }

    #[test]
    fn object_integrity_keeps_hash_and_length() {
        let hash = ShardlineHash::from_bytes([8; 32]);
        let integrity = ObjectIntegrity::new(hash, 42);

        assert_eq!(integrity.hash(), hash);
        assert_eq!(integrity.length(), 42);
    }

    #[test]
    fn object_metadata_keeps_key_length_and_checksum() {
        let hash = ShardlineHash::from_bytes([9; 32]);
        let key = ObjectKey::parse("xorbs/default/aa/bb/hash.xorb");

        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let metadata = ObjectMetadata::new(key.clone(), 64, Some(hash));

        assert_eq!(metadata.key(), &key);
        assert_eq!(metadata.length(), 64);
        assert_eq!(metadata.checksum(), Some(hash));
    }

    #[test]
    fn delete_outcome_distinguishes_deleted_and_missing() {
        assert_eq!(DeleteOutcome::Deleted, DeleteOutcome::Deleted);
        assert_eq!(DeleteOutcome::NotFound, DeleteOutcome::NotFound);
    }
}
