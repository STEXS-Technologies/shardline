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
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_storage::ObjectBody;
    ///
    /// let body = ObjectBody::from_slice(b"payload");
    /// assert_eq!(body.as_slice(), b"payload");
    /// ```
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
    /// Computes the canonical storage integrity metadata for bytes.
    #[must_use]
    pub fn for_bytes(bytes: &[u8]) -> Self {
        Self {
            hash: chunk_hash(bytes),
            length: bytes.len() as u64,
        }
    }

    /// Creates object integrity metadata.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_protocol::ShardlineHash;
    /// use shardline_storage::ObjectIntegrity;
    ///
    /// let integrity = ObjectIntegrity::new(ShardlineHash::from_bytes([7; 32]), 1024);
    /// assert_eq!(integrity.hash().as_bytes(), &[7; 32]);
    /// assert_eq!(integrity.length(), 1024);
    /// ```
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

    pub(crate) fn verify_bytes<Error>(
        &self,
        bytes: &[u8],
        length_error: Error,
        hash_error: Error,
    ) -> Result<(), Error> {
        if u64::try_from(bytes.len()).ok() != Some(self.length()) {
            return Err(length_error);
        }
        if chunk_hash(bytes) != self.hash() {
            return Err(hash_error);
        }
        Ok(())
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
    /// Last-modified time (unix nanoseconds) observed by the OBJECT STORE
    /// backend, not by the writer. `None` when the backend does not expose one
    /// cheaply. This is backend-truth and immune to writer/host clock
    /// divergence, unlike writer-embedded timestamps in object keys.
    modified_unix_nanos: Option<u64>,
}

impl ObjectMetadata {
    /// Creates stored object metadata.
    #[must_use]
    pub const fn new(key: ObjectKey, length: u64, checksum: Option<ShardlineHash>) -> Self {
        Self {
            key,
            length,
            checksum,
            modified_unix_nanos: None,
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

    /// Returns the backend-observed last-modified time in unix nanoseconds,
    /// when the backend exposed one.
    #[must_use]
    pub const fn modified_unix_nanos(&self) -> Option<u64> {
        self.modified_unix_nanos
    }

    /// Sets the backend-observed last-modified time (unix nanoseconds).
    #[must_use]
    pub const fn with_modified(mut self, unix_nanos: u64) -> Self {
        self.modified_unix_nanos = Some(unix_nanos);
        self
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

pub(crate) fn chunk_hash(bytes: &[u8]) -> ShardlineHash {
    ShardlineHash::from_bytes(*blake3::hash(bytes).as_bytes())
}

#[cfg(test)]
mod tests {
    use shardline_protocol::ShardlineHash;

    use super::{ObjectBody, ObjectIntegrity, ObjectMetadata};
    use crate::ObjectKey;

    #[test]
    fn object_bodies_preserve_bytes_and_ownership() {
        for input in [
            b"".as_slice(),
            &[1, 2, 3],
            b"payload",
            &[10, 20, 30, 40],
            b"test-data",
            b"shared-data",
        ] {
            let borrowed = ObjectBody::from_slice(input);
            assert_eq!(borrowed.as_slice().as_ptr(), input.as_ptr());
            let copied = borrowed.into_bytes();
            assert_eq!(copied.as_ref(), input);
            if !input.is_empty() {
                assert_ne!(copied.as_ptr(), input.as_ptr());
            }

            let owned = input.to_vec();
            let pointer = owned.as_ptr();
            let body = ObjectBody::from_vec(owned);
            assert_eq!(body.as_slice(), input);
            if !input.is_empty() {
                assert_eq!(body.as_slice().as_ptr(), pointer);
            }
            let shared = body.into_bytes();
            assert_eq!(shared.as_ref(), input);
            if !input.is_empty() {
                assert_eq!(shared.as_ptr(), pointer);
            }
            let body = ObjectBody::from_bytes(shared);
            assert_eq!(body.as_slice(), input);
            let result = body.into_bytes();
            assert_eq!(result.as_ref(), input);
            if !input.is_empty() {
                assert_eq!(result.as_ptr(), pointer);
            }
        }
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
        let key = ObjectKey::parse("xorbs/default/aa/bb/hash.xorb").unwrap();

        let metadata = ObjectMetadata::new(key.clone(), 64, Some(hash));

        assert_eq!(metadata.key(), &key);
        assert_eq!(metadata.length(), 64);
        assert_eq!(metadata.checksum(), Some(hash));
        assert_eq!(
            metadata.modified_unix_nanos(),
            None,
            "plain `new` carries no modified timestamp"
        );
    }

    #[test]
    fn object_metadata_with_modified_sets_backend_observed_mtime() {
        let key = ObjectKey::parse("xorbs/default/aa/bb/hash.xorb").unwrap();

        let metadata =
            ObjectMetadata::new(key.clone(), 64, None).with_modified(1_700_000_000_000_000_000);

        assert_eq!(metadata.key(), &key);
        assert_eq!(metadata.length(), 64);
        assert_eq!(
            metadata.modified_unix_nanos(),
            Some(1_700_000_000_000_000_000)
        );
    }

    #[test]
    fn object_metadata_with_none_checksum() {
        let key = ObjectKey::parse("xorbs/default/no-checksum.xorb").unwrap();
        let metadata = ObjectMetadata::new(key.clone(), 128, None);
        assert_eq!(metadata.key(), &key);
        assert_eq!(metadata.length(), 128);
        assert!(metadata.checksum().is_none());
    }

    #[test]
    fn object_integrity_zero_length() {
        let hash = ShardlineHash::from_bytes([0; 32]);
        let integrity = ObjectIntegrity::new(hash, 0);
        assert_eq!(integrity.length(), 0);
        assert_eq!(integrity.hash(), hash);
    }
}
