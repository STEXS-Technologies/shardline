//! Owned bytes whose commitment cannot be separated from their contents.
use crate::ObjectIntegrity;
use bytes::Bytes;
use shardline_protocol::ShardlineHash;

/// Immutable storage body with a commitment computed over its exact bytes.
///
/// ```compile_fail
/// let mut body = shardline_storage::VerifiedObjectBody::from_vec(vec![1]);
/// body[0] = 2; // Mutable access would invalidate the bound commitment.
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedObjectBody {
    bytes: Bytes,
    integrity: ObjectIntegrity,
}

impl VerifiedObjectBody {
    /// Authenticate an owned buffer without copying it.
    #[must_use]
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        let mut builder = VerifiedBodyBuilder::default();
        builder.hasher.update(&bytes);
        builder.bytes = bytes;
        builder.finish()
    }

    /// Commitment bound to the immutable bytes.
    #[must_use]
    pub const fn integrity(&self) -> ObjectIntegrity {
        self.integrity
    }

    /// Consume the immutable body without copying it.
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

impl std::ops::Deref for VerifiedObjectBody {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsRef<[u8]> for VerifiedObjectBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// Incremental commitment builder. No API exposes mutable accumulated bytes.
#[derive(Default)]
pub struct VerifiedBodyBuilder {
    bytes: Vec<u8>,
    hasher: blake3::Hasher,
}

impl VerifiedBodyBuilder {
    /// Allocated byte capacity; callers retain their existing memory admission.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.bytes.capacity()
    }

    /// Reserve additional capacity without changing the committed contents.
    /// # Errors
    /// Returns allocation failures.
    pub fn try_reserve_exact(
        &mut self,
        additional: usize,
    ) -> Result<(), std::collections::TryReserveError> {
        self.bytes.try_reserve_exact(additional)
    }

    /// Append and authenticate the same bytes.
    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
        self.hasher.update(bytes);
    }

    /// Discard the commitment when handing bytes to a legacy normalization path.
    #[must_use]
    pub fn into_vec(self) -> Vec<u8> {
        self.bytes
    }

    /// Seal the accumulated bytes. There is no unchecked digest constructor.
    #[must_use]
    pub fn finish(self) -> VerifiedObjectBody {
        let integrity = ObjectIntegrity::new(
            ShardlineHash::from_bytes(*self.hasher.finalize().as_bytes()),
            self.bytes.len() as u64,
        );
        VerifiedObjectBody {
            bytes: Bytes::from(self.bytes),
            integrity,
        }
    }
}

impl std::ops::Deref for VerifiedBodyBuilder {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsRef<[u8]> for VerifiedBodyBuilder {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl std::io::Write for VerifiedBodyBuilder {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{LocalObjectStore, ObjectKey, ObjectStore, PutOutcome};
    use std::io::Write;

    #[test]
    fn partitions_and_generated_suffix_bind_exactly_the_published_bytes() {
        let payload = vec![19; 131_101];
        for frame in [1, 17, 65_536, payload.len()] {
            let mut builder = VerifiedBodyBuilder::default();
            for bytes in payload.chunks(frame) {
                builder.extend_from_slice(bytes);
            }
            builder.write_all(b"generated footer").unwrap();
            let mut expected = payload.clone();
            expected.extend_from_slice(b"generated footer");
            let body = builder.finish();
            assert_eq!(&*body, expected);
            assert_eq!(
                body.integrity(),
                VerifiedObjectBody::from_vec(expected).integrity()
            );
        }
    }

    #[test]
    fn verified_publication_preserves_conflicts_and_rejects_swapped_commitments() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(root.path().to_path_buf()).unwrap();
        let key = ObjectKey::parse("verified/body").unwrap();
        let body = VerifiedObjectBody::from_vec(b"original".to_vec());
        let integrity = body.integrity();
        let wrong_length = ObjectIntegrity::new(integrity.hash(), integrity.length() + 1);
        assert!(
            store
                .put_verified_if_absent(&key, body.clone(), &wrong_length)
                .is_err()
        );
        let other = VerifiedObjectBody::from_vec(b"modified".to_vec());
        assert!(
            store
                .put_verified_if_absent(&key, other, &integrity)
                .is_err()
        );
        assert!(!root.path().join(key.as_str()).exists());
        assert_eq!(
            store
                .put_verified_if_absent(&key, body.clone(), &integrity)
                .unwrap(),
            PutOutcome::Inserted
        );
        assert_eq!(
            store
                .put_verified_if_absent(&key, body.clone(), &integrity)
                .unwrap(),
            PutOutcome::AlreadyExists
        );
        std::fs::write(root.path().join(key.as_str()), b"modified").unwrap();
        assert!(
            store
                .put_verified_if_absent(&key, body, &integrity)
                .is_err()
        );
        assert_eq!(
            std::fs::read(root.path().join(key.as_str())).unwrap(),
            b"modified"
        );
    }
}
