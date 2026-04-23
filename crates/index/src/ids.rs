use shardline_protocol::ShardlineHash;

/// Content-addressed file identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileId(ShardlineHash);

impl FileId {
    /// Creates a file identifier from a protocol hash.
    #[must_use]
    pub const fn new(hash: ShardlineHash) -> Self {
        Self(hash)
    }

    /// Returns the underlying hash.
    #[must_use]
    pub const fn hash(&self) -> ShardlineHash {
        self.0
    }
}

/// Content-addressed stored-object identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StoredObjectId(ShardlineHash);

impl StoredObjectId {
    /// Creates a stored-object identifier from a protocol hash.
    #[must_use]
    pub const fn new(hash: ShardlineHash) -> Self {
        Self(hash)
    }

    /// Returns the underlying hash.
    #[must_use]
    pub const fn hash(&self) -> ShardlineHash {
        self.0
    }
}

/// Backward-compatible Xet alias for [`StoredObjectId`].
pub type XorbId = StoredObjectId;

#[cfg(test)]
mod tests {
    use shardline_protocol::ShardlineHash;

    use super::{FileId, StoredObjectId, XorbId};

    #[test]
    fn file_id_preserves_hash() {
        let hash = ShardlineHash::from_bytes([3; 32]);
        let file_id = FileId::new(hash);

        assert_eq!(file_id.hash(), hash);
    }

    #[test]
    fn xorb_id_preserves_hash() {
        let hash = ShardlineHash::from_bytes([4; 32]);
        let xorb_id = XorbId::new(hash);

        assert_eq!(xorb_id.hash(), hash);
    }

    #[test]
    fn stored_object_id_preserves_hash() {
        let hash = ShardlineHash::from_bytes([8; 32]);
        let object_id = StoredObjectId::new(hash);

        assert_eq!(object_id.hash(), hash);
    }
}
