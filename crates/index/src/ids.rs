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

/// Content-addressed xorb identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct XorbId(ShardlineHash);

impl XorbId {
    /// Creates a xorb identifier from a protocol hash.
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

#[cfg(test)]
mod tests {
    use shardline_protocol::ShardlineHash;

    use super::{FileId, XorbId};

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
}
