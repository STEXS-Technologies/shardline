use shardline_protocol::ShardlineHash;
use shardline_storage::ObjectKey;

/// Durable mapping from a chunk hash to one retained shard object that contains it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupeShardMapping {
    chunk_hash: ShardlineHash,
    shard_object_key: ObjectKey,
}

impl DedupeShardMapping {
    /// Creates one dedupe-shard mapping.
    #[must_use]
    pub const fn new(chunk_hash: ShardlineHash, shard_object_key: ObjectKey) -> Self {
        Self {
            chunk_hash,
            shard_object_key,
        }
    }

    /// Returns the mapped chunk hash.
    #[must_use]
    pub const fn chunk_hash(&self) -> ShardlineHash {
        self.chunk_hash
    }

    /// Returns the retained shard object key.
    #[must_use]
    pub const fn shard_object_key(&self) -> &ObjectKey {
        &self.shard_object_key
    }
}

#[cfg(test)]
mod tests {
    use shardline_protocol::ShardlineHash;
    use shardline_storage::ObjectKey;

    use super::DedupeShardMapping;

    #[test]
    fn dedupe_shard_mapping_preserves_chunk_hash_and_object_key() {
        let chunk_hash = ShardlineHash::from_bytes([5; 32]);
        let shard_object_key = ObjectKey::parse("shards/aa/example.shard");

        assert!(shard_object_key.is_ok());
        let Ok(shard_object_key) = shard_object_key else {
            return;
        };
        let mapping = DedupeShardMapping::new(chunk_hash, shard_object_key.clone());

        assert_eq!(mapping.chunk_hash(), chunk_hash);
        assert_eq!(mapping.shard_object_key(), &shard_object_key);
    }
}
