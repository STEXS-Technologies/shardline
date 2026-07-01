use std::time::Duration;

use crate::merklehash::MerkleHash;

pub static MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS: u64 = 1024;

pub static MDB_SHARD_EXPIRATION_BUFFER: Duration = Duration::from_secs(7 * 24 * 3600);

pub static MDB_SHARD_LOCAL_CACHE_EXPIRATION: Duration = Duration::from_secs(3 * 7 * 24 * 3600);

pub fn hash_is_global_dedup_eligible(h: &MerkleHash) -> bool {
    (*h) % MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS == 0
}
