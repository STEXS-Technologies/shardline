pub mod constants;
mod dedup;
pub mod file_structs;
pub mod shard_file;
pub mod shard_format;
pub mod shard_in_memory;
pub mod xorb_structs;

pub use constants::{
    MDB_SHARD_EXPIRATION_BUFFER, MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS,
    MDB_SHARD_LOCAL_CACHE_EXPIRATION, hash_is_global_dedup_eligible,
};
pub use dedup::{serialize_keyed_dedup_shard, serialize_scoped_keyed_dedup_shard};
pub use file_structs::Sha256;
pub use shard_format::{MDBShardFileFooter, MDBShardFileHeader, MDBShardInfo};
