use std::io::{self, Read};

use crate::utils::serialization_utils::{read_u32, read_u64};
use shard_format::MDB_FILE_INFO_ENTRY_SIZE;

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

const MDB_SHARD_FORMAT_V3: u64 = 3;
const MDB_V2_INFO_ENTRY_SIZE: usize = 48;

fn read_info_entry<R: Read>(
    reader: &mut R,
    version: u64,
) -> std::io::Result<[u8; MDB_FILE_INFO_ENTRY_SIZE]> {
    // Legacy records omit the final 16 bytes; decode those fields as zero.
    let mut bytes = [0; MDB_FILE_INFO_ENTRY_SIZE];
    let length = if version < MDB_SHARD_FORMAT_V3 {
        MDB_V2_INFO_ENTRY_SIZE
    } else {
        bytes.len()
    };
    reader.read_exact(&mut bytes[..length])?;
    Ok(bytes)
}

fn read_info_integer<R: Read>(reader: &mut R, version: u64) -> io::Result<u64> {
    if version < MDB_SHARD_FORMAT_V3 {
        read_u32(reader).map(u64::from)
    } else {
        read_u64(reader)
    }
}
