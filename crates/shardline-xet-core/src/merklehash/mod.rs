pub mod aggregated_hashes;
mod aliases;
pub mod data_hash;

pub use aliases::{HMACKey, MerkleHash};
pub use data_hash::*;

pub use aggregated_hashes::{file_hash, file_hash_with_salt, xorb_hash};
