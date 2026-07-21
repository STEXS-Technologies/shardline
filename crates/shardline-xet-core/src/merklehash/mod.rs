pub mod aggregated_hashes;
pub mod data_hash;

pub use data_hash::*;
pub type MerkleHash = DataHash;
pub type HMACKey = DataHash;

pub use aggregated_hashes::{file_hash, file_hash_with_salt, xorb_hash};
