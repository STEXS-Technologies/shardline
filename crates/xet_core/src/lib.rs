#![allow(
    clippy::missing_panics_doc,
    clippy::missing_const_for_fn,
    clippy::must_use_candidate
)]
#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        unused_imports,
        unused_variables,
        unused_mut,
        non_snake_case,
        dead_code,
    )
)]

pub mod error;
pub mod merklehash;
pub mod metadata_shard;
pub mod utils;
pub mod xorb_object;

pub use error::CoreError;
pub use merklehash::MerkleHash;

#[cfg(test)]
mod tests;
