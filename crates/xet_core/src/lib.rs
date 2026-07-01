#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string
    )
)]

pub mod error;
pub mod merklehash;
pub mod metadata_shard;
pub mod utils;
pub mod xorb_object;

pub use error::CoreError;
pub use merklehash::MerkleHash;
