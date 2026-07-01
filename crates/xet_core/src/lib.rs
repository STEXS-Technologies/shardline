#![deny(unsafe_code)]
#![allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::missing_panics_doc,
    clippy::missing_const_for_fn,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::str_to_string,
    clippy::shadow_unrelated,
    clippy::trivially_copy_pass_by_ref,
    clippy::needless_pass_by_value,
    clippy::option_if_let_else,
    clippy::single_char_lifetime_names,
    clippy::field_reassign_with_default,
    clippy::type_complexity,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]
#![cfg_attr(
    test,
    allow(
        clippy::non_ascii_literal,
        clippy::let_underscore_must_use,
        clippy::format_push_string,
        clippy::redundant_clone,
        clippy::io_other_error,
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
