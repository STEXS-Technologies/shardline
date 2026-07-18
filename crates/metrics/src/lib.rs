#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string
    )
)]

pub mod backend;
pub mod fsck;
pub mod gc;
pub mod middleware;
pub mod protocol;
pub mod provider;
pub mod reconstruction;
pub mod recorders;
pub mod storage;
pub mod system;
pub mod transfer;
pub mod xet;

pub use recorders::*;

#[cfg(test)]
mod tests;
