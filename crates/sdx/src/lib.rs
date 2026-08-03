#![deny(unsafe_code)]
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

//! `sdx` is a native Xet client library for Shardline's Xet frontend.
//!
//! M0 provides the hash and hexadecimal primitives the Xet wire protocol
//! requires: BLAKE3 keyed hashing for chunk data (and term verification) via
//! the pinned upstream `xet-core-structures` crate, plus strict Xet CAS API
//! hexadecimal conversion with 8-byte group reversal.
//!
//! M1 adds the authentication/token service: [`auth::TokenService`] issues and
//! caches repo+revision-scoped read/write CAS tokens against the shardline
//! token-issuance routes (single-flight refresh with a 30-second buffer), with
//! credential resolution from explicit sources, token files, and the
//! `SHARDLINE_TOKEN` / `SHARDLINE_API_KEY` / `SHARDLINE_TOKEN_FILE`
//! environment variables ([`config`]).
//!
//! Later milestones add the client, chunking, xorb, shard, and streaming
//! modules from the module map in `docs/SDX_PLAN.md` §4.2.

pub mod auth;
pub mod config;
pub mod error;
pub mod hash;

pub use auth::{
    Auth, AuthError, HttpConfig, PROVIDER_KEY_HEADER_NAME, REFRESH_BUFFER_SECONDS, RepositoryId,
    ScopedToken, TokenService,
};
pub use config::Credential;
pub use error::XetHashParseError;
pub use hash::{
    compute_chunk_hash, compute_term_verification_hash, parse_xet_hash_hex, xet_hash_hex_string,
};
pub use xet_core_structures::merklehash::MerkleHash;
