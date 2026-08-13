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

//! Shared protocol-facing types used by Shardline clients, storage adapters, and the
//! HTTP server.
//!
//! This crate keeps the wire-level contracts small and explicit:
//!
//! - [`ShardlineHash`] stores validated 32-byte hashes and canonical lowercase
//!   hexadecimal text.
//! - [`ByteRange`] and [`ChunkRange`] validate range boundaries before they reach
//!   storage code.
//! - [`TokenSigner`] signs and verifies scoped bearer tokens without exposing
//!   secret material in debug output.
//! - [`RepositoryScope`] ties provider-issued tokens to one repository and,
//!   optionally, one revision.
//!
//! # Quick start
//!
//! Parse a content hash and sign a scoped read token, then verify it. This
//! example is fully offline — no network or storage is involved:
//!
//! ```
//! use shardline_protocol::{
//!     RepositoryProvider, RepositoryScope, ShardlineHash, TokenClaims, TokenScope, TokenSigner,
//! };
//!
//! // A validated 32-byte hash with canonical lowercase hexadecimal text.
//! let hash = ShardlineHash::parse_hex(
//!     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
//! )?;
//! assert_eq!(hash.hex_string().len(), 64);
//!
//! // Scope a bearer token to one repository (optionally one revision).
//! let repository =
//!     RepositoryScope::new(RepositoryProvider::GitHub, "acme", "assets", Some("main"))?;
//! let claims = TokenClaims::new(
//!     "shardline",
//!     "alice",
//!     TokenScope::Read,
//!     repository,
//!     1_700_000_600,
//! )?;
//!
//! // Sign, then verify against a fixed timestamp.
//! let signer = TokenSigner::new(b"development-only-signing-key-32bytes")?;
//! let token = signer.sign(&claims)?;
//! let verified = signer.verify_at(&token, 1_700_000_000)?;
//!
//! assert_eq!(verified.subject(), "alice");
//! assert_eq!(verified.repository().owner(), "acme");
//! assert!(verified.scope().allows_read());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod hash;
mod ranges;
mod security;
mod text;
mod time;
mod token;
pub use hash::{HashParseError, ShardlineHash};
pub use ranges::{ByteRange, ChunkRange, HttpRangeParseError, RangeError, parse_http_byte_range};
pub use security::{SecretBytes, SecretString};
pub use text::parse_bool;
pub use time::unix_now_seconds_lossy;
pub use token::{
    MAX_TOKEN_STRING_BYTES, RepositoryProvider, RepositoryProviderParseError, RepositoryScope,
    TokenClaims, TokenClaimsError, TokenCodecError, TokenScope, TokenSigner,
    decode_and_validate_claims, encode_token_claims, format_signed_token, split_token,
};
