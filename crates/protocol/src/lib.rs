#![deny(unsafe_code)]

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
//! # Example
//!
//! ```
//! use shardline_protocol::{
//!     RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, TokenSigner,
//! };
//!
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
//! let signer = TokenSigner::new(b"development-only-signing-key")?;
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
    RepositoryProvider, RepositoryScope, TokenClaims, TokenClaimsError, TokenCodecError,
    TokenScope, TokenSigner,
};
