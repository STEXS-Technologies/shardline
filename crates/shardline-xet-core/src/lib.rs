//! Core Xet primitives for Shardline: hashing, compression, and xorb formats.
//!
//! This crate re-implements the `xet-core-structures` surface area without
//! pulling in `xet-runtime` dependencies, so it can be used from pure
//! validation and storage code.
//!
//! # Quick start
//!
//! [`CompressionScheme`](xorb_object::CompressionScheme) is the user-facing
//! entry point for the xorb chunk compression layer:
//!
//! ```
//! use shardline_xet_core::xorb_object::CompressionScheme;
//!
//! // Scheme names parse case-insensitively.
//! let scheme: CompressionScheme = "bg4-lz4".parse()?;
//! assert_eq!(scheme, CompressionScheme::ByteGrouping4LZ4);
//!
//! // `None` and `Auto` produce the same wire name as every other scheme.
//! assert_eq!(CompressionScheme::None.to_string(), "none");
//! assert_eq!(CompressionScheme::Auto.to_string(), "auto");
//!
//! // Compress then decompress round-trips the payload byte-for-byte.
//! let data = b"hello from shardline xet core";
//! let compressed = scheme.compress_from_slice(data)?;
//! let restored = scheme.decompress_from_slice(&compressed)?;
//! assert_eq!(&*restored, data);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! The [`MerkleHash`] (a 256-bit value) is used to address data; it parses
//! from and renders to 64-hex strings:
//!
//! ```
//! use shardline_xet_core::MerkleHash;
//!
//! let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
//! let hash = MerkleHash::from_hex(hex)?;
//! assert_eq!(hash.hex(), hex);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![deny(unsafe_code)]

pub mod error;
pub mod merklehash;
pub mod metadata_shard;
pub mod utils;
pub mod xorb_object;

pub use error::CoreError;
pub use merklehash::MerkleHash;

#[cfg(test)]
mod tests;
