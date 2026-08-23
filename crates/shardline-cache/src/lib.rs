#![deny(unsafe_code)]

//! Reconstruction-cache contracts and adapters for Shardline.
//!
//! Reconstruction responses can be expensive to compute from large metadata
//! indexes. This crate defines a small async cache boundary keyed by file,
//! content hash, and optional repository scope. The in-memory adapter is useful
//! for single-process deployments; the Redis adapter lets multiple server
//! replicas share cached reconstruction payloads.
//!
//! # Quick start
//!
//! Build a cache key for a file reconstruction response — either the latest
//! visible revision or one immutable version — with an optional repository
//! scope:
//!
//! ```
//! use shardline_cache::ReconstructionCacheKey;
//! use shardline_protocol::{RepositoryProvider, RepositoryScope};
//!
//! let scope =
//!     RepositoryScope::new(RepositoryProvider::GitHub, "acme", "assets", Some("main"))?;
//! let latest = ReconstructionCacheKey::latest("file-123", Some(&scope));
//! let immutable = ReconstructionCacheKey::version("file-123", "content-456", Some(&scope));
//!
//! assert_eq!(latest.content_hash(), None);
//! assert_eq!(immutable.content_hash(), Some("content-456"));
//! if let Some(scope_key) = latest.repository_scope() {
//!     assert_eq!(scope_key.provider(), "github");
//!     assert_eq!(scope_key.owner(), "acme");
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Use the keys with [`MemoryReconstructionCache`] (single-process) or
//! [`RedisReconstructionCache`] (shared across server replicas).

mod disabled;
mod error;
mod key;
mod memory;
mod redis;
mod store;

pub use disabled::DisabledReconstructionCache;
pub use error::ReconstructionCacheError;
pub use key::{ReconstructionCacheKey, RepositoryScopeCacheKey};
pub use memory::MemoryReconstructionCache;
pub use redis::{RedisReconstructionCache, RedisTlsConfig};
pub use store::{
    AsyncReconstructionCache, ReconstructionCacheFuture, ReconstructionCacheLookup,
    ReconstructionCacheReservation,
};
