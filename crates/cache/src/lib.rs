#![deny(unsafe_code)]

//! Reconstruction-cache contracts and adapters for Shardline.
//!
//! Reconstruction responses can be expensive to compute from large metadata
//! indexes. This crate defines a small async cache boundary keyed by file,
//! content hash, and optional repository scope. The in-memory adapter is useful
//! for single-process deployments; the Redis adapter lets multiple server
//! replicas share cached reconstruction payloads.
//!
//! # Example
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
//! assert_eq!(latest.repository_scope().unwrap().provider(), "github");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

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
pub use redis::RedisReconstructionCache;
pub use store::{AsyncReconstructionCache, ReconstructionCacheFuture};
