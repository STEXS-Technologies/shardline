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

//! CAS coordinator composition for Shardline.
//!
//! The coordinator ties together a metadata index, an object store, a record store,
//! and explicit bounds for untrusted serialized protocol objects. It owns the
//! protocol-neutral CAS state machine: content-addressed blob storage,
//! file-reconstruction commit, and reachability queries.
//!
//! All frontends (Xet, LFS, OCI, Hub, Bazel) must pass through this coordinator
//! for authorization, admission, ordering, and visibility.
//!
//! # Quick start
//!
//! [`CasLimits`] bounds how large untrusted protocol objects may be, and
//! [`ObjectReachability`] is the single authority lifecycle tools (GC, fsck,
//! repair, deletion) use to decide whether an object may be removed:
//!
//! ```
//! use shardline_cas::CasLimits;
//! use std::num::NonZeroU64;
//!
//! // Bound untrusted xorb, shard, and blob sizes (bytes).
//! let limits = CasLimits::new(
//!     NonZeroU64::new(64 * 1024 * 1024).expect("64 MiB is non-zero"),
//!     NonZeroU64::new(16 * 1024 * 1024).expect("16 MiB is non-zero"),
//!     NonZeroU64::new(8 * 1024 * 1024).expect("8 MiB is non-zero"),
//! );
//! assert_eq!(
//!     limits.max_xorb_bytes().get(),
//!     64 * 1024 * 1024
//! );
//! assert_eq!(limits.max_shard_bytes().get(), 16 * 1024 * 1024);
//! assert_eq!(limits.max_object_bytes().get(), 8 * 1024 * 1024);
//! ```
//!
//! [`ObjectReachability`] is implemented for every async index store, so any
//! `T: AsyncIndexStore` can answer reachability queries:
//!
//! ```
//! use shardline_cas::ObjectReachability;
//! use shardline_index::{MemoryIndexStore, StoredObjectId};
//! use shardline_protocol::ShardlineHash;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let store = MemoryIndexStore::new();
//!     let object_id = StoredObjectId::new(ShardlineHash::from_bytes([7; 32]));
//!
//!     assert!(!ObjectReachability::is_object_reachable(&store, &object_id).await?);
//!
//!     store.insert_object(&object_id)?;
//!     assert!(ObjectReachability::is_object_reachable(&store, &object_id).await?);
//!     Ok(())
//! }
//! ```

mod coordinator;
mod error;
mod limits;
pub mod paths;
mod reachability;

pub use coordinator::CasCoordinator;
pub use error::CasError;
pub use limits::CasLimits;
pub use reachability::ObjectReachability;
