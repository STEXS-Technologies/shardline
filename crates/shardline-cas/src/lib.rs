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

mod coordinator;
mod error;
mod limits;
pub mod paths;
mod reachability;

pub use coordinator::CasCoordinator;
pub use error::CasError;
pub use limits::CasLimits;
pub use reachability::ObjectReachability;
