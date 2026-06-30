#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string
    )
)]

//! CAS coordinator composition for Shardline.
//!
//! The coordinator ties together a metadata index, an object store, and explicit
//! bounds for untrusted serialized protocol objects. It is deliberately generic
//! over the backing adapters so tests, local deployments, and production
//! deployments can share the same composition point.
//!
//! # Example
//!
//! ```
//! use std::num::NonZeroU64;
//!
//! use shardline_cas::{CasCoordinator, CasLimits};
//!
//! #[derive(Debug)]
//! struct IndexAdapter;
//!
//! #[derive(Debug)]
//! struct ObjectAdapter;
//!
//! let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MIN);
//! let coordinator = CasCoordinator::new(IndexAdapter, ObjectAdapter, limits);
//!
//! assert_eq!(coordinator.limits(), limits);
//! ```

mod coordinator;
mod limits;

pub use coordinator::CasCoordinator;
pub use limits::CasLimits;
