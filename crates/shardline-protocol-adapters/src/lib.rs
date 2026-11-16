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

//! Shared Xet wire contracts and protocol adapters for Git LFS, Bazel HTTP cache, and related
//! object-key mapping.
//!
//! Xet wire contracts are available without storage dependencies. The default
//! `storage` feature additionally maps protocol identifiers to validated
//! `shardline_storage::ObjectKey` values using server authorization types.
//!
//! # Quick start
//!
//! Everything here is pure. The most common task is mapping an LFS object ID
//! to a storage key:
//!
//! ```
//! # #[cfg(feature = "storage")]
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use shardline_protocol_adapters::{LfsOperation, TransferAdapter, lfs_object_key};
//! use shardline_server_core::AuthorizedRepository;
//!
//! let oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
//! let key = lfs_object_key(oid, &AuthorizedRepository::anonymous_full_access())?;
//! assert!(key.as_str().starts_with("protocols/lfs/global/objects/"));
//! assert!(key.as_str().ends_with(oid));
//!
//! // Malformed OIDs are rejected instead of being stored under a bad key.
//! assert!(
//!     lfs_object_key("not-a-valid-sha256", &AuthorizedRepository::anonymous_full_access())
//!         .is_err()
//! );
//!
//! // Batch operations and transfer adapters round-trip through their wire names.
//! assert_eq!("download".parse(), Ok(LfsOperation::Download));
//! assert_eq!(TransferAdapter::Xet.as_str(), "xet");
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "storage"))]
//! # fn main() {}
//! ```
//!
//! See also `bazel_cache_object_key` for the Bazel HTTP cache mapping and
//! `scope_namespace` for repository-scoped key prefixes.

/// Storage-independent Xet wire contracts.
pub mod xet;
pub use xet::{
    BatchReconstructionResponse, FileReconstructionResponse, FileReconstructionV2Response,
    ReconstructionChunkRange, ReconstructionFetchInfo, ReconstructionMultiRangeFetch,
    ReconstructionRangeDescriptor, ReconstructionTerm, ReconstructionUrlRange, ShardUploadResponse,
    XET_METADATA_ACCEPT, XET_PATH_ROUTE, XET_READ_TOKEN_ROUTE, XET_REVISION_ROUTE,
    XET_REVISIONS_ROUTE, XET_TREE_ROUTE, XET_WRITE_TOKEN_ROUTE, XorbUploadResponse,
    reconstruction_v2_from_v1,
};

#[cfg(feature = "storage")]
mod bazel;
#[cfg(feature = "storage")]
mod lfs;

#[cfg(feature = "storage")]
mod mapping;

#[cfg(feature = "storage")]
pub use bazel::{BazelCacheKind, bazel_cache_object_key};
#[cfg(feature = "storage")]
pub use lfs::{
    LFS_CONTENT_TYPE, LfsBatchRequest, LfsBatchResponse, LfsObjectError, LfsObjectRequest,
    LfsObjectResponse, LfsOperation, LfsValidationError, TransferAdapter, cas_headers,
    lfs_object_key,
};
#[cfg(feature = "storage")]
pub use mapping::{ProtocolError, object_key, scope_namespace, validate_content_hash};

#[cfg(all(test, feature = "storage"))]
mod tests;
