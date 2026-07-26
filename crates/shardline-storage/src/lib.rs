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

//! Content-addressed object storage contracts and adapters for Shardline.
//!
//! The storage layer is intentionally defensive: callers pass validated
//! [`ObjectKey`] values instead of raw paths, object writes carry expected
//! [`ObjectIntegrity`], and local filesystem writes use anchored directory
//! operations on Unix plus conservative path validation on other targets to reduce
//! symlink-race exposure.
//!
//! The central trait is [`ObjectStore`]. It is implemented by [`LocalObjectStore`]
//! for local deployments and [`S3ObjectStore`] for S3-compatible object storage.
//!
//! # Example
//!
//! ```
//! use shardline_protocol::ShardlineHash;
//! use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix};
//!
//! let key = ObjectKey::parse("xorbs/default/aa/bb/example.xorb")?;
//! let prefix = ObjectPrefix::parse("xorbs/default/")?;
//! let hash = ShardlineHash::from_bytes([7; 32]);
//! let body = ObjectBody::from_slice(b"serialized xorb bytes");
//! let integrity = ObjectIntegrity::new(hash, body.as_slice().len() as u64);
//!
//! assert_eq!(key.as_str(), "xorbs/default/aa/bb/example.xorb");
//! assert_eq!(prefix.as_str(), "xorbs/default/");
//! assert_eq!(integrity.hash(), hash);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

/// Symlink-resistant filesystem helpers used by local storage adapters.
#[cfg(unix)]
#[cfg_attr(docsrs, doc(cfg(unix)))]
pub(crate) mod anchored_fs;
mod async_store;
mod key;
mod local;
mod local_fs;
pub(crate) mod local_path;
mod object;
#[cfg(feature = "s3")]
mod s3;
mod store;

#[cfg(unix)]
pub use anchored_fs::{
    AnchoredPathOptions, AnchoredTarget, ensure_parent_path_matches_anchor, fd_child_path,
    open_anchored_target, open_directory_chain, open_new_file, remove_at, remove_if_present,
    rename_at, temporary_file_name, write_anchored_temporary_file,
};
pub use async_store::{AsyncObjectStore, SyncObjectStoreBridge};
pub use key::{ObjectKey, ObjectKeyError, ObjectPrefix, ObjectPrefixError};
pub use local::{LocalObjectStore, LocalObjectStoreError};
pub use local_path::{
    DirectoryPathError, ensure_directory_path_components_are_not_symlinked,
    resolve_platform_symlinks,
};
pub use object::{DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectMetadata, PutOutcome};
#[cfg(feature = "s3")]
pub use s3::{
    BeginMultipartUploadResult, S3ByteStream, S3MultipartUploadWriter, S3ObjectStore,
    S3ObjectStoreConfig, S3ObjectStoreError,
};
pub use store::ObjectStore;
