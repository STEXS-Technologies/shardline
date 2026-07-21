use shardline_storage::{LocalObjectStoreError, ObjectPrefixError, S3ObjectStoreError};
use thiserror::Error;

/// Object-storage subsystem failure.
#[derive(Debug, Error)]
pub enum ObjectStoreError {
    /// Local storage IO failed.
    #[error("local storage operation failed")]
    Local(#[from] LocalObjectStoreError),
    /// S3-compatible object-storage adapter access failed.
    #[error("s3 object storage adapter operation failed")]
    S3(#[from] S3ObjectStoreError),
    /// Object inventory prefix validation failed.
    #[error("object storage prefix validation failed")]
    Prefix(#[from] ObjectPrefixError),
    /// S3-compatible object storage was selected without concrete configuration.
    #[error("s3 object storage configuration is missing")]
    MissingS3Config,
    /// Stored object metadata disagreed with the expected transfer length.
    #[error("stored object length did not match indexed metadata")]
    StoredLengthMismatch,
    /// Storage migration found a content-addressed source object under the wrong key.
    #[error(
        "storage migration source object hash mismatch for key {key}: expected {expected_hash}, observed {observed_hash}"
    )]
    MigrationSourceHashMismatch {
        /// Content-addressed object key being migrated.
        key: String,
        /// Hash implied by the object key.
        expected_hash: String,
        /// Hash computed from the source object bytes.
        observed_hash: String,
    },
}
