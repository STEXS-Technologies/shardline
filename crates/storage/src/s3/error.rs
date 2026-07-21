use std::io::Error as IoError;

use object_store::Error as ExternalObjectStoreError;
use thiserror::Error;

use crate::ObjectPrefixError;

/// S3 object-store adapter failure.
#[derive(Debug, Error)]
pub enum S3ObjectStoreError {
    /// Required S3 credentials were only partially provided.
    #[error("s3 object store credentials must include both access key id and secret access key")]
    IncompleteCredentials,
    /// The S3 bucket name was empty.
    #[error("s3 object store bucket must not be empty")]
    EmptyBucket,
    /// The S3 region was empty.
    #[error("s3 object store region must not be empty")]
    EmptyRegion,
    /// The configured key prefix could not be represented as a safe storage prefix.
    #[error("s3 object store key prefix was invalid")]
    InvalidKeyPrefix(#[source] ObjectPrefixError),
    /// The supplied body length did not match the expected integrity metadata.
    #[error("object body length did not match expected integrity")]
    IntegrityLengthMismatch,
    /// The supplied body hash did not match the expected integrity metadata.
    #[error("object body hash did not match expected integrity")]
    IntegrityHashMismatch,
    /// An existing object for the same key had different bytes.
    #[error("object key already exists with conflicting bytes")]
    ExistingObjectConflict,
    /// The requested byte range exceeded the stored object length.
    #[error("requested byte range exceeded stored object length")]
    RangeOutOfBounds,
    /// An object listed from S3 could not be represented as a validated object key.
    #[error("s3 listed an object outside the configured key prefix")]
    InvalidListedKey,
    /// An upload parts list had missing, duplicate, or out-of-order part numbers.
    #[error("upload parts list has invalid part numbering")]
    InvalidUploadParts,
    /// Local temporary-file access failed.
    #[error("temporary file operation failed")]
    Io(#[from] IoError),
    /// Object-store path conversion failed.
    #[error("object-store path conversion failed")]
    Path(#[source] object_store::path::Error),
    /// Runtime initialization failed.
    #[error("s3 object store runtime initialization failed")]
    Runtime(#[source] IoError),
    /// No Tokio runtime was available for a synchronous S3 operation.
    #[error("s3 object store runtime is unavailable")]
    RuntimeUnavailable,
    /// S3-compatible object store operation failed.
    #[error("s3 object store operation failed")]
    External(#[from] ExternalObjectStoreError),
}
