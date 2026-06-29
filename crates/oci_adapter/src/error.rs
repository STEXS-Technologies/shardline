use std::{io::Error as IoError, num::TryFromIntError};

use shardline_server_core::ServerObjectStoreError;
use shardline_storage::{LocalObjectStoreError, ObjectPrefixError, S3ObjectStoreError};
use thiserror::Error;
use tokio::task::JoinError;

/// OCI adapter failure.
#[derive(Debug, Error)]
pub enum OciAdapterError {
    /// Local storage IO failed.
    #[error("local storage operation failed")]
    Io(#[from] IoError),
    /// JSON serialization or deserialization failed.
    #[error("json operation failed")]
    Json(#[from] serde_json::Error),
    /// Numeric conversion exceeded supported bounds.
    #[error("numeric conversion exceeded supported bounds")]
    NumericConversion(#[from] TryFromIntError),
    /// Object-storage adapter access failed.
    #[error("object storage adapter operation failed")]
    ObjectStore(#[from] ServerObjectStoreError),
    /// S3-compatible object-storage adapter access failed.
    #[error("s3 object storage adapter operation failed")]
    S3ObjectStore(#[from] S3ObjectStoreError),
    /// Local object-storage adapter access failed.
    #[error("local object storage adapter operation failed")]
    LocalObjectStore(#[from] LocalObjectStoreError),
    /// Object inventory prefix validation failed.
    #[error("object storage prefix validation failed")]
    ObjectPrefix(#[from] ObjectPrefixError),
    /// Requested content was not found.
    #[error("content not found")]
    NotFound,
    /// Arithmetic overflowed a checked bound.
    #[error("arithmetic overflow")]
    Overflow,
    /// A content hash was malformed.
    #[error("content hash must be 64 hexadecimal characters")]
    InvalidContentHash,
    /// A digest string was malformed.
    #[error("digest must use sha256:<64 lowercase hex> format")]
    InvalidDigest,
    /// A repository name or namespace path was malformed.
    #[error("repository name was invalid")]
    InvalidRepositoryName,
    /// A manifest reference or tag was malformed.
    #[error("manifest reference was invalid")]
    InvalidManifestReference,
    /// An upload session identifier was malformed.
    #[error("upload session identifier was invalid")]
    InvalidUploadSession,
    /// Too many OCI upload sessions are currently active.
    #[error("too many active oci upload sessions")]
    TooManyUploadSessions,
    /// The uploaded body did not match the expected SHA-256 identifier.
    #[error("uploaded body hash did not match the expected sha256")]
    ExpectedBodyHashMismatch,
    /// A blocking worker task failed before it could finish storage work.
    #[error("blocking worker task failed")]
    BlockingTask(#[source] JoinError),
}
