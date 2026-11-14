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
    /// Canonical resumable-session evidence could not be validated.
    #[error("oci upload session reliability evidence failed")]
    Reliability(String),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_messages() {
        for (error, expected) in [
            (OciAdapterError::NotFound, "content not found"),
            (OciAdapterError::Overflow, "arithmetic overflow"),
            (
                OciAdapterError::InvalidContentHash,
                "content hash must be 64 hexadecimal characters",
            ),
            (
                OciAdapterError::InvalidDigest,
                "digest must use sha256:<64 lowercase hex> format",
            ),
            (
                OciAdapterError::InvalidRepositoryName,
                "repository name was invalid",
            ),
            (
                OciAdapterError::InvalidManifestReference,
                "manifest reference was invalid",
            ),
            (
                OciAdapterError::InvalidUploadSession,
                "upload session identifier was invalid",
            ),
            (
                OciAdapterError::TooManyUploadSessions,
                "too many active oci upload sessions",
            ),
            (
                OciAdapterError::ExpectedBodyHashMismatch,
                "uploaded body hash did not match the expected sha256",
            ),
            (
                OciAdapterError::ObjectStore(
                    shardline_server_core::ServerObjectStoreError::NotFound,
                ),
                "object storage adapter operation failed",
            ),
            (
                OciAdapterError::S3ObjectStore(shardline_storage::S3ObjectStoreError::Io(
                    std::io::Error::other("test"),
                )),
                "s3 object storage adapter operation failed",
            ),
            (
                OciAdapterError::LocalObjectStore(shardline_storage::LocalObjectStoreError::Io(
                    std::io::Error::other("test"),
                )),
                "local object storage adapter operation failed",
            ),
            (
                OciAdapterError::ObjectPrefix(shardline_storage::ObjectPrefixError::UnsafePath),
                "object storage prefix validation failed",
            ),
        ] {
            assert_eq!(error.to_string(), expected);
            assert!(!format!("{error:?}").is_empty());
        }
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn blocking_task_error_display_message() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            #[allow(clippy::panic)]
            let handle = tokio::spawn(async { panic!("intentional panic for test") });
            let result = handle.await;
            let join_err = result.unwrap_err();
            let err = OciAdapterError::BlockingTask(join_err);
            assert_eq!(err.to_string(), "blocking worker task failed");
        });
    }

    #[allow(clippy::unwrap_used, clippy::shadow_unrelated)]
    #[test]
    fn all_from_trait_conversions() {
        // Verify From<IoError>
        let io_err = std::io::Error::other("test");
        let err: OciAdapterError = io_err.into();
        assert_eq!(err.to_string(), "local storage operation failed");
        assert!(matches!(err, OciAdapterError::Io(_)));

        // Verify From<serde_json::Error>
        let json_err = serde_json::from_str::<serde_json::Value>("bad").unwrap_err();
        let err: OciAdapterError = json_err.into();
        assert_eq!(err.to_string(), "json operation failed");
        assert!(matches!(err, OciAdapterError::Json(_)));

        // Verify From<TryFromIntError>
        let int_err = u8::try_from(-1i32).unwrap_err();
        let err: OciAdapterError = int_err.into();
        assert_eq!(
            err.to_string(),
            "numeric conversion exceeded supported bounds"
        );
        assert!(matches!(err, OciAdapterError::NumericConversion(_)));

        // Verify From<ObjectPrefixError>
        let prefix_err = shardline_storage::ObjectPrefixError::UnsafePath;
        let err: OciAdapterError = prefix_err.into();
        assert!(matches!(err, OciAdapterError::ObjectPrefix(_)));
    }
}
