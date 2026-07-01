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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_display_message() {
        assert_eq!(OciAdapterError::NotFound.to_string(), "content not found");
    }

    #[test]
    fn overflow_display_message() {
        assert_eq!(OciAdapterError::Overflow.to_string(), "arithmetic overflow");
    }

    #[test]
    fn invalid_content_hash_display_message() {
        assert_eq!(
            OciAdapterError::InvalidContentHash.to_string(),
            "content hash must be 64 hexadecimal characters"
        );
    }

    #[test]
    fn invalid_digest_display_message() {
        assert_eq!(
            OciAdapterError::InvalidDigest.to_string(),
            "digest must use sha256:<64 lowercase hex> format"
        );
    }

    #[test]
    fn invalid_repository_name_display_message() {
        assert_eq!(
            OciAdapterError::InvalidRepositoryName.to_string(),
            "repository name was invalid"
        );
    }

    #[test]
    fn invalid_manifest_reference_display_message() {
        assert_eq!(
            OciAdapterError::InvalidManifestReference.to_string(),
            "manifest reference was invalid"
        );
    }

    #[test]
    fn invalid_upload_session_display_message() {
        assert_eq!(
            OciAdapterError::InvalidUploadSession.to_string(),
            "upload session identifier was invalid"
        );
    }

    #[test]
    fn too_many_upload_sessions_display_message() {
        assert_eq!(
            OciAdapterError::TooManyUploadSessions.to_string(),
            "too many active oci upload sessions"
        );
    }

    #[test]
    fn expected_body_hash_mismatch_display_message() {
        assert_eq!(
            OciAdapterError::ExpectedBodyHashMismatch.to_string(),
            "uploaded body hash did not match the expected sha256"
        );
    }

    #[test]
    fn io_error_display_message() {
        let io_err = std::io::Error::other("test");
        assert_eq!(
            OciAdapterError::Io(io_err).to_string(),
            "local storage operation failed"
        );
    }

    #[test]
    fn json_error_display_message() {
        let json_err = serde_json::from_str::<serde_json::Value>("bad").unwrap_err();
        assert_eq!(
            OciAdapterError::Json(json_err).to_string(),
            "json operation failed"
        );
    }

    #[test]
    fn numeric_conversion_error_display_message() {
        let err: TryFromIntError = u8::try_from(-1i32).unwrap_err();
        assert_eq!(
            OciAdapterError::NumericConversion(err).to_string(),
            "numeric conversion exceeded supported bounds"
        );
    }

    #[test]
    fn error_variant_debug_format() {
        // Ensure all variants implement Debug (they derive it)
        let variants = [
            format!("{:?}", OciAdapterError::NotFound),
            format!("{:?}", OciAdapterError::Overflow),
            format!("{:?}", OciAdapterError::InvalidContentHash),
            format!("{:?}", OciAdapterError::InvalidDigest),
            format!("{:?}", OciAdapterError::InvalidRepositoryName),
            format!("{:?}", OciAdapterError::InvalidManifestReference),
            format!("{:?}", OciAdapterError::InvalidUploadSession),
            format!("{:?}", OciAdapterError::TooManyUploadSessions),
            format!("{:?}", OciAdapterError::ExpectedBodyHashMismatch),
        ];
        for v in &variants {
            assert!(!v.is_empty());
        }
    }
}
