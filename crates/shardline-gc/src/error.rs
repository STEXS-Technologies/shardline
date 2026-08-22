//! GC error types.

use std::io::Error as IoError;
use std::num::TryFromIntError;

use shardline_index::{
    FileRecordInvariantError, LocalIndexStoreError, MemoryIndexStoreError, MemoryRecordStoreError,
    PostgresMetadataStoreError, QuarantineCandidateError, RetentionHoldError, WebhookDeliveryError,
};
use shardline_server_core::{InvalidLifecycleMetadataError, ServerObjectStoreError};
use shardline_storage::{LocalObjectStoreError, ObjectPrefixError, S3ObjectStoreError};
use shardline_xet_adapter::XetAdapterError;
use thiserror::Error;

/// Garbage collection runtime failure.
#[derive(Debug, Error)]
pub enum GcError {
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
    /// Local object-storage adapter access failed.
    #[error("local object storage operation failed")]
    LocalObjectStore(#[from] LocalObjectStoreError),
    /// S3-compatible object-storage adapter access failed.
    #[error("s3 object storage operation failed")]
    S3ObjectStore(#[from] S3ObjectStoreError),
    /// Object inventory prefix validation failed.
    #[error("object storage prefix validation failed")]
    ObjectPrefix(#[from] ObjectPrefixError),
    /// Index adapter access failed.
    #[error("index adapter operation failed")]
    IndexStore(#[from] LocalIndexStoreError),
    /// In-memory index adapter access failed.
    #[error("memory index adapter operation failed")]
    MemoryIndexStore(#[from] MemoryIndexStoreError),
    /// In-memory record adapter access failed.
    #[error("memory record adapter operation failed")]
    MemoryRecordStore(#[from] MemoryRecordStoreError),
    /// Postgres metadata adapter access failed.
    #[error("postgres metadata adapter operation failed")]
    PostgresMetadata(#[from] PostgresMetadataStoreError),
    /// Retention hold input was invalid.
    #[error("retention hold input was invalid")]
    RetentionHold(#[from] RetentionHoldError),
    /// Quarantine candidate input was invalid.
    #[error("quarantine candidate input was invalid")]
    QuarantineCandidate(#[from] QuarantineCandidateError),
    /// Webhook delivery metadata was invalid.
    #[error("webhook delivery metadata was invalid")]
    WebhookDelivery(#[from] WebhookDeliveryError),
    /// Stored file metadata could not produce a valid reconstruction plan.
    #[error("stored file metadata was invalid")]
    FileRecordInvariant(#[from] FileRecordInvariantError),
    /// Lifecycle metadata was internally inconsistent for a mutating operator workflow.
    #[error("lifecycle metadata was internally inconsistent")]
    InvalidLifecycleMetadata(#[from] InvalidLifecycleMetadataError),
    /// A content hash was malformed.
    #[error("content hash must be 64 hexadecimal characters")]
    InvalidContentHash,
    /// Arithmetic overflowed a checked bound.
    #[error("arithmetic overflow")]
    Overflow,
    /// Xet adapter operation failed.
    #[error("xet adapter operation failed")]
    XetAdapter(#[from] XetAdapterError),
    /// OCI object-key reconstruction failed.
    #[error("OCI adapter operation failed")]
    OciAdapter(#[from] shardline_oci_adapter::OciAdapterError),
}

impl From<shardline_server_core::ParseStoredFileRecordError> for GcError {
    fn from(err: shardline_server_core::ParseStoredFileRecordError) -> Self {
        match err {
            shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            } => Self::Io(IoError::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "stored file metadata exceeded the bounded parser ceiling: {observed_bytes} > {maximum_bytes}"
                ),
            )),
            shardline_server_core::ParseStoredFileRecordError::Json(e) => Self::Json(e),
        }
    }
}

impl From<shardline_server_core::RebuildOverflowError> for GcError {
    fn from(_: shardline_server_core::RebuildOverflowError) -> Self {
        Self::Overflow
    }
}

impl From<GcError> for shardline_server_core::ServerObjectStoreError {
    fn from(err: GcError) -> Self {
        match err {
            GcError::ObjectStore(e) => e,
            GcError::LocalObjectStore(e) => Self::Local(e),
            GcError::S3ObjectStore(e) => Self::S3(e),
            GcError::Io(e) => Self::Io(e),
            GcError::NumericConversion(e) => Self::NumericConversion(e),
            GcError::InvalidContentHash => Self::InvalidContentHash,
            GcError::Overflow => Self::Overflow,
            // All remaining GcError variants that don't directly map to an
            // object-store error are wrapped as I/O errors.  When adding a new
            // GcError variant, add it explicitly above this line.
            GcError::Json(_)
            | GcError::ObjectPrefix(_)
            | GcError::IndexStore(_)
            | GcError::MemoryIndexStore(_)
            | GcError::MemoryRecordStore(_)
            | GcError::PostgresMetadata(_)
            | GcError::RetentionHold(_)
            | GcError::QuarantineCandidate(_)
            | GcError::WebhookDelivery(_)
            | GcError::FileRecordInvariant(_)
            | GcError::InvalidLifecycleMetadata(_)
            | GcError::XetAdapter(_)
            | GcError::OciAdapter(_) => Self::Io(std::io::Error::other(err)),
        }
    }
}
