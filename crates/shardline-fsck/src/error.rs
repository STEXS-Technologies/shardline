use std::{io::Error as IoError, num::TryFromIntError};

use shardline_cas::CasError;
use shardline_index::{
    LocalIndexStoreError, MemoryIndexStoreError, MemoryRecordStoreError, PostgresMetadataStoreError,
};
use shardline_protocol::HashParseError;
use shardline_server_core::{
    ParseStoredFileRecordError, RebuildOverflowError, ServerObjectStoreError,
    ValidateContentHashError, ValidateIdentifierError,
};
use shardline_storage::{LocalObjectStoreError, S3ObjectStoreError};
use shardline_xet_adapter::{XetAdapterError, XorbParseError};
use thiserror::Error;

/// Fsck operation failure.
#[derive(Debug, Error)]
pub enum FsckError {
    /// A local filesystem I/O error occurred.
    #[error("local storage operation failed")]
    Io(#[from] IoError),
    /// JSON serialization or deserialization failed.
    #[error("json operation failed")]
    Json(#[from] serde_json::Error),
    /// Numeric conversion exceeded supported bounds.
    #[error("numeric conversion exceeded supported bounds")]
    NumericConversion(#[from] TryFromIntError),
    /// CAS (Content-Addressable Storage) coordinator or reachability error.
    #[error("cas operation failed")]
    Cas(#[from] CasError),
    /// Arithmetic overflowed a checked bound.
    #[error("arithmetic overflow")]
    Overflow,
    /// Local storage adapter access failed.
    #[error("local storage adapter operation failed")]
    LocalObjectStore(#[from] LocalObjectStoreError),
    /// S3-compatible object-storage adapter access failed.
    #[error("s3 object storage adapter operation failed")]
    S3ObjectStore(#[from] S3ObjectStoreError),
    /// Object-store backend error.
    #[error("object store operation failed")]
    ObjectStore(#[from] ServerObjectStoreError),
    /// Xet adapter access failed.
    #[error("xet adapter operation failed")]
    XetAdapter(#[from] XetAdapterError),
    /// Local index adapter access failed.
    #[error("local index adapter operation failed")]
    LocalIndexStore(#[from] LocalIndexStoreError),
    /// In-memory index adapter access failed.
    #[error("memory index adapter operation failed")]
    MemoryIndexStore(#[from] MemoryIndexStoreError),
    /// In-memory record adapter access failed.
    #[error("memory record adapter operation failed")]
    MemoryRecordStore(#[from] MemoryRecordStoreError),
    /// Postgres metadata adapter access failed.
    #[error("postgres metadata adapter operation failed")]
    PostgresMetadata(#[from] PostgresMetadataStoreError),
    /// Stored file metadata exceeded the bounded parser ceiling.
    #[error("stored file metadata exceeded the bounded parser ceiling")]
    StoredFileMetadataTooLarge {
        /// Observed file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted file length in bytes.
        maximum_bytes: u64,
    },
}

impl From<ParseStoredFileRecordError> for FsckError {
    fn from(value: ParseStoredFileRecordError) -> Self {
        match value {
            ParseStoredFileRecordError::StoredFileMetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            } => Self::StoredFileMetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            },
            ParseStoredFileRecordError::Json(e) => Self::Json(e),
        }
    }
}

impl From<ValidateIdentifierError> for FsckError {
    fn from(_: ValidateIdentifierError) -> Self {
        Self::Overflow
    }
}

impl From<ValidateContentHashError> for FsckError {
    fn from(_: ValidateContentHashError) -> Self {
        Self::Overflow
    }
}

impl From<RebuildOverflowError> for FsckError {
    fn from(_: RebuildOverflowError) -> Self {
        Self::Overflow
    }
}

impl From<XorbParseError> for FsckError {
    fn from(_: XorbParseError) -> Self {
        Self::Overflow
    }
}

impl From<HashParseError> for FsckError {
    fn from(_: HashParseError) -> Self {
        Self::Overflow
    }
}
