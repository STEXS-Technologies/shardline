use std::io;
use std::num::TryFromIntError;

use serde_json::Error as JsonError;
use shardline_index::{
    LocalIndexStoreError, MemoryIndexStoreError, MemoryRecordStoreError, PostgresMetadataStoreError,
};
use shardline_protocol::HashParseError;
use shardline_server_core::{
    InvalidSerializedShardError, ParseStoredFileRecordError, RebuildOverflowError,
    ServerObjectStoreError, ValidateContentHashError, ValidateIdentifierError,
};
use shardline_storage::{LocalObjectStoreError, ObjectPrefixError, S3ObjectStoreError};
use shardline_xet_adapter::XetAdapterError;
use thiserror::Error;

/// Rebuild operation failure.
#[derive(Debug, Error)]
pub enum RebuildError {
    /// A local filesystem I/O error occurred.
    #[error("local storage operation failed")]
    Io(#[from] io::Error),
    /// JSON serialization or deserialization failed.
    #[error("json operation failed")]
    Json(#[from] JsonError),
    /// Numeric conversion exceeded supported bounds.
    #[error("numeric conversion exceeded supported bounds")]
    NumericConversion(#[from] TryFromIntError),
    /// A content hash was malformed.
    #[error("content hash must be 64 hexadecimal characters")]
    InvalidContentHash,
    /// A file identifier was unsafe.
    #[error(
        "file identifier must be relative and must not contain traversal or control characters"
    )]
    InvalidFileId,
    /// Arithmetic overflowed a checked bound.
    #[error("arithmetic overflow")]
    Overflow,
    /// Object inventory prefix validation failed.
    #[error("object storage prefix validation failed")]
    ObjectPrefix(#[from] ObjectPrefixError),
    /// Local storage adapter access failed.
    #[error("local storage adapter operation failed")]
    LocalObjectStore(#[from] LocalObjectStoreError),
    /// S3-compatible object-storage adapter access failed.
    #[error("s3 object storage adapter operation failed")]
    S3ObjectStore(#[from] S3ObjectStoreError),
    /// Xet adapter access failed.
    #[error("xet adapter operation failed")]
    XetAdapter(#[from] XetAdapterError),
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
    /// Hash parsing failed.
    #[error("hash parsing failed")]
    HashParse(#[from] HashParseError),
    /// Stored file metadata exceeded the bounded parser ceiling.
    #[error("stored file metadata exceeded the bounded parser ceiling")]
    StoredFileMetadataTooLarge {
        /// Observed file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted file length in bytes.
        maximum_bytes: u64,
    },
}

impl From<ParseStoredFileRecordError> for RebuildError {
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

impl From<ValidateIdentifierError> for RebuildError {
    fn from(_: ValidateIdentifierError) -> Self {
        Self::InvalidFileId
    }
}

impl From<ValidateContentHashError> for RebuildError {
    fn from(_: ValidateContentHashError) -> Self {
        Self::InvalidContentHash
    }
}

impl From<RebuildOverflowError> for RebuildError {
    fn from(_: RebuildOverflowError) -> Self {
        Self::Overflow
    }
}

impl From<InvalidSerializedShardError> for RebuildError {
    fn from(value: InvalidSerializedShardError) -> Self {
        Self::XetAdapter(XetAdapterError::InvalidSerializedShard(value))
    }
}

impl From<ServerObjectStoreError> for RebuildError {
    fn from(value: ServerObjectStoreError) -> Self {
        match value {
            ServerObjectStoreError::NotFound => Self::Overflow,
            ServerObjectStoreError::Overflow => Self::Overflow,
            ServerObjectStoreError::InvalidContentHash => Self::Overflow,
            ServerObjectStoreError::StoredObjectLengthMismatch => Self::Overflow,
            ServerObjectStoreError::Local(e) => Self::LocalObjectStore(e),
            ServerObjectStoreError::S3(e) => Self::S3ObjectStore(e),
            ServerObjectStoreError::Io(e) => Self::Io(e),
            ServerObjectStoreError::NumericConversion(e) => Self::NumericConversion(e),
        }
    }
}
