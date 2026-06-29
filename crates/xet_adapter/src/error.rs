use std::{io::Error as IoError, num::TryFromIntError};

use shardline_index::{
    FileRecordInvariantError, LocalIndexStoreError, MemoryIndexStoreError, MemoryRecordStoreError,
    PostgresMetadataStoreError,
};
use shardline_protocol::HashParseError;
use shardline_server_core::{InvalidSerializedShardError, ServerObjectStoreError};
use shardline_storage::{LocalObjectStoreError, S3ObjectStoreError};
use thiserror::Error;

use crate::xorb::XorbParseError;

/// Xet adapter failure.
#[derive(Debug, Error)]
pub enum XetAdapterError {
    /// Local storage IO failed.
    #[error("local storage operation failed")]
    Io(#[from] IoError),
    /// Numeric conversion exceeded supported bounds.
    #[error("numeric conversion exceeded supported bounds")]
    NumericConversion(#[from] TryFromIntError),
    /// Hash parsing failed.
    #[error("invalid content hash")]
    HashParse(#[from] HashParseError),
    /// Object-storage adapter access failed.
    #[error("object storage adapter operation failed")]
    ObjectStore(#[from] ServerObjectStoreError),
    /// Local object-storage adapter access failed.
    #[error("local object storage operation failed")]
    LocalObjectStore(#[from] LocalObjectStoreError),
    /// S3-compatible object-storage adapter access failed.
    #[error("s3 object storage operation failed")]
    S3ObjectStore(#[from] S3ObjectStoreError),
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
    /// Stored file metadata could not produce a valid reconstruction plan.
    #[error("stored file metadata was invalid")]
    FileRecordInvariant(#[from] FileRecordInvariantError),
    /// A content hash was malformed.
    #[error("content hash must be 64 hexadecimal characters")]
    InvalidContentHash,
    /// The xorb transfer path prefix was unsupported.
    #[error("xorb transfer prefix must be default")]
    InvalidXorbPrefix,
    /// The uploaded xorb bytes did not match the requested hash.
    #[error("xorb body hash did not match the requested path hash")]
    XorbHashMismatch,
    /// The uploaded xorb bytes were not a valid serialized xorb object.
    #[error("xorb body was not a valid serialized xorb object")]
    InvalidSerializedXorb,
    /// The uploaded shard bytes were not a valid serialized shard object.
    #[error("shard body was not a valid serialized shard object")]
    InvalidSerializedShard(#[from] InvalidSerializedShardError),
    /// A shard upload referenced a missing xorb.
    #[error("shard referenced a missing xorb")]
    MissingReferencedXorb,
    /// Shard metadata exceeded bounded parser safety limits.
    #[error("shard metadata exceeded bounded parser safety limits")]
    TooManyShardTerms,
    /// Requested content was not found.
    #[error("content not found")]
    NotFound,
    /// Arithmetic overflowed a checked bound.
    #[error("arithmetic overflow")]
    Overflow,
    /// The reconstruction range start exceeded the end of the file.
    #[error("requested range is not satisfiable")]
    RangeNotSatisfiable,
}

impl From<XorbParseError> for XetAdapterError {
    fn from(value: XorbParseError) -> Self {
        match value {
            XorbParseError::HashMismatch => Self::XorbHashMismatch,
            XorbParseError::InvalidFormat(_)
            | XorbParseError::NumericConversion(_)
            | XorbParseError::Io(_) => Self::InvalidSerializedXorb,
        }
    }
}
