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

#[cfg(test)]
mod tests {
    use super::XetAdapterError;
    use crate::xorb::{XorbInvalidFormatError, XorbParseError};
    use shardline_index::FileRecordInvariantError;
    use shardline_protocol::HashParseError;
    use shardline_server_core::InvalidSerializedShardError;
    use shardline_storage::{LocalObjectStoreError, S3ObjectStoreError};

    #[test]
    fn xet_adapter_error_display_all_variants() {
        let cases: &[(XetAdapterError, &str)] = &[
            (XetAdapterError::Io(std::io::Error::other("test")), "storage"),
            (XetAdapterError::NumericConversion(u64::try_from(-1i32).unwrap_err()), "bounds"),
            (XetAdapterError::HashParse(HashParseError::InvalidLength), "hash"),
            (XetAdapterError::ObjectStore(shardline_server_core::ServerObjectStoreError::NotFound), "object"),
            (XetAdapterError::LocalObjectStore(LocalObjectStoreError::Io(std::io::Error::other("test"))), "storage"),
            (XetAdapterError::S3ObjectStore(S3ObjectStoreError::Io(std::io::Error::other("test"))), "s3"),
            (XetAdapterError::InvalidContentHash, "64"),
            (XetAdapterError::InvalidXorbPrefix, "prefix"),
            (XetAdapterError::XorbHashMismatch, "hash"),
            (XetAdapterError::InvalidSerializedXorb, "xorb"),
            (XetAdapterError::InvalidSerializedShard(InvalidSerializedShardError::ParserRejectedMetadata), "shard"),
            (XetAdapterError::MissingReferencedXorb, "xorb"),
            (XetAdapterError::TooManyShardTerms, "shard"),
            (XetAdapterError::NotFound, "found"),
            (XetAdapterError::Overflow, "overflow"),
            (XetAdapterError::RangeNotSatisfiable, "satisfiable"),
        ];
        for (error, substring) in cases {
            let msg = error.to_string();
            assert!(!msg.is_empty(), "empty display for {error:?}");
            assert!(
                msg.contains(substring),
                "expected '{substring}' in '{msg}' from {error:?}"
            );
        }
    }

    #[test]
    fn xet_adapter_error_file_record_invariant_display() {
        let error = XetAdapterError::FileRecordInvariant(FileRecordInvariantError::EmptyChunk);
        let msg = error.to_string();
        assert!(!msg.is_empty());
        assert!(msg.contains("invalid"));
    }

    #[test]
    fn xet_adapter_error_index_store_bridge_display() {
        use shardline_index::LocalIndexStoreError;
        let error = XetAdapterError::IndexStore(LocalIndexStoreError::InvalidLegacyImportState);
        let msg = error.to_string();
        assert!(!msg.is_empty());
    }

    // ── From<XorbParseError> ───────────────────────────────────────────

    #[test]
    fn from_xorb_parse_error_hash_mismatch() {
        let err: XetAdapterError = XorbParseError::HashMismatch.into();
        assert!(matches!(err, XetAdapterError::XorbHashMismatch));
    }

    #[test]
    fn from_xorb_parse_error_invalid_format() {
        let err: XetAdapterError =
            XorbParseError::InvalidFormat(XorbInvalidFormatError::StructuralValidationFailed).into();
        assert!(matches!(err, XetAdapterError::InvalidSerializedXorb));
    }

    #[test]
    fn from_xorb_parse_error_numeric_conversion() {
        let err: XetAdapterError = XorbParseError::NumericConversion(
            u64::try_from(-1i32).unwrap_err(),
        )
        .into();
        assert!(matches!(err, XetAdapterError::InvalidSerializedXorb));
    }

    #[test]
    fn from_xorb_parse_error_io() {
        let err: XetAdapterError = XorbParseError::Io(std::io::Error::other("disk failure")).into();
        assert!(matches!(err, XetAdapterError::InvalidSerializedXorb));
    }
}
