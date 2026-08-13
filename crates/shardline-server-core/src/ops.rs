use shardline_protocol::RepositoryProvider;
use thiserror::Error;

/// Returns the provider directory string for the given repository provider.
#[must_use]
pub const fn provider_directory(provider: RepositoryProvider) -> &'static str {
    provider.as_str()
}

/// Stored file record parsing failure.
#[derive(Debug, Error)]
pub enum ParseStoredFileRecordError {
    /// Stored file metadata exceeded the bounded parser ceiling.
    #[error("stored file metadata exceeded the bounded parser ceiling")]
    StoredFileMetadataTooLarge {
        /// Observed file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted file length in bytes.
        maximum_bytes: u64,
    },
    /// JSON deserialization failed.
    #[error("json operation failed")]
    Json(#[from] serde_json::Error),
}

/// Parses stored file record bytes, rejecting oversized metadata before JSON parsing.
///
/// # Examples
///
/// ```
/// use shardline_server_core::parse_stored_file_record_bytes;
///
/// let record = br#"{
///     "file_id": "assets/logo.png",
///     "content_hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
///     "total_bytes": 1024,
///     "chunk_size": 1024,
///     "storage_repr": "fixed_chunk_v1",
///     "chunks": [{
///         "hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
///         "offset": 0,
///         "length": 1024,
///         "range_start": 0,
///         "range_end": 1,
///         "packed_start": 0,
///         "packed_end": 1
///     }]
/// }"#;
///
/// let parsed = parse_stored_file_record_bytes(record)?;
/// assert_eq!(parsed.file_id, "assets/logo.png");
/// assert_eq!(parsed.total_bytes, 1024);
/// assert_eq!(parsed.chunks.len(), 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Errors
///
/// Returns an error if the metadata exceeds [`crate::MAX_LOCAL_RECORD_METADATA_BYTES`] or
/// if JSON deserialization fails.
pub fn parse_stored_file_record_bytes(
    bytes: &[u8],
) -> Result<shardline_index::FileRecord, ParseStoredFileRecordError> {
    let observed_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    let max_bytes = shardline_validation::MAX_LOCAL_RECORD_METADATA_BYTES;
    if observed_bytes > max_bytes {
        return Err(ParseStoredFileRecordError::StoredFileMetadataTooLarge {
            observed_bytes,
            maximum_bytes: max_bytes,
        });
    }

    Ok(serde_json::from_slice(bytes)?)
}
