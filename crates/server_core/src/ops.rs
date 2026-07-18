use shardline_protocol::RepositoryProvider;
use thiserror::Error;

/// Maximum allowed stored file record metadata size in bytes.
pub const MAX_LOCAL_RECORD_METADATA_BYTES: u64 = 1_073_741_824;

/// Checked addition returning an error on overflow.
///
/// # Errors
///
/// Returns [`RebuildOverflowError`] when the addition overflows.
pub const fn checked_add(left: u64, right: u64) -> Result<u64, RebuildOverflowError> {
    match left.checked_add(right) {
        Some(value) => Ok(value),
        None => Err(RebuildOverflowError),
    }
}

/// Checked increment returning an error on overflow.
///
/// # Errors
///
/// Returns [`RebuildOverflowError`] when the increment overflows.
pub const fn checked_increment(value: u64) -> Result<u64, RebuildOverflowError> {
    checked_add(value, 1)
}

/// Arithmetic overflow during rebuild operations.
#[derive(Debug, Clone, Copy, Error)]
#[error("arithmetic overflow")]
pub struct RebuildOverflowError;

/// Returns the current Unix time in seconds, or an error if the system clock
/// is before the Unix epoch.
///
/// # Errors
///
/// Returns [`RebuildOverflowError`] when the system time is before the Unix
/// epoch.
pub fn unix_now_seconds_checked() -> Result<u64, RebuildOverflowError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_e| RebuildOverflowError)
}

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
/// # Errors
///
/// Returns an error if the metadata exceeds [`MAX_LOCAL_RECORD_METADATA_BYTES`] or
/// if JSON deserialization fails.
pub fn parse_stored_file_record_bytes(
    bytes: &[u8],
) -> Result<shardline_index::FileRecord, ParseStoredFileRecordError> {
    let observed_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if observed_bytes > MAX_LOCAL_RECORD_METADATA_BYTES {
        return Err(ParseStoredFileRecordError::StoredFileMetadataTooLarge {
            observed_bytes,
            maximum_bytes: MAX_LOCAL_RECORD_METADATA_BYTES,
        });
    }

    Ok(serde_json::from_slice(bytes)?)
}
