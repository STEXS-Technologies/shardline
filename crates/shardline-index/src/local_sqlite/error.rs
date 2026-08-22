use std::io::Error as IoError;

use rusqlite::Error as SqliteError;
use serde_json::Error as JsonError;
use shardline_protocol::{HashParseError, RangeError};
use shardline_storage::ObjectKeyError;
use thiserror::Error;

use crate::{
    QuarantineCandidateError, RetentionHoldError, UploadIntentConflictError, WebhookDeliveryError,
};

/// Local metadata-store failure.
#[derive(Debug, Error)]
pub enum LocalIndexStoreError {
    /// Local filesystem access failed.
    #[error("local metadata operation failed")]
    Io(#[from] IoError),
    /// SQLite access failed.
    #[error("local sqlite metadata operation failed")]
    Sqlite(#[from] SqliteError),
    /// JSON serialization or deserialization failed.
    #[error("local metadata json operation failed")]
    Json(#[from] JsonError),
    /// Stored metadata exceeded the bounded parser ceiling.
    #[error("local metadata exceeded the bounded parser ceiling")]
    MetadataTooLarge {
        /// Number of bytes observed in the stored metadata payload.
        observed_bytes: u64,
        /// Maximum accepted metadata payload size.
        maximum_bytes: u64,
    },
    /// Stored metadata changed during a bounded read.
    #[error("local metadata changed during bounded read")]
    MetadataLengthMismatch {
        /// Number of bytes expected from the initial metadata length check.
        expected_bytes: u64,
        /// Number of bytes observed while reading the metadata payload.
        observed_bytes: u64,
    },
    /// A stored hash value was invalid.
    #[error("stored hash value was invalid")]
    HashParse(#[from] HashParseError),
    /// A stored object key was invalid.
    #[error("stored object key was invalid")]
    ObjectKey(#[from] ObjectKeyError),
    /// A stored chunk range was invalid.
    #[error("stored chunk range was invalid")]
    Range(#[from] RangeError),
    /// A stored retention hold was invalid.
    #[error("stored retention hold was invalid")]
    RetentionHold(#[from] RetentionHoldError),
    /// A stored quarantine candidate was invalid.
    #[error("stored quarantine candidate was invalid")]
    QuarantineCandidate(#[from] QuarantineCandidateError),
    /// A stored webhook delivery was invalid.
    #[error("stored webhook delivery was invalid")]
    WebhookDelivery(#[from] WebhookDeliveryError),
    /// An upload intent ID was reused for different object identity.
    #[error("upload intent conflict")]
    UploadIntentConflict(
        #[from]
        #[source]
        UploadIntentConflictError,
    ),
    /// A stored integer exceeded the supported range.
    #[error("stored integer exceeded the supported range: {0}")]
    IntegerOutOfRange(String),
    /// A stored record kind was invalid.
    #[error("stored local record kind was invalid")]
    InvalidRecordKind,
    /// The local metadata database had inconsistent import state.
    #[error("local metadata database had inconsistent legacy import state")]
    InvalidLegacyImportState,
    /// An invalid repository type string was encountered.
    #[error("invalid repository type: {0}")]
    InvalidRepoType(String),
    /// A blocking computation task failed (panicked).
    #[error("blocking task failed: {0}")]
    BlockingTask(String),
    /// An invalid local table name was encountered.
    #[error("invalid local table name")]
    InvalidTableName,
}
