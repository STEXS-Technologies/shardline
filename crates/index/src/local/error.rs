use std::io::Error as IoError;

use serde_json::Error as SerdeJsonError;
use shardline_protocol::{HashParseError, RangeError};
use shardline_storage::ObjectKeyError;
use thiserror::Error;

use crate::{QuarantineCandidateError, RetentionHoldError, WebhookDeliveryError};

/// Local index-store failure.
#[derive(Debug, Error)]
pub(crate) enum LocalIndexStoreError {
    /// Local filesystem access failed.
    #[error("local index store operation failed")]
    Io(#[from] IoError),
    /// JSON serialization or deserialization failed.
    #[error("local index store json operation failed")]
    Json(#[from] SerdeJsonError),
    /// Stored metadata exceeded the bounded local parser ceiling.
    #[error("local index store metadata exceeded the bounded parser ceiling")]
    MetadataTooLarge {
        /// Observed file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted file length in bytes.
        maximum_bytes: u64,
    },
    /// Stored metadata changed after validation and was rejected.
    #[error("local index store metadata changed during bounded read")]
    MetadataLengthMismatch {
        /// Validated file length in bytes.
        expected_bytes: u64,
        /// Observed file length in bytes after bounded read.
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
    /// A blocking computation task failed (panicked).
    #[error("blocking task failed")]
    BlockingTask,
}
