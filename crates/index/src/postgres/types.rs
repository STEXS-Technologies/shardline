use serde_json::Error as JsonError;
use shardline_protocol::{HashParseError, RangeError};
use shardline_storage::ObjectKeyError;
use sqlx::{Error as SqlxError, PgPool};
use thiserror::Error;

use crate::{QuarantineCandidateError, RetentionHoldError, WebhookDeliveryError};

/// Postgres-compatible implementation of the asynchronous index-store contract.
#[derive(Debug, Clone)]
pub struct PostgresIndexStore {
    pub(super) pool: PgPool,
}

impl PostgresIndexStore {
    /// Creates a Postgres index-store adapter from an existing pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Returns the underlying connection pool.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Opaque Postgres file-record locator.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PostgresRecordLocator {
    pub(super) record_key: String,
    pub(super) kind: PostgresRecordKind,
    pub(super) scope_key: String,
    pub(super) file_id: String,
    pub(super) content_hash: Option<String>,
}

impl PostgresRecordLocator {
    /// Returns the stable record key for this locator.
    #[must_use]
    pub fn record_key(&self) -> &str {
        &self.record_key
    }

    /// Returns the file identifier associated with this locator.
    #[must_use]
    pub fn file_id(&self) -> &str {
        &self.file_id
    }

    /// Returns the immutable content hash when this locator points at a version record.
    #[must_use]
    pub fn content_hash(&self) -> Option<&str> {
        self.content_hash.as_deref()
    }
}

/// Postgres-compatible implementation of the record-store contract.
#[derive(Debug, Clone)]
pub struct PostgresRecordStore {
    pub(super) pool: PgPool,
}

impl PostgresRecordStore {
    /// Creates a Postgres record-store adapter from an existing pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Returns the underlying connection pool.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum PostgresRecordKind {
    Latest,
    Version,
}

impl PostgresRecordKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::Version => "version",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, PostgresMetadataStoreError> {
        match value {
            "latest" => Ok(Self::Latest),
            "version" => Ok(Self::Version),
            _other => Err(PostgresMetadataStoreError::InvalidRecordKind),
        }
    }
}

/// Postgres metadata-store failure.
#[derive(Debug, Error)]
pub enum PostgresMetadataStoreError {
    /// Postgres access failed.
    #[error("postgres metadata store operation failed")]
    Sqlx(#[source] Box<SqlxError>),
    /// JSON serialization or deserialization failed.
    #[error("postgres metadata json operation failed")]
    Json(#[from] JsonError),
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
    /// A stored integer exceeded the supported range.
    #[error("stored integer exceeded the supported range")]
    IntegerOutOfRange,
    /// The requested record locator does not exist.
    #[error("postgres record locator was not found")]
    RecordNotFound,
    /// A stored record kind was invalid.
    #[error("stored record kind was invalid")]
    InvalidRecordKind,
    /// An invalid repository type string was encountered.
    #[error("invalid repository type: {0}")]
    InvalidRepoType(String),
}

impl From<SqlxError> for PostgresMetadataStoreError {
    fn from(value: SqlxError) -> Self {
        Self::Sqlx(Box::new(value))
    }
}

pub(crate) fn u64_to_i64(value: u64) -> Result<i64, PostgresMetadataStoreError> {
    i64::try_from(value).map_err(|_error| PostgresMetadataStoreError::IntegerOutOfRange)
}

pub(crate) fn i64_to_u64(value: i64) -> Result<u64, PostgresMetadataStoreError> {
    u64::try_from(value).map_err(|_error| PostgresMetadataStoreError::IntegerOutOfRange)
}
