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
    #[error("stored integer exceeded the supported range: {0}")]
    IntegerOutOfRange(String),
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
    i64::try_from(value)
        .map_err(|err| PostgresMetadataStoreError::IntegerOutOfRange(err.to_string()))
}

pub(crate) fn i64_to_u64(value: i64) -> Result<u64, PostgresMetadataStoreError> {
    u64::try_from(value)
        .map_err(|err| PostgresMetadataStoreError::IntegerOutOfRange(err.to_string()))
}

#[cfg(test)]
mod tests {
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::panic, clippy::unwrap_in_result, clippy::arithmetic_side_effects, clippy::option_if_let_else, clippy::unreachable, clippy::shadow_unrelated, clippy::let_underscore_must_use, clippy::unwrap_err_used)]
    use shardline_protocol::{HashParseError, RangeError};
    use shardline_storage::ObjectKeyError;

    use super::*;

    // ------------------------------------------------------------------
    // PostgresRecordLocator
    // ------------------------------------------------------------------
    #[test]
    fn postgres_record_locator_accessors() {
        let locator = PostgresRecordLocator {
            record_key: "rk:latest:scope:aabb".into(),
            kind: PostgresRecordKind::Latest,
            scope_key: "scope".into(),
            file_id: "aabb".into(),
            content_hash: None,
        };
        assert_eq!(locator.record_key(), "rk:latest:scope:aabb");
        assert_eq!(locator.file_id(), "aabb");
        assert!(locator.content_hash().is_none());

        let version_locator = PostgresRecordLocator {
            record_key: "rk:version:scope:aabb".into(),
            kind: PostgresRecordKind::Version,
            scope_key: "scope".into(),
            file_id: "aabb".into(),
            content_hash: Some("cafebabedeadbeef".repeat(4)),
        };
        assert_eq!(version_locator.record_key(), "rk:version:scope:aabb");
        assert_eq!(version_locator.file_id(), "aabb");
        assert_eq!(
            version_locator.content_hash(),
            Some("cafebabedeadbeef".repeat(4).as_str())
        );
    }

    // ------------------------------------------------------------------
    // PostgresRecordKind
    // ------------------------------------------------------------------
    #[test]
    fn postgres_record_kind_as_str() {
        assert_eq!(PostgresRecordKind::Latest.as_str(), "latest");
        assert_eq!(PostgresRecordKind::Version.as_str(), "version");
    }

    #[test]
    fn postgres_record_kind_parse_valid() {
        assert!(matches!(
            PostgresRecordKind::parse("latest"),
            Ok(PostgresRecordKind::Latest)
        ));
        assert!(matches!(
            PostgresRecordKind::parse("version"),
            Ok(PostgresRecordKind::Version)
        ));
    }

    #[test]
    fn postgres_record_kind_parse_invalid() {
        let result = PostgresRecordKind::parse("unknown");
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(PostgresMetadataStoreError::InvalidRecordKind)
        ));

        let result = PostgresRecordKind::parse("");
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(PostgresMetadataStoreError::InvalidRecordKind)
        ));
    }

    // ------------------------------------------------------------------
    // PostgresMetadataStoreError Display and From impls
    // ------------------------------------------------------------------
    #[test]
    fn postgres_metadata_store_error_display_sqlx() {
        // Create a SqlxError that we can box
        let sqlx_err = sqlx::Error::PoolClosed;
        let err: PostgresMetadataStoreError = sqlx_err.into();
        assert_eq!(err.to_string(), "postgres metadata store operation failed");
        // Verify the From<SqlxError> path works and we can match
        assert!(matches!(err, PostgresMetadataStoreError::Sqlx(_)));
    }

    #[test]
    fn postgres_metadata_store_error_display_json() {
        let json_err = serde_json::from_str::<()>("invalid").unwrap_err();
        let err: PostgresMetadataStoreError = json_err.into();
        assert_eq!(err.to_string(), "postgres metadata json operation failed");
    }

    #[test]
    fn postgres_metadata_store_error_display_hash_parse() {
        let hash_err = HashParseError::InvalidLength;
        let err: PostgresMetadataStoreError = hash_err.into();
        assert_eq!(err.to_string(), "stored hash value was invalid");
    }

    #[test]
    fn postgres_metadata_store_error_display_object_key() {
        let obj_err = ObjectKeyError::Empty;
        let err: PostgresMetadataStoreError = obj_err.into();
        assert_eq!(err.to_string(), "stored object key was invalid");
    }

    #[test]
    fn postgres_metadata_store_error_display_range() {
        let range_err = RangeError::Inverted;
        let err: PostgresMetadataStoreError = range_err.into();
        assert_eq!(err.to_string(), "stored chunk range was invalid");
    }

    #[test]
    fn postgres_metadata_store_error_display_integer_out_of_range() {
        let err = PostgresMetadataStoreError::IntegerOutOfRange("test".to_owned());
        let msg = err.to_string();
        assert!(msg.contains("stored integer exceeded the supported range"));
        assert!(msg.contains("test"));
    }

    #[test]
    fn postgres_metadata_store_error_display_record_not_found() {
        let err = PostgresMetadataStoreError::RecordNotFound;
        assert_eq!(err.to_string(), "postgres record locator was not found");
    }

    #[test]
    fn postgres_metadata_store_error_display_invalid_record_kind() {
        let err = PostgresMetadataStoreError::InvalidRecordKind;
        assert_eq!(err.to_string(), "stored record kind was invalid");
    }

    #[test]
    fn postgres_metadata_store_error_display_retention_hold() {
        let err: PostgresMetadataStoreError = RetentionHoldError::EmptyReason.into();
        assert_eq!(err.to_string(), "stored retention hold was invalid");
    }

    #[test]
    fn postgres_metadata_store_error_display_quarantine_candidate() {
        let err: PostgresMetadataStoreError =
            QuarantineCandidateError::InvertedTimeline.into();
        assert_eq!(err.to_string(), "stored quarantine candidate was invalid");
    }

    #[test]
    fn postgres_metadata_store_error_display_webhook_delivery() {
        let err: PostgresMetadataStoreError =
            WebhookDeliveryError::EmptyRepositoryOwner.into();
        assert_eq!(err.to_string(), "stored webhook delivery was invalid");
    }

    #[test]
    fn postgres_metadata_store_error_display_invalid_repo_type() {
        let err = PostgresMetadataStoreError::InvalidRepoType("custom".into());
        assert_eq!(err.to_string(), "invalid repository type: custom");
    }

    #[test]
    fn postgres_metadata_store_error_from_sqlx() {
        let sqlx_err = sqlx::Error::PoolTimedOut;
        let pg_err: PostgresMetadataStoreError = sqlx_err.into();
        assert!(matches!(pg_err, PostgresMetadataStoreError::Sqlx(_)));
    }

    // ------------------------------------------------------------------
    // u64_to_i64 conversion
    // ------------------------------------------------------------------
    #[test]
    fn u64_to_i64_ok() {
        assert!(matches!(u64_to_i64(0), Ok(v) if v == 0));
        assert!(matches!(u64_to_i64(42), Ok(v) if v == 42));
        assert!(matches!(u64_to_i64(i64::MAX as u64), Ok(v) if v == i64::MAX));
    }

    #[test]
    fn u64_to_i64_overflow() {
        let result = u64_to_i64(i64::MAX as u64 + 1);
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(PostgresMetadataStoreError::IntegerOutOfRange(_))
        ));

        let result = u64_to_i64(u64::MAX);
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(PostgresMetadataStoreError::IntegerOutOfRange(_))
        ));
    }

    // ------------------------------------------------------------------
    // i64_to_u64 conversion
    // ------------------------------------------------------------------
    #[test]
    fn i64_to_u64_ok() {
        assert!(matches!(i64_to_u64(0), Ok(v) if v == 0));
        assert!(matches!(i64_to_u64(42), Ok(v) if v == 42));
        assert!(matches!(i64_to_u64(i64::MAX), Ok(v) if v == i64::MAX as u64));
    }

    #[test]
    fn i64_to_u64_negative() {
        let result = i64_to_u64(-1);
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(PostgresMetadataStoreError::IntegerOutOfRange(_))
        ));

        let result = i64_to_u64(i64::MIN);
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(PostgresMetadataStoreError::IntegerOutOfRange(_))
        ));
    }
}
