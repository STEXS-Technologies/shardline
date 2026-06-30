mod index_store;
mod record_store;

use serde_json::Error as JsonError;
use shardline_protocol::{HashParseError, RangeError};
use shardline_storage::ObjectKeyError;
use sqlx::{Error as SqlxError, PgPool};
use thiserror::Error;

use crate::{QuarantineCandidateError, RetentionHoldError, WebhookDeliveryError};

/// Postgres-compatible implementation of the asynchronous index-store contract.
#[derive(Debug, Clone)]
pub struct PostgresIndexStore {
    pool: PgPool,
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
    record_key: String,
    kind: PostgresRecordKind,
    scope_key: String,
    file_id: String,
    content_hash: Option<String>,
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
    pool: PgPool,
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
pub(super) enum PostgresRecordKind {
    Latest,
    Version,
}

impl PostgresRecordKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::Version => "version",
        }
    }

    fn parse(value: &str) -> Result<Self, PostgresMetadataStoreError> {
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

pub(super) fn u64_to_i64(value: u64) -> Result<i64, PostgresMetadataStoreError> {
    i64::try_from(value).map_err(|_error| PostgresMetadataStoreError::IntegerOutOfRange)
}

pub(super) fn i64_to_u64(value: i64) -> Result<u64, PostgresMetadataStoreError> {
    u64::try_from(value).map_err(|_error| PostgresMetadataStoreError::IntegerOutOfRange)
}

#[cfg(test)]
mod tests {
    use shardline_protocol::{ChunkRange, RepositoryProvider, RepositoryScope, ShardlineHash};

    use super::PostgresRecordKind;
    use super::index_store::PostgresFileReconstructionRecord;
    use super::record_store::record_locator;
    use crate::{
        FileId, FileReconstruction, FileRecord, ReconstructionTerm, RepositoryRecordScope, XorbId,
        record_key::record_key as shared_record_key,
        record_key::{
            repository_record_scope_key as shared_repository_record_scope_key,
            repository_scope_key as shared_repository_scope_key,
        },
        xet_hash_hex_string,
    };

    #[test]
    fn postgres_reconstruction_record_roundtrips_domain_terms() {
        let hash = ShardlineHash::from_bytes([11; 32]);
        let range = ChunkRange::new(1, 4);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };
        let reconstruction =
            FileReconstruction::new(vec![ReconstructionTerm::new(XorbId::new(hash), range, 256)]);

        let record = PostgresFileReconstructionRecord::from_domain(&reconstruction);
        let restored = record.into_domain();

        assert!(matches!(restored, Ok(ref restored) if restored == &reconstruction));
    }

    #[test]
    fn postgres_record_keys_distinguish_scope_file_and_kind_without_parsing() {
        let first_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team:a", "asset", Some("main"));
        let second_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "a:asset", Some("main"));
        assert!(first_scope.is_ok());
        assert!(second_scope.is_ok());
        let (Ok(first_scope), Ok(second_scope)) = (first_scope, second_scope) else {
            return;
        };

        let first_key = shared_repository_scope_key(Some(&first_scope));
        let second_key = shared_repository_scope_key(Some(&second_scope));

        assert_ne!(first_key, second_key);
        assert_ne!(
            shared_record_key(
                PostgresRecordKind::Latest.as_str(),
                &first_key,
                "file",
                None
            ),
            shared_record_key(
                PostgresRecordKind::Version.as_str(),
                &first_key,
                "file",
                Some("a".repeat(64).as_str())
            )
        );
    }

    #[test]
    fn postgres_latest_locator_ignores_content_hash_for_stable_head_keys() {
        let scope = RepositoryScope::new(RepositoryProvider::GitLab, "team", "assets", None);
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let first = file_record(scope.clone(), "a");
        let second = file_record(scope, "b");
        let first_key = record_locator(PostgresRecordKind::Latest, &first, None);
        let second_key = record_locator(PostgresRecordKind::Latest, &second, None);

        assert_eq!(first_key, second_key);
    }

    #[test]
    fn postgres_repository_scope_key_prefix_matches_all_repository_revisions_only() {
        let repository = RepositoryRecordScope::new(RepositoryProvider::GitHub, "team", "assets");
        let revisionless = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None);
        let revisioned =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        let other = RepositoryScope::new(RepositoryProvider::GitHub, "team", "other", Some("main"));
        assert!(revisionless.is_ok());
        assert!(revisioned.is_ok());
        assert!(other.is_ok());
        let (Ok(revisionless), Ok(revisioned), Ok(other)) = (revisionless, revisioned, other)
        else {
            return;
        };

        let repository_key = shared_repository_record_scope_key(&repository);
        let revisionless_key = shared_repository_scope_key(Some(&revisionless));
        let revisioned_key = shared_repository_scope_key(Some(&revisioned));
        let other_key = shared_repository_scope_key(Some(&other));

        assert_eq!(repository_key, revisionless_key);
        assert!(revisioned_key.starts_with(&repository_key));
        assert!(!other_key.starts_with(&repository_key));
    }

    #[test]
    fn postgres_lifecycle_migrations_reject_inverted_timelines() {
        let metadata_migration =
            include_str!("../../migrations/20260417000000_metadata_store.up.sql");
        let retention_migration =
            include_str!("../../migrations/20260417010000_retention_holds.up.sql");

        assert!(metadata_migration.contains(
            "CHECK (delete_after_unix_seconds >= first_seen_unreachable_at_unix_seconds)"
        ));
        assert!(retention_migration.contains("release_after_unix_seconds >= held_at_unix_seconds"));
    }

    fn file_record(scope: RepositoryScope, content_seed: &str) -> FileRecord {
        let hash = ShardlineHash::from_bytes([12; 32]);
        let file_id = xet_hash_hex_string(FileId::new(hash).hash());
        FileRecord {
            file_id,
            content_hash: content_seed.repeat(64),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: Some(scope),
            chunks: Vec::new(),
        }
    }
}
