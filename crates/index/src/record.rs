use std::{future::Future, pin::Pin, time::Duration};

use serde::{Deserialize, Serialize};
use shardline_protocol::{HashParseError, RepositoryProvider, RepositoryScope};
use thiserror::Error;

use crate::parse_xet_hash_hex;

/// Boxed asynchronous record-store operation.
pub type RecordStoreFuture<'operation, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'operation>>;

macro_rules! visit_locators_async {
    ($visit:ident, $list:ident) => {
        fn $visit<'operation, Visitor, VisitorError>(
            &'operation self,
            mut visitor: Visitor,
        ) -> RecordStoreFuture<'operation, (), VisitorError>
        where
            Self: Sync,
            Self::Error: Into<VisitorError> + 'operation,
            Visitor: FnMut(Self::Locator) -> Result<(), VisitorError> + Send + 'operation,
            VisitorError: Send + 'operation,
        {
            Box::pin(async move {
                for locator in self.$list().await.map_err(Into::into)? {
                    visitor(locator)?;
                }
                Ok(())
            })
        }
    };
}

macro_rules! visit_repository_locators_async {
    ($visit:ident, $list:ident) => {
        fn $visit<'operation, Visitor, VisitorError>(
            &'operation self,
            repository: &'operation crate::RepositoryRecordScope,
            mut visitor: Visitor,
        ) -> RecordStoreFuture<'operation, (), VisitorError>
        where
            Self: Sync,
            Self::Error: Into<VisitorError> + 'operation,
            Visitor: FnMut(Self::Locator) -> Result<(), VisitorError> + Send + 'operation,
            VisitorError: Send + 'operation,
        {
            Box::pin(async move {
                for locator in self.$list(repository).await.map_err(Into::into)? {
                    visitor(locator)?;
                }
                Ok(())
            })
        }
    };
}

/// Stored record bytes together with adapter locator and modification time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRecord<Locator> {
    /// Adapter locator for the stored record.
    pub locator: Locator,
    /// Raw record bytes.
    pub bytes: Vec<u8>,
    /// Adapter-reported modification time relative to the Unix epoch.
    pub modified_since_epoch: Duration,
}

/// One chunk term stored for a file version record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileChunkRecord {
    /// Chunk hash in Xet CAS API hexadecimal ordering.
    pub hash: String,
    /// Byte offset inside the reconstructed file.
    pub offset: u64,
    /// Chunk byte length.
    pub length: u64,
    /// Start member index inside the referenced protocol object.
    #[serde(default)]
    pub range_start: u32,
    /// End-exclusive member index inside the referenced protocol object.
    #[serde(default = "default_range_end")]
    pub range_end: u32,
    /// Inclusive start byte for the serialized protocol object range that covers this term.
    #[serde(default)]
    pub packed_start: u64,
    /// Exclusive end byte for the serialized protocol object range that covers this term.
    #[serde(default = "default_packed_end")]
    pub packed_end: u64,
}

const fn default_range_end() -> u32 {
    1
}

const fn default_packed_end() -> u64 {
    0
}

/// Durable file-version or latest-file metadata record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileRecord {
    /// File identifier within the repository scope.
    pub file_id: String,
    /// Immutable content identity for this file version.
    pub content_hash: String,
    /// Total logical byte length of the reconstructed file.
    pub total_bytes: u64,
    /// Chunk size used for this upload. Protocol-object term records may use zero.
    pub chunk_size: u64,
    /// Optional repository namespace for provider-backed storage.
    pub repository_scope: Option<RepositoryScope>,
    /// Ordered chunks needed to reconstruct the file.
    pub chunks: Vec<FileChunkRecord>,
}

/// File-record reconstruction storage layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileRecordStorageLayout {
    /// The file reconstructs directly from stored chunk objects.
    StoredChunks,
    /// The file reconstructs from protocol-owned object terms.
    ReferencedObjectTerms,
}

/// Repository identity used to scope record traversal across all revisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryRecordScope {
    provider: RepositoryProvider,
    owner: String,
    name: String,
}

impl RepositoryRecordScope {
    /// Creates a repository scope.
    #[must_use]
    pub fn new(
        provider: RepositoryProvider,
        owner: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            provider,
            owner: owner.into(),
            name: name.into(),
        }
    }

    /// Returns the repository provider.
    #[must_use]
    pub const fn provider(&self) -> RepositoryProvider {
        self.provider
    }

    /// Returns the repository owner or namespace.
    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Returns the repository name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Builds a repository scope from a protocol repository scope, dropping the revision.
    #[must_use]
    pub fn from_repository_scope(scope: &RepositoryScope) -> Self {
        Self::new(scope.provider(), scope.owner(), scope.name())
    }
}

impl FileRecord {
    /// Returns the storage layout encoded by this record.
    #[must_use]
    pub const fn storage_layout(&self) -> FileRecordStorageLayout {
        if self.chunk_size == 0 {
            return FileRecordStorageLayout::ReferencedObjectTerms;
        }

        FileRecordStorageLayout::StoredChunks
    }

    /// Validates the invariants required to build a deterministic reconstruction plan.
    ///
    /// # Errors
    ///
    /// Returns [`FileRecordInvariantError`] when chunk hashes, offsets, lengths, or
    /// xorb byte ranges are malformed.
    pub fn validate_reconstruction_plan(&self) -> Result<(), FileRecordInvariantError> {
        let mut expected_offset = 0_u64;
        for chunk in &self.chunks {
            parse_xet_hash_hex(&chunk.hash)?;
            if chunk.length == 0 {
                return Err(FileRecordInvariantError::EmptyChunk);
            }
            if chunk.offset != expected_offset {
                return Err(FileRecordInvariantError::NonContiguousChunkOffsets);
            }
            if chunk.range_end <= chunk.range_start {
                return Err(FileRecordInvariantError::InvalidChunkRange);
            }
            if chunk.packed_end <= chunk.packed_start {
                return Err(FileRecordInvariantError::InvalidPackedRange);
            }
            expected_offset = expected_offset
                .checked_add(chunk.length)
                .ok_or(FileRecordInvariantError::LengthOverflow)?;
        }

        if expected_offset != self.total_bytes {
            return Err(FileRecordInvariantError::TotalBytesMismatch);
        }

        if self.chunks.is_empty() && self.total_bytes != 0 {
            return Err(FileRecordInvariantError::TotalBytesMismatch);
        }

        Ok(())
    }
}

/// File-record reconstruction-plan invariant failure.
#[derive(Debug, Clone, Error)]
pub enum FileRecordInvariantError {
    /// A chunk hash was not a lowercase Xet API hash.
    #[error("file record chunk hash is invalid")]
    ChunkHash(#[from] HashParseError),
    /// A reconstruction chunk had zero logical bytes.
    #[error("file record chunk length must be greater than zero")]
    EmptyChunk,
    /// Chunk offsets were not contiguous from the start of the file.
    #[error("file record chunk offsets must be contiguous")]
    NonContiguousChunkOffsets,
    /// A chunk range was empty or inverted.
    #[error("file record chunk range must be non-empty and ordered")]
    InvalidChunkRange,
    /// A packed protocol-object byte range was empty or inverted.
    #[error("file record packed byte range must be non-empty and ordered")]
    InvalidPackedRange,
    /// Logical chunk lengths overflowed the supported integer range.
    #[error("file record chunk lengths overflowed")]
    LengthOverflow,
    /// The chunk length sum did not match the record total byte length.
    #[error("file record total bytes did not match chunk lengths")]
    TotalBytesMismatch,
}

/// Read-only record traversal capability.
///
/// Provides all locator-listing, record-reading, and visitor methods.
/// Use as a trait bound when only read access to records is needed.
pub trait RecordTraversal {
    /// Adapter-specific failure type.
    type Error;
    /// Adapter-specific record locator.
    type Locator: Clone + Eq + Ord + Send + Sync;

    /// Lists visible latest-file record locators.
    fn list_latest_record_locators(&self)
    -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error>;

    visit_locators_async!(visit_latest_record_locators, list_latest_record_locators);

    /// Visits visible latest-file records together with raw bytes and modification time.
    fn visit_latest_records<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            for locator in self
                .list_latest_record_locators()
                .await
                .map_err(Into::into)?
            {
                let bytes = self.read_record_bytes(&locator).await.map_err(Into::into)?;
                let modified_since_epoch = self
                    .modified_since_epoch(&locator)
                    .await
                    .map_err(Into::into)?;
                visitor(StoredRecord {
                    locator,
                    bytes,
                    modified_since_epoch,
                })?;
            }

            Ok(())
        })
    }

    /// Lists visible latest-file record locators for one repository across all revisions.
    fn list_repository_latest_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error>;

    visit_repository_locators_async!(
        visit_repository_latest_record_locators,
        list_repository_latest_record_locators
    );

    /// Visits visible latest-file records for one repository across all revisions.
    fn visit_repository_latest_records<'operation, Visitor, VisitorError>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            for locator in self
                .list_repository_latest_record_locators(repository)
                .await
                .map_err(Into::into)?
            {
                let bytes = self.read_record_bytes(&locator).await.map_err(Into::into)?;
                let modified_since_epoch = self
                    .modified_since_epoch(&locator)
                    .await
                    .map_err(Into::into)?;
                visitor(StoredRecord {
                    locator,
                    bytes,
                    modified_since_epoch,
                })?;
            }

            Ok(())
        })
    }

    /// Lists immutable version-record locators.
    fn list_version_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error>;

    /// Lists immutable version-record locators for one repository across all revisions.
    fn list_repository_version_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error>;

    visit_locators_async!(visit_version_record_locators, list_version_record_locators);

    /// Visits immutable version records together with raw bytes and modification time.
    fn visit_version_records<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            for locator in self
                .list_version_record_locators()
                .await
                .map_err(Into::into)?
            {
                let bytes = self.read_record_bytes(&locator).await.map_err(Into::into)?;
                let modified_since_epoch = self
                    .modified_since_epoch(&locator)
                    .await
                    .map_err(Into::into)?;
                visitor(StoredRecord {
                    locator,
                    bytes,
                    modified_since_epoch,
                })?;
            }

            Ok(())
        })
    }

    visit_repository_locators_async!(
        visit_repository_version_record_locators,
        list_repository_version_record_locators
    );

    /// Visits immutable version records for one repository across all revisions.
    fn visit_repository_version_records<'operation, Visitor, VisitorError>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            for locator in self
                .list_repository_version_record_locators(repository)
                .await
                .map_err(Into::into)?
            {
                let bytes = self.read_record_bytes(&locator).await.map_err(Into::into)?;
                let modified_since_epoch = self
                    .modified_since_epoch(&locator)
                    .await
                    .map_err(Into::into)?;
                visitor(StoredRecord {
                    locator,
                    bytes,
                    modified_since_epoch,
                })?;
            }

            Ok(())
        })
    }

    /// Reads raw record bytes from a locator.
    fn read_record_bytes<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Vec<u8>, Self::Error>;

    /// Reads visible latest-file record bytes for a domain record.
    fn read_latest_record_bytes<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, Option<Vec<u8>>, Self::Error>;

    /// Returns whether a record locator exists.
    fn record_locator_exists<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, bool, Self::Error>;

    /// Returns a locator modification timestamp relative to Unix epoch.
    fn modified_since_epoch<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Duration, Self::Error>;

    /// Computes the visible latest-record locator for a domain record.
    fn latest_record_locator(&self, record: &FileRecord) -> Self::Locator;

    /// Computes the immutable version-record locator for a domain record.
    fn version_record_locator(&self, record: &FileRecord) -> Self::Locator;
}

/// Write/delete record mutation capability.
///
/// Provides record-writing and deletion methods.
/// Use as a trait bound when only mutation access to records is needed.
pub trait RecordMutation: RecordTraversal {
    /// Writes or replaces an immutable version record.
    fn write_version_record<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, (), Self::Error>;

    /// Writes or replaces the visible latest-file record.
    fn write_latest_record<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, (), Self::Error>;

    /// Deletes a record by locator.
    fn delete_record_locator<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, (), Self::Error>;

    /// Removes empty latest-record containers after stale record deletion.
    fn prune_empty_latest_records(&self) -> RecordStoreFuture<'_, (), Self::Error>;
}

/// Combined read + write record-store contract.
///
/// Automatically implemented for all types that implement both
/// [`RecordTraversal`] and [`RecordMutation`].
pub trait RecordStore: RecordTraversal + RecordMutation {}

impl<T: RecordTraversal + RecordMutation> RecordStore for T {}

#[cfg(test)]
mod tests {
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    use super::{FileChunkRecord, FileRecord, FileRecordInvariantError, RepositoryRecordScope};

    #[test]
    fn file_record_storage_layout_referenced_terms_when_chunk_size_is_zero() {
        let record = FileRecord {
            file_id: "a".repeat(64),
            content_hash: "c".repeat(64),
            total_bytes: 8,
            chunk_size: 0,
            repository_scope: None,
            chunks: Vec::new(),
        };
        assert_eq!(
            record.storage_layout(),
            super::FileRecordStorageLayout::ReferencedObjectTerms
        );
    }

    #[test]
    fn file_record_storage_layout_stored_chunks_when_chunk_size_nonzero() {
        let record = FileRecord {
            file_id: "a".repeat(64),
            content_hash: "c".repeat(64),
            total_bytes: 8,
            chunk_size: 4,
            repository_scope: None,
            chunks: Vec::new(),
        };
        assert_eq!(
            record.storage_layout(),
            super::FileRecordStorageLayout::StoredChunks
        );
    }

    #[test]
    fn repository_record_scope_from_repository_scope_drops_revision() {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitLab, "group", "project", Some("v2"))
                .unwrap();
        let record_scope = RepositoryRecordScope::from_repository_scope(&scope);

        assert_eq!(record_scope.provider(), RepositoryProvider::GitLab);
        assert_eq!(record_scope.owner(), "group");
        assert_eq!(record_scope.name(), "project");
    }

    #[test]
    fn file_record_reconstruction_plan_rejects_empty_chunk() {
        let record = FileRecord {
            file_id: "a".repeat(64),
            content_hash: "c".repeat(64),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: "a".repeat(64),
                offset: 0,
                length: 0,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 1,
            }],
        };
        assert!(matches!(
            record.validate_reconstruction_plan(),
            Err(FileRecordInvariantError::EmptyChunk)
        ));
    }

    #[test]
    fn file_record_reconstruction_plan_rejects_invalid_chunk_range() {
        let record = FileRecord {
            file_id: "a".repeat(64),
            content_hash: "c".repeat(64),
            total_bytes: 4,
            chunk_size: 0,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: "a".repeat(64),
                offset: 0,
                length: 4,
                range_start: 2,
                range_end: 1,
                packed_start: 0,
                packed_end: 4,
            }],
        };
        assert!(matches!(
            record.validate_reconstruction_plan(),
            Err(FileRecordInvariantError::InvalidChunkRange)
        ));
    }

    #[test]
    fn file_record_reconstruction_plan_rejects_invalid_packed_range() {
        let record = FileRecord {
            file_id: "a".repeat(64),
            content_hash: "c".repeat(64),
            total_bytes: 4,
            chunk_size: 0,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: "a".repeat(64),
                offset: 0,
                length: 4,
                range_start: 0,
                range_end: 1,
                packed_start: 4,
                packed_end: 2,
            }],
        };
        assert!(matches!(
            record.validate_reconstruction_plan(),
            Err(FileRecordInvariantError::InvalidPackedRange)
        ));
    }

    #[test]
    fn file_record_reconstruction_plan_rejects_empty_chunks_with_nonzero_total_bytes() {
        let record = FileRecord {
            file_id: "a".repeat(64),
            content_hash: "c".repeat(64),
            total_bytes: 4,
            chunk_size: 0,
            repository_scope: None,
            chunks: vec![],
        };
        assert!(matches!(
            record.validate_reconstruction_plan(),
            Err(FileRecordInvariantError::TotalBytesMismatch)
        ));
    }

    #[test]
    fn file_record_preserves_repository_scope_and_chunk_order() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", Some("main"))
            .unwrap();
        let first = FileChunkRecord {
            hash: "a".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        };
        let second = FileChunkRecord {
            hash: "b".repeat(64),
            offset: 4,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        };

        let record = FileRecord {
            file_id: "asset.bin".to_owned(),
            content_hash: "c".repeat(64),
            total_bytes: 8,
            chunk_size: 4,
            repository_scope: Some(scope.clone()),
            chunks: vec![first.clone(), second.clone()],
        };

        assert_eq!(record.repository_scope, Some(scope));
        assert_eq!(record.chunks, vec![first, second]);
    }

    #[test]
    fn file_record_reconstruction_plan_accepts_contiguous_native_chunks() {
        let record = FileRecord {
            file_id: "a".repeat(64),
            content_hash: "c".repeat(64),
            total_bytes: 8,
            chunk_size: 0,
            repository_scope: None,
            chunks: vec![
                FileChunkRecord {
                    hash: "a".repeat(64),
                    offset: 0,
                    length: 4,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 4,
                },
                FileChunkRecord {
                    hash: "b".repeat(64),
                    offset: 4,
                    length: 4,
                    range_start: 1,
                    range_end: 2,
                    packed_start: 4,
                    packed_end: 8,
                },
            ],
        };

        assert!(record.validate_reconstruction_plan().is_ok());
    }

    #[test]
    fn file_record_reconstruction_plan_rejects_gapped_offsets() {
        let record = FileRecord {
            file_id: "a".repeat(64),
            content_hash: "c".repeat(64),
            total_bytes: 8,
            chunk_size: 0,
            repository_scope: None,
            chunks: vec![
                FileChunkRecord {
                    hash: "a".repeat(64),
                    offset: 0,
                    length: 4,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 4,
                },
                FileChunkRecord {
                    hash: "b".repeat(64),
                    offset: 5,
                    length: 4,
                    range_start: 1,
                    range_end: 2,
                    packed_start: 4,
                    packed_end: 8,
                },
            ],
        };

        assert!(matches!(
            record.validate_reconstruction_plan(),
            Err(FileRecordInvariantError::NonContiguousChunkOffsets)
        ));
    }

    // ── FileRecordInvariantError Display ─────────────────────────────────

    #[test]
    fn file_record_invariant_error_chunk_hash_display() {
        let err =
            FileRecordInvariantError::ChunkHash(shardline_protocol::HashParseError::InvalidLength);
        assert_eq!(err.to_string(), "file record chunk hash is invalid");
    }

    #[test]
    fn file_record_invariant_error_empty_chunk_display() {
        let err = FileRecordInvariantError::EmptyChunk;
        assert_eq!(
            err.to_string(),
            "file record chunk length must be greater than zero"
        );
    }

    #[test]
    fn file_record_invariant_error_non_contiguous_display() {
        let err = FileRecordInvariantError::NonContiguousChunkOffsets;
        assert_eq!(
            err.to_string(),
            "file record chunk offsets must be contiguous"
        );
    }

    #[test]
    fn file_record_invariant_error_invalid_chunk_range_display() {
        let err = FileRecordInvariantError::InvalidChunkRange;
        assert_eq!(
            err.to_string(),
            "file record chunk range must be non-empty and ordered"
        );
    }

    #[test]
    fn file_record_invariant_error_invalid_packed_range_display() {
        let err = FileRecordInvariantError::InvalidPackedRange;
        assert_eq!(
            err.to_string(),
            "file record packed byte range must be non-empty and ordered"
        );
    }

    #[test]
    fn file_record_invariant_error_length_overflow_display() {
        let err = FileRecordInvariantError::LengthOverflow;
        assert_eq!(err.to_string(), "file record chunk lengths overflowed");
    }

    #[test]
    fn file_record_invariant_error_total_bytes_mismatch_display() {
        let err = FileRecordInvariantError::TotalBytesMismatch;
        assert_eq!(
            err.to_string(),
            "file record total bytes did not match chunk lengths"
        );
    }

    #[test]
    fn file_record_reconstruction_plan_rejects_length_overflow() {
        let record = FileRecord {
            file_id: "a".repeat(64),
            content_hash: "c".repeat(64),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: None,
            chunks: vec![
                FileChunkRecord {
                    hash: "a".repeat(64),
                    offset: 0,
                    length: u64::MAX,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 1,
                },
                FileChunkRecord {
                    hash: "b".repeat(64),
                    offset: u64::MAX,
                    length: 1,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 1,
                },
            ],
        };
        assert!(matches!(
            record.validate_reconstruction_plan(),
            Err(FileRecordInvariantError::LengthOverflow)
        ));
    }

    #[test]
    fn file_chunk_record_defaults_for_range_end_and_packed_end() {
        // When range_end and packed_end are not set, they should default
        // to 1 (range_end) and 0 (packed_end) via serde defaults.
        let json =
            r#"{"hash":"a".repeat(64),"offset":0,"length":4,"range_start":0,"packed_start":0}"#;
        // Build a JSON string with 64 'a' chars
        let hash_64 = "a".repeat(64);
        let json = format!(
            r#"{{"hash":"{hash}","offset":0,"length":4,"range_start":0,"packed_start":0}}"#,
            hash = hash_64
        );
        let chunk: FileChunkRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(chunk.range_end, 1, "range_end should default to 1");
        assert_eq!(chunk.packed_end, 0, "packed_end should default to 0");
    }

    #[test]
    fn file_chunk_record_range_start_defaults_to_zero() {
        let hash_64 = "a".repeat(64);
        let json = format!(
            r#"{{"hash":"{hash}","offset":0,"length":4,"range_end":1,"packed_start":0,"packed_end":4}}"#,
            hash = hash_64
        );
        let chunk: FileChunkRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(chunk.range_start, 0, "range_start should default to 0");
        assert_eq!(chunk.packed_start, 0, "packed_start should default to 0");
    }

    #[test]
    fn file_chunk_record_deserializes_with_all_fields() {
        let hash_64 = "a".repeat(64);
        let json = format!(
            r#"{{"hash":"{hash}","offset":10,"length":100,"range_start":2,"range_end":5,"packed_start":20,"packed_end":120}}"#,
            hash = hash_64
        );
        let chunk: FileChunkRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(chunk.hash, hash_64);
        assert_eq!(chunk.offset, 10);
        assert_eq!(chunk.length, 100);
        assert_eq!(chunk.range_start, 2);
        assert_eq!(chunk.range_end, 5);
        assert_eq!(chunk.packed_start, 20);
        assert_eq!(chunk.packed_end, 120);
    }

    #[test]
    fn default_range_end_returns_one() {
        assert_eq!(super::default_range_end(), 1);
    }

    #[test]
    fn default_packed_end_returns_zero() {
        assert_eq!(super::default_packed_end(), 0);
    }

    #[test]
    fn file_record_reconstruction_plan_rejects_total_bytes_mismatch() {
        let record = FileRecord {
            file_id: "a".repeat(64),
            content_hash: "c".repeat(64),
            total_bytes: 10,
            chunk_size: 0,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: "a".repeat(64),
                offset: 0,
                length: 8,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 8,
            }],
        };
        assert!(matches!(
            record.validate_reconstruction_plan(),
            Err(FileRecordInvariantError::TotalBytesMismatch)
        ));
    }

    #[test]
    fn file_record_reconstruction_plan_accepts_empty_chunks_zero_bytes() {
        let record = FileRecord {
            file_id: "a".repeat(64),
            content_hash: "c".repeat(64),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: None,
            chunks: vec![],
        };
        assert!(record.validate_reconstruction_plan().is_ok());
    }

    #[test]
    fn file_record_invariant_error_from_hash_parse_error() {
        // The #[from] attribute means ? works for HashParseError -> FileRecordInvariantError
        fn convert(err: shardline_protocol::HashParseError) -> FileRecordInvariantError {
            err.into()
        }
        let err = convert(shardline_protocol::HashParseError::InvalidLength);
        assert_eq!(err.to_string(), "file record chunk hash is invalid");
        let err = convert(shardline_protocol::HashParseError::InvalidCharacter(
            "test".to_owned(),
        ));
        assert_eq!(err.to_string(), "file record chunk hash is invalid");
    }

    #[test]
    fn repository_record_scope_new_and_accessors() {
        let scope = RepositoryRecordScope::new(RepositoryProvider::GitHub, "my_owner", "my_repo");
        assert_eq!(scope.provider(), RepositoryProvider::GitHub);
        assert_eq!(scope.owner(), "my_owner");
        assert_eq!(scope.name(), "my_repo");
    }

    // ── Default visit_* trait impls ──────────────────────────────────────────

    use crate::{RecordMutation, RecordStoreFuture, RecordTraversal, StoredRecord};
    use std::{collections::BTreeMap, sync::Mutex, time::Duration};

    /// Minimal store that backs records via BTreeMap but does NOT override any
    /// of the default `visit_*` implementations from the [`RecordTraversal`] trait.
    struct DefaultVisitStore {
        latest: Mutex<BTreeMap<String, (Vec<u8>, Duration)>>,
        version: Mutex<BTreeMap<String, (Vec<u8>, Duration)>>,
    }

    impl DefaultVisitStore {
        fn new() -> Self {
            Self {
                latest: Mutex::new(BTreeMap::new()),
                version: Mutex::new(BTreeMap::new()),
            }
        }

        fn write_latest(&self, key: &str, bytes: Vec<u8>) {
            self.latest
                .lock()
                .unwrap()
                .insert(key.to_owned(), (bytes, Duration::from_secs(42)));
        }

        fn write_version(&self, key: &str, bytes: Vec<u8>) {
            self.version
                .lock()
                .unwrap()
                .insert(key.to_owned(), (bytes, Duration::from_secs(99)));
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct DefaultVisitLocator(String);

    impl RecordTraversal for DefaultVisitStore {
        type Error = std::convert::Infallible;
        type Locator = DefaultVisitLocator;

        fn list_latest_record_locators(
            &self,
        ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
            let keys: Vec<DefaultVisitLocator> = self
                .latest
                .lock()
                .unwrap()
                .keys()
                .cloned()
                .map(DefaultVisitLocator)
                .collect();
            Box::pin(async move { Ok(keys) })
        }

        fn list_repository_latest_record_locators<'operation>(
            &'operation self,
            _repository: &'operation RepositoryRecordScope,
        ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
            // For test simplicity: same as list_latest
            let keys: Vec<DefaultVisitLocator> = self
                .latest
                .lock()
                .unwrap()
                .keys()
                .cloned()
                .map(DefaultVisitLocator)
                .collect();
            Box::pin(async move { Ok(keys) })
        }

        fn list_version_record_locators(
            &self,
        ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
            let keys: Vec<DefaultVisitLocator> = self
                .version
                .lock()
                .unwrap()
                .keys()
                .cloned()
                .map(DefaultVisitLocator)
                .collect();
            Box::pin(async move { Ok(keys) })
        }

        fn list_repository_version_record_locators<'operation>(
            &'operation self,
            _repository: &'operation RepositoryRecordScope,
        ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
            let keys: Vec<DefaultVisitLocator> = self
                .version
                .lock()
                .unwrap()
                .keys()
                .cloned()
                .map(DefaultVisitLocator)
                .collect();
            Box::pin(async move { Ok(keys) })
        }

        fn read_record_bytes<'operation>(
            &'operation self,
            locator: &'operation Self::Locator,
        ) -> RecordStoreFuture<'operation, Vec<u8>, Self::Error> {
            let entry = self
                .latest
                .lock()
                .unwrap()
                .get(&locator.0)
                .cloned()
                .or_else(|| self.version.lock().unwrap().get(&locator.0).cloned());
            Box::pin(async move {
                entry
                    .ok_or_else(|| unreachable!("test locator not found"))
                    .map(|e| e.0)
            })
        }

        fn read_latest_record_bytes<'operation>(
            &'operation self,
            _record: &'operation FileRecord,
        ) -> RecordStoreFuture<'operation, Option<Vec<u8>>, Self::Error> {
            Box::pin(async { Ok(None) })
        }

        fn record_locator_exists<'operation>(
            &'operation self,
            locator: &'operation Self::Locator,
        ) -> RecordStoreFuture<'operation, bool, Self::Error> {
            let exists = self.latest.lock().unwrap().contains_key(&locator.0)
                || self.version.lock().unwrap().contains_key(&locator.0);
            Box::pin(async move { Ok(exists) })
        }

        fn modified_since_epoch<'operation>(
            &'operation self,
            locator: &'operation Self::Locator,
        ) -> RecordStoreFuture<'operation, Duration, Self::Error> {
            let entry = self
                .latest
                .lock()
                .unwrap()
                .get(&locator.0)
                .cloned()
                .or_else(|| self.version.lock().unwrap().get(&locator.0).cloned());
            Box::pin(async move {
                entry
                    .ok_or_else(|| unreachable!("test locator not found"))
                    .map(|e| e.1)
            })
        }

        fn latest_record_locator(&self, _record: &FileRecord) -> Self::Locator {
            DefaultVisitLocator("latest".to_owned())
        }

        fn version_record_locator(&self, _record: &FileRecord) -> Self::Locator {
            DefaultVisitLocator("version".to_owned())
        }
    }

    #[tokio::test]
    async fn default_visit_latest_records_produces_stored_records() {
        let store = DefaultVisitStore::new();
        store.write_latest("k1", b"data1".to_vec());
        store.write_latest("k2", b"data2".to_vec());

        let mut records = Vec::new();
        store
            .visit_latest_records(|stored| {
                assert!(!stored.bytes.is_empty(), "bytes should be present");
                assert!(
                    stored.modified_since_epoch > Duration::ZERO,
                    "modified time should be > 0"
                );
                records.push(stored);
                Ok::<(), std::convert::Infallible>(())
            })
            .await
            .unwrap();

        assert_eq!(records.len(), 2);
        // Verify the two records have distinct locators
        assert_ne!(records[0].locator, records[1].locator);
    }

    #[tokio::test]
    async fn default_visit_version_records_produces_stored_records() {
        let store = DefaultVisitStore::new();
        store.write_version("v1", b"version1".to_vec());

        let mut records = Vec::new();
        store
            .visit_version_records(|stored| {
                assert_eq!(stored.bytes, b"version1");
                records.push(stored);
                Ok::<(), std::convert::Infallible>(())
            })
            .await
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].locator, DefaultVisitLocator("v1".to_owned()));
    }

    #[tokio::test]
    async fn default_visit_repository_latest_records_produces_stored_records() {
        let store = DefaultVisitStore::new();
        store.write_latest("r1", b"repo-data".to_vec());

        let repo = RepositoryRecordScope::new(RepositoryProvider::GitHub, "owner", "repo");
        let mut records = Vec::new();
        store
            .visit_repository_latest_records(&repo, |stored| {
                records.push(stored);
                Ok::<(), std::convert::Infallible>(())
            })
            .await
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].bytes, b"repo-data");
    }

    #[tokio::test]
    async fn default_visit_repository_version_records_produces_stored_records() {
        let store = DefaultVisitStore::new();
        store.write_version("rv1", b"repo-version".to_vec());

        let repo = RepositoryRecordScope::new(RepositoryProvider::GitLab, "group", "project");
        let mut records = Vec::new();
        store
            .visit_repository_version_records(&repo, |stored| {
                records.push(stored);
                Ok::<(), std::convert::Infallible>(())
            })
            .await
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].bytes, b"repo-version");
    }

    #[tokio::test]
    async fn default_visit_latest_record_locators_returns_locators() {
        let store = DefaultVisitStore::new();
        store.write_latest("a", vec![]);
        store.write_latest("b", vec![]);

        let mut locs = Vec::new();
        store
            .visit_latest_record_locators(|loc| {
                locs.push(loc);
                Ok::<(), std::convert::Infallible>(())
            })
            .await
            .unwrap();

        assert_eq!(locs.len(), 2);
        assert!(locs.contains(&DefaultVisitLocator("a".to_owned())));
        assert!(locs.contains(&DefaultVisitLocator("b".to_owned())));
    }

    #[tokio::test]
    async fn default_visit_version_record_locators_returns_locators() {
        let store = DefaultVisitStore::new();
        store.write_version("x", vec![]);

        let mut locs = Vec::new();
        store
            .visit_version_record_locators(|loc| {
                locs.push(loc);
                Ok::<(), std::convert::Infallible>(())
            })
            .await
            .unwrap();

        assert_eq!(locs, vec![DefaultVisitLocator("x".to_owned())]);
    }

    #[tokio::test]
    async fn default_visit_repository_latest_record_locators_returns_locators() {
        let store = DefaultVisitStore::new();
        store.write_latest("r1", vec![]);

        let repo = RepositoryRecordScope::new(RepositoryProvider::GitHub, "o", "r");
        let mut locs = Vec::new();
        store
            .visit_repository_latest_record_locators(&repo, |loc| {
                locs.push(loc);
                Ok::<(), std::convert::Infallible>(())
            })
            .await
            .unwrap();

        assert_eq!(locs.len(), 1);
    }

    #[tokio::test]
    async fn default_visit_repository_version_record_locators_returns_locators() {
        let store = DefaultVisitStore::new();
        store.write_version("v1", vec![]);

        let repo = RepositoryRecordScope::new(RepositoryProvider::GitLab, "g", "p");
        let mut locs = Vec::new();
        store
            .visit_repository_version_record_locators(&repo, |loc| {
                locs.push(loc);
                Ok::<(), std::convert::Infallible>(())
            })
            .await
            .unwrap();

        assert_eq!(locs.len(), 1);
    }

    #[tokio::test]
    async fn default_visit_latest_records_empty_store_does_not_call_visitor() {
        let store = DefaultVisitStore::new();
        let mut called = false;
        store
            .visit_latest_records(|_| {
                called = true;
                Ok::<(), std::convert::Infallible>(())
            })
            .await
            .unwrap();
        assert!(!called);
    }

    #[tokio::test]
    async fn default_visit_version_records_empty_store_does_not_call_visitor() {
        let store = DefaultVisitStore::new();
        let mut called = false;
        store
            .visit_version_records(|_| {
                called = true;
                Ok::<(), std::convert::Infallible>(())
            })
            .await
            .unwrap();
        assert!(!called);
    }

    #[tokio::test]
    async fn file_record_validate_rejects_invalid_chunk_hash() {
        let record = FileRecord {
            file_id: "a".repeat(64),
            content_hash: "c".repeat(64),
            total_bytes: 1,
            chunk_size: 0,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: "not-a-valid-hex-string".to_owned(),
                offset: 0,
                length: 1,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 1,
            }],
        };
        let result = record.validate_reconstruction_plan();
        assert!(result.is_err());
        // The error should wrap a HashParseError via #[from]
        match result {
            Err(FileRecordInvariantError::ChunkHash(_)) => {} // expected
            other => panic!("Expected ChunkHash error, got: {other:?}"),
        }
    }
}
