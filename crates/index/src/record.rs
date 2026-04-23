use std::{future::Future, pin::Pin, time::Duration};

use serde::{Deserialize, Serialize};
use shardline_protocol::{HashParseError, RepositoryProvider, RepositoryScope, ShardlineHash};
use thiserror::Error;

/// Boxed asynchronous record-store operation.
pub type RecordStoreFuture<'operation, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'operation>>;

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
    /// Start chunk index inside the referenced xorb.
    #[serde(default)]
    pub range_start: u32,
    /// End-exclusive chunk index inside the referenced xorb.
    #[serde(default = "default_range_end")]
    pub range_end: u32,
    /// Inclusive start byte for the serialized xorb range that covers this term.
    #[serde(default)]
    pub packed_start: u64,
    /// Exclusive end byte for the serialized xorb range that covers this term.
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
    /// Chunk size used for this upload. Shard-backed records may use zero.
    pub chunk_size: u64,
    /// Optional repository namespace for provider-backed storage.
    pub repository_scope: Option<RepositoryScope>,
    /// Ordered chunks needed to reconstruct the file.
    pub chunks: Vec<FileChunkRecord>,
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
    /// Validates the invariants required to build a deterministic reconstruction plan.
    ///
    /// # Errors
    ///
    /// Returns [`FileRecordInvariantError`] when chunk hashes, offsets, lengths, or
    /// xorb byte ranges are malformed.
    pub fn validate_reconstruction_plan(&self) -> Result<(), FileRecordInvariantError> {
        let mut expected_offset = 0_u64;
        for chunk in &self.chunks {
            ShardlineHash::parse_api_hex(&chunk.hash)?;
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
#[derive(Debug, Clone, Error, PartialEq, Eq)]
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
    /// A packed xorb byte range was empty or inverted.
    #[error("file record packed byte range must be non-empty and ordered")]
    InvalidPackedRange,
    /// Logical chunk lengths overflowed the supported integer range.
    #[error("file record chunk lengths overflowed")]
    LengthOverflow,
    /// The chunk length sum did not match the record total byte length.
    #[error("file record total bytes did not match chunk lengths")]
    TotalBytesMismatch,
}

/// Durable file-record adapter contract.
pub trait RecordStore {
    /// Adapter-specific failure type.
    type Error;
    /// Adapter-specific record locator.
    type Locator: Clone + Eq + Ord + Send + Sync;

    /// Lists visible latest-file record locators.
    fn list_latest_record_locators(&self)
    -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error>;

    /// Visits visible latest-file record locators.
    fn visit_latest_record_locators<'operation, Visitor, VisitorError>(
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
            for locator in self
                .list_latest_record_locators()
                .await
                .map_err(Into::into)?
            {
                visitor(locator)?;
            }

            Ok(())
        })
    }

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

    /// Visits visible latest-file record locators for one repository across all revisions.
    fn visit_repository_latest_record_locators<'operation, Visitor, VisitorError>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(Self::Locator) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            for locator in self
                .list_repository_latest_record_locators(repository)
                .await
                .map_err(Into::into)?
            {
                visitor(locator)?;
            }

            Ok(())
        })
    }

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

    /// Visits immutable version-record locators.
    fn visit_version_record_locators<'operation, Visitor, VisitorError>(
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
            for locator in self
                .list_version_record_locators()
                .await
                .map_err(Into::into)?
            {
                visitor(locator)?;
            }

            Ok(())
        })
    }

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

    /// Visits immutable version-record locators for one repository across all revisions.
    fn visit_repository_version_record_locators<'operation, Visitor, VisitorError>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(Self::Locator) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            for locator in self
                .list_repository_version_record_locators(repository)
                .await
                .map_err(Into::into)?
            {
                visitor(locator)?;
            }

            Ok(())
        })
    }

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

    /// Returns whether a record locator exists.
    fn record_locator_exists<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, bool, Self::Error>;

    /// Removes empty latest-record containers after stale record deletion.
    fn prune_empty_latest_records(&self) -> RecordStoreFuture<'_, (), Self::Error>;

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

#[cfg(test)]
mod tests {
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    use super::{FileChunkRecord, FileRecord, FileRecordInvariantError};

    #[test]
    fn file_record_preserves_repository_scope_and_chunk_order() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", Some("main"));
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
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

        assert_eq!(record.validate_reconstruction_plan(), Ok(()));
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

        assert_eq!(
            record.validate_reconstruction_plan(),
            Err(FileRecordInvariantError::NonContiguousChunkOffsets)
        );
    }
}
