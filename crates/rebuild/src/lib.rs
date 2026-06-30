#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string
    )
)]

//! Index rebuild logic for the Shardline server ecosystem.
//!
//! This crate provides pure rebuild functions that operate on explicit
//! store parameters rather than server configuration.

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    io,
    num::TryFromIntError,
};

use serde_json::Error as JsonError;
use shardline_index::{
    AsyncIndexStore, DedupeShardMapping, FileId, LocalIndexStoreError, MemoryIndexStoreError,
    MemoryRecordStoreError, PostgresMetadataStoreError, RecordStore, parse_xet_hash_hex,
    xet_hash_hex_string,
};
use shardline_protocol::HashParseError;
use shardline_server_core::{
    InvalidSerializedShardError, OpsRecordStore, ServerObjectStore, ServerObjectStoreError,
    ShardMetadataLimits, checked_increment, read_full_object,
};
use shardline_storage::{
    LocalObjectStoreError, ObjectPrefix, ObjectPrefixError, S3ObjectStoreError,
};
use shardline_xet_adapter::{XetAdapterError, retained_shard_chunk_hashes};
use thiserror::Error;

mod candidates;
use candidates::{VersionCandidate, collect_candidate};

/// Rebuild operation failure.
#[derive(Debug, Error)]
pub enum RebuildError {
    /// A local filesystem I/O error occurred.
    #[error("local storage operation failed")]
    Io(#[from] io::Error),
    /// JSON serialization or deserialization failed.
    #[error("json operation failed")]
    Json(#[from] JsonError),
    /// Numeric conversion exceeded supported bounds.
    #[error("numeric conversion exceeded supported bounds")]
    NumericConversion(#[from] TryFromIntError),
    /// A content hash was malformed.
    #[error("content hash must be 64 hexadecimal characters")]
    InvalidContentHash,
    /// A file identifier was unsafe.
    #[error(
        "file identifier must be relative and must not contain traversal or control characters"
    )]
    InvalidFileId,
    /// Arithmetic overflowed a checked bound.
    #[error("arithmetic overflow")]
    Overflow,
    /// Object inventory prefix validation failed.
    #[error("object storage prefix validation failed")]
    ObjectPrefix(#[from] ObjectPrefixError),
    /// Local storage adapter access failed.
    #[error("local storage adapter operation failed")]
    LocalObjectStore(#[from] LocalObjectStoreError),
    /// S3-compatible object-storage adapter access failed.
    #[error("s3 object storage adapter operation failed")]
    S3ObjectStore(#[from] S3ObjectStoreError),
    /// Xet adapter access failed.
    #[error("xet adapter operation failed")]
    XetAdapter(#[from] XetAdapterError),
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
    /// Hash parsing failed.
    #[error("hash parsing failed")]
    HashParse(#[from] HashParseError),
    /// Stored file metadata exceeded the bounded parser ceiling.
    #[error("stored file metadata exceeded the bounded parser ceiling")]
    StoredFileMetadataTooLarge {
        /// Observed file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted file length in bytes.
        maximum_bytes: u64,
    },
}

impl From<shardline_server_core::ParseStoredFileRecordError> for RebuildError {
    fn from(value: shardline_server_core::ParseStoredFileRecordError) -> Self {
        match value {
            shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            } => Self::StoredFileMetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            },
            shardline_server_core::ParseStoredFileRecordError::Json(e) => Self::Json(e),
        }
    }
}

impl From<shardline_server_core::ValidateIdentifierError> for RebuildError {
    fn from(_: shardline_server_core::ValidateIdentifierError) -> Self {
        Self::InvalidFileId
    }
}

impl From<shardline_server_core::ValidateContentHashError> for RebuildError {
    fn from(_: shardline_server_core::ValidateContentHashError) -> Self {
        Self::InvalidContentHash
    }
}

impl From<shardline_server_core::RebuildOverflowError> for RebuildError {
    fn from(_: shardline_server_core::RebuildOverflowError) -> Self {
        Self::Overflow
    }
}

impl From<InvalidSerializedShardError> for RebuildError {
    fn from(value: InvalidSerializedShardError) -> Self {
        Self::XetAdapter(XetAdapterError::InvalidSerializedShard(value))
    }
}

impl From<ServerObjectStoreError> for RebuildError {
    fn from(value: ServerObjectStoreError) -> Self {
        match value {
            ServerObjectStoreError::NotFound => Self::Overflow,
            ServerObjectStoreError::Overflow => Self::Overflow,
            ServerObjectStoreError::InvalidContentHash => Self::Overflow,
            ServerObjectStoreError::StoredObjectLengthMismatch => Self::Overflow,
            ServerObjectStoreError::Local(e) => Self::LocalObjectStore(e),
            ServerObjectStoreError::S3(e) => Self::S3ObjectStore(e),
            ServerObjectStoreError::Io(e) => Self::Io(e),
            ServerObjectStoreError::NumericConversion(e) => Self::NumericConversion(e),
        }
    }
}

/// Index-rebuild report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRebuildReport {
    /// Number of version records scanned through the configured record store.
    pub scanned_version_records: u64,
    /// Number of retained shard objects scanned through the object-store adapter.
    pub scanned_retained_shards: u64,
    /// Number of latest records recreated or updated through the configured record store.
    pub rebuilt_latest_records: u64,
    /// Number of latest records that already matched the rebuilt head.
    pub unchanged_latest_records: u64,
    /// Number of stale latest records removed because no version record remained.
    pub removed_stale_latest_records: u64,
    /// Number of reconstruction rows inspected through the index adapter.
    pub scanned_reconstructions: u64,
    /// Number of reconstruction rows still backed by immutable version records.
    pub unchanged_reconstructions: u64,
    /// Number of stale reconstruction rows removed because no version record remained.
    pub removed_stale_reconstructions: u64,
    /// Number of dedupe-shard mappings inserted or updated.
    pub rebuilt_dedupe_shard_mappings: u64,
    /// Number of dedupe-shard mappings that already matched the rebuilt view.
    pub unchanged_dedupe_shard_mappings: u64,
    /// Number of stale dedupe-shard mappings removed because no retained shard contained them.
    pub removed_stale_dedupe_shard_mappings: u64,
    /// Collected non-fatal rebuild issues.
    pub issues: Vec<IndexRebuildIssue>,
}

impl IndexRebuildReport {
    /// Returns the total issue count.
    #[must_use]
    pub const fn issue_count(&self) -> usize {
        self.issues.len()
    }

    /// Returns whether the rebuild completed without non-fatal issues.
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        self.issues.is_empty()
    }
}

/// One index-rebuild issue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRebuildIssue {
    /// Problem classification.
    pub kind: IndexRebuildIssueKind,
    /// Stable record location associated with the issue.
    pub location: String,
    /// Structured detail for operators.
    pub detail: IndexRebuildIssueDetail,
}

/// Index-rebuild issue detail.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum IndexRebuildIssueDetail {
    /// Version-record metadata exceeded the bounded parser ceiling.
    #[error("record metadata exceeded the bounded parser ceiling")]
    OversizedVersionRecordMetadata,
    /// Version-record JSON was invalid.
    #[error("record json was invalid")]
    RecordJsonInvalid,
    /// The record file identifier was invalid.
    #[error("record file_id `{file_id}` is invalid")]
    InvalidFileId {
        /// Invalid file identifier.
        file_id: String,
    },
    /// The record content hash was invalid.
    #[error("record content hash `{content_hash}` is invalid")]
    InvalidContentHash {
        /// Invalid content hash.
        content_hash: String,
    },
    /// The repository scope failed validation.
    #[error("record repository scope is invalid")]
    InvalidRepositoryScope,
    /// The version record was stored at an unexpected location.
    #[error("expected version record at {expected_locator}")]
    VersionPathMismatch {
        /// Expected version-record locator.
        expected_locator: String,
    },
    /// The record reconstruction plan was invalid.
    #[error("{0}")]
    InvalidReconstructionPlan(IndexRebuildReconstructionPlanDetail),
    /// The retained shard was invalid.
    #[error("{0}")]
    InvalidRetainedShard(InvalidSerializedShardError),
}

/// Index-rebuild reconstruction-plan issue detail.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum IndexRebuildReconstructionPlanDetail {
    /// A chunk hash was invalid.
    #[error("record chunk hash is invalid")]
    ChunkHashInvalid,
    /// A chunk was empty.
    #[error("record contains an empty chunk")]
    EmptyChunk,
    /// Chunks were not contiguous.
    #[error("record chunks are not contiguous")]
    NonContiguousChunkOffsets,
    /// A chunk range was invalid.
    #[error("record chunk range is invalid")]
    InvalidChunkRange,
    /// A packed range was invalid.
    #[error("record packed range is invalid")]
    InvalidPackedRange,
    /// Record length overflowed.
    #[error("record length overflowed")]
    LengthOverflow,
    /// Total byte count did not match chunks.
    #[error("record total byte count did not match chunks")]
    TotalBytesMismatch,
}

/// Index-rebuild issue kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexRebuildIssueKind {
    /// Version-record metadata exceeded the bounded parser ceiling.
    OversizedVersionRecordMetadata,
    /// Version-record bytes were not valid JSON.
    InvalidVersionRecordJson,
    /// Version-record file identifier was invalid.
    InvalidVersionFileId,
    /// Version-record content hash was invalid.
    InvalidVersionContentHash,
    /// Version-record repository scope was invalid.
    InvalidVersionRepositoryScope,
    /// Version record was stored at an unexpected path.
    VersionPathMismatch,
    /// Version record could not produce a valid reconstruction plan.
    InvalidVersionReconstructionPlan,
    /// Retained shard object could not be parsed as a native Xet shard.
    InvalidRetainedShard,
}

impl IndexRebuildIssueKind {
    /// Stable issue label for CLI and logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OversizedVersionRecordMetadata => "oversized_version_record_metadata",
            Self::InvalidVersionRecordJson => "invalid_version_record_json",
            Self::InvalidVersionFileId => "invalid_version_file_id",
            Self::InvalidVersionContentHash => "invalid_version_content_hash",
            Self::InvalidVersionRepositoryScope => "invalid_version_repository_scope",
            Self::VersionPathMismatch => "version_path_mismatch",
            Self::InvalidVersionReconstructionPlan => "invalid_version_reconstruction_plan",
            Self::InvalidRetainedShard => "invalid_retained_shard",
        }
    }
}

/// Backward-compatible local index-rebuild report alias.
pub type LocalIndexRebuildReport = IndexRebuildReport;

/// Backward-compatible local index-rebuild issue alias.
pub type LocalIndexRebuildIssue = IndexRebuildIssue;

/// Backward-compatible local index-rebuild issue-kind alias.
pub type LocalIndexRebuildIssueKind = IndexRebuildIssueKind;

/// Rebuilds latest-record state from immutable version records.
///
/// # Errors
///
/// Returns [`RebuildError`] when version records cannot be scanned or latest records
/// cannot be written or removed.
pub async fn run_index_rebuild_with_stores<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    shard_metadata_limits: ShardMetadataLimits,
) -> Result<IndexRebuildReport, RebuildError>
where
    RecordAdapter: OpsRecordStore + Sync,
    RecordAdapter::Error: Into<RebuildError>,
    RecordAdapter::Locator: Hash,
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<RebuildError>,
{
    let mut report = IndexRebuildReport {
        scanned_version_records: 0,
        scanned_retained_shards: 0,
        rebuilt_latest_records: 0,
        unchanged_latest_records: 0,
        removed_stale_latest_records: 0,
        scanned_reconstructions: 0,
        unchanged_reconstructions: 0,
        removed_stale_reconstructions: 0,
        rebuilt_dedupe_shard_mappings: 0,
        unchanged_dedupe_shard_mappings: 0,
        removed_stale_dedupe_shard_mappings: 0,
        issues: Vec::new(),
    };
    let mut candidates = HashMap::new();
    RecordStore::visit_version_records(record_store, |entry| {
        report.scanned_version_records = checked_increment(report.scanned_version_records)?;
        collect_candidate(record_store, entry, &mut candidates, &mut report)
    })
    .await?;

    let mut desired_latest_paths = HashSet::new();
    for candidate in candidates.values() {
        let latest_path = RecordStore::latest_record_locator(record_store, &candidate.record);
        desired_latest_paths.insert(latest_path.clone());

        let record_bytes = serde_json::to_vec(&candidate.record)?;
        let existing_bytes = RecordStore::read_latest_record_bytes(record_store, &candidate.record)
            .await
            .map_err(Into::into)?;

        if existing_bytes.as_deref() == Some(record_bytes.as_slice()) {
            report.unchanged_latest_records = checked_increment(report.unchanged_latest_records)?;
            continue;
        }

        RecordStore::write_latest_record(record_store, &candidate.record)
            .await
            .map_err(Into::into)?;
        report.rebuilt_latest_records = checked_increment(report.rebuilt_latest_records)?;
    }

    let mut stale_latest_paths = Vec::new();
    RecordStore::visit_latest_record_locators(record_store, |path| {
        if !desired_latest_paths.contains(&path) {
            stale_latest_paths.push(path);
        }

        Ok::<(), RebuildError>(())
    })
    .await?;
    for path in stale_latest_paths {
        RecordStore::delete_record_locator(record_store, &path)
            .await
            .map_err(Into::into)?;
        report.removed_stale_latest_records =
            checked_increment(report.removed_stale_latest_records)?;
    }

    RecordStore::prune_empty_latest_records(record_store)
        .await
        .map_err(Into::into)?;

    let desired_reconstructions = desired_reconstruction_file_ids(candidates.values());
    prune_stale_reconstructions(index_store, &desired_reconstructions, &mut report).await?;

    rebuild_dedupe_shard_mappings(
        index_store,
        object_store,
        shard_metadata_limits,
        &mut report,
    )
    .await?;

    Ok(report)
}

fn desired_reconstruction_file_ids<'record, Locator, Records>(records: Records) -> HashSet<String>
where
    Records: IntoIterator<Item = &'record VersionCandidate<Locator>>,
    Locator: 'record,
{
    records
        .into_iter()
        .filter_map(|candidate| {
            parse_xet_hash_hex(&candidate.record.file_id)
                .ok()
                .map(xet_hash_hex_string)
        })
        .collect::<HashSet<_>>()
}

async fn prune_stale_reconstructions<IndexAdapter>(
    index_store: &IndexAdapter,
    desired_reconstructions: &HashSet<String>,
    report: &mut IndexRebuildReport,
) -> Result<(), RebuildError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<RebuildError>,
{
    if !report.is_clean() {
        return Ok(());
    }

    let existing_file_ids = index_store
        .list_reconstruction_file_ids()
        .await
        .map_err(Into::into)?;
    for file_id in existing_file_ids {
        report.scanned_reconstructions = checked_increment(report.scanned_reconstructions)?;
        let file_id_hex = xet_hash_hex_string(file_id.hash());
        if desired_reconstructions.contains(&file_id_hex) {
            report.unchanged_reconstructions = checked_increment(report.unchanged_reconstructions)?;
            continue;
        }

        delete_reconstruction(index_store, &file_id).await?;
        report.removed_stale_reconstructions =
            checked_increment(report.removed_stale_reconstructions)?;
    }

    Ok(())
}

async fn delete_reconstruction<IndexAdapter>(
    index_store: &IndexAdapter,
    file_id: &FileId,
) -> Result<(), RebuildError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<RebuildError>,
{
    let _deleted = index_store
        .delete_reconstruction(file_id)
        .await
        .map_err(Into::into)?;
    Ok(())
}

async fn rebuild_dedupe_shard_mappings<IndexAdapter>(
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    shard_metadata_limits: ShardMetadataLimits,
    report: &mut IndexRebuildReport,
) -> Result<(), RebuildError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<RebuildError>,
{
    let prefix =
        ObjectPrefix::parse("shards/").map_err(|_error| RebuildError::InvalidContentHash)?;
    let mut desired = HashMap::<String, DedupeShardMapping>::new();
    let issue_count_before_scan = report.issue_count();

    object_store.visit_prefix(&prefix, |metadata| -> Result<(), RebuildError> {
        report.scanned_retained_shards = checked_increment(report.scanned_retained_shards)?;
        let shard_key = metadata.key().clone();
        let shard_location = shard_key.as_str().to_owned();
        let shard_bytes = read_full_object(object_store, &shard_key, metadata.length())
            .map_err(RebuildError::from)?;
        let chunk_hashes = match retained_shard_chunk_hashes(&shard_bytes, shard_metadata_limits) {
            Ok(chunk_hashes) => chunk_hashes,
            Err(XetAdapterError::InvalidSerializedShard(detail)) => {
                push_issue(
                    report,
                    IndexRebuildIssueKind::InvalidRetainedShard,
                    shard_location,
                    IndexRebuildIssueDetail::InvalidRetainedShard(detail),
                )?;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };

        for chunk_hash_hex in chunk_hashes {
            let mapping =
                DedupeShardMapping::new(parse_xet_hash_hex(&chunk_hash_hex)?, shard_key.clone());
            match desired.get(&chunk_hash_hex) {
                Some(existing)
                    if existing.shard_object_key().as_str()
                        <= mapping.shard_object_key().as_str() => {}
                _ => {
                    desired.insert(chunk_hash_hex, mapping);
                }
            }
        }
        Ok(())
    })?;

    if report.issue_count() != issue_count_before_scan {
        return Ok(());
    }

    let mut existing = HashMap::new();
    index_store
        .visit_dedupe_shard_mappings(|mapping| {
            existing.insert(xet_hash_hex_string(mapping.chunk_hash()), mapping);
            Ok::<(), RebuildError>(())
        })
        .await?;

    for (chunk_hash_hex, mapping) in &desired {
        match existing.get(chunk_hash_hex) {
            Some(existing_mapping)
                if existing_mapping.shard_object_key() == mapping.shard_object_key() =>
            {
                report.unchanged_dedupe_shard_mappings =
                    checked_increment(report.unchanged_dedupe_shard_mappings)?;
            }
            _ => {
                index_store
                    .upsert_dedupe_shard_mapping(mapping)
                    .await
                    .map_err(Into::into)?;
                report.rebuilt_dedupe_shard_mappings =
                    checked_increment(report.rebuilt_dedupe_shard_mappings)?;
            }
        }
    }

    for (chunk_hash_hex, _mapping) in existing {
        if desired.contains_key(&chunk_hash_hex) {
            continue;
        }

        let chunk_hash = parse_xet_hash_hex(&chunk_hash_hex)?;
        let _deleted = index_store
            .delete_dedupe_shard_mapping(&chunk_hash)
            .await
            .map_err(Into::into)?;
        report.removed_stale_dedupe_shard_mappings =
            checked_increment(report.removed_stale_dedupe_shard_mappings)?;
    }

    Ok(())
}

fn push_issue(
    report: &mut IndexRebuildReport,
    kind: IndexRebuildIssueKind,
    location: String,
    detail: IndexRebuildIssueDetail,
) -> Result<(), RebuildError> {
    let _count = u64::try_from(report.issues.len())?;
    report.issues.push(IndexRebuildIssue {
        kind,
        location,
        detail,
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_report() -> IndexRebuildReport {
        IndexRebuildReport {
            scanned_version_records: 0,
            scanned_retained_shards: 0,
            rebuilt_latest_records: 0,
            unchanged_latest_records: 0,
            removed_stale_latest_records: 0,
            scanned_reconstructions: 0,
            unchanged_reconstructions: 0,
            removed_stale_reconstructions: 0,
            rebuilt_dedupe_shard_mappings: 0,
            unchanged_dedupe_shard_mappings: 0,
            removed_stale_dedupe_shard_mappings: 0,
            issues: Vec::new(),
        }
    }

    #[test]
    fn report_is_clean_when_no_issues() {
        let report = empty_report();
        assert!(report.is_clean());
        assert_eq!(report.issue_count(), 0);
    }

    #[test]
    fn report_is_not_clean_with_issues() {
        let mut report = empty_report();
        report.issues.push(IndexRebuildIssue {
            kind: IndexRebuildIssueKind::InvalidVersionRecordJson,
            location: "test/path".to_owned(),
            detail: IndexRebuildIssueDetail::RecordJsonInvalid,
        });
        assert!(!report.is_clean());
        assert_eq!(report.issue_count(), 1);
    }

    #[test]
    fn issue_count_matches_vec_len() {
        let mut report = empty_report();
        for i in 0..5 {
            report.issues.push(IndexRebuildIssue {
                kind: IndexRebuildIssueKind::InvalidVersionFileId,
                location: format!("loc/{i}"),
                detail: IndexRebuildIssueDetail::InvalidFileId {
                    file_id: format!("fid-{i}"),
                },
            });
        }
        assert_eq!(report.issue_count(), 5);
    }

    #[test]
    fn index_rebuild_report_equality() {
        let a = empty_report();
        let mut b = empty_report();
        b.scanned_version_records = 42;
        assert_ne!(a, b);

        b.scanned_version_records = 0;
        assert_eq!(a, b);
    }

    #[test]
    fn issue_kind_as_str_returns_expected_labels() {
        assert_eq!(
            IndexRebuildIssueKind::OversizedVersionRecordMetadata.as_str(),
            "oversized_version_record_metadata"
        );
        assert_eq!(
            IndexRebuildIssueKind::InvalidVersionRecordJson.as_str(),
            "invalid_version_record_json"
        );
        assert_eq!(
            IndexRebuildIssueKind::InvalidVersionFileId.as_str(),
            "invalid_version_file_id"
        );
        assert_eq!(
            IndexRebuildIssueKind::InvalidVersionContentHash.as_str(),
            "invalid_version_content_hash"
        );
        assert_eq!(
            IndexRebuildIssueKind::InvalidVersionRepositoryScope.as_str(),
            "invalid_version_repository_scope"
        );
        assert_eq!(
            IndexRebuildIssueKind::VersionPathMismatch.as_str(),
            "version_path_mismatch"
        );
        assert_eq!(
            IndexRebuildIssueKind::InvalidVersionReconstructionPlan.as_str(),
            "invalid_version_reconstruction_plan"
        );
        assert_eq!(
            IndexRebuildIssueKind::InvalidRetainedShard.as_str(),
            "invalid_retained_shard"
        );
    }

    #[test]
    fn issue_detail_display_messages() {
        let detail = IndexRebuildIssueDetail::OversizedVersionRecordMetadata;
        assert!(!detail.to_string().is_empty());

        let detail = IndexRebuildIssueDetail::RecordJsonInvalid;
        assert!(!detail.to_string().is_empty());

        let detail = IndexRebuildIssueDetail::InvalidFileId {
            file_id: "bad-id".to_owned(),
        };
        assert!(detail.to_string().contains("bad-id"));

        let detail = IndexRebuildIssueDetail::InvalidContentHash {
            content_hash: "abc123".to_owned(),
        };
        assert!(detail.to_string().contains("abc123"));

        let detail = IndexRebuildIssueDetail::InvalidRepositoryScope;
        assert!(!detail.to_string().is_empty());

        let detail = IndexRebuildIssueDetail::VersionPathMismatch {
            expected_locator: "/expected/path".to_owned(),
        };
        assert!(detail.to_string().contains("/expected/path"));

        let detail =
            IndexRebuildIssueDetail::InvalidReconstructionPlan(IndexRebuildReconstructionPlanDetail::ChunkHashInvalid);
        assert!(!detail.to_string().is_empty());

        let detail =
            IndexRebuildIssueDetail::InvalidRetainedShard(shardline_server_core::InvalidSerializedShardError::ParserRejectedMetadata);
        assert!(!detail.to_string().is_empty());
    }

    #[test]
    fn push_issue_increments_count() {
        let mut report = empty_report();
        push_issue(
            &mut report,
            IndexRebuildIssueKind::InvalidVersionFileId,
            "loc".to_owned(),
            IndexRebuildIssueDetail::InvalidFileId {
                file_id: "x".to_owned(),
            },
        )
        .unwrap();
        assert_eq!(report.issue_count(), 1);
    }
}
