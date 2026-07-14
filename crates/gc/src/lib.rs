#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string
    )
)]

//! Garbage collection for Shardline chunk storage.
//!
//! This crate provides local filesystem garbage collection including orphan
//! chunk discovery, quarantine lifecycle management, and retention window
//! enforcement.

use std::{
    collections::{HashMap, HashSet},
    io::Error as IoError,
    num::TryFromIntError,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use shardline_index::{
    AsyncIndexStore, FileRecordInvariantError, LocalIndexStore, LocalIndexStoreError,
    MemoryIndexStoreError, MemoryRecordStoreError, PostgresMetadataStoreError, QuarantineCandidate,
    QuarantineCandidateError, RecordStore, RetentionHoldError, WebhookDeliveryError,
};
use shardline_protocol::unix_now_seconds_lossy;
use shardline_server_core::{
    InvalidLifecycleMetadataError, ServerObjectStore, ServerObjectStoreError,
    server_frontend::ServerFrontend,
};
use shardline_storage::{
    LocalObjectStoreError, ObjectPrefixError, ObjectStore, S3ObjectStoreError,
};
use shardline_xet_adapter::XetAdapterError;
use thiserror::Error;

mod dispatch;
mod quarantine;
mod reachability;

use quarantine::{
    read_active_retention_hold_object_keys, read_quarantine_entries, reconcile_quarantine_entries,
    sweep_quarantine_entries,
};
use reachability::{
    OrphanObject, ReachabilityAccumulator, collect_referenced_object_keys,
    managed_object_hash_or_object_key, scan_orphan_objects,
};

pub use shardline_server_core::server_frontend::{
    ServerFrontend as ServerFrontendKind, ServerFrontendParseError,
};

/// Garbage collection runtime failure.
#[derive(Debug, Error)]
pub enum GcError {
    /// Local storage IO failed.
    #[error("local storage operation failed")]
    Io(#[from] IoError),
    /// JSON serialization or deserialization failed.
    #[error("json operation failed")]
    Json(#[from] serde_json::Error),
    /// Numeric conversion exceeded supported bounds.
    #[error("numeric conversion exceeded supported bounds")]
    NumericConversion(#[from] TryFromIntError),
    /// Object-storage adapter access failed.
    #[error("object storage adapter operation failed")]
    ObjectStore(#[from] ServerObjectStoreError),
    /// Local object-storage adapter access failed.
    #[error("local object storage operation failed")]
    LocalObjectStore(#[from] LocalObjectStoreError),
    /// S3-compatible object-storage adapter access failed.
    #[error("s3 object storage operation failed")]
    S3ObjectStore(#[from] S3ObjectStoreError),
    /// Object inventory prefix validation failed.
    #[error("object storage prefix validation failed")]
    ObjectPrefix(#[from] ObjectPrefixError),
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
    /// Retention hold input was invalid.
    #[error("retention hold input was invalid")]
    RetentionHold(#[from] RetentionHoldError),
    /// Quarantine candidate input was invalid.
    #[error("quarantine candidate input was invalid")]
    QuarantineCandidate(#[from] QuarantineCandidateError),
    /// Webhook delivery metadata was invalid.
    #[error("webhook delivery metadata was invalid")]
    WebhookDelivery(#[from] WebhookDeliveryError),
    /// Stored file metadata could not produce a valid reconstruction plan.
    #[error("stored file metadata was invalid")]
    FileRecordInvariant(#[from] FileRecordInvariantError),
    /// Lifecycle metadata was internally inconsistent for a mutating operator workflow.
    #[error("lifecycle metadata was internally inconsistent")]
    InvalidLifecycleMetadata(#[from] InvalidLifecycleMetadataError),
    /// A content hash was malformed.
    #[error("content hash must be 64 hexadecimal characters")]
    InvalidContentHash,
    /// Arithmetic overflowed a checked bound.
    #[error("arithmetic overflow")]
    Overflow,
    /// Xet adapter operation failed.
    #[error("xet adapter operation failed")]
    XetAdapter(#[from] XetAdapterError),
}

impl From<shardline_server_core::ParseStoredFileRecordError> for GcError {
    fn from(err: shardline_server_core::ParseStoredFileRecordError) -> Self {
        match err {
            shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            } => Self::Io(IoError::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "stored file metadata exceeded the bounded parser ceiling: {observed_bytes} > {maximum_bytes}"
                ),
            )),
            shardline_server_core::ParseStoredFileRecordError::Json(e) => Self::Json(e),
        }
    }
}

impl From<shardline_server_core::RebuildOverflowError> for GcError {
    fn from(_: shardline_server_core::RebuildOverflowError) -> Self {
        Self::Overflow
    }
}

impl From<GcError> for shardline_server_core::ServerObjectStoreError {
    fn from(err: GcError) -> Self {
        match err {
            GcError::ObjectStore(e) => e,
            GcError::LocalObjectStore(e) => Self::Local(e),
            GcError::S3ObjectStore(e) => Self::S3(e),
            GcError::Io(e) => Self::Io(e),
            GcError::NumericConversion(e) => Self::NumericConversion(e),
            GcError::InvalidContentHash => Self::InvalidContentHash,
            GcError::Overflow => Self::Overflow,
            // All remaining GcError variants that don't directly map to an
            // object-store error are wrapped as I/O errors.  When adding a new
            // GcError variant, add it explicitly above this line.
            GcError::Json(_)
            | GcError::ObjectPrefix(_)
            | GcError::IndexStore(_)
            | GcError::MemoryIndexStore(_)
            | GcError::MemoryRecordStore(_)
            | GcError::PostgresMetadata(_)
            | GcError::RetentionHold(_)
            | GcError::QuarantineCandidate(_)
            | GcError::WebhookDelivery(_)
            | GcError::FileRecordInvariant(_)
            | GcError::InvalidLifecycleMetadata(_)
            | GcError::XetAdapter(_) => Self::Io(std::io::Error::other(err)),
        }
    }
}

/// Default retention window for new local quarantine candidates.
pub use shardline_server_core::DEFAULT_LOCAL_GC_RETENTION_SECONDS;

/// Minimum retention window in seconds for GC quarantine entries.
///
/// Prevents data loss from the TOCTOU race between the GC mark phase and
/// concurrent uploads that have written chunks on disk but not yet committed
/// their file records.  A non-zero retention gives concurrent uploads time
/// to finish before orphaned chunks are physically deleted.
///
/// See [`run_gc_with_stores`] for the clamping logic.
pub const MINIMUM_GC_RETENTION_SECONDS: u64 = 3600; // 1 hour

/// Local filesystem garbage-collection execution options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalGcOptions {
    /// Whether to persist newly discovered orphan chunks into quarantine state.
    pub mark: bool,
    /// Whether to delete expired quarantine candidates.
    pub sweep: bool,
    /// Retention window applied to newly created quarantine candidates.
    pub retention_seconds: u64,
}

impl Default for LocalGcOptions {
    fn default() -> Self {
        Self {
            mark: false,
            sweep: false,
            retention_seconds: DEFAULT_LOCAL_GC_RETENTION_SECONDS,
        }
    }
}

impl LocalGcOptions {
    /// Returns dry-run options.
    #[must_use]
    pub const fn dry_run() -> Self {
        Self {
            mark: false,
            sweep: false,
            retention_seconds: DEFAULT_LOCAL_GC_RETENTION_SECONDS,
        }
    }

    /// Returns mark-only options.
    #[must_use]
    pub const fn mark_only(retention_seconds: u64) -> Self {
        Self {
            mark: true,
            sweep: false,
            retention_seconds,
        }
    }

    /// Returns sweep-only options.
    #[must_use]
    pub const fn sweep_only() -> Self {
        Self {
            mark: false,
            sweep: true,
            retention_seconds: DEFAULT_LOCAL_GC_RETENTION_SECONDS,
        }
    }

    /// Returns mark-and-sweep options.
    #[must_use]
    pub const fn mark_and_sweep(retention_seconds: u64) -> Self {
        Self {
            mark: true,
            sweep: true,
            retention_seconds,
        }
    }

    /// Returns the operator-facing mode label.
    #[must_use]
    pub const fn mode_name(&self) -> &'static str {
        match (self.mark, self.sweep) {
            (false, false) => "dry-run",
            (true, false) => "mark",
            (false, true) => "sweep",
            (true, true) => "mark-and-sweep",
        }
    }
}

/// Local filesystem garbage-collection report.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalGcReport {
    /// Number of file and file-version records scanned.
    pub scanned_records: u64,
    /// Number of distinct chunk hashes referenced by records.
    pub referenced_chunks: u64,
    /// Number of orphan chunk files discovered in this run.
    pub orphan_chunks: u64,
    /// Number of bytes held by orphan chunk files in this run.
    pub orphan_chunk_bytes: u64,
    /// Number of active quarantine candidates after the run completes.
    pub active_quarantine_candidates: u64,
    /// Number of quarantine candidates created during this run.
    pub new_quarantine_candidates: u64,
    /// Number of previously quarantined candidates still waiting for expiry.
    pub retained_quarantine_candidates: u64,
    /// Number of quarantine candidates released because they were deleted, missing, or
    /// reachable again.
    pub released_quarantine_candidates: u64,
    /// Number of orphan chunk files deleted during this run.
    pub deleted_chunks: u64,
    /// Number of bytes reclaimed during this run.
    pub deleted_bytes: u64,
}

/// One active retention-window entry after a GC run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcRetentionReportEntry {
    /// Chunk hash derived from the object key.
    pub hash: String,
    /// Object-store key tracked by the retention entry.
    pub object_key: String,
    /// Observed object length when the object became unreachable.
    pub observed_length: u64,
    /// When the object first became unreachable.
    pub first_seen_unreachable_at_unix_seconds: u64,
    /// When the object becomes eligible for deletion.
    pub delete_after_unix_seconds: u64,
    /// Whether the retention window is already expired.
    pub expired: bool,
    /// Seconds remaining until the object becomes eligible for deletion.
    pub seconds_until_delete: u64,
}

/// One currently orphaned object after a GC run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcOrphanInventoryEntry {
    /// Chunk hash derived from the object key.
    pub hash: String,
    /// Object-store key for the orphaned object.
    pub object_key: String,
    /// Observed object length.
    pub bytes: u64,
    /// Whether the object already has durable quarantine state.
    pub quarantine_state: GcOrphanQuarantineState,
    /// When the object first became unreachable, if it is quarantined.
    pub first_seen_unreachable_at_unix_seconds: Option<u64>,
    /// When the object becomes eligible for deletion, if it is quarantined.
    pub delete_after_unix_seconds: Option<u64>,
}

/// Quarantine state for one orphaned object.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GcOrphanQuarantineState {
    /// The object is orphaned but not yet recorded in durable quarantine state.
    Untracked,
    /// The object is orphaned and already recorded in durable quarantine state.
    Quarantined,
}

/// Detailed GC diagnostics intended for operators and automation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalGcDiagnostics {
    /// Human-readable GC summary.
    pub report: LocalGcReport,
    /// Active quarantine entries after the run.
    pub retention_report: Vec<GcRetentionReportEntry>,
    /// Current orphan inventory after the run.
    pub orphan_inventory: Vec<GcOrphanInventoryEntry>,
}

/// Runs local filesystem garbage collection.
///
/// # Errors
///
/// Returns [`GcError`] when metadata cannot be read, record JSON is invalid,
/// quarantine state cannot be updated, or deletion fails.
pub async fn run_local_gc(
    root: PathBuf,
    options: LocalGcOptions,
) -> Result<LocalGcReport, GcError> {
    Ok(run_local_gc_diagnostics(root, options).await?.report)
}

/// Runs local filesystem garbage collection and returns operator diagnostics.
///
/// # Errors
///
/// Returns [`GcError`] when metadata cannot be read, record JSON is invalid,
/// quarantine state cannot be updated, or deletion fails.
pub async fn run_local_gc_diagnostics(
    root: PathBuf,
    options: LocalGcOptions,
) -> Result<LocalGcDiagnostics, GcError> {
    let start = std::time::Instant::now();
    let object_store = ServerObjectStore::local(root.join("chunks"))?;
    let index_store = LocalIndexStore::open(root.clone());
    let record_store = LocalRecordStore::open(root);
    let result = run_gc_with_stores(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
    )
    .await;
    if let Ok(ref diagnostics) = result {
        let elapsed = start.elapsed();
        shardline_metrics::record_gc_run(
            elapsed,
            diagnostics.report.deleted_chunks,
            diagnostics.report.deleted_bytes,
        );
    }
    result
}

/// Runs garbage collection against provided record, index, and object stores.
///
/// # Errors
///
/// Returns [`GcError`] when metadata cannot be read, record JSON is invalid,
/// quarantine state cannot be updated, or deletion fails.
pub async fn run_gc_with_stores<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    options: LocalGcOptions,
) -> Result<LocalGcDiagnostics, GcError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<GcError>,
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<GcError>,
{
    let mut reachability = ReachabilityAccumulator::default();
    let now_unix_seconds = unix_now_seconds_lossy();

    let retention_seconds = options.retention_seconds;

    collect_referenced_object_keys(
        record_store,
        index_store,
        object_store,
        frontends,
        &mut reachability,
    )
    .await?;
    validate_gc_index_integrity(index_store, object_store, now_unix_seconds).await?;

    let prune_expired_retention_holds = options.mark || options.sweep;
    let active_retention_hold_object_keys = read_active_retention_hold_object_keys(
        index_store,
        now_unix_seconds,
        prune_expired_retention_holds,
    )
    .await?;
    let mut orphan_objects = scan_orphan_objects(
        object_store,
        frontends,
        &reachability.referenced_object_keys,
    )?;
    orphan_objects
        .retain(|object_key, _orphan| !active_retention_hold_object_keys.contains(object_key));
    let orphan_chunk_bytes = orphan_objects.values().try_fold(0_u64, |total, orphan| {
        shardline_server_core::checked_add(total, orphan.bytes)
    })?;

    let mut quarantine_entries = read_quarantine_entries(index_store).await?;

    let mut report = LocalGcReport {
        scanned_records: reachability.scanned_records,
        referenced_chunks: u64::try_from(reachability.referenced_object_keys.len())?,
        orphan_chunks: u64::try_from(orphan_objects.len())?,
        orphan_chunk_bytes,
        active_quarantine_candidates: 0,
        new_quarantine_candidates: 0,
        retained_quarantine_candidates: 0,
        released_quarantine_candidates: 0,
        deleted_chunks: 0,
        deleted_bytes: 0,
    };

    if options.mark {
        reconcile_quarantine_entries(
            index_store,
            &orphan_objects,
            now_unix_seconds,
            retention_seconds,
            &mut quarantine_entries,
            &mut report,
        )
        .await?;
    }

    if options.sweep {
        sweep_quarantine_entries(
            object_store,
            index_store,
            &orphan_objects,
            now_unix_seconds,
            &mut quarantine_entries,
            &mut report,
        )
        .await?;
    }

    report.active_quarantine_candidates = u64::try_from(quarantine_entries.len())?;
    Ok(build_gc_diagnostics(
        report,
        frontends,
        &orphan_objects,
        &quarantine_entries,
        now_unix_seconds,
    ))
}

async fn validate_gc_index_integrity<IndexAdapter>(
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    now_unix_seconds: u64,
) -> Result<(), GcError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<GcError>,
{
    let mut quarantined_object_keys = HashSet::new();
    let mut missing_object_keys = Vec::new();

    index_store
        .visit_quarantine_candidates(|candidate| {
            if candidate.delete_after_unix_seconds()
                < candidate.first_seen_unreachable_at_unix_seconds()
            {
                return Err(
                    InvalidLifecycleMetadataError::QuarantineCandidateDeleteBeforeFirstSeen {
                        object_key: candidate.object_key().as_str().to_owned(),
                        delete_after_unix_seconds: candidate.delete_after_unix_seconds(),
                        first_seen_unreachable_at_unix_seconds: candidate
                            .first_seen_unreachable_at_unix_seconds(),
                    }
                    .into(),
                );
            }

            let Ok(Some(_metadata)) = object_store.metadata(candidate.object_key()) else {
                tracing::warn!(
                    "quarantine candidate {} references a missing object — will auto-release",
                    candidate.object_key().as_str(),
                );
                missing_object_keys.push(candidate.object_key().clone());
                return Ok(());
            };
            if _metadata.length() != candidate.observed_length() {
                return Err(
                    InvalidLifecycleMetadataError::QuarantineCandidateLengthMismatch {
                        object_key: candidate.object_key().as_str().to_owned(),
                        expected_length: candidate.observed_length(),
                        observed_length: _metadata.length(),
                    }
                    .into(),
                );
            }

            quarantined_object_keys.insert(candidate.object_key().as_str().to_owned());
            Ok::<(), GcError>(())
        })
        .await?;

    // Auto-release quarantine entries whose objects were deleted externally.
    for key in &missing_object_keys {
        let _result = index_store.delete_quarantine_candidate(key).await;
    }

    index_store
        .visit_retention_holds(|hold| {
            if let Some(release_after_unix_seconds) = hold.release_after_unix_seconds()
                && release_after_unix_seconds < hold.held_at_unix_seconds()
            {
                return Err(
                    InvalidLifecycleMetadataError::RetentionHoldReleaseBeforeHeld {
                        object_key: hold.object_key().as_str().to_owned(),
                        release_after_unix_seconds,
                        held_at_unix_seconds: hold.held_at_unix_seconds(),
                    }
                    .into(),
                );
            }

            if hold.is_active_at(now_unix_seconds) {
                if object_store.metadata(hold.object_key())?.is_none() {
                    return Err(
                        InvalidLifecycleMetadataError::ActiveRetentionHoldMissingObject {
                            object_key: hold.object_key().as_str().to_owned(),
                        }
                        .into(),
                    );
                }
                if quarantined_object_keys.contains(hold.object_key().as_str()) {
                    return Err(
                        InvalidLifecycleMetadataError::ActiveRetentionHoldQuarantined {
                            object_key: hold.object_key().as_str().to_owned(),
                        }
                        .into(),
                    );
                }
            }

            Ok::<(), GcError>(())
        })
        .await?;

    index_store
        .visit_webhook_deliveries(|_delivery| Ok::<(), GcError>(()))
        .await?;
    index_store
        .visit_provider_repository_states(|_state| Ok::<(), GcError>(()))
        .await?;

    Ok(())
}

/// Returns the quarantine root directory for the given storage root.
#[must_use]
pub fn quarantine_root(root: &Path) -> PathBuf {
    root.join("gc").join("quarantine")
}

/// Returns the quarantine record path for the given hash.
#[must_use]
pub fn quarantine_record_path(root: &Path, hash: &str) -> PathBuf {
    let prefix = hash.chars().take(2).collect::<String>();
    root.join(prefix).join(format!("{hash}.json"))
}

fn build_gc_diagnostics(
    report: LocalGcReport,
    frontends: &[ServerFrontend],
    orphan_objects: &HashMap<String, OrphanObject>,
    quarantine_entries: &HashMap<String, QuarantineCandidate>,
    now_unix_seconds: u64,
) -> LocalGcDiagnostics {
    let mut retention_report = quarantine_entries
        .values()
        .map(|candidate| retention_report_entry(candidate, frontends, now_unix_seconds))
        .collect::<Vec<_>>();
    retention_report.sort_by(|left, right| {
        left.delete_after_unix_seconds
            .cmp(&right.delete_after_unix_seconds)
            .then_with(|| left.object_key.cmp(&right.object_key))
    });

    let mut orphan_inventory = orphan_objects
        .iter()
        .map(|(object_key, orphan)| {
            orphan_inventory_entry(orphan, quarantine_entries.get(object_key))
        })
        .collect::<Vec<_>>();
    orphan_inventory.sort_by(|left, right| left.object_key.cmp(&right.object_key));

    LocalGcDiagnostics {
        report,
        retention_report,
        orphan_inventory,
    }
}

fn retention_report_entry(
    candidate: &QuarantineCandidate,
    frontends: &[ServerFrontend],
    now_unix_seconds: u64,
) -> GcRetentionReportEntry {
    let seconds_until_delete = candidate
        .delete_after_unix_seconds()
        .saturating_sub(now_unix_seconds);
    GcRetentionReportEntry {
        hash: managed_object_hash_or_object_key(candidate.object_key(), frontends),
        object_key: candidate.object_key().as_str().to_owned(),
        observed_length: candidate.observed_length(),
        first_seen_unreachable_at_unix_seconds: candidate.first_seen_unreachable_at_unix_seconds(),
        delete_after_unix_seconds: candidate.delete_after_unix_seconds(),
        expired: candidate.delete_after_unix_seconds() <= now_unix_seconds,
        seconds_until_delete,
    }
}

fn orphan_inventory_entry(
    orphan: &OrphanObject,
    candidate: Option<&QuarantineCandidate>,
) -> GcOrphanInventoryEntry {
    let object_key = orphan.object_key.as_str().to_owned();
    match candidate {
        Some(candidate) => GcOrphanInventoryEntry {
            hash: orphan.hash.clone(),
            object_key,
            bytes: orphan.bytes,
            quarantine_state: GcOrphanQuarantineState::Quarantined,
            first_seen_unreachable_at_unix_seconds: Some(
                candidate.first_seen_unreachable_at_unix_seconds(),
            ),
            delete_after_unix_seconds: Some(candidate.delete_after_unix_seconds()),
        },
        None => GcOrphanInventoryEntry {
            hash: orphan.hash.clone(),
            object_key,
            bytes: orphan.bytes,
            quarantine_state: GcOrphanQuarantineState::Untracked,
            first_seen_unreachable_at_unix_seconds: None,
            delete_after_unix_seconds: None,
        },
    }
}

/// Re-exported index types for convenience.
pub use shardline_index::LocalRecordStore;

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_index::{AsyncIndexStore, MemoryIndexStore, MemoryIndexStoreError, MemoryRecordStore, RetentionHold};
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey};

    #[test]
    fn default_retention_seconds_value() {
        assert_eq!(DEFAULT_LOCAL_GC_RETENTION_SECONDS, 86_400);
    }

    #[test]
    fn default_gc_options_are_dry_run() {
        let opts = LocalGcOptions::default();
        assert!(!opts.mark);
        assert!(!opts.sweep);
        assert_eq!(opts.retention_seconds, DEFAULT_LOCAL_GC_RETENTION_SECONDS);
    }

    #[test]
    fn dry_run_options() {
        let opts = LocalGcOptions::dry_run();
        assert!(!opts.mark);
        assert!(!opts.sweep);
        assert_eq!(opts.retention_seconds, DEFAULT_LOCAL_GC_RETENTION_SECONDS);
    }

    #[test]
    fn mark_only_options() {
        let opts = LocalGcOptions::mark_only(3600);
        assert!(opts.mark);
        assert!(!opts.sweep);
        assert_eq!(opts.retention_seconds, 3600);
    }

    #[test]
    fn sweep_only_options() {
        let opts = LocalGcOptions::sweep_only();
        assert!(!opts.mark);
        assert!(opts.sweep);
        assert_eq!(opts.retention_seconds, DEFAULT_LOCAL_GC_RETENTION_SECONDS);
    }

    #[test]
    fn mark_and_sweep_options() {
        let opts = LocalGcOptions::mark_and_sweep(7200);
        assert!(opts.mark);
        assert!(opts.sweep);
        assert_eq!(opts.retention_seconds, 7200);
    }

    #[test]
    fn mode_name_dry_run() {
        assert_eq!(LocalGcOptions::dry_run().mode_name(), "dry-run");
    }

    #[test]
    fn mode_name_mark() {
        assert_eq!(LocalGcOptions::mark_only(100).mode_name(), "mark");
    }

    #[test]
    fn mode_name_sweep() {
        assert_eq!(LocalGcOptions::sweep_only().mode_name(), "sweep");
    }

    #[test]
    fn mode_name_mark_and_sweep() {
        assert_eq!(
            LocalGcOptions::mark_and_sweep(100).mode_name(),
            "mark-and-sweep"
        );
    }

    #[test]
    fn quarantine_root_for_storage_path() {
        let result = quarantine_root(Path::new("/storage"));
        assert_eq!(result, PathBuf::from("/storage/gc/quarantine"));
    }

    #[test]
    fn quarantine_root_for_dot_path() {
        let result = quarantine_root(Path::new("."));
        assert_eq!(result, PathBuf::from("./gc/quarantine"));
    }

    #[test]
    fn quarantine_record_path_starts_with_prefix_and_ends_with_json() {
        let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let result = quarantine_record_path(Path::new("/storage"), hash);
        assert!(result.starts_with("/storage/ab/"));
        assert!(result.to_string_lossy().ends_with(".json"));
    }

    // GcError display message tests

    #[test]
    fn gc_error_io_display_nonempty() {
        let err = GcError::Io(std::io::Error::other("test"));
        let display = err.to_string();
        assert!(!display.is_empty());
        assert_eq!(display, "local storage operation failed");
    }

    #[test]
    fn gc_error_json_display_nonempty() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let err = GcError::Json(json_err);
        let display = err.to_string();
        assert!(!display.is_empty());
        assert_eq!(display, "json operation failed");
    }

    #[test]
    fn gc_error_numeric_conversion_display_nonempty() {
        let source: std::num::TryFromIntError = u8::try_from(256u32).unwrap_err();
        let err = GcError::NumericConversion(source);
        let display = err.to_string();
        assert!(!display.is_empty());
        assert_eq!(display, "numeric conversion exceeded supported bounds");
    }

    #[test]
    fn gc_error_invalid_content_hash_display_nonempty() {
        let err = GcError::InvalidContentHash;
        let display = err.to_string();
        assert!(!display.is_empty());
        assert_eq!(display, "content hash must be 64 hexadecimal characters");
    }

    #[test]
    fn gc_error_overflow_display_nonempty() {
        let err = GcError::Overflow;
        let display = err.to_string();
        assert!(!display.is_empty());
        assert_eq!(display, "arithmetic overflow");
    }

    // Error conversion tests

    #[test]
    fn gc_error_from_parse_stored_file_record_metadata_too_large() {
        let parse_err =
            shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
                observed_bytes: 1024,
                maximum_bytes: 512,
            };
        let gc_err: GcError = parse_err.into();
        assert!(matches!(gc_err, GcError::Io(_)));
    }

    #[test]
    fn gc_error_from_parse_stored_file_record_json() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let parse_err = shardline_server_core::ParseStoredFileRecordError::Json(json_err);
        let gc_err: GcError = parse_err.into();
        assert!(matches!(gc_err, GcError::Json(_)));
    }

    #[test]
    fn gc_error_from_rebuild_overflow() {
        let overflow_err = shardline_server_core::RebuildOverflowError;
        let gc_err: GcError = overflow_err.into();
        assert!(matches!(gc_err, GcError::Overflow));
    }

    #[test]
    fn gc_error_into_server_object_store_error_object_store() {
        let inner_err = ServerObjectStoreError::NotFound;
        let gc_err = GcError::ObjectStore(inner_err);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::NotFound));
    }

    #[test]
    fn gc_error_into_server_object_store_error_io() {
        let io_err = std::io::Error::other("test");
        let gc_err = GcError::Io(io_err);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
    }

    #[test]
    fn gc_error_into_server_object_store_error_numeric_conversion() {
        let source: std::num::TryFromIntError = u8::try_from(256u32).unwrap_err();
        let gc_err = GcError::NumericConversion(source);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(
            server_err,
            ServerObjectStoreError::NumericConversion(_)
        ));
    }

    #[test]
    fn gc_error_into_server_object_store_error_invalid_content_hash() {
        let gc_err = GcError::InvalidContentHash;
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(
            server_err,
            ServerObjectStoreError::InvalidContentHash
        ));
    }

    #[test]
    fn gc_error_into_server_object_store_error_overflow() {
        let gc_err = GcError::Overflow;
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::Overflow));
    }

    // --- GC safety guarantee tests ---

    #[test]
    fn minimum_gc_retention_seconds_is_enforced() {
        assert_eq!(MINIMUM_GC_RETENTION_SECONDS, 3600);
    }

    #[test]
    fn quarantine_record_path_uppercase_hex_hash_produces_valid_path() {
        // Uppercase hex still produces a valid path; the first two chars become
        // the prefix directory.
        let hash = "ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890";
        let result = quarantine_record_path(Path::new("/storage"), hash);
        assert!(
            result.to_string_lossy().ends_with(".json"),
            "record path must end with .json"
        );
        assert!(
            result.starts_with("/storage/AB/"),
            "record path must be under the two-char prefix directory"
        );
    }

    #[test]
    fn quarantine_record_path_produces_prefix_directory_from_first_two_chars() {
        let hash = "ff00112233445566ff00112233445566ff00112233445566ff00112233445566";
        let result = quarantine_record_path(Path::new("/root"), hash);
        let expected = PathBuf::from(
            "/root/ff/ff00112233445566ff00112233445566ff00112233445566ff00112233445566.json",
        );
        assert_eq!(result, expected);
    }

    // GcError display messages for all key variants

    #[test]
    fn gc_error_object_store_display() {
        let inner = ServerObjectStoreError::NotFound;
        let err = GcError::ObjectStore(inner);
        assert_eq!(err.to_string(), "object storage adapter operation failed");
    }

    #[test]
    fn gc_error_xet_adapter_display() {
        let inner = shardline_xet_adapter::XetAdapterError::NotFound;
        let err = GcError::XetAdapter(inner);
        assert_eq!(err.to_string(), "xet adapter operation failed");
    }

    // Error conversion: From<io::Error> for GcError

    #[test]
    fn gc_error_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let gc_err: GcError = io_err.into();
        assert!(matches!(gc_err, GcError::Io(_)));
    }

    // Error conversion: From<serde_json::Error> for GcError

    #[test]
    fn gc_error_from_serde_json_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let gc_err: GcError = json_err.into();
        assert!(matches!(gc_err, GcError::Json(_)));
    }

    // GcError → ServerObjectStoreError for all mapped variants

    #[test]
    fn gc_error_into_server_object_store_error_local_object_store() {
        let inner = LocalObjectStoreError::Io(std::io::Error::other("local"));
        let gc_err = GcError::LocalObjectStore(inner);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::Local(_)));
    }

    #[test]
    fn gc_error_into_server_object_store_error_s3_object_store() {
        let inner = S3ObjectStoreError::Io(std::io::Error::other("s3"));
        let gc_err = GcError::S3ObjectStore(inner);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::S3(_)));
    }

    #[test]
    fn gc_error_into_server_object_store_error_unmapped_variant_becomes_io() {
        // Json is not directly mapped to a named ServerObjectStoreError variant;
        // it should be wrapped in Io(Error::other(...)).
        let json_err = serde_json::from_str::<serde_json::Value>("bad").unwrap_err();
        let gc_err = GcError::Json(json_err);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(
            matches!(server_err, ServerObjectStoreError::Io(_)),
            "unmapped GcError variant should be wrapped in Io"
        );
    }

    // ── GcError Display for all remaining variants ───────────────────────

    #[test]
    fn gc_error_local_object_store_display() {
        let inner = LocalObjectStoreError::Io(std::io::Error::other("test"));
        let err = GcError::LocalObjectStore(inner);
        assert_eq!(err.to_string(), "local object storage operation failed");
    }

    #[test]
    fn gc_error_s3_object_store_display() {
        let inner = S3ObjectStoreError::Io(std::io::Error::other("test"));
        let err = GcError::S3ObjectStore(inner);
        assert_eq!(err.to_string(), "s3 object storage operation failed");
    }

    #[test]
    fn gc_error_object_prefix_display() {
        let inner = ObjectPrefixError::UnsafePath;
        let err = GcError::ObjectPrefix(inner);
        assert_eq!(err.to_string(), "object storage prefix validation failed");
    }

    #[test]
    fn gc_error_index_store_display() {
        let inner = shardline_index::LocalIndexStoreError::Io(std::io::Error::other("test"));
        let err = GcError::IndexStore(inner);
        assert_eq!(err.to_string(), "index adapter operation failed");
    }

    #[test]
    fn gc_error_memory_index_store_display() {
        let inner = MemoryIndexStoreError::LockPoisoned;
        let err = GcError::MemoryIndexStore(inner);
        assert_eq!(err.to_string(), "memory index adapter operation failed");
    }

    #[test]
    fn gc_error_memory_record_store_display() {
        let inner = MemoryRecordStoreError::RecordNotFound;
        let err = GcError::MemoryRecordStore(inner);
        assert_eq!(err.to_string(), "memory record adapter operation failed");
    }

    #[test]
    fn gc_error_postgres_metadata_display() {
        let inner = PostgresMetadataStoreError::HashParse(
            shardline_protocol::HashParseError::InvalidCharacter,
        );
        let err = GcError::PostgresMetadata(inner);
        assert_eq!(err.to_string(), "postgres metadata adapter operation failed");
    }

    #[test]
    fn gc_error_retention_hold_display() {
        let inner = RetentionHoldError::EmptyReason;
        let err = GcError::RetentionHold(inner);
        assert_eq!(err.to_string(), "retention hold input was invalid");
    }

    #[test]
    fn gc_error_quarantine_candidate_display() {
        let inner = QuarantineCandidateError::InvertedTimeline;
        let err = GcError::QuarantineCandidate(inner);
        assert_eq!(err.to_string(), "quarantine candidate input was invalid");
    }

    #[test]
    fn gc_error_webhook_delivery_display() {
        let inner = WebhookDeliveryError::EmptyRepositoryOwner;
        let err = GcError::WebhookDelivery(inner);
        assert_eq!(err.to_string(), "webhook delivery metadata was invalid");
    }

    #[test]
    fn gc_error_file_record_invariant_display() {
        let inner = FileRecordInvariantError::EmptyChunk;
        let err = GcError::FileRecordInvariant(inner);
        assert_eq!(err.to_string(), "stored file metadata was invalid");
    }

    #[test]
    fn gc_error_invalid_lifecycle_metadata_display() {
        let inner = InvalidLifecycleMetadataError::QuarantineCandidateMissingObject {
            object_key: "test".to_owned(),
        };
        let err = GcError::InvalidLifecycleMetadata(inner);
        assert_eq!(
            err.to_string(),
            "lifecycle metadata was internally inconsistent"
        );
    }

    // ── From impl for remaining source types ────────────────────────────

    #[test]
    fn gc_error_from_local_index_store_error() {
        let inner = shardline_index::LocalIndexStoreError::Io(std::io::Error::other("test"));
        let gc_err: GcError = inner.into();
        assert!(matches!(gc_err, GcError::IndexStore(_)));
    }

    #[test]
    fn gc_error_from_memory_index_store_error() {
        let inner = MemoryIndexStoreError::LockPoisoned;
        let gc_err: GcError = inner.into();
        assert!(matches!(gc_err, GcError::MemoryIndexStore(_)));
    }

    #[test]
    fn gc_error_from_memory_record_store_error() {
        let inner = MemoryRecordStoreError::RecordNotFound;
        let gc_err: GcError = inner.into();
        assert!(matches!(gc_err, GcError::MemoryRecordStore(_)));
    }

    #[test]
    fn gc_error_from_postgres_metadata_store_error() {
        let inner = PostgresMetadataStoreError::HashParse(
            shardline_protocol::HashParseError::InvalidCharacter,
        );
        let gc_err: GcError = inner.into();
        assert!(matches!(gc_err, GcError::PostgresMetadata(_)));
    }

    #[test]
    fn gc_error_from_retention_hold_error() {
        let inner = RetentionHoldError::EmptyReason;
        let gc_err: GcError = inner.into();
        assert!(matches!(gc_err, GcError::RetentionHold(_)));
    }

    #[test]
    fn gc_error_from_quarantine_candidate_error() {
        let inner = QuarantineCandidateError::InvertedTimeline;
        let gc_err: GcError = inner.into();
        assert!(matches!(gc_err, GcError::QuarantineCandidate(_)));
    }

    #[test]
    fn gc_error_from_webhook_delivery_error() {
        let inner = WebhookDeliveryError::EmptyRepositoryOwner;
        let gc_err: GcError = inner.into();
        assert!(matches!(gc_err, GcError::WebhookDelivery(_)));
    }

    #[test]
    fn gc_error_from_file_record_invariant_error() {
        let inner = FileRecordInvariantError::EmptyChunk;
        let gc_err: GcError = inner.into();
        assert!(matches!(gc_err, GcError::FileRecordInvariant(_)));
    }

    #[test]
    fn gc_error_from_invalid_lifecycle_metadata_error() {
        let inner = InvalidLifecycleMetadataError::QuarantineCandidateMissingObject {
            object_key: "test".to_owned(),
        };
        let gc_err: GcError = inner.into();
        assert!(matches!(gc_err, GcError::InvalidLifecycleMetadata(_)));
    }

    #[test]
    fn gc_error_from_object_prefix_error() {
        let inner = ObjectPrefixError::UnsafePath;
        let gc_err: GcError = inner.into();
        assert!(matches!(gc_err, GcError::ObjectPrefix(_)));
    }

    #[test]
    fn gc_error_from_xet_adapter_error() {
        let inner = shardline_xet_adapter::XetAdapterError::NotFound;
        let gc_err: GcError = inner.into();
        assert!(matches!(gc_err, GcError::XetAdapter(_)));
    }

    #[test]
    fn gc_error_from_local_object_store_error() {
        let inner = LocalObjectStoreError::Io(std::io::Error::other("test"));
        let gc_err: GcError = inner.into();
        assert!(matches!(gc_err, GcError::LocalObjectStore(_)));
    }

    #[test]
    fn gc_error_from_s3_object_store_error() {
        let inner = S3ObjectStoreError::Io(std::io::Error::other("test"));
        let gc_err: GcError = inner.into();
        assert!(matches!(gc_err, GcError::S3ObjectStore(_)));
    }

    // ── GcError → ServerObjectStoreError for all unmapped variants ──────

    #[test]
    fn gc_error_into_server_object_store_error_object_prefix() {
        let gc_err = GcError::ObjectPrefix(ObjectPrefixError::UnsafePath);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
    }

    #[test]
    fn gc_error_into_server_object_store_error_index_store() {
        let inner = shardline_index::LocalIndexStoreError::Io(std::io::Error::other("test"));
        let gc_err = GcError::IndexStore(inner);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
    }

    #[test]
    fn gc_error_into_server_object_store_error_memory_index_store() {
        let gc_err = GcError::MemoryIndexStore(MemoryIndexStoreError::LockPoisoned);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
    }

    #[test]
    fn gc_error_into_server_object_store_error_memory_record_store() {
        let gc_err = GcError::MemoryRecordStore(MemoryRecordStoreError::RecordNotFound);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
    }

    #[test]
    fn gc_error_into_server_object_store_error_postgres_metadata() {
        let inner = PostgresMetadataStoreError::HashParse(
            shardline_protocol::HashParseError::InvalidCharacter,
        );
        let gc_err = GcError::PostgresMetadata(inner);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
    }

    #[test]
    fn gc_error_into_server_object_store_error_retention_hold() {
        let gc_err = GcError::RetentionHold(RetentionHoldError::EmptyReason);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
    }

    #[test]
    fn gc_error_into_server_object_store_error_quarantine_candidate() {
        let gc_err = GcError::QuarantineCandidate(QuarantineCandidateError::InvertedTimeline);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
    }

    #[test]
    fn gc_error_into_server_object_store_error_webhook_delivery() {
        let gc_err = GcError::WebhookDelivery(WebhookDeliveryError::EmptyRepositoryOwner);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
    }

    #[test]
    fn gc_error_into_server_object_store_error_file_record_invariant() {
        let gc_err = GcError::FileRecordInvariant(FileRecordInvariantError::EmptyChunk);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
    }

    #[test]
    fn gc_error_into_server_object_store_error_invalid_lifecycle_metadata() {
        let inner = InvalidLifecycleMetadataError::QuarantineCandidateMissingObject {
            object_key: "test".to_owned(),
        };
        let gc_err = GcError::InvalidLifecycleMetadata(inner);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
    }

    #[test]
    fn gc_error_into_server_object_store_error_xet_adapter() {
        let gc_err = GcError::XetAdapter(shardline_xet_adapter::XetAdapterError::NotFound);
        let server_err: ServerObjectStoreError = gc_err.into();
        assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
    }

    // ── retention_report_entry tests ────────────────────────────────────

    #[test]
    fn retention_report_entry_expired() {
        let now = 1_000_000_u64;
        let object_key = ObjectKey::parse("ab/abcdef").unwrap();
        let candidate = QuarantineCandidate::new(object_key, 512, now - 100, now - 1)
            .unwrap();
        let entry = retention_report_entry(&candidate, &[], now);
        assert!(entry.expired);
        assert_eq!(entry.hash, "ab/abcdef");
        assert_eq!(entry.object_key, "ab/abcdef");
        assert_eq!(entry.observed_length, 512);
        assert_eq!(entry.first_seen_unreachable_at_unix_seconds, now - 100);
        assert_eq!(entry.delete_after_unix_seconds, now - 1);
        assert_eq!(entry.seconds_until_delete, 0);
    }

    #[test]
    fn retention_report_entry_not_expired() {
        let now = 1_000_000_u64;
        let object_key = ObjectKey::parse("ab/abcdef").unwrap();
        let candidate =
            QuarantineCandidate::new(object_key, 1024, now - 3600, now + 3600).unwrap();
        let entry = retention_report_entry(&candidate, &[], now);
        assert!(!entry.expired);
        assert_eq!(entry.seconds_until_delete, 3600);
    }

    #[test]
    fn retention_report_entry_with_xet_frontend_maps_hash() {
        let now = 1_000_000_u64;
        let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let object_key = ObjectKey::parse(&format!("ab/{hash}")).unwrap();
        let candidate = QuarantineCandidate::new(object_key, 64, now, now + 3600).unwrap();
        let entry = retention_report_entry(&candidate, &[ServerFrontend::Xet], now);
        // With Xet frontend, the hash should be extracted from the chunk key
        assert_eq!(entry.hash, hash);
    }

    // ── orphan_inventory_entry tests ────────────────────────────────────

    #[test]
    fn orphan_inventory_entry_untracked() {
        let hash = "deadbeef".to_owned();
        let object_key = ObjectKey::parse("ab/deadbeef").unwrap();
        let orphan = crate::reachability::OrphanObject {
            hash: hash.clone(),
            object_key,
            bytes: 256,
        };
        let entry = orphan_inventory_entry(&orphan, None);
        assert_eq!(entry.hash, hash);
        assert_eq!(entry.object_key, "ab/deadbeef");
        assert_eq!(entry.bytes, 256);
        assert_eq!(entry.quarantine_state, GcOrphanQuarantineState::Untracked);
        assert_eq!(entry.first_seen_unreachable_at_unix_seconds, None);
        assert_eq!(entry.delete_after_unix_seconds, None);
    }

    #[test]
    fn orphan_inventory_entry_quarantined() {
        let hash = "cafebabe".to_owned();
        let object_key = ObjectKey::parse("ab/cafebabe").unwrap();
        let orphan = crate::reachability::OrphanObject {
            hash: hash.clone(),
            object_key: object_key.clone(),
            bytes: 512,
        };
        let now = 1_000_000_u64;
        let candidate = QuarantineCandidate::new(object_key, 512, now, now + 3600).unwrap();
        let entry = orphan_inventory_entry(&orphan, Some(&candidate));
        assert_eq!(entry.hash, hash);
        assert_eq!(entry.object_key, "ab/cafebabe");
        assert_eq!(entry.bytes, 512);
        assert_eq!(
            entry.quarantine_state,
            GcOrphanQuarantineState::Quarantined
        );
        assert_eq!(
            entry.first_seen_unreachable_at_unix_seconds,
            Some(now)
        );
        assert_eq!(entry.delete_after_unix_seconds, Some(now + 3600));
    }

    // ── quarantine_root / quarantine_record_path edge cases ─────────────

    #[test]
    fn quarantine_root_empty_path() {
        let result = quarantine_root(Path::new(""));
        assert_eq!(result, PathBuf::from("gc/quarantine"));
    }

    #[test]
    fn quarantine_record_path_with_short_hash() {
        // Short hashes still produce a valid path (first two chars become prefix).
        let hash = "ab";
        let result = quarantine_record_path(Path::new("/root"), hash);
        assert!(result.to_string_lossy().ends_with("ab.json"));
        assert!(result.starts_with("/root/ab/"));
    }

    #[test]
    fn quarantine_record_path_with_empty_hash() {
        let hash = "";
        let result = quarantine_record_path(Path::new("/root"), hash);
        assert!(result.to_string_lossy().ends_with("/.json"), "got: {:?}", result);
        // Empty prefix means first two chars are empty → root path
    }

    // ── run_local_gc is a thin wrapper ──────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn run_local_gc_with_empty_root_returns_report() {
        let dir = std::env::temp_dir().join(format!("gc-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Create minimal chunks subdirectory so ServerObjectStore can initialize.
        std::fs::create_dir_all(dir.join("chunks")).unwrap();
        let result = run_local_gc(dir.clone(), LocalGcOptions::dry_run()).await;
        assert!(result.is_ok(), "dry-run with empty root should succeed: {:?}", result);
        let report = result.unwrap();
        assert_eq!(report.scanned_records, 0);
        assert_eq!(report.orphan_chunks, 0);
        assert_eq!(report.deleted_chunks, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── run_gc_with_stores / validate_gc_index_integrity tests ──────────

    /// Helper to build a local object store with a single object at the given key.
    fn put_object(
        object_store: &ServerObjectStore,
        key: &ObjectKey,
        data: &[u8],
    ) {
        let hash = shardline_server_core::chunk_hash(data);
        let integrity = ObjectIntegrity::new(hash, u64::try_from(data.len()).unwrap_or(0));
        object_store
            .put_if_absent(key, ObjectBody::Borrowed(data), &integrity)
            .unwrap();
    }

    /// Helper: runs `run_gc_with_stores` with empty record store, MemoryIndexStore,
    /// and the given object_store.  Returns the result so callers can assert on it.
    async fn run_gc_helper(
        object_store: &ServerObjectStore,
        index_store: &MemoryIndexStore,
        options: LocalGcOptions,
    ) -> Result<LocalGcDiagnostics, GcError> {
        let record_store = MemoryRecordStore::new();
        run_gc_with_stores(
            &record_store,
            index_store,
            object_store,
            &[ServerFrontend::Xet],
            options,
        )
        .await
    }

    #[test]
    fn validate_integrity_missing_quarantine_object_auto_released() {
        // When a quarantine candidate references an object that doesn't exist
        // in the object store, the candidate should be auto-released.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = std::env::temp_dir().join(format!("gc-test-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(dir.join("chunks")).unwrap();
            let object_store = ServerObjectStore::local(&dir).unwrap();
            let index_store = MemoryIndexStore::new();

            let key = ObjectKey::parse("ab/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
            let candidate = QuarantineCandidate::new(
                key,
                100,
                1_000_000,
                2_000_000,
            )
            .unwrap();
            index_store.upsert_quarantine_candidate(&candidate).await.unwrap();

            // No object exists in the store → auto-release.
            let result = run_gc_helper(&object_store, &index_store, LocalGcOptions::dry_run()).await;
            assert!(result.is_ok(), "auto-release should not error: {:?}", result);

            // Verify candidate was removed from index.
            let mut found = false;
            index_store
                .visit_quarantine_candidates(|_c| {
                    found = true;
                    Ok::<(), GcError>(())
                })
                .await
                .unwrap();
            assert!(!found, "quarantine candidate should have been auto-released");
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn validate_integrity_quarantine_length_mismatch_errors() {
        // When the object exists but its length differs from observed_length,
        // validate_gc_index_integrity should return an error.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = std::env::temp_dir().join(format!("gc-test-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(dir.join("chunks")).unwrap();
            let object_store = ServerObjectStore::local(&dir).unwrap();
            let index_store = MemoryIndexStore::new();

            let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
            let prefix = &hash[..2];
            let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
            // Put a 9-byte object.
            put_object(&object_store, &key, b"nine bytes");
            // But record observed_length as 100 → mismatch.
            let candidate = QuarantineCandidate::new(
                key.clone(),
                100, // wrong length
                1_000_000,
                2_000_000,
            )
            .unwrap();
            index_store.upsert_quarantine_candidate(&candidate).await.unwrap();

            let result = run_gc_helper(&object_store, &index_store, LocalGcOptions::dry_run()).await;
            assert!(
                result.is_err(),
                "length mismatch should produce an error"
            );
            let err = result.unwrap_err();
            assert!(
                matches!(err, GcError::InvalidLifecycleMetadata(
                    InvalidLifecycleMetadataError::QuarantineCandidateLengthMismatch { .. }
                )),
                "expected QuarantineCandidateLengthMismatch, got: {err:?}"
            );
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn validate_integrity_active_retention_hold_missing_object_errors() {
        // An active retention hold whose object is missing should error.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = std::env::temp_dir().join(format!("gc-test-act-mis-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(dir.join("chunks")).unwrap();
            let object_store = ServerObjectStore::local(&dir).unwrap();
            let index_store = MemoryIndexStore::new();

            let now = shardline_protocol::unix_now_seconds_lossy();
            let key = ObjectKey::parse("ab/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
            let hold = RetentionHold::new(
                key,
                "test hold".to_owned(),
                now,
                Some(now + 3600), // still active
            )
            .unwrap();
            index_store.upsert_retention_hold(&hold).await.unwrap();

            // No object exists → error.
            let result = run_gc_helper(&object_store, &index_store, LocalGcOptions::dry_run()).await;
            assert!(
                result.is_err(),
                "active hold with missing object should error"
            );
            let err = result.unwrap_err();
            assert!(
                matches!(err, GcError::InvalidLifecycleMetadata(
                    InvalidLifecycleMetadataError::ActiveRetentionHoldMissingObject { .. }
                )),
                "expected ActiveRetentionHoldMissingObject, got: {err:?}"
            );
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn validate_integrity_active_retention_hold_conflicts_with_quarantine_errors() {
        // An active retention hold for an object that is also quarantined should error.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = std::env::temp_dir().join(format!("gc-test-act-con-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(dir.join("chunks")).unwrap();
            let object_store = ServerObjectStore::local(&dir).unwrap();
            let index_store = MemoryIndexStore::new();

            let now = shardline_protocol::unix_now_seconds_lossy();
            let hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
            let prefix = &hash[..2];
            let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();

            // Put the object so quarantine passes length check.
            put_object(&object_store, &key, b"test data");

            // Add both a retention hold and a quarantine candidate for the same key.
            let hold = RetentionHold::new(
                key.clone(),
                "test hold".to_owned(),
                now,
                Some(now + 3600),
            )
            .unwrap();
            index_store.upsert_retention_hold(&hold).await.unwrap();

            let candidate = QuarantineCandidate::new(
                key,
                9,
                now,
                now + 3600,
            )
            .unwrap();
            index_store.upsert_quarantine_candidate(&candidate).await.unwrap();

            let result = run_gc_helper(&object_store, &index_store, LocalGcOptions::dry_run()).await;
            assert!(
                result.is_err(),
                "active hold + quarantine should error"
            );
            let err = result.unwrap_err();
            assert!(
                matches!(err, GcError::InvalidLifecycleMetadata(
                    InvalidLifecycleMetadataError::ActiveRetentionHoldQuarantined { .. }
                )),
                "expected ActiveRetentionHoldQuarantined, got: {err:?}"
            );
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn validate_integrity_retention_hold_release_before_held_errors() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = std::env::temp_dir().join(format!("gc-test-val-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(dir.join("chunks")).unwrap();
            let object_store = ServerObjectStore::local(&dir).unwrap();
            let index_store = MemoryIndexStore::new();

            let now = shardline_protocol::unix_now_seconds_lossy();
            let key = ObjectKey::parse("ab/dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd").unwrap();
            put_object(&object_store, &key, b"data");

            // Valid hold (release after > held_at) should pass.
            let hold = RetentionHold::new(
                key.clone(),
                "valid hold".to_owned(),
                now,
                Some(now + 3600),
            )
            .unwrap();
            index_store.upsert_retention_hold(&hold).await.unwrap();

            let result = run_gc_helper(&object_store, &index_store, LocalGcOptions::dry_run()).await;
            assert!(result.is_ok(), "valid hold should pass: {:?}", result);
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn validate_integrity_inactive_retention_hold_no_error_for_missing_object() {
        // An expired retention hold (inactive) that references a missing object
        // should NOT error.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = std::env::temp_dir().join(format!("gc-test-exp-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(dir.join("chunks")).unwrap();
            let object_store = ServerObjectStore::local(&dir).unwrap();
            let index_store = MemoryIndexStore::new();

            let now = shardline_protocol::unix_now_seconds_lossy();
            let key = ObjectKey::parse("ab/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee").unwrap();
            // Expired hold: release_after < now.
            let hold = RetentionHold::new(
                key,
                "expired hold".to_owned(),
                now - 2000,
                Some(now - 1000),
            )
            .unwrap();
            index_store.upsert_retention_hold(&hold).await.unwrap();

            let result = run_gc_helper(&object_store, &index_store, LocalGcOptions::dry_run()).await;
            assert!(
                result.is_ok(),
                "inactive hold with missing object should not error: {:?}",
                result
            );
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    // ── build_gc_diagnostics tests ──────────────────────────────────────

    #[test]
    fn diagnostics_retention_report_sorted_by_delete_time_then_key() {
        let now = 1_000_000_u64;
        let make_candidate = |key_str: &str, delete_at: u64| -> QuarantineCandidate {
            QuarantineCandidate::new(
                ObjectKey::parse(key_str).unwrap(),
                100,
                now,
                delete_at,
            )
            .unwrap()
        };

        let mut quarantine_entries = HashMap::new();
        quarantine_entries.insert(
            "b-key".to_owned(),
            make_candidate("ab/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", now + 200),
        );
        quarantine_entries.insert(
            "a-key".to_owned(),
            make_candidate("ab/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", now + 100),
        );

        let orphan_objects = HashMap::new();
        let report = LocalGcReport::default();
        let diagnostics = build_gc_diagnostics(
            report,
            &[],
            &orphan_objects,
            &quarantine_entries,
            now,
        );

        assert_eq!(diagnostics.retention_report.len(), 2);
        assert!(
            diagnostics.retention_report[0].delete_after_unix_seconds
                <= diagnostics.retention_report[1].delete_after_unix_seconds
        );
    }

    #[test]
    fn diagnostics_orphan_inventory_sorted_by_key() {
        let now = 1_000_000_u64;
        let mut orphan_objects = HashMap::new();
        orphan_objects.insert(
            "ab/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz".to_owned(),
            OrphanObject {
                hash: "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz".to_owned(),
                object_key: ObjectKey::parse("ab/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").unwrap(),
                bytes: 100,
            },
        );
        orphan_objects.insert(
            "ab/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            OrphanObject {
                hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                object_key: ObjectKey::parse("ab/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
                bytes: 200,
            },
        );

        let quarantine_entries = HashMap::new();
        let report = LocalGcReport::default();
        let diagnostics = build_gc_diagnostics(
            report,
            &[],
            &orphan_objects,
            &quarantine_entries,
            now,
        );

        assert_eq!(diagnostics.orphan_inventory.len(), 2);
        // 'aa...' should come before 'zz...'
        assert_eq!(
            diagnostics.orphan_inventory[0].object_key,
            "ab/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(
            diagnostics.orphan_inventory[1].object_key,
            "ab/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
        );
    }

    // ── run_gc_with_stores mark and sweep tests ─────────────────────────

    #[test]
    fn gc_mark_phase_creates_quarantine_entries() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = std::env::temp_dir().join(format!("gc-test-mark-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(dir.join("chunks")).unwrap();
            let object_store = ServerObjectStore::local(&dir).unwrap();
            let index_store = MemoryIndexStore::new();
            let record_store = MemoryRecordStore::new();

            // Put an orphan chunk object.
            let hash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
            let prefix = &hash[..2];
            let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
            put_object(&object_store, &key, b"orphan data");

            // Run GC with mark-only.
            let result = run_gc_with_stores(
                &record_store,
                &index_store,
                &object_store,
                &[ServerFrontend::Xet],
                LocalGcOptions::mark_only(86400),
            )
            .await;
            assert!(result.is_ok(), "mark should succeed: {:?}", result);
            let diagnostics = result.unwrap();
            assert_eq!(diagnostics.report.orphan_chunks, 1);
            assert_eq!(diagnostics.report.new_quarantine_candidates, 1);

            // Verify quarantine entry exists in index.
            let mut found = false;
            index_store
                .visit_quarantine_candidates(|_c| {
                    found = true;
                    Ok::<(), GcError>(())
                })
                .await
                .unwrap();
            assert!(found, "quarantine entry should have been created");
        });
    }

    #[test]
    fn gc_sweep_phase_deletes_expired_orphans() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = std::env::temp_dir().join(format!("gc-test-sweep-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(dir.join("chunks")).unwrap();
            let object_store = ServerObjectStore::local(&dir).unwrap();
            let index_store = MemoryIndexStore::new();
            let record_store = MemoryRecordStore::new();

            let hash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
            let prefix = &hash[..2];
            let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
            put_object(&object_store, &key, b"expired orphan");

            // First mark (create quarantine entry with retention=0 → immediately expired).
            let mark_result = run_gc_with_stores(
                &record_store,
                &index_store,
                &object_store,
                &[ServerFrontend::Xet],
                LocalGcOptions::mark_only(0),
            )
            .await;
            assert!(mark_result.is_ok());

            // Now sweep — the entry is already expired.
            let sweep_result = run_gc_with_stores(
                &record_store,
                &index_store,
                &object_store,
                &[ServerFrontend::Xet],
                LocalGcOptions::sweep_only(),
            )
            .await;
            assert!(sweep_result.is_ok(), "sweep should succeed: {:?}", sweep_result);
            let diagnostics = sweep_result.unwrap();
            assert_eq!(diagnostics.report.deleted_chunks, 1);
            assert_eq!(diagnostics.report.deleted_bytes, 14); // "expired orphan" is 14 bytes
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    // ── orphan_objects.retain test (retention hold filtering in run_gc_with_stores) ─

    #[test]
    fn gc_skips_orphans_with_active_retention_holds() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = std::env::temp_dir().join(format!("gc-test-hold-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(dir.join("chunks")).unwrap();
            let object_store = ServerObjectStore::local(&dir).unwrap();
            let index_store = MemoryIndexStore::new();
            let record_store = MemoryRecordStore::new();

            let now = shardline_protocol::unix_now_seconds_lossy();
            // Create an orphan chunk.
            let hash = "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh";
            let prefix = &hash[..2];
            let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
            put_object(&object_store, &key, b"retained orphan");

            // Add an active retention hold for the orphan.
            let hold = RetentionHold::new(
                key.clone(),
                "operator hold".to_owned(),
                now - 100,
                Some(now + 3600),
            )
            .unwrap();
            index_store.upsert_retention_hold(&hold).await.unwrap();

            // Run GC mark — the orphan should be skipped due to the retention hold.
            let result = run_gc_with_stores(
                &record_store,
                &index_store,
                &object_store,
                &[ServerFrontend::Xet],
                LocalGcOptions::mark_only(86400),
            )
            .await;
            assert!(result.is_ok(), "mark should succeed: {:?}", result);
            let diagnostics = result.unwrap();
            assert_eq!(
                diagnostics.report.orphan_chunks, 0,
                "orphan with active hold should not be counted"
            );
            assert_eq!(
                diagnostics.report.new_quarantine_candidates, 0,
                "orphan with active hold should not be quarantined"
            );
        });
    }

    // ── run_gc_with_stores with non-Xet frontend ────────────────────────

    #[test]
    fn gc_with_lfs_frontend_does_not_error() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let record_store = MemoryRecordStore::new();
            let index_store = MemoryIndexStore::new();
            let object_store = ServerObjectStore::blackhole();

            let result = run_gc_with_stores(
                &record_store,
                &index_store,
                &object_store,
                &[],
                LocalGcOptions::dry_run(),
            )
            .await;
            assert!(result.is_ok(), "empty frontend should not error: {:?}", result);
        });
    }

    // ── Additional GcError boundary tests ───────────────────────────────

    #[test]
    fn gc_error_from_s3_object_store_error_display() {
        let inner = S3ObjectStoreError::Io(std::io::Error::other("s3 error"));
        let err = GcError::from(inner);
        assert_eq!(err.to_string(), "s3 object storage operation failed");
        let inner2 = S3ObjectStoreError::Io(std::io::Error::other("s3 error"));
        let err2 = GcError::S3ObjectStore(inner2);
        assert!(matches!(err2, GcError::S3ObjectStore(_)));
    }

    #[test]
    fn gc_error_display_for_object_store_variant() {
        let inner = ServerObjectStoreError::NotFound;
        let err = GcError::ObjectStore(inner);
        assert_eq!(err.to_string(), "object storage adapter operation failed");
    }

    // ── LocalGcOptions boundary ─────────────────────────────────────────

    #[test]
    fn gc_options_all_zero_retention_is_accepted() {
        // Even zero retention should not panic — the minimum is enforced elsewhere.
        let opts = LocalGcOptions::mark_only(0);
        assert_eq!(opts.retention_seconds, 0);
        assert!(opts.mark);
        assert!(!opts.sweep);
    }

    #[test]
    fn gc_options_max_retention_does_not_panic() {
        let opts = LocalGcOptions::mark_and_sweep(u64::MAX);
        assert_eq!(opts.retention_seconds, u64::MAX);
        assert!(opts.mark);
        assert!(opts.sweep);
    }

    // ── GcReport default ────────────────────────────────────────────────

    #[test]
    fn gc_report_default_all_zeros() {
        let report = LocalGcReport::default();
        assert_eq!(report.scanned_records, 0);
        assert_eq!(report.referenced_chunks, 0);
        assert_eq!(report.orphan_chunks, 0);
        assert_eq!(report.orphan_chunk_bytes, 0);
        assert_eq!(report.active_quarantine_candidates, 0);
        assert_eq!(report.new_quarantine_candidates, 0);
        assert_eq!(report.retained_quarantine_candidates, 0);
        assert_eq!(report.released_quarantine_candidates, 0);
        assert_eq!(report.deleted_chunks, 0);
        assert_eq!(report.deleted_bytes, 0);
    }
}
