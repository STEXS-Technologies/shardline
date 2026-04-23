#[cfg(test)]
use std::path::Path;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

mod quarantine;
mod reachability;

use serde::{Deserialize, Serialize};
use shardline_index::{
    AsyncIndexStore, LocalIndexStore, PostgresIndexStore, PostgresRecordStore, QuarantineCandidate,
    RecordStore,
};
use shardline_protocol::unix_now_seconds_lossy;

use crate::{
    InvalidLifecycleMetadataError, ServerConfig, ServerError,
    object_store::{ServerObjectStore, object_store_from_config},
    overflow::checked_add,
    postgres_backend::connect_postgres_metadata_pool,
    record_store::LocalRecordStore,
    server_frontend::ServerFrontend,
};
use quarantine::{
    read_active_retention_hold_object_keys, read_quarantine_entries, reconcile_quarantine_entries,
    sweep_quarantine_entries,
};
use reachability::{
    OrphanObject, ReachabilityAccumulator, collect_referenced_object_keys,
    managed_object_hash_or_object_key, scan_orphan_objects,
};

/// Default retention window for new local quarantine candidates.
pub const DEFAULT_LOCAL_GC_RETENTION_SECONDS: u64 = 86_400;

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
#[derive(Debug, Clone, PartialEq, Eq)]
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
/// Returns [`ServerError`] when metadata cannot be read, record JSON is invalid,
/// quarantine state cannot be updated, or deletion fails.
pub async fn run_local_gc(
    root: PathBuf,
    options: LocalGcOptions,
) -> Result<LocalGcReport, ServerError> {
    Ok(run_local_gc_diagnostics(root, options).await?.report)
}

/// Runs local filesystem garbage collection and returns operator diagnostics.
///
/// # Errors
///
/// Returns [`ServerError`] when metadata cannot be read, record JSON is invalid,
/// quarantine state cannot be updated, or deletion fails.
pub async fn run_local_gc_diagnostics(
    root: PathBuf,
    options: LocalGcOptions,
) -> Result<LocalGcDiagnostics, ServerError> {
    let object_store = ServerObjectStore::local(root.join("chunks"))?;
    let index_store = LocalIndexStore::open(root.clone());
    let record_store = LocalRecordStore::open(root);
    run_gc_with_stores(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
    )
    .await
}

/// Runs garbage collection against the configured metadata backend and local chunk storage.
///
/// # Errors
///
/// Returns [`ServerError`] when metadata cannot be read, quarantine state cannot be
/// updated, or deletion fails.
pub async fn run_gc(
    config: ServerConfig,
    options: LocalGcOptions,
) -> Result<LocalGcReport, ServerError> {
    Ok(run_gc_diagnostics(config, options).await?.report)
}

/// Runs garbage collection and returns operator diagnostics.
///
/// # Errors
///
/// Returns [`ServerError`] when metadata cannot be read, quarantine state cannot be
/// updated, or deletion fails.
pub async fn run_gc_diagnostics(
    config: ServerConfig,
    options: LocalGcOptions,
) -> Result<LocalGcDiagnostics, ServerError> {
    let object_store = object_store_from_config(&config)?;
    if let Some(index_postgres_url) = config.index_postgres_url() {
        let pool = connect_postgres_metadata_pool(index_postgres_url, 4)?;
        let index_store = PostgresIndexStore::new(pool.clone());
        let record_store = PostgresRecordStore::new(pool);
        return run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            config.server_frontends(),
            options,
        )
        .await;
    }

    let index_store = LocalIndexStore::open(config.root_dir().to_path_buf());
    let record_store = LocalRecordStore::open(config.root_dir().to_path_buf());
    run_gc_with_stores(
        &record_store,
        &index_store,
        &object_store,
        config.server_frontends(),
        options,
    )
    .await
}

async fn run_gc_with_stores<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    options: LocalGcOptions,
) -> Result<LocalGcDiagnostics, ServerError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ServerError>,
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<ServerError>,
{
    let mut reachability = ReachabilityAccumulator::default();
    let now_unix_seconds = unix_now_seconds_lossy();

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
    let orphan_chunk_bytes = orphan_objects
        .values()
        .try_fold(0_u64, |total, orphan| checked_add(total, orphan.bytes))?;

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
            options.retention_seconds,
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
) -> Result<(), ServerError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<ServerError>,
{
    let mut quarantined_object_keys = HashSet::new();

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

            let Some(metadata) = object_store.metadata(candidate.object_key())? else {
                return Err(
                    InvalidLifecycleMetadataError::QuarantineCandidateMissingObject {
                        object_key: candidate.object_key().as_str().to_owned(),
                    }
                    .into(),
                );
            };
            if metadata.length() != candidate.observed_length() {
                return Err(
                    InvalidLifecycleMetadataError::QuarantineCandidateLengthMismatch {
                        object_key: candidate.object_key().as_str().to_owned(),
                        expected_length: candidate.observed_length(),
                        observed_length: metadata.length(),
                    }
                    .into(),
                );
            }

            quarantined_object_keys.insert(candidate.object_key().as_str().to_owned());
            Ok::<(), ServerError>(())
        })
        .await?;

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

            Ok::<(), ServerError>(())
        })
        .await?;

    index_store
        .visit_webhook_deliveries(|_delivery| Ok::<(), ServerError>(()))
        .await?;
    index_store
        .visit_provider_repository_states(|_state| Ok::<(), ServerError>(()))
        .await?;

    Ok(())
}

#[cfg(test)]
fn quarantine_root(root: &Path) -> PathBuf {
    root.join("gc").join("quarantine")
}

#[cfg(test)]
fn quarantine_record_path(root: &Path, hash: &str) -> PathBuf {
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

#[cfg(test)]
mod tests;
