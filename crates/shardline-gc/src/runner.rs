//! GC execution logic.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use shardline_index::{AsyncIndexStore, LocalIndexStore, LocalRecordStore, QuarantineCandidate};
use shardline_protocol::unix_now_seconds_lossy;
use shardline_server_core::{
    InvalidLifecycleMetadataError, ServerObjectStore, server_frontend::ServerFrontend,
};
use shardline_storage::ObjectStore;

use crate::{
    error::GcError,
    quarantine::{
        read_active_retention_hold_object_keys, read_quarantine_entries,
        reconcile_quarantine_entries, sweep_quarantine_entries,
    },
    reachability::{
        OrphanObject, ReachabilityAccumulator, collect_referenced_object_keys,
        managed_object_hash_or_object_key, scan_orphan_objects,
        scan_stale_temporary_chunk_artifacts,
    },
    types::{
        GcOrphanInventoryEntry, GcOrphanQuarantineState, GcRetentionReportEntry,
        LocalGcDiagnostics, LocalGcOptions, LocalGcReport,
    },
};

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
    RecordAdapter: shardline_index::RecordStore + Sync,
    RecordAdapter::Error: Into<GcError>,
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<GcError>,
{
    let mark_start = std::time::Instant::now();
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
    shardline_metrics::record_gc_mark_duration(mark_start.elapsed());

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
        reaped_stale_temporary_chunks: 0,
        reaped_stale_temporary_bytes: 0,
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
        let sweep_start = std::time::Instant::now();
        sweep_quarantine_entries(
            record_store,
            object_store,
            index_store,
            frontends,
            &orphan_objects,
            now_unix_seconds,
            &mut quarantine_entries,
            &mut report,
        )
        .await?;
        shardline_metrics::record_gc_sweep_duration(sweep_start.elapsed());

        // Reap stranded chunk temp artifacts. `scan_orphan_objects` skips
        // `.tmp-*` keys (so an in-progress write never aborts the pass), which
        // previously left abandoned temps from killed/crashed writers in place
        // forever. The age bound is applied to the GC-observed mtime (with a
        // writer-embedded-nanos fallback), so a live in-flight write is never
        // reaped even under writer/GC wall-clock divergence. As defense in
        // depth, if this GC host's own clock appears to be behind the newest
        // writer-embedded temp timestamp (a backwards clock step), skip temp
        // reaping entirely for this run.
        let temp_scan = scan_stale_temporary_chunk_artifacts(object_store, now_unix_seconds)?;
        if temp_reaping_clock_is_skewed(now_unix_seconds, temp_scan.max_embedded_temp_nanos) {
            tracing::warn!(
                "GC wall clock ({now_unix_seconds}s) is behind the newest writer-embedded \
                 temp timestamp; skipping chunk temp reaping this run"
            );
        } else {
            for (temp_key, temp_bytes) in temp_scan.stale {
                object_store
                    .delete_if_present(&temp_key)
                    .map_err(GcError::ObjectStore)?;
                report.reaped_stale_temporary_chunks =
                    shardline_server_core::checked_add(report.reaped_stale_temporary_chunks, 1)?;
                report.reaped_stale_temporary_bytes = shardline_server_core::checked_add(
                    report.reaped_stale_temporary_bytes,
                    temp_bytes,
                )?;
            }
        }
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

/// Returns true when the GC host's wall clock appears to be behind the newest
/// writer-embedded temp timestamp observed in the object store — i.e. the GC
/// clock has likely stepped backwards relative to the writers.
///
/// When this fires, `now_unix_seconds` is not a trustworthy reference point for
/// the temp age bound, so temp reaping is skipped for the run rather than risk
/// misclassifying artifacts. A small slack absorbs the writer rounding its
/// creation time up to the next second plus normal jitter.
#[must_use]
pub(crate) fn temp_reaping_clock_is_skewed(
    now_unix_seconds: u64,
    max_embedded_temp_nanos: Option<u128>,
) -> bool {
    let Some(max_embedded_nanos) = max_embedded_temp_nanos else {
        return false;
    };
    let now_nanos = u128::from(now_unix_seconds) * 1_000_000_000;
    const CLOCK_SLACK_NANOS: u128 = 60 * 1_000_000_000;
    now_nanos.saturating_add(CLOCK_SLACK_NANOS) < max_embedded_nanos
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

pub(crate) fn build_gc_diagnostics(
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

pub(crate) fn retention_report_entry(
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

pub(crate) fn orphan_inventory_entry(
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
mod tests {
    use super::temp_reaping_clock_is_skewed;

    #[test]
    fn temp_reaping_clock_skewed_when_gc_clock_behind_embedded_nanos() {
        // GC clock is 2h behind the newest writer-embedded temp timestamp
        // (a backwards step on the GC host): reaping must be skipped.
        let now_secs = 2_000_000_000_u64;
        let future_nanos = u128::from(now_secs + 2 * 3600) * 1_000_000_000;
        assert!(temp_reaping_clock_is_skewed(now_secs, Some(future_nanos)));
    }

    #[test]
    fn temp_reaping_clock_not_skewed_within_slack() {
        // A temp embedded a few seconds ahead (writer rounding / jitter) is
        // within the slack and must NOT disable reaping.
        let now_secs = 2_000_000_000_u64;
        let near_future_nanos = u128::from(now_secs + 5) * 1_000_000_000;
        assert!(!temp_reaping_clock_is_skewed(
            now_secs,
            Some(near_future_nanos)
        ));
    }

    #[test]
    fn temp_reaping_clock_not_skewed_when_embedded_is_past() {
        // All embedded timestamps are in the past relative to the GC clock: a
        // normal, trustworthy run.
        let now_secs = 2_000_000_000_u64;
        let past_nanos = u128::from(now_secs - 10) * 1_000_000_000;
        assert!(!temp_reaping_clock_is_skewed(now_secs, Some(past_nanos)));
    }

    #[test]
    fn temp_reaping_clock_not_skewed_without_observed_temps() {
        // No chunk temp artifacts observed at all: nothing to guard against.
        assert!(!temp_reaping_clock_is_skewed(2_000_000_000_u64, None));
    }
}
