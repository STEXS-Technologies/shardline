//! GC execution logic.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    /// Test-only wall-clock pin for [`run_gc_with_stores`], mirroring the
    /// `COLLECT_CALL_COUNT` thread-local in `reachability`: each `#[test]`
    /// runs on its own thread, so a test that pins the clock cannot perturb the
    /// `run_gc_with_stores` calls of other tests. `None` (the default)
    /// preserves production behavior: the real system clock.
    static TEST_NOW_UNIX_SECONDS_OVERRIDE: Cell<Option<u64>> = const { Cell::new(None) };
}

/// Test-only: pins the wall clock consulted by [`run_gc_with_stores`] for the
/// calling test thread, so a regression test can simulate a forward clock jump
/// between two consecutive runs (F-88). Pass `None` to restore the real clock.
#[cfg(test)]
pub(crate) fn set_gc_now_unix_seconds_override(now_unix_seconds: Option<u64>) {
    TEST_NOW_UNIX_SECONDS_OVERRIDE.with(|slot| slot.set(now_unix_seconds));
}

/// Returns the wall clock for this GC run: the test override when one is pinned
/// on this thread, otherwise the real system clock.
#[cfg(test)]
#[must_use]
fn gc_now_unix_seconds() -> u64 {
    TEST_NOW_UNIX_SECONDS_OVERRIDE
        .with(|slot| slot.get())
        .unwrap_or_else(unix_now_seconds_lossy)
}

/// Returns the wall clock for this GC run.
#[cfg(not(test))]
#[must_use]
fn gc_now_unix_seconds() -> u64 {
    unix_now_seconds_lossy()
}

use shardline_index::{AsyncIndexStore, LocalIndexStore, LocalRecordStore, QuarantineCandidate};
use shardline_protocol::unix_now_seconds_lossy;
use shardline_server_core::{
    InvalidLifecycleMetadataError, ServerObjectStore, server_frontend::ServerFrontend,
};
use shardline_storage::ObjectStore;

use crate::{
    error::GcError,
    quarantine::{
        read_active_retention_hold_object_keys, read_last_gc_clock_anchor,
        read_newest_stored_creation_timestamp, read_quarantine_entries,
        reconcile_quarantine_entries, sweep_quarantine_entries, write_last_gc_clock_anchor,
    },
    reachability::{
        OrphanObject, ReachabilityAccumulator, collect_referenced_object_keys,
        managed_object_hash_or_object_key, scan_orphan_objects, scan_stale_temporary_artifacts,
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
    let now_unix_seconds = gc_now_unix_seconds();

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

    // Forward-clock sanity guard (F-45, hardened by F-57 and F-88): a forward
    // NTP step / VM time-sync-after-pause makes every quarantine candidate
    // eligible AND every time-bounded hold inactive, which would mass-delete
    // recoverable data and strip all hold protection in one pass. When `now`
    // jumps forward beyond a slack relative to the newest trustworthy
    // wall-clock observation (the newest stored lifecycle CREATION timestamp
    // and the persisted last-GC-clock anchor), skip the sweep and hold pruning
    // for this run (warn), mirroring the backwards `temp_reaping_clock_is_skewed`
    // guard for the reaper.
    //
    // The reference is creation-timestamps-only: `delete_after` (=
    // `first_seen` + retention) and hold `release_after` are future-dated on
    // any deployment with an active hold or a retention longer than the slack,
    // and including them would blind the guard to forward jumps of days to
    // weeks (the F-57 bypass). The anchor keeps the reference as fresh as the
    // last trusted GC run so a low-churn deployment does not spuriously age the
    // creation-only reference past the slack.
    //
    // F-88 hardening: a fired guard ALSO DEFERS THE MARK
    // (`reconcile_quarantine_entries`) for the run. The mark stamps new
    // quarantine candidates' `first_seen_unreachable_at` with `now`, so a
    // jumped clock would write the jumped timestamp into the creation
    // reference — the next run's guard would then see a "fresh" creation
    // timestamp and go silent, letting the sweep delete pre-jump candidates up
    // to (jump − slack) before their real retention elapsed and permanently
    // poisoning the anchor with the jumped clock. Untrusted-clock timestamps
    // must never enter the creation reference: a fired run stamps nothing,
    // marks nothing, and deletes nothing, so the guard stays armed until a
    // later trusted run (self-healing via the anchor and via server-side
    // lifecycle events) refreshes the reference.
    let newest_stored_creation_timestamp =
        read_newest_stored_creation_timestamp(index_store).await?;
    let last_gc_clock_anchor = read_last_gc_clock_anchor(object_store)?;
    let retention_clock_is_skewed_forward = retention_clock_is_skewed_forward(
        now_unix_seconds,
        newest_stored_creation_timestamp,
        last_gc_clock_anchor,
    );
    if retention_clock_is_skewed_forward {
        tracing::warn!(
            "GC wall clock ({now_unix_seconds}s) jumped forward relative to the newest stored \
             lifecycle creation timestamp; deferring the quarantine mark, sweep, and hold \
             pruning this run (a jumped clock must never stamp quarantine timestamps)"
        );
    } else if options.mark || options.sweep {
        // The forward guard cleared: this run's wall clock is trustworthy
        // (within the slack of every stored creation timestamp and the previous
        // anchor), so persist it as the new last-GC-clock anchor. A fired run's
        // `now` is suspect and is never stamped — the anchor stays at the last
        // TRUSTED observation, which is exactly what makes a between-runs jump
        // detectable on a low-churn deployment.
        //
        // Only runs that actually mutate the object store (mark and/or sweep)
        // persist the anchor; a pure dry run must remain read-only. A failed
        // anchor write is tolerated (warn and continue): the anchor is an
        // optimization, not a correctness requirement, and the forward guard
        // falls back to the creation-timestamp-only reference on later runs —
        // the safe pre-anchor behavior. This keeps read-only stores (chmod 0555
        // chunks dir; least-privilege S3 cron roles) fully operational: they
        // still get complete GC diagnostics and passes instead of aborting with
        // a PermissionDenied error (F-76).
        if let Err(error) = write_last_gc_clock_anchor(object_store, now_unix_seconds) {
            tracing::warn!(
                "failed to persist the last-GC-clock anchor at gc/last-gc-clock-anchor \
                 ({error}); continuing with the creation-timestamp-only reference"
            );
        }
    }

    let prune_expired_retention_holds =
        (options.mark || options.sweep) && !retention_clock_is_skewed_forward;
    // When the clock is untrustworthy (forward jump suspected), `now` is not
    // usable for hold activity: treat every hold as active (now = 0) so held
    // data is never quarantined or swept, and skip expired-hold pruning.
    let hold_activity_now = if retention_clock_is_skewed_forward {
        0
    } else {
        now_unix_seconds
    };
    let active_retention_hold_object_keys = read_active_retention_hold_object_keys(
        index_store,
        hold_activity_now,
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

    // The mark is gated on the same `!retention_clock_is_skewed_forward`
    // condition as the sweep below (F-88): `reconcile_quarantine_entries`
    // stamps new candidates' `first_seen_unreachable_at` with
    // `now_unix_seconds`, so a fired run's jumped `now` must never enter the
    // creation-timestamp reference that keeps the guard armed. Deferring the
    // whole mark (not just the stamping) also defers the stale-entry release
    // repair by one run — a safe, data-preserving choice while the clock is
    // untrustworthy (held data is already protected: with the guard fired,
    // `hold_activity_now` is 0, so every hold is treated as active).
    if options.mark && !retention_clock_is_skewed_forward {
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
        if retention_clock_is_skewed_forward {
            // The forward-clock guard fired above: the sweep's eligibility
            // (`delete_after <= now`) and the temp-reaper age bounds both
            // depend on a trustworthy `now`. Skipping the whole sweep block
            // (quarantine sweep + chunk temp reaping + S3 temp reaping) is the
            // safe, data-preserving choice.
            tracing::warn!(
                "skipping the quarantine sweep and temp reaping this run: GC wall clock \
                 ({now_unix_seconds}s) jumped forward relative to stored lifecycle timestamps"
            );
        } else {
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
            let temp_scan =
                scan_stale_temporary_artifacts(object_store, frontends, now_unix_seconds)?;
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
                    report.reaped_stale_temporary_chunks = shardline_server_core::checked_add(
                        report.reaped_stale_temporary_chunks,
                        1,
                    )?;
                    report.reaped_stale_temporary_bytes = shardline_server_core::checked_add(
                        report.reaped_stale_temporary_bytes,
                        temp_bytes,
                    )?;
                }
            }

            // Reap stranded S3 temp-upload artifacts when the backing object
            // store is the S3 backend (F-48). This sits inside the non-skewed
            // branch: the age bound is computed from GC's `now`, so a forward
            // clock jump (guard above) would make the cutoff too old and could
            // reap live artifacts. The chunk-temp reaper above only matches
            // local `.tmp-*` keys, and the S3 listing paths skip temp keys
            // entirely, so crash-stranded `<key>.tmp.<counter>.<pid>.<nanos>`
            // and `__tmp/shardline-stream-upload/...` keys would otherwise
            // accumulate forever. `sweep_stale_temp_keys` does its own raw
            // enumeration and applies the age bound to the S3-observed
            // LastModified (backend truth), so a live in-flight upload is
            // never reaped.
            if let ServerObjectStore::S3(store) = object_store {
                let (reaped, reaped_bytes) = store.sweep_stale_temp_keys(now_unix_seconds)?;
                if reaped != 0 {
                    tracing::info!(
                        reaped,
                        reaped_bytes,
                        "reaped stale S3 temp upload artifacts"
                    );
                }
                report.reaped_stale_temporary_chunks = shardline_server_core::checked_add(
                    report.reaped_stale_temporary_chunks,
                    u64::try_from(reaped)?,
                )?;
                report.reaped_stale_temporary_bytes = shardline_server_core::checked_add(
                    report.reaped_stale_temporary_bytes,
                    reaped_bytes,
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

/// Returns true when the GC wall clock appears to have jumped FORWARD relative
/// to the newest trustworthy wall-clock observation: the newest CREATION
/// timestamp stored in the index (quarantine `first_seen_unreachable_at`,
/// retention-hold `held_at`) and the persisted last-GC-clock anchor.
///
/// The mirror of [`temp_reaping_clock_is_skewed`] for the forward direction: a
/// forward NTP step or a VM time-sync-after-pause makes every quarantine
/// candidate look expired (mass deletion of recoverable data) and every
/// time-bounded hold look inactive (hold protection gone) in a single pass.
/// When this fires, `now_unix_seconds` is not a trustworthy reference for the
/// sweep's eligibility and hold-activity decisions, so the sweep and hold
/// pruning are skipped for the run rather than risk deleting held/recoverable
/// data.
///
/// Future-dated lifecycle fields (`delete_after = first_seen + retention` and
/// hold `release_after`) are deliberately EXCLUDED from the reference: an
/// active hold or a retention longer than the clock slack keeps those fields
/// in the future, which would blind the guard to forward jumps of up to
/// (newest future timestamp - real now + slack) — days to weeks — and let the
/// sweep delete candidates before their real retention elapsed (F-57).
///
/// The anchor is the wall clock of the last GC run whose clock the guard
/// trusted. It keeps the reference as fresh as the last trusted run, so a
/// deployment where GC runs more often than lifecycle activity happens
/// (low churn) does not spuriously age the creation-only reference past the
/// slack; it also makes a forward jump that occurs between two consecutive runs
/// detectable even when no lifecycle activity occurred in between.
///
/// A slack absorbs the normal gap between lifecycle activity and the sweep on
/// an active deployment. The guard errs toward preserving data: when no
/// lifecycle entry or anchor is recent enough, reclamation is deferred rather
/// than risked.
#[must_use]
pub(crate) const fn retention_clock_is_skewed_forward(
    now_unix_seconds: u64,
    newest_stored_creation_timestamp: Option<u64>,
    last_gc_clock_anchor: Option<u64>,
) -> bool {
    let reference = match (newest_stored_creation_timestamp, last_gc_clock_anchor) {
        (Some(creation), Some(anchor)) => Some(if creation > anchor { creation } else { anchor }),
        (Some(creation), None) => Some(creation),
        (None, Some(anchor)) => Some(anchor),
        (None, None) => None,
    };
    let Some(newest_stored) = reference else {
        return false;
    };
    const CLOCK_FORWARD_SLACK_SECONDS: u64 = 86_400;
    now_unix_seconds > newest_stored.saturating_add(CLOCK_FORWARD_SLACK_SECONDS)
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
                    // A held+quarantined object is a REPAIRABLE state, not a
                    // hard abort. A hold and a quarantine entry on the same key
                    // are contradictory: the hold keeps the data, and the
                    // quarantine entry is stale. Let the run proceed so
                    // `reconcile_quarantine_entries` (mark) or the sweep
                    // releases the stale entry; erroring here gated run start
                    // (before that repair code) and wedged every subsequent GC
                    // run until the hold expired — permanently for holds with
                    // `release_after = None`.
                    tracing::warn!(
                        "retention hold and quarantine entry both present for {}; \
                         releasing the stale quarantine candidate this run \
                         (the hold keeps the data)",
                        hold.object_key().as_str(),
                    );
                    return Ok(());
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
    use super::{retention_clock_is_skewed_forward, temp_reaping_clock_is_skewed};

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

    #[test]
    fn retention_clock_skewed_forward_when_now_beyond_stored_timestamps() {
        // Stored lifecycle creation timestamps are ~3 days old while the GC
        // wall clock reads now — a forward NTP step / VM time-sync-after-pause.
        // The sweep and hold pruning must be skipped.
        let now_secs = 2_000_000_000_u64;
        let stale_stored = now_secs - 3 * 86_400;
        assert!(retention_clock_is_skewed_forward(
            now_secs,
            Some(stale_stored),
            None
        ));
    }

    #[test]
    fn retention_clock_not_skewed_forward_within_slack() {
        // A stored creation timestamp a few hours old (the normal gap between
        // lifecycle activity and the sweep on an active deployment) is within
        // the slack and must NOT disable the sweep.
        let now_secs = 2_000_000_000_u64;
        let recent_stored = now_secs - 3600;
        assert!(!retention_clock_is_skewed_forward(
            now_secs,
            Some(recent_stored),
            None
        ));
    }

    #[test]
    fn retention_clock_fires_on_forward_jump_despite_future_dated_hold() {
        // F-57 regression: the guard reference must be CREATION timestamps
        // only. The proven bypass put a candidate 1 day old (7-day retention)
        // alongside an active hold with release_after = +30 days; the old
        // guard's reference (max incl. delete_after/release_after) sat 30 days
        // in the future, so a 10-day forward jump left it SILENT and the sweep
        // deleted the candidate ~6 days before its real retention elapsed.
        // With the creation-only reference (newest creation = first_seen, which
        // is <= real now for a healthy clock) the same jump must fire and skip
        // the sweep.
        let real_now = 2_000_000_000_u64;
        let first_seen = real_now - 86_400; // candidate is 1 day old
        let jumped_now = real_now + 10 * 86_400; // 10-day forward jump
        let _release_after = real_now + 30 * 86_400; // the future-dated hold field

        assert!(
            retention_clock_is_skewed_forward(jumped_now, Some(first_seen), None),
            "a >1-day forward jump must fire even when the hold release_after is \
             future-dated — the reference must be creation timestamps only"
        );
    }

    #[test]
    fn retention_clock_forward_jump_fires_identically_without_future_dated_entries() {
        // Control for the F-57 regression: with the same forward jump but no
        // future-dated entries at all (newest creation = first_seen, no hold),
        // the guard fires exactly as before the fix — the creation-only
        // reference is unchanged for deployments without future-dated rows.
        let real_now = 2_000_000_000_u64;
        let first_seen = real_now - 86_400; // candidate is 1 day old
        let jumped_now = real_now + 10 * 86_400; // 10-day forward jump
        assert!(retention_clock_is_skewed_forward(
            jumped_now,
            Some(first_seen),
            None
        ));
    }

    #[test]
    fn retention_clock_not_skewed_forward_when_anchor_is_recent() {
        // Low-churn deployment: the newest lifecycle CREATION timestamp is 5
        // days old (no recent lifecycle activity), but the persisted last-GC
        // anchor was written by a trusted run an hour ago. The anchor keeps the
        // reference fresh, so a healthy deployment that runs GC more often than
        // lifecycle activity happens must NOT have its sweep disabled.
        let now_secs = 2_000_000_000_u64;
        let stale_creation = now_secs - 5 * 86_400;
        let recent_anchor = now_secs - 3600;
        assert!(!retention_clock_is_skewed_forward(
            now_secs,
            Some(stale_creation),
            Some(recent_anchor)
        ));
    }

    #[test]
    fn retention_clock_skewed_forward_when_jump_since_last_trusted_anchor() {
        // A forward jump of more than the slack since the last trusted GC run
        // fires even when no lifecycle activity refreshed the creation
        // timestamps in between (the anchor's purpose): the sweep must be
        // skipped.
        let now_secs = 2_000_000_000_u64;
        let stale_creation = now_secs - 3 * 86_400;
        let stale_anchor = now_secs - 2 * 86_400;
        assert!(retention_clock_is_skewed_forward(
            now_secs,
            Some(stale_creation),
            Some(stale_anchor)
        ));
    }

    #[test]
    fn retention_clock_not_skewed_forward_without_stored_timestamps() {
        // No quarantine candidates, retention holds, or persisted anchor at
        // all: nothing to guard against.
        assert!(!retention_clock_is_skewed_forward(
            2_000_000_000_u64,
            None,
            None
        ));
    }
}
