use std::collections::{HashMap, HashSet};

use shardline_index::{AsyncIndexStore, QuarantineCandidate, RecordStore};
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
use shardline_xet_adapter::xorb_hash_from_object_key_if_present;

use crate::{
    GcError, ServerFrontend,
    reachability::{OrphanObject, ReachabilityAccumulator, collect_referenced_object_keys},
    types::LocalGcReport,
};
use shardline_server_core::{ServerObjectStore, checked_add, checked_increment, chunk_hash};

pub(super) async fn read_quarantine_entries<IndexAdapter>(
    index_store: &IndexAdapter,
) -> Result<HashMap<String, QuarantineCandidate>, GcError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<GcError>,
{
    let mut entries_by_object_key = HashMap::new();
    index_store
        .visit_quarantine_candidates(|candidate| {
            entries_by_object_key.insert(candidate.object_key().as_str().to_owned(), candidate);
            Ok::<(), GcError>(())
        })
        .await?;

    Ok(entries_by_object_key)
}

/// Returns the newest CREATION timestamp stored in the index: the maximum of
/// every quarantine candidate's `first_seen_unreachable_at` and every retention
/// hold's `held_at`.
///
/// This is the reference point for the forward-clock guard: creation
/// timestamps are written by the same wall clock the GC reads at
/// lifecycle-event time, so a GC `now` far ahead of the newest stored creation
/// timestamp indicates a forward NTP step / VM time-sync-after-pause rather
/// than genuine elapsed time.
///
/// Future-dated lifecycle fields (`delete_after = first_seen + retention` and
/// hold `release_after`) are deliberately EXCLUDED: any deployment with an
/// active hold or a retention longer than the clock slack keeps those fields
/// in the future, which previously blinded the guard to forward jumps of up to
/// (newest future timestamp - real now + slack) — days to weeks — and let the
/// sweep delete candidates before their real retention elapsed (F-57).
///
/// Returns `None` when the index holds no lifecycle entries at all.
pub(super) async fn read_newest_stored_creation_timestamp<IndexAdapter>(
    index_store: &IndexAdapter,
) -> Result<Option<u64>, GcError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<GcError>,
{
    let mut newest_stored: Option<u64> = None;
    index_store
        .visit_quarantine_candidates(|candidate| {
            let candidate_newest = candidate.first_seen_unreachable_at_unix_seconds();
            newest_stored =
                Some(newest_stored.map_or(candidate_newest, |newest| newest.max(candidate_newest)));
            Ok::<(), GcError>(())
        })
        .await?;
    index_store
        .visit_retention_holds(|hold| {
            let hold_newest = hold.held_at_unix_seconds();
            newest_stored =
                Some(newest_stored.map_or(hold_newest, |newest| newest.max(hold_newest)));
            Ok::<(), GcError>(())
        })
        .await?;

    Ok(newest_stored)
}

/// Reserved object key for the persisted last-GC-clock anchor.
///
/// Written on every store-mutating GC run (mark and/or sweep) whose wall clock
/// the forward guard deemed trustworthy; a pure dry run remains read-only and
/// never writes it, and a failed write is tolerated (the anchor is an
/// optimization, not a correctness requirement). It records that run's `now` so
/// a forward jump occurring between two consecutive runs is detected even when
/// no lifecycle activity has refreshed the creation-timestamp reference
/// (low-churn deployments). Without the anchor, a healthy deployment where GC
/// runs more often than lifecycle activity happens would age the creation-only
/// reference past the clock slack and spuriously disable the sweep and temp
/// reaping.
///
/// The `gc/` namespace is not a managed object namespace (the reachability
/// scans never recognize it), so the anchor is invisible to every GC path:
/// `scan_orphan_objects` skips it, the temp reaper's `.tmp-` grammar cannot
/// match it, and the S3 temp sweep only matches temp-upload keys. It can never
/// be mistaken for — or reaped as — a managed object.
/// The reserved object key for the persisted last-GC-clock anchor.
///
/// `pub(super)` (visible to the whole crate) so the stale-temp reaper in
/// `reachability` can recognize a stranded `<anchor>.tmp-*` write artifact
/// (F-99).
pub(super) const LAST_GC_CLOCK_ANCHOR_KEY: &str = "gc/last-gc-clock-anchor";

fn last_gc_clock_anchor_key() -> Result<ObjectKey, GcError> {
    ObjectKey::parse(LAST_GC_CLOCK_ANCHOR_KEY).map_err(|_error| GcError::InvalidContentHash)
}

/// Reads the persisted last-GC-clock anchor: the wall clock recorded by the
/// most recent GC run whose clock the forward guard trusted.
///
/// Returns `None` when no anchor has been persisted yet. An unreadable or
/// malformed anchor (for example a torn write) is treated as absent — it is
/// logged and will be overwritten by a later store-mutating run's anchor write.
pub(super) fn read_last_gc_clock_anchor(
    object_store: &ServerObjectStore,
) -> Result<Option<u64>, GcError> {
    let key = last_gc_clock_anchor_key()?;
    let Some(metadata) = object_store.metadata(&key)? else {
        return Ok(None);
    };
    let body = object_store.read_full_object(&key, metadata.length())?;
    let parsed = std::str::from_utf8(&body)
        .ok()
        .and_then(|text| text.trim().parse::<u64>().ok());
    if parsed.is_none() {
        tracing::warn!(
            "ignoring unreadable last-GC-clock anchor at {LAST_GC_CLOCK_ANCHOR_KEY} ({} bytes); \
             it will be overwritten this run",
            body.len(),
        );
    }
    Ok(parsed)
}

/// Persists the supplied wall clock as the last-GC-clock anchor.
///
/// Called on every run that mutates the object store (mark and/or sweep) whose
/// clock the forward guard deemed trustworthy (the guard did not fire). A pure
/// dry run never calls this — dry runs must remain read-only. A fired run's
/// `now` is suspect — the sweep and hold pruning were skipped for that reason —
/// so it is never stamped as an anchor.
///
/// A write failure must be tolerated by the caller (warn and continue): the
/// anchor is an optimization, not a correctness requirement, and the forward
/// guard safely falls back to the creation-timestamp-only reference (the
/// pre-anchor behavior) when no anchor is persisted.
pub(super) fn write_last_gc_clock_anchor(
    object_store: &ServerObjectStore,
    now_unix_seconds: u64,
) -> Result<(), GcError> {
    let key = last_gc_clock_anchor_key()?;
    let body = now_unix_seconds.to_string();
    let integrity = ObjectIntegrity::new(
        chunk_hash(body.as_bytes()),
        u64::try_from(body.len()).unwrap_or(0),
    );
    object_store.put_overwrite(&key, ObjectBody::Borrowed(body.as_bytes()), &integrity)?;
    Ok(())
}

pub(super) async fn read_active_retention_hold_object_keys<IndexAdapter>(
    index_store: &IndexAdapter,
    now_unix_seconds: u64,
    prune_expired: bool,
) -> Result<HashSet<String>, GcError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<GcError>,
{
    let mut hold_object_keys = HashSet::new();
    let mut expired_object_keys = Vec::new();
    index_store
        .visit_retention_holds(|hold| {
            if hold.is_active_at(now_unix_seconds) {
                hold_object_keys.insert(hold.object_key().as_str().to_owned());
            } else if prune_expired && hold.release_after_unix_seconds().is_some() {
                expired_object_keys.push(hold.object_key().clone());
            }

            Ok::<(), GcError>(())
        })
        .await?;
    for object_key in expired_object_keys {
        let _deleted = index_store
            .delete_retention_hold(&object_key)
            .await
            .map_err(Into::into)?;
    }

    Ok(hold_object_keys)
}

pub(super) async fn reconcile_quarantine_entries<IndexAdapter>(
    index_store: &IndexAdapter,
    orphan_objects: &HashMap<String, OrphanObject>,
    now_unix_seconds: u64,
    retention_seconds: u64,
    quarantine_entries: &mut HashMap<String, QuarantineCandidate>,
    report: &mut LocalGcReport,
) -> Result<(), GcError>
where
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<GcError>,
{
    let stale_object_keys = quarantine_entries
        .keys()
        .filter(|object_key| !orphan_objects.contains_key(*object_key))
        .cloned()
        .collect::<Vec<_>>();

    for object_key in stale_object_keys {
        let Some(candidate) = quarantine_entries.remove(&object_key) else {
            continue;
        };
        index_store
            .delete_quarantine_candidate(candidate.object_key())
            .await
            .map_err(Into::into)?;
        report.released_quarantine_candidates =
            checked_increment(report.released_quarantine_candidates)?;
    }

    for (object_key, orphan) in orphan_objects {
        if let Some(existing_candidate) = quarantine_entries.get(object_key)
            && existing_candidate.observed_length() == orphan.bytes
        {
            report.retained_quarantine_candidates =
                checked_increment(report.retained_quarantine_candidates)?;
            continue;
        }

        let delete_after_unix_seconds = checked_add(now_unix_seconds, retention_seconds)?;
        let candidate = QuarantineCandidate::new(
            orphan.object_key.clone(),
            orphan.bytes,
            now_unix_seconds,
            delete_after_unix_seconds,
        )?;
        index_store
            .upsert_quarantine_candidate(&candidate)
            .await
            .map_err(Into::into)?;
        quarantine_entries.insert(object_key.clone(), candidate);
        report.new_quarantine_candidates = checked_increment(report.new_quarantine_candidates)?;
    }

    Ok(())
}

/// Sweeps expired quarantine candidates whose objects are confirmed still
/// unreferenced at delete time.
///
/// This is an internal helper called only from the runner; the eight
/// parameters bundle the store context plus the per-cycle state, so the
/// `too_many_arguments` default warning is intentionally suppressed.
#[allow(clippy::too_many_arguments)]
pub(super) async fn sweep_quarantine_entries<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    object_store: &ServerObjectStore,
    index_store: &IndexAdapter,
    frontends: &[ServerFrontend],
    orphan_objects: &HashMap<String, OrphanObject>,
    now_unix_seconds: u64,
    quarantine_entries: &mut HashMap<String, QuarantineCandidate>,
    report: &mut LocalGcReport,
) -> Result<(), GcError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<GcError>,
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<GcError>,
{
    let object_keys = quarantine_entries.keys().cloned().collect::<Vec<_>>();

    // Collect-once-then-check delete-time reachability.
    //
    // The cycle snapshot (`orphan_objects`) is taken at mark time. Between
    // that snapshot and this sweep a chunk may have been re-referenced (e.g.
    // re-uploaded, or an object-store write landing between
    // `collect_referenced_object_keys` and `scan_orphan_objects` in the
    // runner). Deleting it then would destroy live data and cause
    // reconstruction 404s.
    //
    // To close that stale-snapshot window we re-run the reachability mark
    // against the *current* index/record state once, immediately before the
    // sweep, and hold the resulting referenced-key set in memory. Every
    // expired candidate is then checked with an O(1) set membership test
    // against that single pre-built set — NOT a fresh full mark per candidate
    // (a full mark visits every record, every dedupe shard mapping, and for
    // XorbCdcV1 records reads + parses the stored xorb container; doing it
    // once per candidate would make the sweep O(candidates × store_size)).
    //
    // The supported server wrapper closes the residual mark-to-delete window
    // with an exclusive GC/write barrier. Every mutating HTTP request holds
    // the shared side from before it can write objects until its handler has
    // published metadata. Direct callers of `run_gc_with_stores` must provide
    // the same exclusion contract. If a candidate is
    // referenced in this set we skip the storage delete and only release the
    // (now stale) quarantine entry; the object stays on disk and is seen as
    // live on the next cycle, so no data is lost and nothing is resurrected.
    //
    // We only pay for the mark when at least one candidate actually reaches
    // the delete-time check (expired + still in `orphan_objects`); a sweep
    // with nothing to delete skips it entirely.
    let requires_reachability_check = object_keys.iter().any(|object_key| {
        orphan_objects.contains_key(object_key)
            && quarantine_entries
                .get(object_key)
                .is_some_and(|candidate| candidate.delete_after_unix_seconds() <= now_unix_seconds)
    });
    let mut referenced_object_keys = HashSet::new();
    if requires_reachability_check {
        let mut reachability = ReachabilityAccumulator::default();
        collect_referenced_object_keys(
            record_store,
            index_store,
            object_store,
            frontends,
            &mut reachability,
        )
        .await?;
        referenced_object_keys = reachability.referenced_object_keys;
    }

    for object_key in object_keys {
        let orphan = orphan_objects.get(&object_key);
        if orphan.is_none() {
            let Some(candidate) = quarantine_entries.remove(&object_key) else {
                continue;
            };
            index_store
                .delete_quarantine_candidate(candidate.object_key())
                .await
                .map_err(Into::into)?;
            report.released_quarantine_candidates =
                checked_increment(report.released_quarantine_candidates)?;
            continue;
        }

        let Some(candidate) = quarantine_entries.get(&object_key) else {
            continue;
        };
        if candidate.delete_after_unix_seconds() > now_unix_seconds {
            continue;
        }

        let Some(orphan) = orphan else {
            continue;
        };
        // Remove the quarantine entry from the durable index first, then
        // delete the object.
        //
        // Crash-recovery semantics: if the process crashes after the index
        // delete but before the storage delete, the object will exist on
        // disk with no quarantine entry.  On the next GC scan it will be
        // rediscovered as an orphan and re-quarantined by
        // `reconcile_quarantine_entries`, extending the effective retention
        // window for that object.  This is the intended recovery behavior —
        // the alternative (deleting storage first, then the index) risks
        // data loss if the index delete fails, which would leave a
        // permanently stuck quarantine entry referencing a non-existent
        // object.
        let Some(removed_candidate) = quarantine_entries.remove(&object_key) else {
            continue;
        };
        index_store
            .delete_quarantine_candidate(removed_candidate.object_key())
            .await
            .map_err(Into::into)?;

        // Re-verify reachability at delete time against the single
        // referenced-set collected once above (O(1) membership check, no
        // per-candidate mark). If the object is now referenced we skip the
        // storage delete and only release the (now stale) quarantine entry;
        // the object stays on disk and will be seen as live on the next
        // cycle, so no data is lost and nothing is resurrected.
        let still_unreferenced = !referenced_object_keys.contains(orphan.object_key.as_str());
        if !still_unreferenced {
            tracing::debug!(
                "skipping sweep delete for {object_key}: object became referenced after snapshot",
            );
            report.released_quarantine_candidates =
                checked_increment(report.released_quarantine_candidates)?;
            continue;
        }

        // Re-verify at delete time that no retention hold now covers the key.
        //
        // The runner snapshots `read_active_retention_hold_object_keys` at run
        // start and filters the orphan set once with it. A hold placed after
        // that snapshot but before this per-candidate delete is invisible to
        // the snapshot, so the sweep would otherwise destroy held data. Fetch
        // the current hold for exactly this key (an O(1) single-row lookup, not
        // a full hold re-scan per candidate) and skip the storage delete when a
        // hold now covers the key. The quarantine entry is released either way:
        // the hold keeps the data, and the stale entry is de-marked so the next
        // cycle does not re-quarantine a held object.
        let hold_now_active = index_store
            .retention_hold(&orphan.object_key)
            .await
            .map_err(Into::into)?
            .is_some_and(|hold| hold.is_active_at(now_unix_seconds));
        if hold_now_active {
            tracing::debug!(
                "skipping sweep delete for {object_key}: retention hold placed after snapshot",
            );
            report.released_quarantine_candidates =
                checked_increment(report.released_quarantine_candidates)?;
            continue;
        }

        let _outcome = object_store
            .delete_if_present(&orphan.object_key)
            .map_err(GcError::ObjectStore)?;

        // When a xorb container is swept, also remove its chunk-hash cache
        // sidecar (_xorb_chunks/{prefix}/{hash}) so orphaned cache entries
        // do not accumulate in the store.
        if let Some(hash) = xorb_hash_from_object_key_if_present(&orphan.object_key)?
            && let Ok(cache_key) =
                ObjectKey::parse(&format!("_xorb_chunks/{}/{}", &hash[..2], hash))
        {
            let _cache_outcome = object_store
                .delete_if_present(&cache_key)
                .map_err(GcError::ObjectStore);
        }

        report.deleted_chunks = checked_increment(report.deleted_chunks)?;
        report.deleted_bytes = checked_add(report.deleted_bytes, orphan.bytes)?;
        report.released_quarantine_candidates =
            checked_increment(report.released_quarantine_candidates)?;
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use shardline_index::{MemoryIndexStore, MemoryRecordStore};
    use shardline_server_core::ServerObjectStore;
    use shardline_storage::ObjectKey;

    fn make_orphan(hash: &str, bytes: u64) -> OrphanObject {
        let prefix = &hash[..2];
        let object_key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
        OrphanObject {
            hash: hash.to_owned(),
            object_key,
            bytes,
        }
    }

    // --- classify_quarantine_missing_object ---
    // An object that is not in orphan_objects (e.g. it was deleted) should
    // have its quarantine entry released during reconcile.

    #[test]
    fn classify_quarantine_missing_object() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let now = 1_000_000_u64;
            let retention = 86_400_u64;

            let orphan = make_orphan(
                "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233",
                512,
            );
            let candidate = QuarantineCandidate::new(
                orphan.object_key.clone(),
                orphan.bytes,
                now,
                checked_add(now, retention).unwrap(),
            )
            .unwrap();
            index_store
                .upsert_quarantine_candidate(&candidate)
                .await
                .unwrap();

            // Reconcile with empty orphan_objects — the quarantine entry should
            // be released because the object is no longer orphaned.
            let mut quarantine_entries = HashMap::new();
            quarantine_entries.insert(
                orphan.hash.clone(),
                QuarantineCandidate::new(
                    orphan.object_key.clone(),
                    orphan.bytes,
                    now,
                    checked_add(now, retention).unwrap(),
                )
                .unwrap(),
            );
            let mut report = LocalGcReport {
                scanned_records: 0,
                referenced_chunks: 0,
                orphan_chunks: 0,
                orphan_chunk_bytes: 0,
                active_quarantine_candidates: 0,
                new_quarantine_candidates: 0,
                retained_quarantine_candidates: 0,
                released_quarantine_candidates: 0,
                deleted_chunks: 0,
                deleted_bytes: 0,
                reaped_stale_temporary_chunks: 0,
                reaped_stale_temporary_bytes: 0,
                pruned_revisions_over_cap: 0,
            };

            reconcile_quarantine_entries(
                &index_store,
                &HashMap::new(),
                now,
                retention,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            assert!(quarantine_entries.is_empty());
            assert_eq!(report.released_quarantine_candidates, 1);
            assert_eq!(report.new_quarantine_candidates, 0);

            // Verify the candidate was also deleted from the index store.
            let mut found = false;
            index_store
                .visit_quarantine_candidates(|c| {
                    if c.object_key() == &orphan.object_key {
                        found = true;
                    }
                    Ok::<(), GcError>(())
                })
                .await
                .unwrap();
            assert!(!found, "quarantine candidate should have been released");
        });
    }

    // --- classify_quarantine_reachable_object ---
    // An object that is reachable (not an orphan) should not be quarantined.

    #[test]
    fn classify_quarantine_reachable_object() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let now = 1_000_000_u64;
            let retention = 86_400_u64;

            let mut quarantine_entries = HashMap::new();
            let mut report = LocalGcReport {
                scanned_records: 0,
                referenced_chunks: 0,
                orphan_chunks: 0,
                orphan_chunk_bytes: 0,
                active_quarantine_candidates: 0,
                new_quarantine_candidates: 0,
                retained_quarantine_candidates: 0,
                released_quarantine_candidates: 0,
                deleted_chunks: 0,
                deleted_bytes: 0,
                reaped_stale_temporary_chunks: 0,
                reaped_stale_temporary_bytes: 0,
                pruned_revisions_over_cap: 0,
            };

            reconcile_quarantine_entries(
                &index_store,
                &HashMap::new(),
                now,
                retention,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            assert!(quarantine_entries.is_empty());
            assert_eq!(report.new_quarantine_candidates, 0);
            assert_eq!(report.released_quarantine_candidates, 0);
        });
    }

    // --- classify_quarantine_held_object ---
    // An object with an active retention hold should appear in the hold set.

    #[test]
    fn classify_quarantine_held_object() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let now = 1_000_000_u64;

            let orphan = make_orphan(
                "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233",
                256,
            );
            let hold = shardline_index::RetentionHold::new(
                orphan.object_key.clone(),
                "operator hold".to_owned(),
                now,
                Some(now + 3600),
            )
            .unwrap();
            index_store.upsert_retention_hold(&hold).await.unwrap();

            let holds = read_active_retention_hold_object_keys(&index_store, now, false)
                .await
                .unwrap();
            assert!(holds.contains(orphan.object_key.as_str()));
        });
    }

    // --- classify_quarantine_orphan ---
    // An object that exists, is not reachable, and has no retention hold
    // should be quarantined.

    #[test]
    fn classify_quarantine_orphan() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let now = 1_000_000_u64;
            let retention = 86_400_u64;

            let orphan = make_orphan(
                "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233",
                1024,
            );
            let mut orphan_objects = HashMap::new();
            orphan_objects.insert(orphan.hash.clone(), orphan.clone());

            let mut quarantine_entries = HashMap::new();
            let mut report = LocalGcReport {
                scanned_records: 0,
                referenced_chunks: 0,
                orphan_chunks: 0,
                orphan_chunk_bytes: 0,
                active_quarantine_candidates: 0,
                new_quarantine_candidates: 0,
                retained_quarantine_candidates: 0,
                released_quarantine_candidates: 0,
                deleted_chunks: 0,
                deleted_bytes: 0,
                reaped_stale_temporary_chunks: 0,
                reaped_stale_temporary_bytes: 0,
                pruned_revisions_over_cap: 0,
            };

            reconcile_quarantine_entries(
                &index_store,
                &orphan_objects,
                now,
                retention,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            assert_eq!(quarantine_entries.len(), 1);
            let candidate = quarantine_entries.get(&orphan.hash).unwrap();
            assert_eq!(candidate.observed_length(), 1024);
            assert_eq!(candidate.first_seen_unreachable_at_unix_seconds(), now);
            assert_eq!(candidate.delete_after_unix_seconds(), now + retention);
            assert_eq!(report.new_quarantine_candidates, 1);

            // Verify the candidate was persisted to the index store.
            let mut found = false;
            index_store
                .visit_quarantine_candidates(|c| {
                    if c.object_key() == &orphan.object_key {
                        found = true;
                    }
                    Ok::<(), GcError>(())
                })
                .await
                .unwrap();
            assert!(found, "orphan should have been quarantined");
        });
    }

    // --- Additional edge-case tests ---

    #[test]
    fn reconcile_retains_existing_orphan_with_matching_length() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let now = 1_000_000_u64;
            let retention = 86_400_u64;

            let orphan = make_orphan(
                "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233",
                512,
            );
            let mut orphan_objects = HashMap::new();
            orphan_objects.insert(orphan.hash.clone(), orphan.clone());

            // Pre-populate quarantine entry with matching length.
            let mut quarantine_entries = HashMap::new();
            let candidate = QuarantineCandidate::new(
                orphan.object_key.clone(),
                512,
                now - 100,
                checked_add(now, retention).unwrap() - 100,
            )
            .unwrap();
            quarantine_entries.insert(orphan.hash.clone(), candidate);

            let mut report = LocalGcReport {
                scanned_records: 0,
                referenced_chunks: 0,
                orphan_chunks: 0,
                orphan_chunk_bytes: 0,
                active_quarantine_candidates: 0,
                new_quarantine_candidates: 0,
                retained_quarantine_candidates: 0,
                released_quarantine_candidates: 0,
                deleted_chunks: 0,
                deleted_bytes: 0,
                reaped_stale_temporary_chunks: 0,
                reaped_stale_temporary_bytes: 0,
                pruned_revisions_over_cap: 0,
            };

            reconcile_quarantine_entries(
                &index_store,
                &orphan_objects,
                now,
                retention,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            // Existing candidate with matching length is retained, not re-created.
            assert_eq!(report.retained_quarantine_candidates, 1);
            assert_eq!(report.new_quarantine_candidates, 0);
            assert_eq!(quarantine_entries.len(), 1);
        });
    }

    #[test]
    fn read_quarantine_entries_empty() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let entries = read_quarantine_entries(&index_store).await.unwrap();
            assert!(entries.is_empty());
        });
    }

    #[test]
    fn read_active_retention_holds_empty() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let holds = read_active_retention_hold_object_keys(&index_store, 1_000_000, false)
                .await
                .unwrap();
            assert!(holds.is_empty());
        });
    }

    #[test]
    fn read_active_retention_holds_expired_not_pruned() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let orphan = make_orphan(
                "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233",
                100,
            );
            // Hold that has already expired.
            let hold = shardline_index::RetentionHold::new(
                orphan.object_key.clone(),
                "expired hold".to_owned(),
                900_000,
                Some(950_000),
            )
            .unwrap();
            index_store.upsert_retention_hold(&hold).await.unwrap();

            // prune_expired = false → expired holds are not removed.
            let holds = read_active_retention_hold_object_keys(&index_store, 1_000_000, false)
                .await
                .unwrap();
            assert!(!holds.contains(orphan.object_key.as_str()));

            // Verify the hold still exists in the store.
            let mut found = false;
            index_store
                .visit_retention_holds(|h| {
                    if h.object_key() == &orphan.object_key {
                        found = true;
                    }
                    Ok::<(), GcError>(())
                })
                .await
                .unwrap();
            assert!(found, "expired hold should not be pruned");
        });
    }

    // ── sweep_quarantine_entries tests ────────────────────────────────────

    #[test]
    fn sweep_removes_expired_orphan() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let record_store = MemoryRecordStore::new();
            let object_store = shardline_server_core::ServerObjectStore::blackhole();

            let now = 1_000_000_u64;
            let orphan = make_orphan(
                "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233",
                512,
            );

            // Pre-populate orphan objects
            let mut orphan_objects = HashMap::new();
            orphan_objects.insert(orphan.hash.clone(), orphan.clone());

            // Create an expired quarantine candidate
            let candidate = QuarantineCandidate::new(
                orphan.object_key.clone(),
                orphan.bytes,
                now - 200_000, // first seen long ago
                now - 1,       // expired before now
            )
            .unwrap();
            index_store
                .upsert_quarantine_candidate(&candidate)
                .await
                .unwrap();

            let mut quarantine_entries = HashMap::new();
            quarantine_entries.insert(orphan.hash.clone(), candidate);

            let mut report = LocalGcReport {
                scanned_records: 0,
                referenced_chunks: 0,
                orphan_chunks: 1,
                orphan_chunk_bytes: 512,
                active_quarantine_candidates: 0,
                new_quarantine_candidates: 0,
                retained_quarantine_candidates: 0,
                released_quarantine_candidates: 0,
                deleted_chunks: 0,
                deleted_bytes: 0,
                reaped_stale_temporary_chunks: 0,
                reaped_stale_temporary_bytes: 0,
                pruned_revisions_over_cap: 0,
            };

            sweep_quarantine_entries(
                &record_store,
                &object_store,
                &index_store,
                &[ServerFrontend::Xet],
                &orphan_objects,
                now,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            assert_eq!(report.deleted_chunks, 1);
            assert_eq!(report.deleted_bytes, 512);
            assert_eq!(report.released_quarantine_candidates, 1);
            assert!(quarantine_entries.is_empty());

            // Verify candidate was removed from index
            let mut found = false;
            index_store
                .visit_quarantine_candidates(|_c| {
                    found = true;
                    Ok::<(), GcError>(())
                })
                .await
                .unwrap();
            assert!(!found);
        });
    }

    // ── sweep: collect-once reachability regression (AR-1) ─────────────
    //
    // Regression test for the quadratic sweep regression: the sweep must
    // collect the referenced set exactly once for the whole sweep, regardless
    // of how many expired quarantine candidates it processes. Previously each
    // candidate triggered its own full reachability mark (every record, every
    // dedupe shard mapping, and for XorbCdcV1 a read + parse of the stored
    // xorb container), making the sweep O(candidates × store_size).
    //
    // We instrument the collector with a test-only per-thread counter
    // (`collect_call_count`/`reset_collect_call_count`) and assert that
    // sweeping several expired candidates runs the mark exactly once.

    #[test]
    fn sweep_collects_referenced_set_once_for_multiple_expired_candidates() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let record_store = MemoryRecordStore::new();
            let object_store = ServerObjectStore::blackhole();

            let now = 1_000_000_u64;
            let mut orphan_objects = HashMap::new();
            let mut quarantine_entries = HashMap::new();

            // Several expired quarantine candidates (each with a distinct
            // object key). None are referenced by any live record, so every
            // one is a valid sweep delete.
            let candidate_count = 5_u64;
            for i in 0..candidate_count {
                let hash = format!("{i:0>64}");
                let orphan = make_orphan(&hash, 64 + i);
                orphan_objects.insert(orphan.hash.clone(), orphan.clone());
                let candidate = QuarantineCandidate::new(
                    orphan.object_key.clone(),
                    orphan.bytes,
                    now - 100_000, // first seen long ago
                    now - 1,       // expired before now
                )
                .unwrap();
                index_store
                    .upsert_quarantine_candidate(&candidate)
                    .await
                    .unwrap();
                quarantine_entries.insert(orphan.hash.clone(), candidate);
            }

            let mut report = LocalGcReport::default();
            crate::reachability::reset_collect_call_count();
            sweep_quarantine_entries(
                &record_store,
                &object_store,
                &index_store,
                &[ServerFrontend::Xet],
                &orphan_objects,
                now,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            // The referenced set is built once, not once per candidate.
            assert_eq!(
                crate::reachability::collect_call_count(),
                1,
                "collect_referenced_object_keys must run exactly once for the whole sweep"
            );
            // Every expired, unreferenced candidate was swept.
            assert_eq!(
                report.deleted_chunks, candidate_count,
                "all expired unreferenced candidates should be deleted"
            );
            assert_eq!(report.released_quarantine_candidates, candidate_count);
            assert!(quarantine_entries.is_empty());
        });
    }

    #[test]
    fn sweep_skips_non_expired_orphan() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let record_store = MemoryRecordStore::new();
            let object_store = shardline_server_core::ServerObjectStore::blackhole();

            let now = 1_000_000_u64;
            let orphan = make_orphan(
                "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233",
                256,
            );
            let mut orphan_objects = HashMap::new();
            orphan_objects.insert(orphan.hash.clone(), orphan.clone());

            // Non-expired candidate
            let candidate =
                QuarantineCandidate::new(orphan.object_key.clone(), orphan.bytes, now, now + 3600)
                    .unwrap();
            index_store
                .upsert_quarantine_candidate(&candidate)
                .await
                .unwrap();

            let mut quarantine_entries = HashMap::new();
            quarantine_entries.insert(orphan.hash.clone(), candidate);

            let mut report = LocalGcReport {
                scanned_records: 0,
                referenced_chunks: 0,
                orphan_chunks: 1,
                orphan_chunk_bytes: 256,
                active_quarantine_candidates: 0,
                new_quarantine_candidates: 0,
                retained_quarantine_candidates: 0,
                released_quarantine_candidates: 0,
                deleted_chunks: 0,
                deleted_bytes: 0,
                reaped_stale_temporary_chunks: 0,
                reaped_stale_temporary_bytes: 0,
                pruned_revisions_over_cap: 0,
            };

            sweep_quarantine_entries(
                &record_store,
                &object_store,
                &index_store,
                &[ServerFrontend::Xet],
                &orphan_objects,
                now,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            assert_eq!(report.deleted_chunks, 0);
            assert_eq!(report.deleted_bytes, 0);
            assert_eq!(quarantine_entries.len(), 1);
        });
    }

    #[test]
    fn sweep_releases_entry_for_missing_orphan() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let record_store = MemoryRecordStore::new();
            let object_store = shardline_server_core::ServerObjectStore::blackhole();

            let now = 1_000_000_u64;
            let orphan = make_orphan(
                "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233",
                128,
            );

            // Create a quarantine entry but don't add orphan to orphan_objects
            let candidate = QuarantineCandidate::new(
                orphan.object_key.clone(),
                orphan.bytes,
                now - 100,
                now - 1,
            )
            .unwrap();
            index_store
                .upsert_quarantine_candidate(&candidate)
                .await
                .unwrap();

            let mut quarantine_entries = HashMap::new();
            quarantine_entries.insert(orphan.hash.clone(), candidate);

            let mut report = LocalGcReport {
                scanned_records: 0,
                referenced_chunks: 0,
                orphan_chunks: 0,
                orphan_chunk_bytes: 0,
                active_quarantine_candidates: 0,
                new_quarantine_candidates: 0,
                retained_quarantine_candidates: 0,
                released_quarantine_candidates: 0,
                deleted_chunks: 0,
                deleted_bytes: 0,
                reaped_stale_temporary_chunks: 0,
                reaped_stale_temporary_bytes: 0,
                pruned_revisions_over_cap: 0,
            };

            sweep_quarantine_entries(
                &record_store,
                &object_store,
                &index_store,
                &[ServerFrontend::Xet],
                &HashMap::new(), // empty orphan_objects
                now,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            assert_eq!(report.released_quarantine_candidates, 1);
            assert_eq!(report.deleted_chunks, 0);
            assert!(quarantine_entries.is_empty());
        });
    }

    #[test]
    fn read_active_retention_holds_with_prune_expired() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let orphan = make_orphan(
                "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233",
                100,
            );
            // Expired hold
            let hold = shardline_index::RetentionHold::new(
                orphan.object_key.clone(),
                "expired".to_owned(),
                900_000,
                Some(950_000),
            )
            .unwrap();
            index_store.upsert_retention_hold(&hold).await.unwrap();

            // prune_expired = true → expired hold should be pruned
            let holds = read_active_retention_hold_object_keys(&index_store, 1_000_000, true)
                .await
                .unwrap();
            assert!(!holds.contains(orphan.object_key.as_str()));

            // Verify hold was removed from index
            let mut found = false;
            index_store
                .visit_retention_holds(|h| {
                    if h.object_key() == &orphan.object_key {
                        found = true;
                    }
                    Ok::<(), GcError>(())
                })
                .await
                .unwrap();
            assert!(!found, "expired hold should have been pruned");
        });
    }

    #[test]
    fn sweep_non_expired_retention_hold_not_in_hold_set() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let orphan = make_orphan(
                "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233",
                100,
            );
            // Active hold (not expired)
            let hold = shardline_index::RetentionHold::new(
                orphan.object_key.clone(),
                "active".to_owned(),
                1_000_000,
                Some(2_000_000),
            )
            .unwrap();
            index_store.upsert_retention_hold(&hold).await.unwrap();

            let holds = read_active_retention_hold_object_keys(&index_store, 1_500_000, true)
                .await
                .unwrap();
            assert!(holds.contains(orphan.object_key.as_str()));
        });
    }

    // ── read_quarantine_entries with pre-populated entries ───────────────

    #[test]
    fn read_quarantine_entries_with_candidates() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let now = 1_000_000_u64;

            // Insert a few quarantine candidates into the store.
            let orphan1 = make_orphan(
                "1111111111111111111111111111111111111111111111111111111111111111",
                64,
            );
            let candidate1 =
                QuarantineCandidate::new(orphan1.object_key.clone(), 64, now, now + 3600).unwrap();
            index_store
                .upsert_quarantine_candidate(&candidate1)
                .await
                .unwrap();

            let orphan2 = make_orphan(
                "2222222222222222222222222222222222222222222222222222222222222222",
                128,
            );
            let candidate2 =
                QuarantineCandidate::new(orphan2.object_key.clone(), 128, now, now + 7200).unwrap();
            index_store
                .upsert_quarantine_candidate(&candidate2)
                .await
                .unwrap();

            let entries = read_quarantine_entries(&index_store).await.unwrap();
            assert_eq!(entries.len(), 2);
            assert!(entries.contains_key(orphan1.object_key.as_str()));
            assert!(entries.contains_key(orphan2.object_key.as_str()));
        });
    }

    // ── reconcile: existing orphan with different length → re-quarantine ─

    #[test]
    fn reconcile_orphan_with_different_length_replaces_candidate() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let now = 1_000_000_u64;
            let retention = 86_400_u64;

            let orphan = make_orphan(
                "3333333333333333333333333333333333333333333333333333333333333333",
                256,
            );
            let mut orphan_objects = HashMap::new();
            orphan_objects.insert(orphan.hash.clone(), orphan.clone());

            // Pre-populate quarantine entry with *different* length (should be replaced).
            let mut quarantine_entries = HashMap::new();
            let old_candidate = QuarantineCandidate::new(
                orphan.object_key.clone(),
                128, // different length
                now - 100,
                checked_add(now, retention).unwrap() - 100,
            )
            .unwrap();
            quarantine_entries.insert(orphan.hash.clone(), old_candidate);

            let mut report = LocalGcReport {
                scanned_records: 0,
                referenced_chunks: 0,
                orphan_chunks: 0,
                orphan_chunk_bytes: 0,
                active_quarantine_candidates: 0,
                new_quarantine_candidates: 0,
                retained_quarantine_candidates: 0,
                released_quarantine_candidates: 0,
                deleted_chunks: 0,
                deleted_bytes: 0,
                reaped_stale_temporary_chunks: 0,
                reaped_stale_temporary_bytes: 0,
                pruned_revisions_over_cap: 0,
            };

            reconcile_quarantine_entries(
                &index_store,
                &orphan_objects,
                now,
                retention,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            assert_eq!(report.new_quarantine_candidates, 1);
            assert_eq!(report.retained_quarantine_candidates, 0);
            assert_eq!(quarantine_entries.len(), 1);
            let new_candidate = quarantine_entries.get(&orphan.hash).unwrap();
            assert_eq!(new_candidate.observed_length(), 256);
        });
    }

    // ── reconcile: orphan object key not in quarantine map (edge) ───────

    #[test]
    fn reconcile_stale_key_removed_from_quarantine_when_orphan_removed() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let now = 1_000_000_u64;
            let retention = 86_400_u64;

            // Create a quarantine entry for an orphan that is NOT in orphan_objects.
            let orphan = make_orphan(
                "4444444444444444444444444444444444444444444444444444444444444444",
                512,
            );
            let candidate = QuarantineCandidate::new(
                orphan.object_key.clone(),
                512,
                now,
                checked_add(now, retention).unwrap(),
            )
            .unwrap();
            // Insert into index store so delete_quarantine_candidate will work.
            index_store
                .upsert_quarantine_candidate(&candidate)
                .await
                .unwrap();

            let mut quarantine_entries = HashMap::new();
            quarantine_entries.insert(orphan.hash.clone(), candidate);

            let mut report = LocalGcReport {
                scanned_records: 0,
                referenced_chunks: 0,
                orphan_chunks: 0,
                orphan_chunk_bytes: 0,
                active_quarantine_candidates: 0,
                new_quarantine_candidates: 0,
                retained_quarantine_candidates: 0,
                released_quarantine_candidates: 0,
                deleted_chunks: 0,
                deleted_bytes: 0,
                reaped_stale_temporary_chunks: 0,
                reaped_stale_temporary_bytes: 0,
                pruned_revisions_over_cap: 0,
            };

            // Empty orphan_objects → the stale entry should be released.
            reconcile_quarantine_entries(
                &index_store,
                &HashMap::new(),
                now,
                retention,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            assert_eq!(report.released_quarantine_candidates, 1);
            assert!(quarantine_entries.is_empty());
        });
    }

    // ── sweep: entry not in orphan_objects → release (different code path) ─

    #[test]
    fn sweep_releases_entry_when_orphan_not_in_map() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let record_store = MemoryRecordStore::new();
            let object_store = ServerObjectStore::blackhole();

            let now = 1_000_000_u64;
            let orphan = make_orphan(
                "5555555555555555555555555555555555555555555555555555555555555555",
                64,
            );

            let candidate =
                QuarantineCandidate::new(orphan.object_key.clone(), 64, now, now + 3600).unwrap();
            index_store
                .upsert_quarantine_candidate(&candidate)
                .await
                .unwrap();

            let mut quarantine_entries = HashMap::new();
            quarantine_entries.insert(orphan.hash.clone(), candidate);

            let mut report = LocalGcReport::default();

            // Empty orphan_objects → release.
            sweep_quarantine_entries(
                &record_store,
                &object_store,
                &index_store,
                &[ServerFrontend::Xet],
                &HashMap::new(),
                now,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            assert_eq!(report.released_quarantine_candidates, 1);
            assert!(quarantine_entries.is_empty());
        });
    }

    // ── sweep: entry in orphan_objects but candidate removed from map concurrently ─

    #[test]
    fn sweep_skips_when_candidate_vanishes_from_map() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let record_store = MemoryRecordStore::new();
            let object_store = ServerObjectStore::blackhole();

            let now = 1_000_000_u64;
            // Use a key that's in orphan_objects but not in quarantine_entries.
            let orphan = make_orphan(
                "6666666666666666666666666666666666666666666666666666666666666666",
                32,
            );
            let mut orphan_objects = HashMap::new();
            orphan_objects.insert(orphan.hash.clone(), orphan.clone());

            // Empty quarantine_entries → nothing to sweep.
            let mut quarantine_entries = HashMap::new();
            let mut report = LocalGcReport::default();

            sweep_quarantine_entries(
                &record_store,
                &object_store,
                &index_store,
                &[ServerFrontend::Xet],
                &orphan_objects,
                now,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            assert_eq!(report.deleted_chunks, 0);
            assert_eq!(report.released_quarantine_candidates, 0);
        });
    }

    // ── sweep: non-expired entry skipped ────────────────────────────────

    #[test]
    fn sweep_skips_non_expired_entry_in_orphan_map() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let record_store = MemoryRecordStore::new();
            let object_store = ServerObjectStore::blackhole();

            let now = 1_000_000_u64;
            let orphan = make_orphan(
                "7777777777777777777777777777777777777777777777777777777777777777",
                128,
            );
            let mut orphan_objects = HashMap::new();
            orphan_objects.insert(orphan.hash.clone(), orphan.clone());

            let candidate = QuarantineCandidate::new(
                orphan.object_key.clone(),
                128,
                now,
                now + 7200, // not expired
            )
            .unwrap();

            let mut quarantine_entries = HashMap::new();
            quarantine_entries.insert(orphan.hash.clone(), candidate);

            let mut report = LocalGcReport::default();

            sweep_quarantine_entries(
                &record_store,
                &object_store,
                &index_store,
                &[ServerFrontend::Xet],
                &orphan_objects,
                now,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            assert_eq!(report.deleted_chunks, 0);
            assert_eq!(quarantine_entries.len(), 1);
        });
    }

    // ── sweep: object re-referenced after snapshot → delete skipped ─────
    //
    // This is the delete-time TOCTOU regression test for F-deep-3.1. The
    // snapshot (`orphan_objects`) says the chunk is an orphan with an expired
    // quarantine candidate, but by sweep time the chunk is referenced by a
    // live record. The sweep must NOT delete it.
    //
    // An integration test would additionally assert this across processes
    // (server re-uploading while a separate GC process sweeps) against a
    // shared index: the object re-referenced between snapshot and delete is
    // not deleted. This harness test reproduces the same state transition
    // within one process and asserts the observable behaviour.

    #[test]
    fn sweep_skips_delete_when_object_becomes_referenced() {
        use shardline_index::{FileChunkRecord, FileRecord, RecordMutation, StorageRepresentation};
        use shardline_storage::{ObjectBody, ObjectIntegrity};

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let record_store = MemoryRecordStore::new();
            let index_store = MemoryIndexStore::new();

            let dir = tempfile::tempdir().unwrap();
            let object_store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();

            let now = 1_000_000_u64;
            let hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
            let key = make_orphan(hash, 0).object_key;
            let data = b"referenced chunk data".to_vec();

            // Put the chunk on disk.
            let integrity = ObjectIntegrity::new(
                shardline_server_core::chunk_hash(&data),
                u64::try_from(data.len()).unwrap_or(0),
            );
            object_store
                .put_if_absent(&key, ObjectBody::Borrowed(&data), &integrity)
                .unwrap();

            // The chunk is now referenced by a live record (the state that
            // would be produced by a concurrent re-upload landing between
            // `collect_referenced_object_keys` and `scan_orphan_objects`).
            let record = FileRecord {
                file_id: "live-file".to_owned(),
                content_hash: hash.to_owned(),
                total_bytes: 100,
                chunk_size: 100,
                storage_repr: StorageRepresentation::FixedChunkV1,
                repository_scope: None,
                chunks: vec![FileChunkRecord {
                    hash: hash.to_owned(),
                    offset: 0,
                    length: 100,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 0,
                }],
            };
            record_store.write_latest_record(&record).await.unwrap();

            // But the cycle snapshot still treats it as an orphan with an
            // expired quarantine candidate.
            let orphan = make_orphan(hash, u64::try_from(data.len()).unwrap_or(0));
            let mut orphan_objects = HashMap::new();
            orphan_objects.insert(orphan.hash.clone(), orphan.clone());

            let candidate = QuarantineCandidate::new(
                orphan.object_key.clone(),
                orphan.bytes,
                now - 200_000, // first seen long ago
                now - 1,       // expired before now
            )
            .unwrap();
            index_store
                .upsert_quarantine_candidate(&candidate)
                .await
                .unwrap();
            let mut quarantine_entries = HashMap::new();
            quarantine_entries.insert(orphan.hash.clone(), candidate);

            let mut report = LocalGcReport::default();

            sweep_quarantine_entries(
                &record_store,
                &object_store,
                &index_store,
                &[ServerFrontend::Xet],
                &orphan_objects,
                now,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            // The delete-time re-check sees the object is now referenced and
            // skips the storage delete: no data loss.
            assert_eq!(report.deleted_chunks, 0);
            assert_eq!(report.deleted_bytes, 0);
            assert_eq!(report.released_quarantine_candidates, 1);
            assert!(quarantine_entries.is_empty());
            // The chunk is still present on disk.
            assert!(object_store.contains(&orphan.object_key).unwrap());
        });
    }

    // ── sweep: retention hold placed after the run-start snapshot → delete skipped ─
    //
    // Regression test for F-42: the runner snapshots the active-hold set at run
    // start, so a hold placed mid-run (after the snapshot, before the sweep's
    // per-candidate delete) is invisible to that snapshot. The sweep must
    // re-check the hold at delete time and skip the storage delete; the hold
    // keeps the data and the stale quarantine entry is released.
    //
    // In this harness the `orphan_objects` map plays the role of the run-start
    // snapshot (the object is still classified as an orphan), and the hold is
    // registered afterwards — exactly the mid-run transition the finding proved.

    #[test]
    fn sweep_skips_delete_when_hold_placed_after_snapshot() {
        use shardline_index::RetentionHold;
        use shardline_storage::{ObjectBody, ObjectIntegrity};

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let record_store = MemoryRecordStore::new();
            let index_store = MemoryIndexStore::new();

            let dir = tempfile::tempdir().unwrap();
            let object_store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();

            let now = 1_000_000_u64;
            let hash = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
            let key = make_orphan(hash, 0).object_key;
            let data = b"held after snapshot data".to_vec();

            // Put the chunk on disk.
            let integrity = ObjectIntegrity::new(
                shardline_server_core::chunk_hash(&data),
                u64::try_from(data.len()).unwrap_or(0),
            );
            object_store
                .put_if_absent(&key, ObjectBody::Borrowed(&data), &integrity)
                .unwrap();

            // Cycle snapshot: the chunk is an orphan with an expired quarantine
            // candidate and is NOT in any hold set (the hold does not exist yet).
            let orphan = make_orphan(hash, u64::try_from(data.len()).unwrap_or(0));
            let mut orphan_objects = HashMap::new();
            orphan_objects.insert(orphan.hash.clone(), orphan.clone());

            let candidate = QuarantineCandidate::new(
                orphan.object_key.clone(),
                orphan.bytes,
                now - 200_000, // first seen long ago
                now - 1,       // expired before now
            )
            .unwrap();
            index_store
                .upsert_quarantine_candidate(&candidate)
                .await
                .unwrap();
            let mut quarantine_entries = HashMap::new();
            quarantine_entries.insert(orphan.hash.clone(), candidate);

            // The hold is placed AFTER the snapshot but BEFORE the sweep's
            // per-candidate delete — the exact mid-run TOCTOU window. It must
            // not be visible to the run-start snapshot, so it is registered on
            // the shared index store after `orphan_objects` was built.
            let hold = RetentionHold::new(
                orphan.object_key.clone(),
                "mid-run operator hold".to_owned(),
                now - 100,
                Some(now + 3600), // still active at sweep time
            )
            .unwrap();
            index_store.upsert_retention_hold(&hold).await.unwrap();

            let mut report = LocalGcReport::default();

            sweep_quarantine_entries(
                &record_store,
                &object_store,
                &index_store,
                &[ServerFrontend::Xet],
                &orphan_objects,
                now,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            // The delete-time hold re-check sees the mid-run hold and skips the
            // storage delete: no data loss.
            assert_eq!(report.deleted_chunks, 0);
            assert_eq!(report.deleted_bytes, 0);
            assert_eq!(report.released_quarantine_candidates, 1);
            assert!(quarantine_entries.is_empty());
            // The chunk is still present on disk.
            assert!(object_store.contains(&orphan.object_key).unwrap());
            // The hold is still present afterwards.
            let mut hold_present = false;
            index_store
                .visit_retention_holds(|h| {
                    if h.object_key() == &orphan.object_key {
                        hold_present = true;
                    }
                    Ok::<(), GcError>(())
                })
                .await
                .unwrap();
            assert!(hold_present, "mid-run hold must survive the sweep");
        });
    }

    #[test]
    fn sweep_deletes_expired_orphan_without_any_hold() {
        // Control: with no hold registered at all (before or mid-run), the
        // sweep's delete-time hold re-check finds nothing and the expired
        // candidate is deleted as usual.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let record_store = MemoryRecordStore::new();
            let index_store = MemoryIndexStore::new();
            let object_store = ServerObjectStore::blackhole();

            let now = 1_000_000_u64;
            let hash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
            let orphan = make_orphan(hash, 64);
            let mut orphan_objects = HashMap::new();
            orphan_objects.insert(orphan.hash.clone(), orphan.clone());

            let candidate = QuarantineCandidate::new(
                orphan.object_key.clone(),
                orphan.bytes,
                now - 200_000,
                now - 1,
            )
            .unwrap();
            index_store
                .upsert_quarantine_candidate(&candidate)
                .await
                .unwrap();
            let mut quarantine_entries = HashMap::new();
            quarantine_entries.insert(orphan.hash.clone(), candidate);

            let mut report = LocalGcReport::default();
            sweep_quarantine_entries(
                &record_store,
                &object_store,
                &index_store,
                &[ServerFrontend::Xet],
                &orphan_objects,
                now,
                &mut quarantine_entries,
                &mut report,
            )
            .await
            .unwrap();

            assert_eq!(report.deleted_chunks, 1);
            assert_eq!(report.deleted_bytes, 64);
            assert!(quarantine_entries.is_empty());
        });
    }

    // ── read_active_retention_holds with active holds ───────────────────

    #[test]
    fn read_active_retention_holds_returns_active_only() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let now = 1_000_000_u64;

            // Active hold
            let active_key = ObjectKey::parse(
                "ab/8888888888888888888888888888888888888888888888888888888888888888",
            )
            .unwrap();
            let active_hold = shardline_index::RetentionHold::new(
                active_key.clone(),
                "active".to_owned(),
                now,
                Some(now + 3600),
            )
            .unwrap();
            index_store
                .upsert_retention_hold(&active_hold)
                .await
                .unwrap();

            // Expired hold (no release_after — permanent hold, always active)
            let permanent_key = ObjectKey::parse(
                "ab/9999999999999999999999999999999999999999999999999999999999999999",
            )
            .unwrap();
            let permanent_hold = shardline_index::RetentionHold::new(
                permanent_key.clone(),
                "permanent".to_owned(),
                now,
                None,
            )
            .unwrap();
            index_store
                .upsert_retention_hold(&permanent_hold)
                .await
                .unwrap();

            let holds = read_active_retention_hold_object_keys(&index_store, now, false)
                .await
                .unwrap();
            assert!(holds.contains(active_key.as_str()));
            assert!(holds.contains(permanent_key.as_str()));
        });
    }

    // ── read_newest_stored_creation_timestamp (F-57) ────────────────────

    #[test]
    fn newest_stored_creation_timestamp_excludes_future_dated_fields() {
        // F-57 regression: the forward-clock guard reference must be creation
        // timestamps only. A candidate 1 day old with a 7-day retention has
        // `delete_after` 6 days in the future, and an active hold's
        // `release_after` can be 30 days out; including either would put the
        // reference in the future and blind the guard to a forward jump of
        // days to weeks. The reader must return the newest CREATION timestamp
        // (max first_seen / held_at) only.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let real_now = 2_000_000_000_u64;

            // Candidate created 1 day ago with a 7-day retention →
            // delete_after = real_now + 6 days (future-dated).
            let candidate_key = ObjectKey::parse(
                "aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .unwrap();
            let candidate = QuarantineCandidate::new(
                candidate_key.clone(),
                512,
                real_now - 86_400,
                real_now + 6 * 86_400,
            )
            .unwrap();
            index_store
                .upsert_quarantine_candidate(&candidate)
                .await
                .unwrap();

            // Hold placed an hour ago with release_after = +30 days
            // (future-dated).
            let hold_key = ObjectKey::parse(
                "bb/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            )
            .unwrap();
            let hold = shardline_index::RetentionHold::new(
                hold_key.clone(),
                "operator hold".to_owned(),
                real_now - 3600,
                Some(real_now + 30 * 86_400),
            )
            .unwrap();
            index_store.upsert_retention_hold(&hold).await.unwrap();

            let newest_creation = read_newest_stored_creation_timestamp(&index_store)
                .await
                .unwrap();

            assert_eq!(
                newest_creation,
                Some(real_now - 3600),
                "the newest CREATION timestamp is the hold's held_at; \
                 delete_after/release_after must never enter the reference"
            );
        });
    }

    #[test]
    fn newest_stored_creation_timestamp_empty_store_returns_none() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            assert_eq!(
                read_newest_stored_creation_timestamp(&index_store)
                    .await
                    .unwrap(),
                None
            );
        });
    }

    #[test]
    fn newest_stored_creation_timestamp_ignores_future_release_after_hold() {
        // A hold with an infinite/remote release_after must not move the
        // reference into the future; only held_at counts.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let index_store = MemoryIndexStore::new();
            let now = 1_000_000_u64;
            let hold_key = ObjectKey::parse(
                "cc/cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            )
            .unwrap();
            let hold = shardline_index::RetentionHold::new(
                hold_key.clone(),
                "infinite hold".to_owned(),
                now - 500,
                Some(now + 1_000_000_000),
            )
            .unwrap();
            index_store.upsert_retention_hold(&hold).await.unwrap();

            let newest_creation = read_newest_stored_creation_timestamp(&index_store)
                .await
                .unwrap();
            assert_eq!(newest_creation, Some(now - 500));
        });
    }

    // ── last-GC-clock anchor (F-57) ─────────────────────────────────────

    #[test]
    fn last_gc_clock_anchor_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();

        // No anchor persisted yet.
        assert_eq!(
            read_last_gc_clock_anchor(&object_store).unwrap(),
            None,
            "a fresh store has no anchor"
        );

        let now = 2_000_000_000_u64;
        write_last_gc_clock_anchor(&object_store, now).unwrap();

        assert_eq!(
            read_last_gc_clock_anchor(&object_store).unwrap(),
            Some(now),
            "the anchor must round-trip"
        );

        // Overwriting with a newer trusted clock is the normal per-run update.
        write_last_gc_clock_anchor(&object_store, now + 86_400).unwrap();
        assert_eq!(
            read_last_gc_clock_anchor(&object_store).unwrap(),
            Some(now + 86_400)
        );
    }

    #[test]
    fn last_gc_clock_anchor_malformed_reads_as_absent() {
        let dir = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();

        // A torn/corrupt anchor (valid integrity, non-numeric body) must be
        // treated as absent rather than aborting the run; it will be
        // overwritten by the next anchor write.
        let key = last_gc_clock_anchor_key().unwrap();
        let body = b"not-a-timestamp";
        let integrity =
            ObjectIntegrity::new(chunk_hash(body), u64::try_from(body.len()).unwrap_or(0));
        object_store
            .put_overwrite(&key, ObjectBody::Borrowed(body), &integrity)
            .unwrap();

        assert_eq!(read_last_gc_clock_anchor(&object_store).unwrap(), None);
    }
}
