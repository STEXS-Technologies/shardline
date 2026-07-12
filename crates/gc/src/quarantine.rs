use std::collections::{HashMap, HashSet};

use shardline_index::{AsyncIndexStore, QuarantineCandidate};
use shardline_storage::ObjectStore;

use super::{LocalGcReport, reachability::OrphanObject};
use crate::GcError;
use shardline_server_core::{checked_add, checked_increment};

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

pub(super) async fn sweep_quarantine_entries<ObjectAdapter, IndexAdapter>(
    object_store: &ObjectAdapter,
    index_store: &IndexAdapter,
    orphan_objects: &HashMap<String, OrphanObject>,
    now_unix_seconds: u64,
    quarantine_entries: &mut HashMap<String, QuarantineCandidate>,
    report: &mut LocalGcReport,
) -> Result<(), GcError>
where
    ObjectAdapter: ObjectStore,
    ObjectAdapter::Error: Into<GcError>,
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<GcError>,
{
    let object_keys = quarantine_entries.keys().cloned().collect::<Vec<_>>();
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
        let _outcome = object_store
            .delete_if_present(&orphan.object_key)
            .map_err(Into::into)?;
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
    use shardline_index::MemoryIndexStore;
    use shardline_storage::ObjectKey;

    fn make_orphan(hash: &str, bytes: u64) -> OrphanObject {
        let prefix = &hash[..2];
        let object_key = ObjectKey::parse(&format!("chunks/{prefix}/{hash}")).unwrap();
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
}
