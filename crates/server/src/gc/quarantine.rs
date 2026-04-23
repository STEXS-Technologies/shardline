use std::collections::{HashMap, HashSet};

use shardline_index::{AsyncIndexStore, QuarantineCandidate};
use shardline_storage::ObjectStore;

use super::{LocalGcReport, reachability::OrphanObject};
use crate::{
    ServerError,
    overflow::{checked_add, checked_increment},
};

pub(super) async fn read_quarantine_entries<IndexAdapter>(
    index_store: &IndexAdapter,
) -> Result<HashMap<String, QuarantineCandidate>, ServerError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<ServerError>,
{
    let mut entries_by_object_key = HashMap::new();
    index_store
        .visit_quarantine_candidates(|candidate| {
            entries_by_object_key.insert(candidate.object_key().as_str().to_owned(), candidate);
            Ok::<(), ServerError>(())
        })
        .await?;

    Ok(entries_by_object_key)
}

pub(super) async fn read_active_retention_hold_object_keys<IndexAdapter>(
    index_store: &IndexAdapter,
    now_unix_seconds: u64,
    prune_expired: bool,
) -> Result<HashSet<String>, ServerError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<ServerError>,
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

            Ok::<(), ServerError>(())
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
) -> Result<(), ServerError>
where
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ServerError>,
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
) -> Result<(), ServerError>
where
    ObjectAdapter: ObjectStore,
    ObjectAdapter::Error: Into<ServerError>,
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ServerError>,
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
        let _outcome = object_store
            .delete_if_present(&orphan.object_key)
            .map_err(Into::into)?;

        let Some(removed_candidate) = quarantine_entries.remove(&object_key) else {
            continue;
        };
        index_store
            .delete_quarantine_candidate(removed_candidate.object_key())
            .await
            .map_err(Into::into)?;
        report.deleted_chunks = checked_increment(report.deleted_chunks)?;
        report.deleted_bytes = checked_add(report.deleted_bytes, orphan.bytes)?;
        report.released_quarantine_candidates =
            checked_increment(report.released_quarantine_candidates)?;
    }

    Ok(())
}
