use async_trait::async_trait;
use std::collections::HashMap;

use shardline_index::{
    FileRecord, LocalRecordStore, OciObjectKind, OciObjectStore, PostgresRecordStore, RecordStore,
    RecordTraversal,
};
use shardline_oci_adapter::{
    oci_blob_key_from_namespace, oci_manifest_key_from_namespace,
    oci_manifest_media_type_key_from_namespace,
};
use shardline_server_core::{
    ServerObjectStore, parse_stored_file_record_bytes, protocol_support::protocol_object_file_id,
};
use shardline_storage::ObjectStore as _;

use crate::{
    GcError, LocalGcOptions,
    quarantine::{read_last_gc_clock_anchor, read_newest_stored_creation_timestamp},
    runner::{gc_now_unix_seconds, retention_clock_is_skewed_forward},
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct OciTombstoneGcReport {
    pub(crate) scanned: u64,
    pub(crate) eligible: u64,
    pub(crate) reclaimed: u64,
}

#[doc(hidden)]
#[async_trait]
pub trait OciRecordReclaimer {
    async fn delete_protocol_record(&self, record: &FileRecord) -> Result<(), GcError>;
}

async fn latest_records_by_file_id<RecordAdapter>(
    record_store: &RecordAdapter,
) -> Result<HashMap<String, FileRecord>, GcError>
where
    RecordAdapter: RecordTraversal + Sync,
    RecordAdapter::Error: Into<GcError>,
{
    let mut records = HashMap::new();
    for locator in record_store
        .list_latest_record_locators()
        .await
        .map_err(Into::into)?
    {
        let bytes = record_store
            .read_record_bytes(&locator)
            .await
            .map_err(Into::into)?;
        let record = parse_stored_file_record_bytes(&bytes)?;
        records.insert(record.file_id.clone(), record);
    }
    Ok(records)
}

#[async_trait]
impl OciRecordReclaimer for LocalRecordStore {
    async fn delete_protocol_record(&self, record: &FileRecord) -> Result<(), GcError> {
        self.delete_file_version_metadata(record).await?;
        Ok(())
    }
}

#[async_trait]
impl OciRecordReclaimer for PostgresRecordStore {
    async fn delete_protocol_record(&self, record: &FileRecord) -> Result<(), GcError> {
        self.delete_file_version_metadata(record).await?;
        Ok(())
    }
}

pub(crate) async fn reclaim_oci_tombstones<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    options: LocalGcOptions,
) -> Result<OciTombstoneGcReport, GcError>
where
    RecordAdapter: RecordStore + OciRecordReclaimer + Sync,
    RecordAdapter::Error: Into<GcError>,
    IndexAdapter: shardline_index::AsyncIndexStore
        + OciObjectStore<Error = <IndexAdapter as shardline_index::AsyncIndexStore>::Error>
        + Sync,
    <IndexAdapter as shardline_index::AsyncIndexStore>::Error: Into<GcError>,
{
    let tombstones = index_store
        .list_oci_object_tombstones()
        .await
        .map_err(Into::into)?;
    let mut report = OciTombstoneGcReport {
        scanned: u64::try_from(tombstones.len())?,
        ..OciTombstoneGcReport::default()
    };
    if !options.sweep {
        return Ok(report);
    }

    let now = gc_now_unix_seconds();
    let newest_stored_creation_timestamp =
        read_newest_stored_creation_timestamp(index_store).await?;
    let last_gc_clock_anchor = read_last_gc_clock_anchor(object_store)?;
    if retention_clock_is_skewed_forward(
        now,
        newest_stored_creation_timestamp,
        last_gc_clock_anchor,
    ) {
        tracing::warn!(
            "skipping OCI tombstone reclamation: GC wall clock jumped forward relative to stored lifecycle timestamps"
        );
        return Ok(report);
    }
    let mut latest_records = latest_records_by_file_id(record_store).await?;

    for tombstone in tombstones {
        let Some(delete_after) = tombstone
            .deleted_at_unix_seconds
            .checked_add(options.retention_seconds)
        else {
            continue;
        };
        if delete_after > now {
            continue;
        }
        report.eligible = shardline_server_core::checked_increment(report.eligible)?;

        let key = &tombstone.key;
        match key.kind {
            OciObjectKind::Blob => {
                let object_key = oci_blob_key_from_namespace(
                    &key.repository,
                    &key.digest_hex,
                    &key.scope_namespace,
                )?;
                object_store
                    .delete_if_present(&object_key)
                    .map_err(GcError::ObjectStore)?;
                let file_id = protocol_object_file_id(&object_key);
                if let Some(record) = latest_records.remove(&file_id) {
                    record_store.delete_protocol_record(&record).await?;
                }
            }
            OciObjectKind::Manifest => {
                let manifest_key = oci_manifest_key_from_namespace(
                    &key.repository,
                    &key.digest_hex,
                    &key.scope_namespace,
                )?;
                object_store
                    .delete_if_present(&manifest_key)
                    .map_err(GcError::ObjectStore)?;
                let media_type_key = oci_manifest_media_type_key_from_namespace(
                    &key.repository,
                    &key.digest_hex,
                    &key.scope_namespace,
                )?;
                object_store
                    .delete_if_present(&media_type_key)
                    .map_err(GcError::ObjectStore)?;
            }
        }

        if index_store
            .delete_oci_object_tombstone_if_unchanged(&tombstone)
            .await
            .map_err(Into::into)?
        {
            report.reclaimed = shardline_server_core::checked_increment(report.reclaimed)?;
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use shardline_index::{FileChunkRecord, LocalIndexStore, OciObjectKey, StorageRepresentation};
    use shardline_storage::{ObjectBody, ObjectIntegrity};

    use super::*;
    use crate::{LocalGcOptions, runner::run_gc_with_oci_tombstones};

    fn sweep_now() -> LocalGcOptions {
        LocalGcOptions {
            mark: false,
            sweep: true,
            retention_seconds: 0,
            max_revisions_per_repo: None,
        }
    }

    fn put(object_store: &ServerObjectStore, key: &shardline_storage::ObjectKey, bytes: &[u8]) {
        let integrity = ObjectIntegrity::new(
            shardline_server_core::chunk_hash(bytes),
            u64::try_from(bytes.len()).unwrap(),
        );
        object_store
            .put_if_absent(key, ObjectBody::Borrowed(bytes), &integrity)
            .unwrap();
    }

    fn tombstone(kind: OciObjectKind, digest_hex: &str) -> OciObjectKey {
        OciObjectKey {
            scope_namespace: "global".to_owned(),
            repository: "team/assets".to_owned(),
            kind,
            digest_hex: digest_hex.to_owned(),
        }
    }

    proptest! {
        #[test]
        fn physical_first_reclaim_never_resurrects_retained_bytes(
            first_crash_boundary in 0_u8..=2,
            retry_crash_boundary in 0_u8..=2,
        ) {
            let mut bytes_present = true;
            let mut tombstone_present = true;
            for crash_boundary in [first_crash_boundary, retry_crash_boundary, 2] {
                if crash_boundary >= 1 {
                    bytes_present = false;
                }
                prop_assert!(tombstone_present || !bytes_present);
                if crash_boundary >= 2 {
                    tombstone_present = false;
                }
                prop_assert!(tombstone_present || !bytes_present);
            }
            prop_assert!(!bytes_present);
            prop_assert!(!tombstone_present);
        }

        #[test]
        fn tombstone_eligibility_is_monotonic_after_retention(
            deleted_at in any::<u64>(),
            retention in any::<u64>(),
            first_now in any::<u64>(),
            advance in any::<u64>(),
        ) {
            let delete_after = deleted_at.checked_add(retention);
            let eligible_first = delete_after.is_some_and(|deadline| deadline <= first_now);
            let later_now = first_now.saturating_add(advance);
            let eligible_later = delete_after.is_some_and(|deadline| deadline <= later_now);
            prop_assert!(!eligible_first || eligible_later);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn manifest_reclamation_is_retained_then_idempotently_removes_both_objects() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_path_buf();
        let object_store = ServerObjectStore::local(root.join("chunks")).unwrap();
        let index_store = LocalIndexStore::new(root.clone()).unwrap();
        let record_store = LocalRecordStore::new(root).unwrap();
        let digest_hex = "a".repeat(64);
        let object = tombstone(OciObjectKind::Manifest, &digest_hex);
        let manifest_key =
            oci_manifest_key_from_namespace("team/assets", &digest_hex, "global").unwrap();
        let media_type_key =
            oci_manifest_media_type_key_from_namespace("team/assets", &digest_hex, "global")
                .unwrap();
        put(&object_store, &manifest_key, b"manifest");
        put(&object_store, &media_type_key, b"application/test");
        index_store.delete_oci_object(&object).await.unwrap();

        let retained = reclaim_oci_tombstones(
            &record_store,
            &index_store,
            &object_store,
            LocalGcOptions::sweep_only(),
        )
        .await
        .unwrap();
        assert_eq!(retained.scanned, 1);
        assert_eq!(retained.eligible, 0);
        assert!(object_store.contains(&manifest_key).unwrap());
        assert!(object_store.contains(&media_type_key).unwrap());

        // Simulate a process death after physical reclamation but before the
        // final compare-delete of tombstone metadata. The retry must regard
        // already-missing immutable objects as success and finish the row.
        object_store.delete_if_present(&manifest_key).unwrap();
        object_store.delete_if_present(&media_type_key).unwrap();
        assert!(index_store.oci_object_is_deleted(&object).await.unwrap());

        let reclaimed =
            reclaim_oci_tombstones(&record_store, &index_store, &object_store, sweep_now())
                .await
                .unwrap();
        assert_eq!(reclaimed.eligible, 1);
        assert_eq!(reclaimed.reclaimed, 1);
        assert!(!object_store.contains(&manifest_key).unwrap());
        assert!(!object_store.contains(&media_type_key).unwrap());
        assert!(!index_store.oci_object_is_deleted(&object).await.unwrap());

        let replay =
            reclaim_oci_tombstones(&record_store, &index_store, &object_store, sweep_now())
                .await
                .unwrap();
        assert_eq!(replay, OciTombstoneGcReport::default());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn blob_reclamation_removes_only_its_record_and_preserves_shared_chunk() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_path_buf();
        let object_store = ServerObjectStore::local(root.join("chunks")).unwrap();
        let index_store = LocalIndexStore::new(root.clone()).unwrap();
        let record_store = LocalRecordStore::new(root).unwrap();
        let digest_hex = "b".repeat(64);
        let blob_key = oci_blob_key_from_namespace("team/assets", &digest_hex, "global").unwrap();
        let deleted_file_id = protocol_object_file_id(&blob_key);
        let chunk_bytes = b"shared chunk";
        let chunk_hash = shardline_server_core::chunk_hash(chunk_bytes).hex_string();
        let chunk_key = shardline_server_core::chunk_object_key(&chunk_hash).unwrap();
        put(&object_store, &chunk_key, chunk_bytes);

        let record = |file_id: String, content_hash: char| FileRecord {
            file_id,
            content_hash: content_hash.to_string().repeat(64),
            total_bytes: u64::try_from(chunk_bytes.len()).unwrap(),
            chunk_size: 128,
            storage_repr: StorageRepresentation::FixedChunkV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: chunk_hash.clone(),
                offset: 0,
                length: u64::try_from(chunk_bytes.len()).unwrap(),
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: u64::try_from(chunk_bytes.len()).unwrap(),
            }],
        };
        record_store
            .commit_file_version_metadata(&record(deleted_file_id.clone(), 'c'))
            .await
            .unwrap();
        record_store
            .commit_file_version_metadata(&record("survivor".to_owned(), 'd'))
            .await
            .unwrap();
        index_store
            .delete_oci_object(&tombstone(OciObjectKind::Blob, &digest_hex))
            .await
            .unwrap();

        let diagnostics = run_gc_with_oci_tombstones(
            &record_store,
            &index_store,
            &object_store,
            &[shardline_server_core::server_frontend::ServerFrontend::Oci],
            sweep_now(),
        )
        .await
        .unwrap();
        assert_eq!(diagnostics.report.reclaimed_oci_tombstones, 1);
        assert_eq!(diagnostics.report.orphan_chunks, 0);
        assert!(object_store.contains(&chunk_key).unwrap());

        let mut latest_file_ids = Vec::new();
        for locator in record_store.list_latest_record_locators().await.unwrap() {
            let bytes = record_store.read_record_bytes(&locator).await.unwrap();
            latest_file_ids.push(parse_stored_file_record_bytes(&bytes).unwrap().file_id);
        }
        assert_eq!(latest_file_ids, vec!["survivor"]);
        assert!(!latest_file_ids.contains(&deleted_file_id));
    }
}
