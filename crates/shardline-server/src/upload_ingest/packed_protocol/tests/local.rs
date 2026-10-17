use super::*;
use shardline_storage::{LocalObjectStore, ObjectKey, ObjectPrefix, ObjectStore};

async fn ingest(
    store: &ServerObjectStore,
    bytes: &[u8],
    direct: bool,
) -> shardline_index::FileRecord {
    let mut upload = FileUploadIngestor::new_with_parallelism(
        NonZeroUsize::new(64 * 1024).unwrap(),
        false,
        NonZeroUsize::new(8).unwrap(),
    );
    if direct {
        upload = upload.with_packed_protocol_storage(
            store,
            &crate::admission::ExecutionPools::default_sizes().hashing,
        );
    }
    for frame in bytes.chunks(16381) {
        upload
            .ingest_body_chunk(store, &Bytes::copy_from_slice(frame))
            .await
            .unwrap();
    }
    upload
        .finish(store, "identity", None, None)
        .await
        .unwrap()
        .0
}

async fn assert_read(
    store: &ServerObjectStore,
    record: &shardline_index::FileRecord,
    expected: &[u8],
) {
    use futures_util::StreamExt;
    let mut reader = crate::download_stream::file_record_byte_stream(
        store.clone(),
        record.clone(),
        None,
        crate::admission::ExecutionPools::default_sizes().blocking_io,
    )
    .await
    .unwrap();
    let mut offset = 0_usize;
    while let Some(bytes) = reader.next().await {
        let bytes = bytes.unwrap();
        let end = offset.checked_add(bytes.len()).unwrap();
        assert_eq!(&expected[offset..end], bytes.as_ref());
        offset = end;
    }
    assert_eq!(offset, expected.len());
}

#[tokio::test]
async fn local_direct_packing_matches_legacy_across_batch_boundary_and_reopen() {
    let legacy_root = tempfile::tempdir().unwrap();
    let direct_root = tempfile::tempdir().unwrap();
    let legacy = ServerObjectStore::local(legacy_root.path().to_owned()).unwrap();
    let direct = ServerObjectStore::local(direct_root.path().to_owned()).unwrap();
    let bytes: Vec<_> = (0..36 * 1024 * 1024)
        .map(|index| (index % 251) as u8)
        .collect();
    let expected = ingest(&legacy, &bytes, false).await;
    let actual = ingest(&direct, &bytes, true).await;
    assert_eq!(actual, expected);
    let objects =
        ObjectStore::list_prefix(&direct, &ObjectPrefix::parse("xorbs/").unwrap()).unwrap();
    assert!(
        objects.len() >= 2,
        "fixture must cross a container batch boundary"
    );
    assert_eq!(
        ObjectStore::list_prefix(&direct, &ObjectPrefix::parse("").unwrap())
            .unwrap()
            .len(),
        objects.len(),
        "direct packing must leave only containers, with no standalone chunks",
    );
    let reopened = ServerObjectStore::local(direct_root.path().to_owned()).unwrap();
    assert_read(&reopened, &actual, &bytes).await;
    assert_eq!(ingest(&reopened, &bytes, true).await, actual);
}

#[tokio::test]
#[cfg(unix)]
async fn local_container_sync_failure_and_conflicting_bytes_use_verified_fallback() {
    let legacy_root = tempfile::tempdir().unwrap();
    let legacy = ServerObjectStore::local(legacy_root.path().to_owned()).unwrap();
    let bytes = vec![b'a'; 100 * 1024];
    let expected = ingest(&legacy, &bytes, false).await;
    let key = crate::xet_adapter::xorb_object_key(&expected.chunks[0].hash).unwrap();
    for corrupt in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let local = LocalObjectStore::new(root.path().to_owned()).unwrap();
        let path = local.path_for_key(&key);
        let _guard = if corrupt {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, b"corrupt preexisting container").unwrap();
            None
        } else {
            Some(shardline_storage::arm_fault(
                path,
                shardline_storage::LocalPublishBoundary::BeforeParentSync,
                shardline_storage::LocalPublishFault::SyncFailure,
            ))
        };
        let store = ServerObjectStore::Local(local);
        let actual = ingest(&store, &bytes, true).await;
        assert!(
            actual
                .chunks
                .iter()
                .all(|chunk| chunk.hash != expected.chunks[0].hash)
        );
        assert_read(&store, &actual, &bytes).await;
    }
}

#[tokio::test]
async fn local_generic_upload_retains_standalone_chunk_keys() {
    let root = tempfile::tempdir().unwrap();
    let backend = crate::LocalBackend::new(
        root.path().to_owned(),
        "http://localhost".into(),
        NonZeroUsize::new(64 * 1024).unwrap(),
    )
    .await
    .unwrap();
    let response = backend
        .upload_file("public", Bytes::from(vec![b'a'; 100 * 1024]), None)
        .await
        .unwrap();
    for chunk in response.chunks {
        let key: ObjectKey = crate::chunk_store::chunk_object_key(&chunk.hash).unwrap();
        assert!(ObjectStore::contains(&backend.object_store(), &key).unwrap());
    }
}
