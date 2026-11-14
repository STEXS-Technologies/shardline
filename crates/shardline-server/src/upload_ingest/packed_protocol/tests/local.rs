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
        &crate::admission::ExecutionPools::default_sizes().hashing,
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

pub(super) async fn assert_read(
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
async fn public_upload_packs_before_eof_and_cancellation_releases_batch_admission() {
    let root = tempfile::tempdir().unwrap();
    let store = ServerObjectStore::local(root.path().to_owned()).unwrap();
    let pool = crate::admission::BoundedPool::new(NonZeroUsize::MIN);
    let mut upload = FileUploadIngestor::new_with_parallelism(
        NonZeroUsize::new(64 * 1024).unwrap(),
        false,
        NonZeroUsize::new(8).unwrap(),
        &pool,
    );
    let frame = Bytes::from(vec![0x5a; 64 * 1024]);
    for _ in 0..(34 * 1024 / 64) {
        upload.ingest_body_chunk(&store, &frame).await.unwrap();
    }
    assert!(
        !ObjectStore::list_prefix(&store, &ObjectPrefix::parse("xorbs/").unwrap())
            .unwrap()
            .is_empty(),
        "a bounded container must persist before the whole upload reaches EOF"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), pool.acquire_buffer())
            .await
            .is_err(),
        "the pending raw batch must retain admission"
    );
    crate::admission::blocking_work(&pool, || Ok(()))
        .await
        .unwrap();
    drop(upload);
    let _released = tokio::time::timeout(std::time::Duration::from_secs(5), pool.acquire_buffer())
        .await
        .unwrap()
        .unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protocol_ingest_reuses_physical_members_across_s3_and_basic_lfs() {
    use sha2::{Digest as _, Sha256};
    for remote in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let provider = if remote {
            Some(Provider::new(0).await)
        } else {
            None
        };
        let objects = provider.as_ref().map_or_else(
            || ServerObjectStore::local(root.path().join("chunks")).unwrap(),
            |provider| provider.objects.clone(),
        );
        let backend = crate::LocalBackend::new_with_object_store(
            root.path().to_owned(),
            "http://localhost".to_owned(),
            NonZeroUsize::new(64 * 1024).unwrap(),
            objects.clone(),
        )
        .await
        .unwrap();
        let mut state = 0x1234_5678_90ab_cdef_u64;
        let original: Vec<u8> = (0..4 * 1024 * 1024)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect();
        let first = backend
            .upload_s3_protocol_stream(
                "s3-source",
                crate::upload_ingest::RequestBodyReader::from_bytes(Bytes::copy_from_slice(
                    &original,
                )),
            )
            .await
            .unwrap();
        let physical_bytes = || -> u64 {
            provider.as_ref().map_or_else(
                || {
                    ObjectStore::list_prefix(&objects, &ObjectPrefix::parse("xorbs/").unwrap())
                        .unwrap()
                        .iter()
                        .map(|entry| entry.length())
                        .sum()
                },
                |provider| {
                    provider
                        .state
                        .lock()
                        .unwrap()
                        .objects
                        .values()
                        .map(|bytes| bytes.len() as u64)
                        .sum()
                },
            )
        };
        let before = physical_bytes();
        let exact = backend
            .upload_digest_protocol_stream(
                "lfs-exact",
                crate::upload_ingest::RequestBodyReader::from_bytes(Bytes::copy_from_slice(
                    &original,
                )),
                &hex::encode(Sha256::digest(&original)),
            )
            .await
            .unwrap();
        assert!(exact.reused_chunks > 0);
        assert_eq!(exact.inserted_chunks, 0);
        assert_eq!(
            physical_bytes(),
            before,
            "cross-protocol exact reuse must write no new payload"
        );
        let mut edited = original.clone();
        edited[2 * 1024 * 1024..2 * 1024 * 1024 + 4096].fill(0x5a);
        let changed = backend
            .upload_digest_protocol_stream(
                "lfs-edited",
                crate::upload_ingest::RequestBodyReader::from_bytes(Bytes::copy_from_slice(
                    &edited,
                )),
                &hex::encode(Sha256::digest(&edited)),
            )
            .await
            .unwrap();
        assert!(changed.reused_chunks > changed.inserted_chunks);
        assert!(
            physical_bytes() - before < original.len() as u64 / 4,
            "a 4 KiB edit must not retain another complete 4 MiB container"
        );
        assert_eq!(
            backend
                .download_file("s3-source", Some(&first.content_hash), None)
                .await
                .unwrap(),
            original
        );
        assert_eq!(
            backend
                .download_file("lfs-edited", Some(&changed.content_hash), None)
                .await
                .unwrap(),
            edited
        );
        assert!(backend.delete_file_reference("s3-source").await.unwrap());
        assert!(backend.delete_file_reference("lfs-exact").await.unwrap());
        assert_eq!(
            backend
                .download_file("lfs-edited", Some(&changed.content_hash), None)
                .await
                .unwrap(),
            edited
        );
    }
}
