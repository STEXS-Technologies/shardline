#![allow(clippy::unwrap_used)]
use super::*;
use std::time::Duration;

#[tokio::test]
async fn public_local_file_upload_waits_before_writing_payload() {
    let root = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(
        root.path().to_path_buf(),
        "http://localhost:8080".to_owned(),
        NonZeroUsize::new(128).unwrap(),
    )
    .await
    .unwrap();
    let barrier = crate::maintenance_barrier::acquire_local_exclusive(root.path())
        .await
        .unwrap();
    let upload = backend.upload_file("library-file", Bytes::from_static(b"body"), None);
    tokio::pin!(upload);
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut upload)
            .await
            .is_err()
    );
    assert!(
        shardline_storage::ObjectStore::list_prefix(
            &backend.object_store(),
            &shardline_storage::ObjectPrefix::parse("").unwrap()
        )
        .unwrap()
        .is_empty()
    );
    drop(barrier);
    tokio::time::timeout(Duration::from_secs(10), upload)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        backend
            .download_file("library-file", None, None)
            .await
            .unwrap(),
        b"body"
    );
}

#[tokio::test]
async fn cancelled_public_local_file_upload_releases_gc_without_publication() {
    let root = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(
        root.path().to_path_buf(),
        "http://localhost:8080".to_owned(),
        NonZeroUsize::new(128).unwrap(),
    )
    .await
    .unwrap();
    let publication = backend.metadata_write_lock.lock().await;
    let uploader = backend.clone();
    let upload = tokio::spawn(async move {
        uploader
            .upload_file(
                "cancelled-library-file",
                Bytes::from_static(b"unpublished body"),
                None,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if !shardline_storage::ObjectStore::list_prefix(
                &backend.object_store(),
                &shardline_storage::ObjectPrefix::parse("").unwrap(),
            )
            .unwrap()
            .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let exclusive = crate::maintenance_barrier::acquire_local_exclusive(root.path());
    tokio::pin!(exclusive);
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut exclusive)
            .await
            .is_err()
    );
    upload.abort();
    assert!(upload.await.unwrap_err().is_cancelled());
    drop(publication);
    let gc = tokio::time::timeout(Duration::from_secs(10), exclusive)
        .await
        .unwrap()
        .unwrap();
    drop(gc);
    assert!(matches!(
        backend
            .download_file("cancelled-library-file", None, None)
            .await,
        Err(crate::ServerError::NotFound)
    ));
}

#[tokio::test]
async fn public_local_xorb_upload_waits_for_exclusive_gc() {
    let root = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(
        root.path().to_path_buf(),
        "http://localhost:8080".to_owned(),
        NonZeroUsize::new(128).unwrap(),
    )
    .await
    .unwrap();
    let (_, canonical, hash) = crate::test_fixtures::footerless_xorb().unwrap();
    let barrier = crate::maintenance_barrier::acquire_local_exclusive(root.path())
        .await
        .unwrap();
    let upload = backend.upload_xorb(&hash, canonical);
    tokio::pin!(upload);
    let while_gc_owns_storage = tokio::time::timeout(Duration::from_millis(250), &mut upload).await;
    drop(barrier);
    assert!(
        while_gc_owns_storage.is_err(),
        "local public upload bypassed exclusive GC"
    );
    tokio::time::timeout(Duration::from_secs(10), upload)
        .await
        .unwrap()
        .unwrap();
}
