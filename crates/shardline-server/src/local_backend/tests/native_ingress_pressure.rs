//! A connected idle upload must not occupy unrelated verification execution.
use super::*;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_native_upload_allows_another_upload_and_verified_read() {
    let root = tempfile::tempdir().unwrap();
    let mut backend = LocalBackend::new(
        root.path().to_path_buf(),
        "http://localhost:8080".to_owned(),
        NonZeroUsize::new(128).unwrap(),
    )
    .await
    .unwrap();
    backend.stream_work_pool = crate::admission::BoundedPool::new(NonZeroUsize::MIN);
    let stalled_backend = backend.clone();
    let (body, stalled_hash) = single_chunk_xorb(b"client remains connected");
    let missing_hash = stalled_hash.clone();
    let (idle, observed_idle) = tokio::sync::oneshot::channel();
    let stream = futures_util::StreamExt::chain(
        futures_util::stream::iter([Ok(body.slice(..1))]),
        futures_util::stream::once(async move {
            idle.send(()).unwrap();
            std::future::pending().await
        }),
    );
    let stalled = tokio::spawn(async move {
        stalled_backend
            .upload_xorb_stream(&stalled_hash, RequestBodyReader::from_stream(stream))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), observed_idle)
        .await
        .expect("the first frame must enter the verification queue")
        .unwrap();

    let (complete, hash) = single_chunk_xorb(b"independent upload completes");
    tokio::time::timeout(Duration::from_secs(5), backend.upload_xorb(&hash, complete))
        .await
        .expect("a connected idle client must not block another upload")
        .unwrap();
    let verified = tokio::time::timeout(
        Duration::from_secs(5),
        backend.prepare_xorb_range(&hash, None),
    )
    .await
    .expect("the completed object must remain readable while the client is idle")
    .unwrap();
    assert!(verified.total_length() > 0);
    assert!(!stalled.is_finished());
    assert!(backend.xorb_length(&missing_hash).await.is_err());
    stalled.abort();
    assert!(stalled.await.unwrap_err().is_cancelled());
    let _available =
        tokio::time::timeout(Duration::from_secs(5), backend.stream_work_pool.acquire())
            .await
            .unwrap()
            .unwrap();
}
