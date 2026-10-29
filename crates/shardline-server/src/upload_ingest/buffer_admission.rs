//! Aggregate admission for request bodies retained before publication.
use crate::{ServerError, admission::BoundedPool};
use std::{num::NonZeroUsize, sync::LazyLock};
use tokio::sync::OwnedSemaphorePermit;

static BUFFERED_UPLOADS: LazyLock<BoundedPool> =
    LazyLock::new(|| BoundedPool::new(NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN)));

/// Acquire before consuming a body; retain through all users of its bytes.
pub(super) async fn acquire() -> Result<OwnedSemaphorePermit, ServerError> {
    BUFFERED_UPLOADS
        .acquire()
        .await
        .map_err(|error| ServerError::Io(std::io::Error::other(error)))
}

/// Slices/clones and storage jobs retain admission with the payload itself.
/// Accept a shared guard when the raw batch and transport overlap in lifetime.
pub(super) fn into_bytes(bytes: Vec<u8>, admission: impl Send + 'static) -> axum::body::Bytes {
    crate::admission::retain_bytes(bytes, admission)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_packing_keeps_shared_admission_with_transport_payload() {
        let pool = BoundedPool::new(NonZeroUsize::MIN);
        let request_permit = std::sync::Arc::new(pool.acquire().await.unwrap());
        let payload = into_bytes(vec![0x5a; 4096], request_permit.clone());
        let transport = payload.slice(1024..2048);
        drop(payload);
        drop(request_permit);
        assert!(
            pool.try_acquire().is_none(),
            "cancelling publication cannot release retained transport memory"
        );
        assert_eq!(transport.as_ref(), &[0x5a; 1024]);
        drop(transport);
        assert!(pool.try_acquire().is_some());
    }

    #[test]
    fn cancelled_staging_retains_admission_until_queued_storage_finishes() {
        use std::future::Future as _;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let root = tempfile::tempdir().unwrap();
        let store = crate::object_store::ServerObjectStore::local(root.path()).unwrap();
        runtime.block_on(async {
            let pool = BoundedPool::new(NonZeroUsize::MIN);
            let bytes = into_bytes(b"admitted storage".to_vec(), pool.acquire().await.unwrap());
            let (release, wait) = std::sync::mpsc::channel();
            let (started, ready) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                wait.recv().unwrap();
            });
            ready.await.unwrap();
            let operation = crate::object_store::stage_owned_bytes_content_addressed(
                &store,
                "staging/admitted",
                bytes,
            );
            let mut operation = Box::pin(operation);
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(
                    operation.as_mut().poll(cx).is_pending()
                ))
                .await
            );
            drop(operation);
            assert!(
                pool.try_acquire().is_none(),
                "queued storage must retain the actual memory permit"
            );
            release.send(()).unwrap();
            blocker.await.unwrap();
            let _released = tokio::time::timeout(std::time::Duration::from_secs(5), pool.acquire())
                .await
                .unwrap()
                .unwrap();
            let path = root.path().join(format!(
                "staging/admitted/{}",
                blake3::hash(b"admitted storage").to_hex()
            ));
            assert_eq!(std::fs::read(path).unwrap(), b"admitted storage");
        });
    }
    #[tokio::test]
    async fn replay_slices_and_storage_body_retain_admission() {
        let pool = BoundedPool::new(NonZeroUsize::MIN);
        let bytes = into_bytes(vec![0x5a; 4096], pool.acquire().await.unwrap());
        let mut reader = crate::upload_ingest::RequestBodyReader::from_chunked_bytes(
            bytes.clone(),
            NonZeroUsize::new(1024).unwrap(),
        );
        let frame = reader.next_bytes().await.unwrap().unwrap();
        drop(bytes);
        drop(reader);
        assert!(pool.try_acquire().is_none());
        let storage_bytes = shardline_storage::ObjectBody::from_bytes(frame).into_bytes();
        assert!(pool.try_acquire().is_none());
        assert_eq!(storage_bytes.len(), 1024);
        drop(storage_bytes);
        assert!(pool.try_acquire().is_some());
    }
}
