use super::*;
use futures_util::FutureExt;

#[tokio::test]
async fn packing_admission_precedes_raw_batch_allocation_and_drop_releases_it() {
    let provider = Provider::new(0).await;
    let pool = crate::admission::BoundedPool::new(NonZeroUsize::new(1).unwrap());
    let held = pool.acquire_buffer().await.unwrap();
    let mut upload = PackedProtocolUpload::new(provider.objects.clone(), pool.clone());
    assert!(upload.push(b"pending", 0).now_or_never().is_none());
    assert!(upload.pending.is_empty());
    assert_eq!(upload.pending_bytes, 0);
    assert!(provider.state.lock().unwrap().requests.is_empty());
    drop(held);
    upload.push(b"admitted", 0).await.unwrap();
    assert!(
        pool.available_buffer_permits() == 0,
        "buffered raw content retains admission"
    );
    crate::admission::blocking_work(&pool, || Ok(()))
        .await
        .unwrap();
    let mut second = PackedProtocolUpload::new(provider.objects.clone(), pool.clone());
    assert!(second.push(b"blocked", 0).now_or_never().is_none());
    assert!(second.pending.is_empty());
    drop(upload);
    assert!(
        pool.available_buffer_permits() == 1,
        "cancelled accumulation releases admission"
    );
    second.push(b"released", 0).await.unwrap();
    let result = second.finish().await.unwrap();
    assert!(!result.records.is_empty());
    assert_eq!(pool.available_buffer_permits(), 1);
    assert!(pool.try_acquire().is_some());
}
