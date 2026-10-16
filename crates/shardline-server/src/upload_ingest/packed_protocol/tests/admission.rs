use super::*;
use futures_util::FutureExt;

#[tokio::test]
async fn packing_admission_precedes_raw_batch_allocation_and_drop_releases_it() {
    let provider = Provider::new(0).await;
    let pool = crate::admission::BoundedPool::new(NonZeroUsize::new(1).unwrap());
    let held = pool.acquire().await.unwrap();
    let mut upload = PackedProtocolUpload::new(provider.s3_store().clone(), pool.clone());
    assert!(upload.push(b"pending", 0).now_or_never().is_none());
    assert!(upload.pending.is_empty());
    assert_eq!(upload.pending_bytes, 0);
    assert!(provider.state.lock().unwrap().requests.is_empty());
    drop(held);
    upload.push(b"admitted", 0).await.unwrap();
    assert!(
        pool.try_acquire().is_none(),
        "buffered raw content retains admission"
    );
    drop(upload);
    assert!(
        pool.try_acquire().is_some(),
        "cancelled accumulation releases admission"
    );
}
