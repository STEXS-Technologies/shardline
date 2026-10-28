use super::*;
use crate::{admission::BoundedPool, upload_ingest::NativeBody};
use std::num::NonZeroUsize;

async fn input(bytes: &[u8], memory: &BoundedPool) -> NativeBody {
    NativeBody {
        bytes: bytes.to_vec(),
        admission: memory.acquire().await.unwrap(),
    }
}

#[tokio::test]
async fn canonical_and_footerless_uploads_share_identity_and_retain_admission() {
    let memory = BoundedPool::new(NonZeroUsize::MIN);
    let worker = BoundedPool::new(NonZeroUsize::MIN);
    let (wire, canonical, hash) = crate::test_fixtures::footerless_xorb().unwrap();
    for bytes in [&wire, &canonical] {
        let (prepared, length, resources) =
            xorb_length(&hash, input(bytes, &memory).await, &worker)
                .await
                .unwrap();
        assert_eq!(length, canonical.len() as u64);
        assert_eq!(prepared.length(), length);
        assert!(memory.try_acquire().is_none());
        assert!(worker.try_acquire().is_none());
        drop((prepared, resources));
        assert!(memory.try_acquire().is_some());
        assert!(worker.try_acquire().is_some());
    }
}

#[tokio::test]
async fn cancelled_queued_validation_releases_buffer_capacity_without_releasing_another_owner() {
    let memory = BoundedPool::new(NonZeroUsize::MIN);
    let worker = BoundedPool::new(NonZeroUsize::MIN);
    let held = worker.acquire().await.unwrap();
    let (wire, _, hash) = crate::test_fixtures::footerless_xorb().unwrap();
    let mut validation = Box::pin(xorb_length(&hash, input(&wire, &memory).await, &worker));
    assert!(futures_util::poll!(&mut validation).is_pending());
    assert!(memory.try_acquire().is_none());
    drop(validation);
    assert!(memory.try_acquire().is_some());
    assert!(worker.try_acquire().is_none());
    drop(held);
    assert!(worker.try_acquire().is_some());
}

#[tokio::test]
async fn invalid_and_mismatched_uploads_release_all_admission() {
    let memory = BoundedPool::new(NonZeroUsize::MIN);
    let worker = BoundedPool::new(NonZeroUsize::MIN);
    let (wire, canonical, hash) = crate::test_fixtures::footerless_xorb().unwrap();
    for bytes in [&wire, &canonical] {
        assert!(
            xorb_length(&"00".repeat(32), input(bytes, &memory).await, &worker)
                .await
                .is_err()
        );
        assert!(memory.try_acquire().is_some());
        assert!(worker.try_acquire().is_some());
    }
    let mut corrupt = canonical.to_vec();
    *corrupt.last_mut().unwrap() ^= 0xff;
    assert!(
        xorb_length(&hash, input(&corrupt, &memory).await, &worker)
            .await
            .is_err()
    );
    assert!(memory.try_acquire().is_some());
    assert!(worker.try_acquire().is_some());
}
