use super::*;
use crate::{admission::BoundedPool, upload_ingest::StagedRequestBody};
use std::{io::Write, num::NonZeroUsize, path::PathBuf};

fn staged(bytes: &[u8]) -> (crate::upload_ingest::StagedXorbBody, PathBuf) {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(bytes).unwrap();
    let path = file.path().to_path_buf();
    (
        StagedRequestBody::File {
            file,
            length: u64::try_from(bytes.len()).unwrap(),
            hash: (),
        },
        path,
    )
}

#[test]
fn memory_identity_retains_owned_canonical_preparation_for_storage() {
    let (wire, canonical, hash) = crate::test_fixtures::footerless_xorb().unwrap();
    for bytes in [&wire, &canonical] {
        let input = StagedRequestBody::Memory {
            bytes: axum::body::Bytes::copy_from_slice(bytes),
            length: bytes.len() as u64,
            hash: (),
        };
        let (prepared, length) = prepare_staged_xorb(&hash, input, &|| Ok(())).unwrap();
        assert!(matches!(prepared, PreparedNativeXorb::Memory(_)));
        assert_eq!(length, canonical.len() as u64);
        if let PreparedNativeXorb::Memory(prepared) = prepared {
            assert_eq!(prepared.length(), length);
        }
    }
}

#[test]
fn native_preparation_cancels_at_every_checkpoint_and_reclaims_owned_staging() {
    use std::cell::Cell;
    let (wire, canonical, hash) = crate::test_fixtures::footerless_xorb().unwrap();
    for bytes in [&wire, &canonical] {
        let calls = Cell::new(0_usize);
        let (input, path) = staged(bytes);
        let prepared = prepare_staged_xorb(&hash, input, &|| {
            calls.set(calls.get() + 1);
            Ok(())
        })
        .unwrap();
        drop(prepared);
        assert!(!path.exists());
        let checkpoints = calls.get();
        assert!(checkpoints > 1, "validation must check beyond admission");
        // Includes canonical probing, footer reconstruction and the serialized
        // digest read after appending a footer. Never swallow cancellation as
        // format detection, continue hashing or retain the owned staging file.
        for stop in 1..=checkpoints {
            calls.set(0);
            let (input, path) = staged(bytes);
            let result = prepare_staged_xorb(&hash, input, &|| {
                calls.set(calls.get() + 1);
                if calls.get() >= stop {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "cancelled native preparation",
                    ))
                } else {
                    Ok(())
                }
            });
            assert!(result.is_err(), "checkpoint {stop} ignored cancellation");
            assert_eq!(calls.get(), stop, "work continued after cancellation");
            assert!(!path.exists(), "checkpoint {stop} leaked owned staging");
        }
    }
}

#[tokio::test]
async fn cancelled_native_validation_reclaims_queued_input_without_releasing_another_owner() {
    let pool = BoundedPool::new(NonZeroUsize::new(1).unwrap());
    let held = pool.acquire().await.unwrap();
    let (wire, _, hash) = crate::test_fixtures::footerless_xorb().unwrap();
    let (input, path) = staged(&wire);
    let mut validation = Box::pin(xorb_length(&hash, input, &pool));
    assert!(futures_util::poll!(&mut validation).is_pending());
    assert!(path.exists(), "queued validation must retain its source");
    drop(validation);
    assert!(!path.exists(), "cancelled validation leaked its source");
    assert!(
        pool.try_acquire().is_none(),
        "unrelated work lost admission"
    );
    drop(held);
    assert!(
        pool.try_acquire().is_some(),
        "cancelled waiter retained admission"
    );
}

#[tokio::test]
async fn invalid_native_validation_reclaims_source_and_admission() {
    let pool = BoundedPool::new(NonZeroUsize::new(1).unwrap());
    let (_, canonical, hash) = crate::test_fixtures::footerless_xorb().unwrap();
    let mut invalid = canonical.to_vec();
    *invalid.last_mut().unwrap() ^= 0xff;
    let (input, path) = staged(&invalid);
    assert!(xorb_length(&hash, input, &pool).await.is_err());
    assert!(!path.exists(), "failed validation leaked its source");
    assert!(
        pool.try_acquire().is_some(),
        "failed validation leaked admission"
    );
}

#[tokio::test]
#[allow(clippy::panic)] // A wrong staged representation must fail this test immediately.
async fn successful_native_normalization_preserves_owned_staging_and_updates_integrity() {
    let pool = BoundedPool::new(NonZeroUsize::new(1).unwrap());
    let (wire, canonical, hash) = crate::test_fixtures::footerless_xorb().unwrap();
    let (input, source) = staged(&wire);
    let (output, length) = xorb_length(&hash, input, &pool).await.unwrap();
    assert!(source.exists(), "owned staging was unnecessarily replaced");
    let PreparedNativeXorb::File(file) = output else {
        panic!("normalization must keep file-backed ownership");
    };
    let output_path = file.path().to_path_buf();
    assert_eq!(output_path, source);
    assert_eq!(std::fs::read(&output_path).unwrap(), canonical);
    assert_eq!(length, u64::try_from(canonical.len()).unwrap());
    assert_eq!(file.length(), length);
    assert_eq!(
        file.integrity().hash().as_bytes(),
        blake3::hash(&canonical).as_bytes()
    );
    drop(file);
    assert!(!output_path.exists(), "normalization leaked its output");
    assert!(pool.try_acquire().is_some());
}

#[tokio::test]
#[allow(clippy::panic)] // A canonical upload must retain file-backed ownership.
async fn canonical_native_identity_keeps_staging_bytes_and_digest_unchanged() {
    let pool = BoundedPool::new(NonZeroUsize::new(1).unwrap());
    let (_, canonical, hash) = crate::test_fixtures::footerless_xorb().unwrap();
    let (input, source) = staged(&canonical);
    let (output, length) = xorb_length(&hash, input, &pool).await.unwrap();
    let PreparedNativeXorb::File(file) = output else {
        panic!("canonical upload lost file-backed ownership");
    };
    assert_eq!(file.path(), source);
    assert_eq!(std::fs::read(file.path()).unwrap(), canonical);
    assert_eq!(length, u64::try_from(canonical.len()).unwrap());
    assert_eq!(
        file.integrity().hash().as_bytes(),
        blake3::hash(&canonical).as_bytes()
    );
    drop(file);
    assert!(!source.exists());
}

#[tokio::test]
#[allow(clippy::panic)]
async fn already_normalized_staging_repairs_the_earlier_footerless_identity() {
    let pool = BoundedPool::new(NonZeroUsize::new(1).unwrap());
    let (wire, canonical, expected) = crate::test_fixtures::footerless_xorb().unwrap();
    let (mut input, _) = staged(&canonical);
    if let StagedRequestBody::File { length, .. } = &mut input {
        *length = u64::try_from(wire.len()).unwrap();
    }
    let (output, length) = xorb_length(&expected, input, &pool).await.unwrap();
    let PreparedNativeXorb::File(file) = output else {
        panic!("canonical upload lost file-backed ownership");
    };
    assert_eq!(length, u64::try_from(canonical.len()).unwrap());
    assert_eq!(
        file.integrity().hash().as_bytes(),
        blake3::hash(&canonical).as_bytes()
    );
    assert_eq!(std::fs::read(file.path()).unwrap(), canonical);
}

#[tokio::test]
async fn rejected_native_identity_reclaims_owned_staging_and_admission() {
    let pool = BoundedPool::new(NonZeroUsize::new(1).unwrap());
    let (wire, canonical, _) = crate::test_fixtures::footerless_xorb().unwrap();
    for bytes in [&wire, &canonical] {
        let (input, source) = staged(bytes);
        assert!(xorb_length(&"00".repeat(32), input, &pool).await.is_err());
        assert!(!source.exists(), "rejected upload retained private staging");
        assert!(
            pool.try_acquire().is_some(),
            "rejected upload leaked admission"
        );
    }
}
