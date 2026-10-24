//! Cancellation must be observed before invoking a publication callback.

use super::*;
use shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS;
use shardline_xet_core::{
    merklehash::file_hash,
    metadata_shard::{
        file_structs::{FileDataSequenceHeader, MDBFileInfo},
        shard_in_memory::MDBInMemoryShard,
    },
};
use std::{
    future::Future,
    io::Write,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Waker},
};

fn empty_shard() -> Vec<u8> {
    let mut shard = MDBInMemoryShard::default();
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash(&[]), 0, false, false),
            segments: Vec::new(),
            verification: Vec::new(),
            metadata_ext: None,
        })
        .unwrap();
    shard.to_bytes().unwrap()
}

#[test]
fn cancelled_read_only_preparation_never_persists_a_shard() {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let checks = AtomicUsize::new(0);
    let error = crate::prepare_uploaded_shard_with_checkpoint(
        &mut std::io::Cursor::new(empty_shard()),
        &objects,
        None,
        DEFAULT_SHARD_METADATA_LIMITS,
        &|| {
            if checks.fetch_add(1, Ordering::SeqCst) >= 1 {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "cancelled",
                ))
            } else {
                Ok(())
            }
        },
    )
    .unwrap_err();
    assert!(matches!(error, XetAdapterError::Io(ref error)
        if error.kind() == std::io::ErrorKind::Interrupted));
    let mut count = 0_usize;
    objects
        .visit_prefix(
            &shardline_storage::ObjectPrefix::parse("shards/").unwrap(),
            |_| {
                count = count.checked_add(1).unwrap();
                Ok::<(), XetAdapterError>(())
            },
        )
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn read_only_shard_preparation_publishes_nothing_until_registration() {
    use shardline_storage::ObjectPrefix;
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let bytes = empty_shard();
    let count_shards = || {
        let mut count = 0_usize;
        objects
            .visit_prefix(&ObjectPrefix::parse("shards/").unwrap(), |_metadata| {
                count = count.checked_add(1).unwrap();
                Ok::<(), XetAdapterError>(())
            })
            .unwrap();
        count
    };
    let prepared = crate::prepare_uploaded_shard(
        &mut std::io::Cursor::new(&bytes),
        &objects,
        None,
        DEFAULT_SHARD_METADATA_LIMITS,
    )
    .unwrap();
    assert_eq!(count_shards(), 0, "verification must be read-only");
    drop(prepared);
    assert_eq!(
        count_shards(),
        0,
        "abandoning verified work must not publish"
    );
    let prepared = crate::prepare_uploaded_shard(
        &mut std::io::Cursor::new(&bytes),
        &objects,
        None,
        DEFAULT_SHARD_METADATA_LIMITS,
    )
    .unwrap();
    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(prepared.register(|records, _, _, _, _| async move {
            assert_eq!(records.len(), 1);
            Ok(())
        }))
        .unwrap();
    assert_eq!(response.result, 1);
    assert_eq!(count_shards(), 1);
}

#[test]
fn validated_shard_cancellation_never_invokes_metadata_callback_and_retry_can_publish() {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let bytes = empty_shard();
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(&bytes).unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    for variant in 0..4 {
        let committed = AtomicUsize::new(0);
        for cancel in [true, false] {
            let callback = |records: Vec<FileRecord>| {
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].total_bytes, 0);
                committed.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            };
            let mut registration: Pin<
                Box<dyn Future<Output = Result<ShardUploadResponse, XetAdapterError>> + '_>,
            > = match variant {
                0 => Box::pin(register_uploaded_shard_bytes(
                    &objects,
                    &bytes,
                    None,
                    DEFAULT_SHARD_METADATA_LIMITS,
                    |records, _| callback(records),
                )),
                1 => Box::pin(register_uploaded_shard_file(
                    &objects,
                    file.path(),
                    None,
                    DEFAULT_SHARD_METADATA_LIMITS,
                    |records, _| callback(records),
                )),
                2 => Box::pin(register_uploaded_shard_bytes_with_verified_descriptors(
                    &objects,
                    &bytes,
                    None,
                    DEFAULT_SHARD_METADATA_LIMITS,
                    |records, _, _, _, _| callback(records),
                )),
                _ => Box::pin(register_uploaded_shard_file_with_verified_descriptors(
                    &objects,
                    file.path(),
                    None,
                    DEFAULT_SHARD_METADATA_LIMITS,
                    |records, _, _, _, _| callback(records),
                )),
            };
            if cancel {
                let mut context = Context::from_waker(Waker::noop());
                assert!(
                    registration.as_mut().poll(&mut context).is_pending(),
                    "variant {variant} published without a cancellation boundary"
                );
                drop(registration);
                assert_eq!(committed.load(Ordering::SeqCst), 0);
            } else {
                runtime.block_on(registration).unwrap();
                assert_eq!(committed.load(Ordering::SeqCst), 1);
            }
        }
    }
}
