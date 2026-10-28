use super::*;
use shardline_xet_core::xorb_object::{
    CompressionScheme, SerializedXorbObject,
    xorb_format_test_utils::{ChunkSize, build_raw_xorb},
};

#[test]
fn incremental_members_match_reference_for_split_headers_payloads_and_footers() {
    for compression in [
        CompressionScheme::None,
        CompressionScheme::LZ4,
        CompressionScheme::ByteGrouping4LZ4,
    ] {
        for footer in [false, true] {
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                build_raw_xorb(4, ChunkSize::Fixed(4096)),
                compression,
                footer,
            )
            .unwrap();
            let hash = serialized.hash.hex();
            let reference = prepare_xorb_memory(&hash, &serialized.serialized_data).unwrap();
            for frame_size in [1, 7, 8, 17, 4096, 65536] {
                let mut state =
                    IncrementalXorbMemory::new(&hash, serialized.serialized_data.len()).unwrap();
                let mut saw_member_before_eof = false;
                let frames: Vec<_> = serialized.serialized_data.chunks(frame_size).collect();
                for (index, frame) in frames.iter().enumerate() {
                    state = state.push(frame, &|| Ok(())).unwrap();
                    saw_member_before_eof |=
                        index + 1 < frames.len() && state.verified_member_count() > 0;
                }
                if frame_size < 4096 {
                    assert!(saw_member_before_eof);
                }
                let prepared = state.finish(&|| Ok(())).unwrap();
                assert_eq!(prepared.bytes, reference.bytes);
                assert_eq!(prepared.validated, reference.validated);
                assert_eq!(prepared.integrity, reference.integrity);
            }
        }
    }
}

#[test]
fn incremental_input_never_publishes_incomplete_modified_or_oversized_content() {
    for footer in [false, true] {
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(4, ChunkSize::Fixed(256)),
            CompressionScheme::None,
            footer,
        )
        .unwrap();
        let hash = serialized.hash.hex();
        for removed in [1, 7, 8, serialized.serialized_data.len() - 1] {
            let result = IncrementalXorbMemory::new(&hash, serialized.serialized_data.len())
                .unwrap()
                .push(
                    &serialized.serialized_data[..serialized.serialized_data.len() - removed],
                    &|| Ok(()),
                )
                .and_then(|state| state.finish(&|| Ok(())));
            assert!(result.is_err());
        }
        let mut changed = serialized.serialized_data.clone();
        changed[8] ^= 1;
        assert!(
            IncrementalXorbMemory::new(&hash, changed.len())
                .unwrap()
                .push(&changed, &|| Ok(()))
                .and_then(|state| state.finish(&|| Ok(())))
                .is_err()
        );
        assert!(
            IncrementalXorbMemory::new(&hash, 1)
                .unwrap()
                .push(&serialized.serialized_data, &|| Ok(()))
                .is_err()
        );
        assert!(
            IncrementalXorbMemory::new(&hash, serialized.serialized_data.len())
                .unwrap()
                .push(&serialized.serialized_data, &|| {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "cancel",
                    ))
                })
                .is_err()
        );
    }
}

#[test]
fn incremental_corruption_decisions_match_complete_validation() {
    for compression in [
        CompressionScheme::None,
        CompressionScheme::LZ4,
        CompressionScheme::ByteGrouping4LZ4,
    ] {
        for footer in [false, true] {
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                build_raw_xorb(4, ChunkSize::Fixed(32)),
                compression,
                footer,
            )
            .unwrap();
            let hash = serialized.hash.hex();
            for offset in 0..serialized.serialized_data.len() {
                let mut changed = serialized.serialized_data.clone();
                changed[offset] ^= 1;
                let reference = prepare_xorb_memory(&hash, &changed);
                let mut state = Ok(IncrementalXorbMemory::new(&hash, changed.len()).unwrap());
                for frame in changed.chunks(7) {
                    state = state.and_then(|state| state.push(frame, &|| Ok(())));
                }
                let result = state.and_then(|state| state.finish(&|| Ok(())));
                assert_eq!(
                    result.is_ok(),
                    reference.is_ok(),
                    "compression={compression:?}, footer={footer}, offset={offset}"
                );
                if let (Ok(prepared), Ok(reference)) = (result, reference) {
                    assert_eq!(prepared.bytes, reference.bytes);
                    assert_eq!(prepared.validated, reference.validated);
                }
            }
        }
    }
}

#[test]
fn incremental_preserves_normalization_limit_and_larger_canonical_input() {
    for footer in [false, true] {
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(1025, ChunkSize::Fixed(65536)),
            CompressionScheme::LZ4,
            footer,
        )
        .unwrap();
        let hash = serialized.hash.hex();
        let reference = prepare_xorb_memory(&hash, &serialized.serialized_data);
        let mut state =
            IncrementalXorbMemory::new(&hash, serialized.serialized_data.len()).unwrap();
        for frame in serialized.serialized_data.chunks(65536) {
            state = state.push(frame, &|| Ok(())).unwrap();
        }
        let prepared = state.finish(&|| Ok(()));
        if footer {
            let prepared = prepared.unwrap();
            let reference = reference.unwrap();
            assert_eq!(prepared.bytes, reference.bytes);
            assert_eq!(prepared.validated, reference.validated);
        } else {
            assert!(reference.is_err());
            assert!(prepared.is_err());
        }
    }
}

#[test]
fn incremental_preserves_previously_valid_large_member_tables() {
    let serialized = SerializedXorbObject::from_xorb_with_compression(
        build_raw_xorb(8193, ChunkSize::Fixed(8)),
        CompressionScheme::None,
        true,
    )
    .unwrap();
    let hash = serialized.hash.hex();
    let reference = prepare_xorb_memory(&hash, &serialized.serialized_data).unwrap();
    let mut state = IncrementalXorbMemory::new(&hash, serialized.serialized_data.len()).unwrap();
    for frame in serialized.serialized_data.chunks(4096) {
        state = state.push(frame, &|| Ok(())).unwrap();
    }
    let prepared = state.finish(&|| Ok(())).unwrap();
    assert_eq!(prepared.bytes, reference.bytes);
    assert_eq!(prepared.validated, reference.validated);
}

#[test]
fn owned_memory_matches_reference_formats_and_reuses_canonical_allocation() {
    for compression in [
        CompressionScheme::None,
        CompressionScheme::LZ4,
        CompressionScheme::ByteGrouping4LZ4,
    ] {
        for footer in [false, true] {
            for count in [4, 8192] {
                let serialized = SerializedXorbObject::from_xorb_with_compression(
                    build_raw_xorb(
                        count,
                        ChunkSize::Fixed(if count == 8192 { 8 } else { 4096 }),
                    ),
                    compression,
                    footer,
                )
                .unwrap();
                let hash = serialized.hash.hex();
                let reference = prepare_xorb_memory(&hash, &serialized.serialized_data).unwrap();
                let pointer = serialized.serialized_data.as_ptr();
                let prepared = prepare_owned_xorb_memory_with_checkpoint(
                    &hash,
                    serialized.serialized_data,
                    &|| Ok(()),
                )
                .unwrap();
                assert_eq!(prepared.bytes, reference.bytes);
                assert_eq!(prepared.validated, reference.validated);
                assert_eq!(prepared.integrity, reference.integrity);
                if footer {
                    assert_eq!(prepared.bytes.as_ptr(), pointer);
                }
            }
        }
    }
}

#[test]
fn owned_memory_cancellation_stops_at_every_validation_checkpoint() {
    use std::cell::Cell;
    for footer in [false, true] {
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(4, ChunkSize::Fixed(4096)),
            CompressionScheme::LZ4,
            footer,
        )
        .unwrap();
        let hash = serialized.hash.hex();
        let calls = Cell::new(0);
        prepare_owned_xorb_memory_with_checkpoint(
            &hash,
            serialized.serialized_data.clone(),
            &|| {
                calls.set(calls.get() + 1);
                Ok(())
            },
        )
        .unwrap();
        let total = calls.get();
        assert!(total > 4);
        for stop in 1..=total {
            calls.set(0);
            let result = prepare_owned_xorb_memory_with_checkpoint(
                &hash,
                serialized.serialized_data.clone(),
                &|| {
                    calls.set(calls.get() + 1);
                    if calls.get() >= stop {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "cancelled",
                        ))
                    } else {
                        Ok(())
                    }
                },
            );
            assert!(result.is_err(), "checkpoint {stop} ignored cancellation");
            assert_eq!(calls.get(), stop, "work continued after cancellation");
        }
    }
}

#[test]
fn owned_memory_rejects_bad_roots_mutated_members_and_trailing_bytes() {
    for footer in [false, true] {
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(4, ChunkSize::Fixed(4096)),
            CompressionScheme::None,
            footer,
        )
        .unwrap();
        let hash = serialized.hash.hex();
        let bytes = serialized.serialized_data;
        assert!(
            prepare_owned_xorb_memory_with_checkpoint(&"00".repeat(32), bytes.clone(), &|| Ok(()))
                .is_err()
        );
        let mut changed = bytes.clone();
        changed[16] ^= 1;
        assert!(prepare_owned_xorb_memory_with_checkpoint(&hash, changed, &|| Ok(())).is_err());
        let mut trailing = bytes.clone();
        trailing.extend_from_slice(b"not a chunk or footer");
        assert!(prepare_owned_xorb_memory_with_checkpoint(&hash, trailing, &|| Ok(())).is_err());
        let truncated = bytes[..bytes.len() - 1].to_vec();
        assert!(prepare_owned_xorb_memory_with_checkpoint(&hash, truncated, &|| Ok(())).is_err());
    }
}

#[tokio::test]
async fn prepared_memory_owns_canonical_bytes_and_preserves_idempotent_replay() {
    for footer in [false, true] {
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(4, ChunkSize::Fixed(4096)),
            CompressionScheme::LZ4,
            footer,
        )
        .unwrap();
        let hash = serialized.hash.hex();
        let mut input = serialized.serialized_data;
        let prepared = prepare_xorb_memory(&hash, &input).unwrap();
        let expected = prepared.bytes.clone();
        let length = prepared.length();
        // Mutating the caller's buffer cannot change already validated bytes.
        input.fill(0);
        let root = tempfile::tempdir().unwrap();
        let objects = ServerObjectStore::local(root.path()).unwrap();
        let stored = store_prepared_xorb_memory(&objects, prepared, ())
            .await
            .unwrap();
        assert!(stored.was_inserted);
        assert_eq!(stored.stored_bytes, length);
        let key = xorb_object_key(&hash).unwrap();
        assert_eq!(
            std::fs::read(objects.local_path_for_key(&key).unwrap()).unwrap(),
            expected.to_vec()
        );
        let replay = prepare_xorb_memory(&hash, &expected).unwrap();
        let stored = store_prepared_xorb_memory(&objects, replay, ())
            .await
            .unwrap();
        assert!(!stored.was_inserted);
        assert_eq!(stored.stored_bytes, 0);
    }
}

#[tokio::test]
async fn streamed_commitment_rejects_post_validation_changes_and_allows_clean_retry() {
    for footer in [false, true] {
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(2, ChunkSize::Fixed(4096)),
            CompressionScheme::None,
            footer,
        )
        .unwrap();
        let hash = serialized.hash.hex();
        for change_footer in [false, true] {
            let prepare = || {
                let mut state =
                    IncrementalXorbMemory::new(&hash, serialized.serialized_data.len()).unwrap();
                for frame in serialized.serialized_data.chunks(17) {
                    state = state.push(frame, &|| Ok(())).unwrap();
                }
                state.finish(&|| Ok(())).unwrap()
            };
            let mut prepared = prepare();
            let position = if change_footer {
                prepared.bytes.len() - 1
            } else {
                16
            };
            // Inject a fault after validation; public callers cannot mutate it.
            let mut changed = prepared.bytes.to_vec();
            changed[position] ^= 1;
            prepared.bytes = VerifiedObjectBody::from_vec(changed);
            let root = tempfile::tempdir().unwrap();
            let objects = ServerObjectStore::local(root.path()).unwrap();
            assert!(
                store_prepared_xorb_memory(&objects, prepared, ())
                    .await
                    .is_err()
            );
            let path = objects
                .local_path_for_key(&xorb_object_key(&hash).unwrap())
                .unwrap();
            assert!(!path.exists());
            let clean = prepare();
            let expected = clean.bytes.clone();
            assert!(
                store_prepared_xorb_memory(&objects, clean, ())
                    .await
                    .unwrap()
                    .was_inserted
            );
            assert_eq!(std::fs::read(path).unwrap(), expected.to_vec());
        }
    }
}

#[tokio::test]
async fn publication_rechecks_prepared_memory_digest_and_rejects_bad_identity() {
    let serialized = SerializedXorbObject::from_xorb_with_compression(
        build_raw_xorb(2, ChunkSize::Fixed(4096)),
        CompressionScheme::None,
        true,
    )
    .unwrap();
    let hash = serialized.hash.hex();
    assert!(prepare_xorb_memory(&"0".repeat(64), &serialized.serialized_data).is_err());
    let mut prepared =
        prepare_owned_xorb_memory_with_checkpoint(&hash, serialized.serialized_data, &|| Ok(()))
            .unwrap();
    // Internal fault injection: no public operation exposes mutable bytes.
    let mut changed = prepared.bytes.to_vec();
    changed[16] ^= 1;
    prepared.bytes = VerifiedObjectBody::from_vec(changed);
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path()).unwrap();
    assert!(
        store_prepared_xorb_memory(&objects, prepared, ())
            .await
            .is_err()
    );
    assert!(
        !objects
            .local_path_for_key(&xorb_object_key(&hash).unwrap())
            .unwrap()
            .exists()
    );
}
