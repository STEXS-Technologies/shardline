use super::*;
use shardline_xet_core::xorb_object::{
    CompressionScheme, SerializedXorbObject,
    xorb_format_test_utils::{ChunkSize, build_raw_xorb},
};

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
            expected
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
async fn publication_rechecks_prepared_memory_digest_and_rejects_bad_identity() {
    let serialized = SerializedXorbObject::from_xorb_with_compression(
        build_raw_xorb(2, ChunkSize::Fixed(4096)),
        CompressionScheme::None,
        true,
    )
    .unwrap();
    let hash = serialized.hash.hex();
    assert!(prepare_xorb_memory(&"0".repeat(64), &serialized.serialized_data).is_err());
    let mut prepared = prepare_xorb_memory(&hash, &serialized.serialized_data).unwrap();
    // Internal fault injection: no public operation exposes mutable bytes.
    prepared.bytes[16] ^= 1;
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
