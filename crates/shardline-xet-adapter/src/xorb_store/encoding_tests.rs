//! Equivalent verified content must not conflict merely because its encoding differs.

use super::*;
use shardline_xet_core::{
    merklehash::{compute_data_hash, xorb_hash},
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

fn encodings() -> (String, Vec<u8>, Vec<u8>) {
    let payload = vec![b'a'; 128 * 1024];
    let length = u64::try_from(payload.len()).unwrap();
    let member = compute_data_hash(&payload);
    let identity = xorb_hash(&[(member, length)]);
    let hash = identity.hex();
    let plain = serialized_xorb_object_from_components(
        &identity,
        payload.clone(),
        vec![(member, length)],
        CompressionScheme::None,
    )
    .unwrap()
    .serialized_data;
    let compressed = serialized_xorb_object_from_components(
        &identity,
        payload,
        vec![(member, length)],
        CompressionScheme::LZ4,
    )
    .unwrap()
    .serialized_data;
    assert_ne!(plain, compressed);
    (hash, plain, compressed)
}

#[tokio::test]
async fn equivalent_compression_encodings_preserve_the_first_verified_container() {
    let (hash, plain, compressed) = encodings();
    for (first, second) in [(&plain, &compressed), (&compressed, &plain)] {
        for file_backed in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let store = ServerObjectStore::local(directory.path().join("objects")).unwrap();
            assert!(
                store_uploaded_xorb(&store, &hash, first)
                    .await
                    .unwrap()
                    .was_inserted
            );
            let duplicate = if file_backed {
                let mut file = tempfile::NamedTempFile::new().unwrap();
                std::io::Write::write_all(file.as_file_mut(), second).unwrap();
                store_uploaded_xorb_file(&store, &hash, file.path())
                    .await
                    .unwrap()
            } else {
                store_uploaded_xorb(&store, &hash, second).await.unwrap()
            };
            assert!(!duplicate.was_inserted);
            assert_eq!(duplicate.stored_bytes, 0);
            assert_eq!(stored_bytes(&store, &hash), *first);
        }
    }
}

#[tokio::test]
async fn alternate_encoding_cannot_hide_corrupt_retained_content() {
    let (hash, plain, compressed) = encodings();
    let directory = tempfile::tempdir().unwrap();
    let store = ServerObjectStore::local(directory.path().join("objects")).unwrap();
    store_uploaded_xorb(&store, &hash, &plain).await.unwrap();
    let key = xorb_object_key(&hash).unwrap();
    let path = store.local_path_for_key(&key).unwrap();
    let mut damaged = plain;
    damaged[8] ^= 1;
    std::fs::write(path, &damaged).unwrap();
    assert!(matches!(
        store_uploaded_xorb(&store, &hash, &compressed).await,
        Err(XetAdapterError::ObjectStore(_))
    ));
    assert_eq!(stored_bytes(&store, &hash), damaged);
}

fn stored_bytes(store: &ServerObjectStore, hash: &str) -> Vec<u8> {
    let key = xorb_object_key(hash).unwrap();
    let (mut stored, _) = store
        .materialize_bounded_object_if_present(&key, 0)
        .unwrap()
        .unwrap();
    let mut actual = Vec::new();
    std::io::Read::read_to_end(&mut stored, &mut actual).unwrap();
    actual
}

#[tokio::test]
async fn equivalent_content_does_not_hide_a_storage_access_error() {
    let (hash, plain, _) = encodings();
    let directory = tempfile::tempdir().unwrap();
    let store = ServerObjectStore::local(directory.path().join("objects")).unwrap();
    store_uploaded_xorb(&store, &hash, &plain).await.unwrap();
    let key = xorb_object_key(&hash).unwrap();
    let verified =
        validate_serialized_xorb(&mut Cursor::new(plain), parse_xet_hash_hex(&hash).unwrap())
            .unwrap();
    let result = encoding::resolve_conflict(
        &store,
        &key,
        &verified,
        Err(shardline_server_core::ServerObjectStoreError::Io(
            std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        )),
    );
    assert!(matches!(result, Err(XetAdapterError::ObjectStore(
        shardline_server_core::ServerObjectStoreError::Io(ref error)
    )) if error.kind() == std::io::ErrorKind::PermissionDenied));
}
