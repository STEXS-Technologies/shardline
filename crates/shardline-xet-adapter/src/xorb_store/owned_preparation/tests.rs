use super::*;
use shardline_storage::ObjectStore;
use shardline_xet_core::xorb_object::{
    CompressionScheme, SerializedXorbObject,
    xorb_format_test_utils::{ChunkSize, build_raw_xorb},
};
use std::io::Write;

mod container_only;

fn source() -> (String, Vec<u8>, tempfile::NamedTempFile) {
    let serialized = SerializedXorbObject::from_xorb_with_compression(
        build_raw_xorb(2, ChunkSize::Fixed(65536)),
        CompressionScheme::None,
        true,
    )
    .unwrap();
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(&serialized.serialized_data).unwrap();
    (serialized.hash.hex(), serialized.serialized_data, file)
}

#[tokio::test]
async fn prepared_owned_container_preserves_bytes_and_reclaims_staging() {
    let (hash, bytes, file) = source();
    let path = file.path().to_owned();
    let prepared = prepare_owned_xorb_file_with_checkpoint(&hash, file, &|| Ok(())).unwrap();
    assert_eq!(prepared.path(), path);
    assert_eq!(prepared.length(), u64::try_from(bytes.len()).unwrap());
    assert_eq!(
        prepared.integrity().hash().as_bytes(),
        blake3::hash(&bytes).as_bytes()
    );
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path()).unwrap();
    assert!(
        store_prepared_xorb_file(&objects, prepared, ())
            .await
            .unwrap()
            .was_inserted
    );
    let key = super::super::xorb_object_key(&hash).unwrap();
    assert_eq!(
        std::fs::read(objects.local_path_for_key(&key).unwrap()).unwrap(),
        bytes
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn prepared_owned_container_rejects_member_and_footer_mutation_before_publication() {
    for mutate_footer in [false, true] {
        let (hash, mut bytes, file) = source();
        let path = file.path().to_owned();
        let prepared = prepare_owned_xorb_file_with_checkpoint(&hash, file, &|| Ok(())).unwrap();
        let offset = if mutate_footer { bytes.len() - 1 } else { 16 };
        bytes[offset] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        let root = tempfile::tempdir().unwrap();
        let objects = ServerObjectStore::local(root.path()).unwrap();
        assert!(
            store_prepared_xorb_file(&objects, prepared, ())
                .await
                .is_err()
        );
        assert!(
            objects
                .metadata(&super::super::xorb_object_key(&hash).unwrap())
                .unwrap()
                .is_none()
        );
        assert!(!path.exists());
    }
}

#[tokio::test]
async fn prepared_owned_footerless_container_persists_its_verified_canonical_form() {
    let serialized = SerializedXorbObject::from_xorb_with_compression(
        build_raw_xorb(2, ChunkSize::Fixed(65536)),
        CompressionScheme::None,
        false,
    )
    .unwrap();
    let hash = serialized.hash.hex();
    let mut staging = tempfile::NamedTempFile::new().unwrap();
    staging.write_all(&serialized.serialized_data).unwrap();
    let path = staging.path().to_owned();
    let prepared = prepare_owned_xorb_file_with_checkpoint(&hash, staging, &|| Ok(())).unwrap();
    let canonical = std::fs::read(prepared.path()).unwrap();
    assert!(canonical.len() > serialized.serialized_data.len());
    assert_eq!(prepared.path(), path);
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path()).unwrap();
    assert!(
        store_prepared_xorb_file(&objects, prepared, ())
            .await
            .unwrap()
            .was_inserted
    );
    let key = super::super::xorb_object_key(&hash).unwrap();
    assert_eq!(
        std::fs::read(objects.local_path_for_key(&key).unwrap()).unwrap(),
        canonical
    );
    assert!(!path.exists());
}

#[test]
fn abandoning_prepared_owned_container_reclaims_staging_without_publication() {
    let (hash, _, file) = source();
    let path = file.path().to_owned();
    let prepared = prepare_owned_xorb_file_with_checkpoint(&hash, file, &|| Ok(())).unwrap();
    assert!(path.exists());
    drop(prepared);
    assert!(!path.exists());
}
