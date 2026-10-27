//! Qualify physical payload accounting and source authentication without copies.
use super::*;

fn distinct_source() -> (String, Vec<u8>, tempfile::NamedTempFile) {
    use shardline_xet_core::xorb_object::{Chunk, RawXorbData};
    let members: Vec<_> = [0x41, 0x42]
        .into_iter()
        .map(|seed| {
            let data = vec![seed; 65536];
            Chunk {
                hash: shardline_xet_core::merklehash::compute_data_hash(&data),
                data: data.into(),
            }
        })
        .collect();
    let serialized = SerializedXorbObject::from_xorb_with_compression(
        RawXorbData::from_chunks(&members, vec![0]),
        CompressionScheme::None,
        true,
    )
    .unwrap();
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(&serialized.serialized_data).unwrap();
    (serialized.hash.hex(), serialized.serialized_data, file)
}

#[tokio::test]
async fn container_only_retains_one_payload_and_serves_verified_members_after_reopen() {
    let (hash, bytes, file) = distinct_source();
    let path = file.path().to_owned();
    let validated = crate::validate_serialized_xorb(
        &mut std::io::Cursor::new(&bytes),
        shardline_index::parse_xet_hash_hex(&hash).unwrap(),
    )
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path()).unwrap();
    let prepared = prepare_owned_xorb_file_with_checkpoint(&hash, file, &|| Ok(())).unwrap();
    let stored = store_prepared_xorb_file(&objects, prepared, ())
        .await
        .unwrap();
    assert!(stored.was_inserted);
    assert_eq!(stored.stored_bytes, bytes.len() as u64);
    assert!(!path.exists());
    drop(objects);
    let objects = ServerObjectStore::local(root.path()).unwrap();
    let all = objects
        .list_prefix(&shardline_storage::ObjectPrefix::parse("").unwrap())
        .unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].length(), bytes.len() as u64);
    let memory_root = tempfile::tempdir().unwrap();
    let memory = ServerObjectStore::local(memory_root.path()).unwrap();
    let memory_stored = crate::store_uploaded_xorb(&memory, &hash, &bytes)
        .await
        .unwrap();
    let memory_objects = memory
        .list_prefix(&shardline_storage::ObjectPrefix::parse("").unwrap())
        .unwrap();
    let memory_bytes: u64 = memory_objects.iter().map(|object| object.length()).sum();
    assert_eq!(memory_objects.len(), 1);
    assert_eq!(memory_bytes, bytes.len() as u64);
    assert_eq!(memory_stored.stored_bytes, memory_bytes);
    eprintln!(
        "same verified source: memory={} object/{} bytes, file={} object/{} bytes",
        memory_objects.len(),
        memory_bytes,
        all.len(),
        all[0].length(),
    );
    for member in validated.chunks() {
        let member_hash = shardline_index::xet_hash_hex_string(member.hash());
        let key = shardline_server_core::chunk_object_key(&member_hash).unwrap();
        assert!(objects.metadata(&key).unwrap().is_none());
        let decoded = crate::read_verified_xorb_chunk(&objects, &hash, &member_hash)
            .unwrap()
            .unwrap();
        assert_eq!(decoded.len() as u64, member.unpacked_len());
    }
    let retry = tempfile::NamedTempFile::new().unwrap();
    // Use exactly the already persisted bytes rather than a fresh random fixture.
    std::fs::write(retry.path(), &bytes).unwrap();
    let prepared = prepare_owned_xorb_file_with_checkpoint(&hash, retry, &|| Ok(())).unwrap();
    let stored = store_prepared_xorb_file(&objects, prepared, ())
        .await
        .unwrap();
    assert!(!stored.was_inserted);
    assert_eq!(stored.stored_bytes, 0);
}

#[tokio::test]
async fn container_only_rejects_mutated_payload_and_footer_without_publishing() {
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
                .metadata(&crate::xorb_object_key(&hash).unwrap())
                .unwrap()
                .is_none()
        );
        assert!(!path.exists());
    }
}
