use futures_util::TryStreamExt;
mod read_cost;
use shardline_index::{FileChunkRecord, FileRecord, StorageRepresentation};
use shardline_protocol::ByteRange;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};

use crate::{
    ServerError,
    admission::ExecutionPools,
    download_stream::file_record_byte_stream,
    object_store::ServerObjectStore,
    upload_ingest::xorb_packer::{pack_chunks_into_xorb, store_xorb},
};

async fn container(store: &ServerObjectStore, pieces: &[&[u8]]) -> Vec<FileChunkRecord> {
    let mut offset = 0_u64;
    let chunks = pieces
        .iter()
        .map(|bytes| {
            let start = offset;
            offset = offset.checked_add(bytes.len() as u64).unwrap();
            (bytes.to_vec(), start)
        })
        .collect::<Vec<_>>();
    let packed = pack_chunks_into_xorb(&chunks).unwrap();
    store_xorb(store, &packed.xorb_hash_hex, &packed.serialized)
        .await
        .unwrap();
    packed
        .chunk_entries
        .iter()
        .zip(pieces)
        .map(|(entry, bytes)| FileChunkRecord {
            hash: packed.xorb_hash_hex.clone(),
            offset: entry.raw_offset,
            length: bytes.len() as u64,
            range_start: u64::from(entry.chunk_index),
            range_end: u64::from(entry.chunk_index.checked_add(1).unwrap()),
            packed_start: u64::from(entry.packed_offset),
            packed_end: u64::from(
                entry
                    .packed_offset
                    .checked_add(entry.packed_length)
                    .unwrap(),
            ),
        })
        .collect()
}

fn file(mut chunks: Vec<FileChunkRecord>) -> FileRecord {
    let mut total_bytes = 0_u64;
    for chunk in &mut chunks {
        chunk.offset = total_bytes;
        total_bytes = total_bytes.checked_add(chunk.length).unwrap();
    }
    FileRecord {
        file_id: "multi-container".to_owned(),
        content_hash: chunks.first().unwrap().hash.clone(),
        total_bytes,
        chunk_size: 0,
        storage_repr: StorageRepresentation::XorbCdcV1,
        repository_scope: None,
        chunks,
    }
}

async fn collect(
    store: &ServerObjectStore,
    record: &FileRecord,
    range: Option<ByteRange>,
) -> Result<Vec<u8>, ServerError> {
    file_record_byte_stream(
        store.clone(),
        record.clone(),
        range,
        ExecutionPools::default_sizes().blocking_io,
    )
    .await?
    .try_fold(Vec::new(), |mut bytes, chunk| async move {
        bytes.extend_from_slice(&chunk);
        Ok(bytes)
    })
    .await
}

#[tokio::test]
async fn collected_validation_reuses_complete_payload_but_preserves_logical_terms() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path()).unwrap();
    let terms = container(&store, &[b"first", b"second"]).await;
    let record = file(terms.clone());
    let retained = super::file_stream_with_decode_budget(
        store.clone(),
        record.clone(),
        None,
        ExecutionPools::default_sizes().blocking_io,
        11,
    )
    .await
    .unwrap()
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0], b"firstsecond".as_slice());
    let streamed = file_record_byte_stream(
        store.clone(),
        record.clone(),
        None,
        ExecutionPools::default_sizes().blocking_io,
    )
    .await
    .unwrap()
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    assert_eq!(
        streamed.len(),
        2,
        "streaming callers must not retain the complete payload"
    );
    let reordered = file(vec![terms[1].clone(), terms[0].clone(), terms[1].clone()]);
    let bytes = crate::download_stream::collect_file_record_bytes(
        store,
        reordered,
        ExecutionPools::default_sizes().blocking_io,
    )
    .await
    .unwrap();
    assert_eq!(bytes, b"secondfirstsecond");
}

#[tokio::test]
async fn legacy_32_mib_container_remains_readable_with_smaller_new_write_batches() {
    const LEGACY_BATCH_BYTES: usize = 32 * 1024 * 1024;
    const CHUNK_BYTES: usize = 1024 * 1024;

    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path()).unwrap();
    // The prior server grouped a batch up to 32 MiB before serializing it.
    // Build that same one-container shape from valid 1 MiB CDC-sized terms.
    let pieces: Vec<Vec<u8>> = (0..LEGACY_BATCH_BYTES / CHUNK_BYTES)
        .map(|chunk| {
            (0..CHUNK_BYTES)
                .map(|offset| ((chunk * 31 + offset * 17) % 251) as u8)
                .collect()
        })
        .collect();
    let piece_refs: Vec<_> = pieces.iter().map(Vec::as_slice).collect();
    let terms = container(&store, &piece_refs).await;
    let record = file(terms.clone());

    assert_eq!(record.total_bytes, LEGACY_BATCH_BYTES as u64);
    assert!(terms.iter().all(|term| term.hash == terms[0].hash));
    let expected = pieces.into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(collect(&store, &record, None).await.unwrap(), expected);
}

#[tokio::test]
#[allow(
    clippy::panic,
    reason = "a non-local store violates this test's fixture"
)]
async fn collected_validation_rejects_corruption_after_a_valid_prefix() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path()).unwrap();
    let terms = container(&store, &[b"valid prefix", b"corrupt tail"]).await;
    let key = crate::xet_adapter::xorb_object_key(&terms[0].hash).unwrap();
    let ServerObjectStore::Local(local) = &store else {
        panic!("fixture must use local storage")
    };
    let path = local.path_for_key(&key);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[usize::try_from(terms[1].packed_end - 1).unwrap()] ^= 0x40;
    std::fs::write(path, bytes).unwrap();
    assert!(
        crate::download_stream::collect_file_record_bytes(
            store,
            file(terms),
            ExecutionPools::default_sizes().blocking_io,
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn multiple_containers_support_selected_multi_chunk_terms_and_reuse() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path()).unwrap();
    let a = container(&store, &[b"unused", b"abc", b"DEF", b"unused-tail"]).await;
    let b = container(&store, &[b"12", b"345"]).await;
    let mut selected = a.get(1).unwrap().clone();
    selected.length = selected
        .length
        .checked_add(a.get(2).unwrap().length)
        .unwrap();
    selected.range_end = a.get(2).unwrap().range_end;
    selected.packed_end = a.get(2).unwrap().packed_end;
    let record = file(vec![
        selected.clone(),
        selected.clone(),
        b.first().unwrap().clone(),
        b.get(1).unwrap().clone(),
        selected,
    ]);
    let expected = b"abcDEFabcDEF12345abcDEF";
    assert_eq!(collect(&store, &record, None).await.unwrap(), expected);
    for (start, end) in [(0, 0), (2, 9), (10, 18), (17, 22)] {
        assert_eq!(
            collect(&store, &record, Some(ByteRange::new(start, end).unwrap()))
                .await
                .unwrap(),
            expected.get(start as usize..=end as usize).unwrap()
        );
    }
    assert!(matches!(
        collect(
            &store,
            &record,
            Some(ByteRange::new(0, record.total_bytes).unwrap())
        )
        .await,
        Err(ServerError::RangeNotSatisfiable)
    ));
}

#[tokio::test]
async fn mixed_container_and_individual_chunk_fallback_preserves_bytes() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path()).unwrap();
    let a = container(&store, &[b"head", b"tail"]).await;
    let bytes = b"fallback";
    let compressed = lz4_flex::compress_prepend_size(bytes);
    let hash = shardline_index::xet_hash_hex_string(crate::local_backend::chunk_hash(bytes));
    store
        .put_if_absent(
            &crate::chunk_store::chunk_object_key(&hash).unwrap(),
            ObjectBody::from_vec(compressed.clone()),
            &ObjectIntegrity::new(
                crate::local_backend::chunk_hash(&compressed),
                compressed.len() as u64,
            ),
        )
        .unwrap();
    let fallback = FileChunkRecord {
        hash,
        offset: 0,
        length: bytes.len() as u64,
        range_start: 0,
        range_end: 1,
        packed_start: 0,
        packed_end: compressed.len() as u64,
    };
    let record = file(vec![
        a.first().unwrap().clone(),
        fallback.clone(),
        fallback,
        a.get(1).unwrap().clone(),
    ]);
    assert_eq!(
        collect(&store, &record, None).await.unwrap(),
        b"headfallbackfallbacktail"
    );
    assert_eq!(
        collect(&store, &record, Some(ByteRange::new(2, 21).unwrap()))
            .await
            .unwrap(),
        b"adfallbackfallbackta"
    );
}

#[tokio::test]
async fn container_term_must_match_authenticated_chunk_lengths_and_ranges() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path()).unwrap();
    let chunks = container(&store, &[b"first", b"second"]).await;
    for change in 0..3 {
        let mut chunk = chunks.first().unwrap().clone();
        match change {
            0 => chunk.length = chunk.length.checked_add(1).unwrap(),
            1 => chunk.packed_end = chunk.packed_end.checked_add(1).unwrap(),
            _ => {
                chunk.range_start = 9;
                chunk.range_end = 10;
            }
        }
        assert!(collect(&store, &file(vec![chunk]), None).await.is_err());
    }
}
