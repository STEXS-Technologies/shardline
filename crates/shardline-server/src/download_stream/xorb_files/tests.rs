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
