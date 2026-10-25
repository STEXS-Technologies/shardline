//! Retained encoding offsets must work beyond the first member and across ranges.
use std::borrow::Cow;

use futures_util::StreamExt;
use shardline_protocol::ByteRange;
use shardline_xet_core::xorb_object::{CompressionScheme, RawXorbData, SerializedXorbObject};

use super::*;

pub(super) async fn assert_ranges(store: &ServerObjectStore) {
    let chunks = vec![
        (vec![b'a'; 128 * 1024], 0),
        (vec![b'b'; 96 * 1024], 128 * 1024),
        (vec![b'c'; 64 * 1024], 224 * 1024),
    ];
    let incoming = crate::upload_ingest::xorb_packer::pack_chunks_into_xorb(&chunks).unwrap();
    let raw = RawXorbData::from_owned_data(
        chunks
            .iter()
            .map(|(data, _)| Cow::Owned(data.clone()))
            .collect(),
        Vec::new(),
    );
    let retained =
        SerializedXorbObject::from_xorb_ref_with_compression(&raw, CompressionScheme::None, true)
            .unwrap();
    assert_eq!(retained.hash.hex(), incoming.xorb_hash_hex);
    assert_ne!(retained.serialized_data, incoming.serialized);
    let key = crate::xet_adapter::xorb_object_key(&incoming.xorb_hash_hex).unwrap();
    let integrity = ObjectIntegrity::new(
        crate::local_backend::chunk_hash(&retained.serialized_data),
        u64::try_from(retained.serialized_data.len()).unwrap(),
    );
    shardline_storage::AsyncObjectStore::put_if_absent(
        store,
        &key,
        ObjectBody::from_bytes(Bytes::from(retained.serialized_data)),
        &integrity,
    )
    .await
    .unwrap();
    let mut upload = PackedProtocolUpload::new(
        store.clone(),
        crate::admission::ExecutionPools::default_sizes().hashing,
    );
    for (data, offset) in &chunks {
        upload.push(data, *offset).await.unwrap();
    }
    let result = upload.finish().await.unwrap();
    assert_eq!(result.inserted, 0);
    assert_eq!(result.reused, 3);
    assert_eq!(result.records.len(), 3);
    for (actual, original) in result
        .records
        .iter()
        .skip(1)
        .zip(incoming.chunk_entries.iter().skip(1))
    {
        assert_ne!(actual.packed_start, u64::from(original.packed_offset));
        assert_eq!(actual.hash, incoming.xorb_hash_hex);
    }
    let bytes: Vec<_> = chunks
        .iter()
        .flat_map(|(data, _)| data.iter().copied())
        .collect();
    let record = shardline_index::FileRecord {
        file_id: "multi-member-encoding".to_owned(),
        content_hash: incoming.xorb_hash_hex,
        total_bytes: u64::try_from(bytes.len()).unwrap(),
        chunk_size: 0,
        storage_repr: shardline_index::StorageRepresentation::XorbCdcV1,
        repository_scope: None,
        chunks: result.records,
    };
    local::assert_read(store, &record, &bytes).await;
    for (start, end) in [
        (131_000_usize, 131_144_usize),
        (229_300, 229_450),
        (150_000, 151_000),
    ] {
        let range =
            ByteRange::new(u64::try_from(start).unwrap(), u64::try_from(end).unwrap()).unwrap();
        let mut stream = crate::download_stream::file_record_byte_stream(
            store.clone(),
            record.clone(),
            Some(range),
            crate::admission::ExecutionPools::default_sizes().blocking_io,
        )
        .await
        .unwrap();
        let mut actual = Vec::new();
        while let Some(part) = stream.next().await {
            actual.extend_from_slice(&part.unwrap());
        }
        assert_eq!(actual, bytes[start..=end]);
    }
}
