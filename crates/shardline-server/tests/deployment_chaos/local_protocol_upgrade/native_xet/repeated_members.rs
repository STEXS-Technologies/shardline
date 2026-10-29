//! Real persisted repeated-member content, including unchanged-reader rollback.
use super::*;
use shardline_xet_core::xorb_object::{
    Chunk, CompressionScheme, RawXorbData, SerializedXorbObject,
};

pub(super) async fn publish(base: &str, token: &str) -> NativeFile {
    let data = vec![0x61; 4096];
    let chunks = (0..128)
        .map(|_| Chunk {
            hash: compute_data_hash(&data),
            data: data.clone().into(),
        })
        .collect::<Vec<_>>();
    let serialized = SerializedXorbObject::from_xorb_with_compression(
        RawXorbData::from_chunks(&chunks, vec![0]),
        CompressionScheme::None,
        true,
    )
    .unwrap();
    let hash = serialized.hash.hex();
    let client = http_client();
    client
        .post(format!("{base}/v1/xorbs/default/{hash}"))
        .bearer_auth(token)
        .body(serialized.serialized_data)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let leaves = chunks
        .iter()
        .map(|chunk| (chunk.hash, 4096))
        .collect::<Vec<_>>();
    let identity = file_hash(&leaves);
    let mut shard = MDBInMemoryShard::default();
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(identity, 1, false, false),
            segments: vec![FileDataSequenceEntry::new(
                serialized.hash,
                128 * 4096,
                0,
                128,
            )],
            verification: Vec::new(),
            metadata_ext: None,
        })
        .unwrap();
    client
        .post(format!("{base}/v1/shards"))
        .bearer_auth(token)
        .body(shard.to_bytes().unwrap())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    NativeFile {
        file_id: identity.hex(),
        members: vec![(
            hash,
            chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter().copied())
                .collect(),
        )],
    }
}
