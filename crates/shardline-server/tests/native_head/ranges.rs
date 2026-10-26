//! Real shard publication must grant only its verified serialized members.
use super::*;
use shardline_xet_core::{
    merklehash::xorb_hash,
    metadata_shard::xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

async fn publish(
    client: &Client,
    base: &str,
    credential: &str,
    info: &MDBXorbInfo,
    data: &[u8],
    member: u64,
) {
    let length = u64::try_from(data.len()).unwrap();
    let mut shard = MDBInMemoryShard::default();
    shard.add_xorb_block(info.clone()).unwrap();
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(
                file_hash(&[(compute_data_hash(data), length)]),
                1,
                false,
                true,
            ),
            segments: vec![FileDataSequenceEntry::new(
                info.metadata.xorb_hash,
                length,
                member,
                member.checked_add(1).unwrap(),
            )],
            verification: Vec::new(),
            metadata_ext: Some(FileMetadataExt::new(
                MerkleHash::from_hex(&format!("{:x}", Sha256::digest(data))).unwrap(),
            )),
        })
        .unwrap();
    let response = client
        .post(format!("{base}/v1/shards"))
        .bearer_auth(credential)
        .body(shard.to_bytes().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

async fn read(client: &Client, url: &str, credential: &str, range: &str) -> reqwest::Response {
    client
        .get(url)
        .bearer_auth(credential)
        .header(reqwest::header::RANGE, range)
        .send()
        .await
        .unwrap()
}

pub(super) async fn qualify(client: &Client, base: &str, owner: &str, other: &str) {
    let left = vec![b'L'; 32 * 1024];
    let right = vec![b'R'; 32 * 1024];
    let length = u64::try_from(left.len()).unwrap();
    let total = length.checked_mul(2).unwrap();
    let left_hash = compute_data_hash(&left);
    let right_hash = compute_data_hash(&right);
    let hash = xorb_hash(&[(left_hash, length), (right_hash, length)]);
    let bytes = serialized_xorb_object_from_components(
        &hash,
        [left.as_slice(), right.as_slice()].concat(),
        vec![(left_hash, length), (right_hash, total)],
        CompressionScheme::None,
    )
    .unwrap()
    .serialized_data;
    let verified = shardline_xet_adapter::validate_serialized_xorb(
        &mut std::io::Cursor::new(&bytes),
        shardline_protocol::ShardlineHash::from_bytes(hash.into()),
    )
    .unwrap();
    let boundary = verified.chunks().first().unwrap().packed_end();
    let payload_end = verified.packed_content_length();
    let end = u64::try_from(bytes.len()).unwrap().checked_sub(1).unwrap();
    let info = MDBXorbInfo {
        metadata: XorbChunkSequenceHeader::new(hash, 2, total),
        chunks: vec![
            XorbChunkSequenceEntry::new(left_hash, length, 0),
            XorbChunkSequenceEntry::new(right_hash, length, length),
        ],
    };
    let url = format!("{base}/transfer/xorb/default/{}", hash.hex());
    assert_eq!(
        client
            .put(&url)
            .bearer_auth(owner)
            .body(bytes.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    publish(client, base, owner, &info, &left, 0).await;
    // The right member belongs to a different repository, not to the left owner.
    let other_writer = token("other", TokenScope::Write);
    publish(client, base, &other_writer, &info, &right, 1).await;
    let left_range = format!("bytes=0-{}", boundary.checked_sub(1).unwrap());
    let left_response = read(client, &url, owner, &left_range).await;
    assert_eq!(left_response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        left_response.bytes().await.unwrap().as_ref(),
        bytes.get(..usize::try_from(boundary).unwrap()).unwrap()
    );
    for range in [
        format!("bytes={boundary}-{}", payload_end.checked_sub(1).unwrap()),
        format!("bytes={payload_end}-{end}"),
        format!("bytes=0-{end}"),
        "bytes=-1".to_owned(),
    ] {
        let response = read(client, &url, owner, &range).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(!response.headers().contains_key(reqwest::header::LOCATION));
    }
    assert_eq!(
        read(client, &url, other, &left_range).await.status(),
        StatusCode::NOT_FOUND
    );
    // A second genuine publication in the same repository completes coverage;
    // neither individual file alone covers the whole container payload.
    publish(client, base, owner, &info, &right, 1).await;
    let response = read(client, &url, owner, &format!("bytes=0-{end}")).await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    let footer = read(client, &url, owner, "bytes=-1").await;
    assert_eq!(footer.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        footer.bytes().await.unwrap().as_ref(),
        bytes.last_chunk::<1>().unwrap()
    );
}
