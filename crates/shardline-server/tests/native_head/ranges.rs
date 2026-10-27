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

pub(super) async fn qualify(
    client: &Client,
    base: &str,
    owner: &str,
    other: &str,
    objects: &shardline_server_core::ServerObjectStore,
) {
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
    signed_member(client, base, owner, &left, &bytes, boundary).await;
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
    // A native hash identifies decoded members, not their compression layout.
    // Simulate replacement in the owned backend with another valid encoding.
    // Old serialized offsets must never grant access to the other member.
    let alternate = serialized_xorb_object_from_components(
        &hash,
        [left.as_slice(), right.as_slice()].concat(),
        vec![(left_hash, length), (right_hash, total)],
        CompressionScheme::LZ4,
    )
    .unwrap()
    .serialized_data;
    shardline_xet_adapter::validate_serialized_xorb(
        &mut std::io::Cursor::new(&alternate),
        shardline_protocol::ShardlineHash::from_bytes(hash.into()),
    )
    .unwrap();
    assert!(u64::try_from(alternate.len()).unwrap() < boundary);
    let key = shardline_xet_adapter::xorb_object_key(&hash.hex()).unwrap();
    replace_source(objects, &key, &alternate).await;
    let replaced = read(client, &url, owner, &left_range).await;
    let replaced_status = replaced.status();
    // Restore before asserting so later fixture cleanup sees the original object.
    replace_source(objects, &key, &bytes).await;
    assert_eq!(
        replaced_status,
        StatusCode::NOT_FOUND,
        "a changed valid encoding must not widen a published member grant"
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

async fn replace_source(
    objects: &shardline_server_core::ServerObjectStore,
    key: &shardline_storage::ObjectKey,
    bytes: &[u8],
) {
    use shardline_storage::{AsyncObjectStore, ObjectBody, ObjectIntegrity};
    objects.delete_if_present(key).await.unwrap();
    let integrity = ObjectIntegrity::new(
        shardline_server_core::validation::chunk_hash(bytes),
        u64::try_from(bytes.len()).unwrap(),
    );
    objects
        .put_if_absent(key, ObjectBody::from_slice(bytes), &integrity)
        .await
        .unwrap();
}

async fn signed_get(client: &Client, url: &reqwest::Url, range: Option<&str>) -> reqwest::Response {
    let mut request = client.get(url.clone());
    if let Some(range) = range {
        request = request.header(reqwest::header::RANGE, range);
    }
    request
        .send()
        .await
        .map_err(|_error| "signed transfer transport failed; URL omitted")
        .unwrap()
}

async fn signed_member(
    client: &Client,
    base: &str,
    owner: &str,
    data: &[u8],
    bytes: &[u8],
    boundary: u64,
) {
    let file_id = file_hash(&[(compute_data_hash(data), u64::try_from(data.len()).unwrap())]).hex();
    let reconstruction_response = client
        .get(format!("{base}/v1/reconstructions/{file_id}"))
        .bearer_auth(owner)
        .send()
        .await
        .unwrap();
    assert_eq!(reconstruction_response.status(), StatusCode::OK);
    let body: serde_json::Value = reconstruction_response.json().await.unwrap();
    let fetch = body
        .get("fetch_info")
        .and_then(serde_json::Value::as_object)
        .unwrap();
    assert_eq!(fetch.len(), 1);
    let entry = fetch
        .values()
        .next()
        .unwrap()
        .as_array()
        .unwrap()
        .first()
        .unwrap();
    let url = reqwest::Url::parse(entry.get("url").unwrap().as_str().unwrap()).unwrap();
    assert_eq!(url.origin(), reqwest::Url::parse(base).unwrap().origin());
    assert!(url.path().starts_with("/transfer/xorb/default/"));
    assert!(
        url.query_pairs()
            .any(|(key, _value)| key == "xorb_read_capability")
    );
    let allowed = format!("bytes=0-{}", boundary.checked_sub(1).unwrap());
    let granted_response = signed_get(client, &url, Some(&allowed)).await;
    assert_eq!(granted_response.status(), StatusCode::PARTIAL_CONTENT);
    let observed = granted_response
        .bytes()
        .await
        .map_err(|_error| "signed transfer body failed; URL omitted")
        .unwrap();
    assert_eq!(
        observed.as_ref(),
        bytes.get(..usize::try_from(boundary).unwrap()).unwrap()
    );
    assert_eq!(
        signed_get(client, &url, None).await.status(),
        StatusCode::BAD_REQUEST
    );
    let widened = format!("bytes=0-{}", bytes.len().checked_sub(1).unwrap());
    let narrowed = format!("bytes=1-{}", boundary.checked_sub(1).unwrap());
    for range in [widened.as_str(), narrowed.as_str(), "bytes=-1"] {
        let denied_response = signed_get(client, &url, Some(range)).await;
        assert_eq!(denied_response.status(), StatusCode::NOT_FOUND);
        assert!(
            !denied_response
                .headers()
                .contains_key(reqwest::header::LOCATION)
        );
    }
}
