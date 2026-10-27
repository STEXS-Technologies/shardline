//! Upload-local repetition sharing, source mutation and fresh-call verification.
use super::*;

#[tokio::test]
async fn repeated_members_share_one_conditional_write() {
    repeated_member_requests(&[0x41; 128], 1).await;
}

#[tokio::test]
async fn interleaved_members_share_their_first_conditional_writes() {
    let seeds: Vec<_> = (0..128).map(|index| 0x41 + (index % 2)).collect();
    repeated_member_requests(&seeds, 2).await;
}

#[tokio::test]
async fn sharing_bound_preserves_members_outside_the_identity_set() {
    let seeds: Vec<_> = (0..129).chain(0..129).collect();
    repeated_member_requests(&seeds, 130).await;
}

async fn repeated_member_requests(seeds: &[u8], expected_requests: usize) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let observed = Arc::clone(&attempts);
    let payloads: std::collections::HashMap<_, _> = seeds
        .iter()
        .map(|seed| {
            let hash = xet_hash_hex_string(ShardlineHash::from_bytes(
                shardline_xet_core::merklehash::compute_data_hash(&vec![*seed; 4096]).into(),
            ));
            (hash, *seed)
        })
        .collect();
    let unique = payloads.len();
    let app = Router::new().fallback(move |method: Method, uri: Uri, headers: HeaderMap| {
        observed.fetch_add(1, Ordering::SeqCst);
        let hash = uri.path().rsplit('/').next().unwrap();
        let seed = *payloads.get(hash).unwrap();
        async move {
            if method == Method::GET {
                (StatusCode::OK, [("etag", "\"fixture\"")], vec![seed; 4096])
            } else {
                assert_eq!(method, Method::PUT);
                assert_eq!(headers["if-none-match"], "*");
                (StatusCode::OK, [("etag", "\"fixture\"")], Vec::new())
            }
        }
    });
    let _stop = Stop(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap()
    }));
    let objects = ServerObjectStore::s3(
        S3ObjectStoreConfig::new("fixture".to_owned(), "us-east-1".to_owned())
            .with_endpoint(Some(endpoint))
            .with_allow_http(true)
            .with_credentials(
                Some(SecretString::new("fixture".to_owned())),
                Some(SecretString::new("fixture-secret".to_owned())),
                None,
            ),
    )
    .unwrap();
    let members = seeds
        .iter()
        .map(|seed| {
            let data = vec![*seed; 4096];
            Chunk {
                hash: shardline_xet_core::merklehash::compute_data_hash(&data),
                data: data.into(),
            }
        })
        .collect::<Vec<_>>();
    let serialized = SerializedXorbObject::from_xorb_with_compression(
        RawXorbData::from_chunks(&members, vec![0]),
        CompressionScheme::None,
        true,
    )
    .unwrap();
    let mut reader = Cursor::new(serialized.serialized_data);
    let validated = validate_serialized_xorb(
        &mut reader,
        parse_xet_hash_hex(&serialized.hash.hex()).unwrap(),
    )
    .unwrap();
    assert_eq!(
        store(&objects, &mut reader, &validated).await.unwrap(),
        u64::try_from(unique).unwrap().checked_mul(4096).unwrap()
    );
    let attempts = attempts.load(Ordering::SeqCst);
    eprintln!(
        "HTTP requests for {} repeated members: {attempts}",
        seeds.len()
    );
    assert_eq!(attempts, expected_requests);
}

#[tokio::test]
async fn repeated_member_reread_still_rejects_mutation_and_allows_retry() {
    repeated_member_mutation_and_retry(false).await;
}

#[tokio::test]
async fn interleaved_member_reread_still_rejects_mutation_and_allows_retry() {
    repeated_member_mutation_and_retry(true).await;
}

async fn repeated_member_mutation_and_retry(interleaved: bool) {
    let data = vec![0x41; 4096];
    let members = (0..4)
        .map(|index| {
            let member = if interleaved && index % 2 == 1 {
                vec![0x42; 4096]
            } else {
                data.clone()
            };
            Chunk {
                hash: shardline_xet_core::merklehash::compute_data_hash(&member),
                data: member.into(),
            }
        })
        .collect::<Vec<_>>();
    let serialized = SerializedXorbObject::from_xorb_with_compression(
        RawXorbData::from_chunks(&members, vec![0]),
        CompressionScheme::None,
        true,
    )
    .unwrap();
    let mut reader = Cursor::new(serialized.serialized_data.clone());
    let validated = validate_serialized_xorb(
        &mut reader,
        parse_xet_hash_hex(&serialized.hash.hex()).unwrap(),
    )
    .unwrap();
    let repeated_index = if interleaved { 2 } else { 1 };
    let offset = usize::try_from(validated.chunks()[repeated_index].packed_end())
        .unwrap()
        .checked_sub(1)
        .unwrap();
    reader.get_mut()[offset] ^= 1;
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path()).unwrap();
    {
        let mut intact = Cursor::new(serialized.serialized_data.clone());
        let mut upload = Box::pin(store(&objects, &mut intact, &validated));
        assert!(
            futures_util::poll!(upload.as_mut()).is_pending(),
            "repeated members must retain a cooperative cancellation point"
        );
    }
    assert!(matches!(
        store(&objects, &mut reader, &validated).await,
        Err(XetAdapterError::XorbHashMismatch)
    ));
    let mut retry = Cursor::new(serialized.serialized_data);
    assert_eq!(
        store(&objects, &mut retry, &validated).await.unwrap(),
        if interleaved { 8192 } else { 4096 }
    );
    let key = chunk_object_key_local(&xet_hash_hex_string(validated.chunks()[0].hash())).unwrap();
    assert_eq!(
        shardline_server_core::read_full_object(&objects, &key, 4096).unwrap(),
        data
    );
    // A new upload must not reuse the previous call's storage observation.
    shardline_storage::ObjectStore::delete_if_present(&objects, &key).unwrap();
    let conflicting = vec![0x42; 4096];
    shardline_storage::ObjectStore::put_if_absent(
        &objects,
        &key,
        ObjectBody::from_vec(conflicting.clone()),
        &ObjectIntegrity::new(chunk_hash(&conflicting), 4096),
    )
    .unwrap();
    assert!(store(&objects, &mut retry, &validated).await.is_err());
    assert_eq!(
        shardline_server_core::read_full_object(&objects, &key, 4096).unwrap(),
        conflicting,
        "sharing repeated writes must not hide or overwrite an existing conflict"
    );
    shardline_storage::ObjectStore::delete_if_present(&objects, &key).unwrap();
    assert_eq!(store(&objects, &mut retry, &validated).await.unwrap(), 4096);
}
