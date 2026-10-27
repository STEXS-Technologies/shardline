//! Repeated logical chunks need only one verified backend payload per stream.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consecutive_fallback_chunks_reuse_one_verified_get() {
    let mut reads_by_representation = Vec::new();
    for compressed in [false, true] {
        for payload_length in [65_536, 65_537] {
            let provider = Arc::new(Mutex::new(Provider::default()));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let app = Router::new().fallback(request).with_state(provider.clone());
            let _server = StopServer(tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            }));
            let store = ServerObjectStore::s3(
                S3ObjectStoreConfig::new("fixture".to_owned(), "us-east-1".to_owned())
                    .with_endpoint(Some(endpoint))
                    .with_allow_http(true)
                    .with_credentials(
                        Some(SecretString::new("fixture-access".to_owned())),
                        Some(SecretString::new("fixture-secret".to_owned())),
                        None,
                    ),
            )
            .unwrap();
            let payload: Vec<u8> = (0..payload_length).map(|n| (n % 251) as u8).collect();
            let hash =
                shardline_index::xet_hash_hex_string(crate::local_backend::chunk_hash(&payload));
            let key = crate::chunk_store::chunk_object_key(&hash).unwrap();
            let encoded = if compressed {
                lz4_flex::compress_prepend_size(&payload)
            } else {
                payload.clone()
            };
            let stored_length = encoded.len() as u64;
            let path = format!("/fixture/{}", key.as_str());
            provider
                .lock()
                .unwrap()
                .objects
                .insert(path.clone(), Bytes::from(encoded));
            let mut record = super::super::file(
                (0..8)
                    .map(|_| FileChunkRecord {
                        hash: hash.clone(),
                        offset: 0,
                        length: payload.len() as u64,
                        range_start: 0,
                        range_end: 1,
                        packed_start: 0,
                        packed_end: stored_length,
                    })
                    .collect(),
            );
            record.storage_repr = if compressed {
                StorageRepresentation::XorbCdcV1
            } else {
                StorageRepresentation::FixedChunkV1
            };
            assert_eq!(
                super::super::collect(&store, &record, None).await.unwrap(),
                payload.repeat(8)
            );
            let reads = provider
                .lock()
                .unwrap()
                .requests
                .iter()
                .filter(|(method, key)| *method == Method::GET && key == &path)
                .count();
            reads_by_representation.push((compressed, payload_length, reads));
            // Partial first/last chunks keep only their sliced output.
            provider.lock().unwrap().requests.clear();
            let end = payload.len() + 31;
            let partial = super::super::collect(
                &store,
                &record,
                Some(ByteRange::new(17, end as u64).unwrap()),
            )
            .await
            .unwrap();
            assert_eq!(partial, payload.repeat(8)[17..=end]);
            let reads = provider
                .lock()
                .unwrap()
                .requests
                .iter()
                .filter(|(method, key)| *method == Method::GET && key == &path)
                .count();
            assert_eq!(reads, 2, "partial reads do not retain whole chunks");
            // Reuse is confined to this stream. A new read must authenticate storage again.
            provider
                .lock()
                .unwrap()
                .objects
                .insert(path, Bytes::from(vec![0; stored_length as usize]));
            assert!(super::super::collect(&store, &record, None).await.is_err());
        }
    }
    assert_eq!(
        reads_by_representation,
        [
            (false, 65_536, 1),
            (false, 65_537, 8),
            (true, 65_536, 1),
            (true, 65_537, 8),
        ]
    );
}
