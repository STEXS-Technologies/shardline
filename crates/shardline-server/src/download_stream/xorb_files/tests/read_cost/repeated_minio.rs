//! Real MinIO request counts for legacy repeated chunks and the reuse limit.
use super::*;
use shardline_protocol::SecretString;
use shardline_storage::{AsyncObjectStore, ObjectBody, ObjectIntegrity};
use shardline_test_support::{DockerLocalStack, S3FaultProxy, S3ProxyFault};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker MinIO"]
async fn repeated_legacy_chunks_on_real_minio_use_bounded_verified_reuse() {
    let services = tokio::task::spawn_blocking(|| {
        DockerLocalStack::builder()
            .with_minio()
            .start()
            .unwrap()
            .expect("Docker required")
    })
    .await
    .unwrap();
    let raw = services.s3_raw_config(None).unwrap();
    let proxy = S3FaultProxy::start_disarmed(
        raw.endpoint.as_deref().unwrap(),
        S3ProxyFault::ServiceUnavailableOnce,
    )
    .unwrap();
    let store = ServerObjectStore::s3(
        S3ObjectStoreConfig::new(raw.bucket, raw.region)
            .with_endpoint(Some(proxy.endpoint().to_owned()))
            .with_key_prefix(Some(&services.unique_s3_key_prefix("legacy-repeated")))
            .with_allow_http(true)
            .with_credentials(
                raw.access_key.map(SecretString::new),
                raw.secret_key.map(SecretString::new),
                None,
            ),
    )
    .unwrap();
    for compressed in [false, true] {
        for length in [65_536, 65_537] {
            let mut payload: Vec<u8> = (0..length).map(|n| (n % 251) as u8).collect();
            payload[0] = u8::from(compressed);
            let hash =
                shardline_index::xet_hash_hex_string(crate::local_backend::chunk_hash(&payload));
            let key = crate::chunk_store::chunk_object_key(&hash).unwrap();
            let encoded = if compressed {
                lz4_flex::compress_prepend_size(&payload)
            } else {
                payload.clone()
            };
            let stored_length = encoded.len() as u64;
            let integrity =
                ObjectIntegrity::new(crate::local_backend::chunk_hash(&encoded), stored_length);
            AsyncObjectStore::put_if_absent(
                &store,
                &key,
                ObjectBody::from_vec(encoded),
                &integrity,
            )
            .await
            .unwrap();
            let mut record = super::super::file(
                (0..8)
                    .map(|_| FileChunkRecord {
                        hash: hash.clone(),
                        offset: 0,
                        length: length as u64,
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
            let before = proxy.get_requests();
            let before_bytes = proxy.get_response_bytes();
            assert_eq!(
                super::super::collect(&store, &record, None).await.unwrap(),
                payload.repeat(8)
            );
            let copies = if length == 65_536 { 1 } else { 8 };
            assert_eq!(
                proxy.get_requests() - before,
                1 + copies,
                "one missing-container probe plus payload GETs"
            );
            assert_eq!(
                proxy.get_response_bytes() - before_bytes,
                usize::try_from(stored_length).unwrap() * copies
            );
        }
    }
}
