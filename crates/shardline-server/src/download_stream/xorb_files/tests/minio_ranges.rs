//! Real provider qualification for authenticated member reads and legacy fallback.
use super::*;
use shardline_protocol::SecretString;
use shardline_storage::S3ObjectStoreConfig;
use shardline_test_support::{DockerLocalStack, S3FaultProxy, S3ProxyFault};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker MinIO"]
async fn authenticated_ranges_on_real_minio_preserve_full_reads_and_member_integrity() {
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
            .with_key_prefix(Some(&services.unique_s3_key_prefix("authenticated-ranges")))
            .with_allow_http(true)
            .with_credentials(
                raw.access_key.map(SecretString::new),
                raw.secret_key.map(SecretString::new),
                None,
            ),
    )
    .unwrap();
    let mut seed = 973249_u64;
    let payload: Vec<u8> = (0..4 * 1024 * 1024)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as u8
        })
        .collect();
    let terms = container(&store, &payload.chunks(64 * 1024).collect::<Vec<_>>()).await;
    let record = file(terms.clone());
    let before = proxy.get_requests();
    let before_bytes = proxy.get_response_bytes();
    assert_eq!(collect(&store, &record, None).await.unwrap(), payload);
    assert_eq!(
        proxy.get_requests() - before,
        1,
        "full download remains one GET"
    );
    let full_bytes = proxy.get_response_bytes() - before_bytes;
    for start in [0, 2 * 1024 * 1024, payload.len() - 4096] {
        let before = proxy.get_requests();
        let before_bytes = proxy.get_response_bytes();
        let range = Some(ByteRange::new(start as u64, (start + 4095) as u64).unwrap());
        assert_eq!(
            collect(&store, &record, range).await.unwrap(),
            payload[start..start + 4096]
        );
        assert_eq!(
            proxy.get_requests() - before,
            3,
            "suffix, footer, selected member"
        );
        let partial_bytes = proxy.get_response_bytes() - before_bytes;
        assert!(
            partial_bytes < full_bytes / 32,
            "selected members reduce actual provider bytes"
        );
        eprintln!(
            "range offset={start} logical_bytes=4096 full_backend_bytes={full_bytes} partial_backend_bytes={partial_bytes} full_gets=1 partial_gets=3"
        );
    }
    let reordered = file(vec![terms[63].clone(), terms[0].clone(), terms[63].clone()]);
    // This small logical file intentionally retains the legacy one-GET policy.
    assert_eq!(
        collect(&store, &reordered, Some(ByteRange::new(0, 4095).unwrap()))
            .await
            .unwrap(),
        payload[payload.len() - 64 * 1024..payload.len() - 64 * 1024 + 4096]
    );
    let key = crate::xet_adapter::xorb_object_key(&terms[0].hash).unwrap();
    let length = store.metadata(&key).unwrap().unwrap().length();
    let mut encoded = store
        .read_range(&key, ByteRange::new(0, length - 1).unwrap())
        .unwrap();
    // A selected-member proof authenticates the requested bytes, rather than
    // silently promising that unrequested provider payloads are healthy.
    let tail = usize::try_from(terms.last().unwrap().packed_end - 1).unwrap();
    encoded[tail] ^= 0x40;
    store.delete_if_present(&key).unwrap();
    store
        .put_if_absent(
            &key,
            ObjectBody::from_slice(&encoded),
            &ObjectIntegrity::new(crate::local_backend::chunk_hash(&encoded), length),
        )
        .unwrap();
    assert_eq!(
        collect(&store, &record, Some(ByteRange::new(0, 4095).unwrap()))
            .await
            .unwrap(),
        payload[..4096]
    );
    assert!(
        collect(&store, &record, None).await.is_err(),
        "full reads must still detect corruption outside the selected range"
    );
    assert!(
        file_record_byte_stream(
            store.clone(),
            record.clone(),
            Some(ByteRange::new(payload.len() as u64 - 4096, payload.len() as u64 - 1).unwrap()),
            ExecutionPools::default_sizes().blocking_io
        )
        .await
        .is_err(),
        "selecting the corrupt tail must fail before a response stream is returned"
    );
    encoded[tail] ^= 0x40;
    encoded[usize::try_from(terms[0].packed_end - 1).unwrap()] ^= 0x40;
    store.delete_if_present(&key).unwrap();
    store
        .put_if_absent(
            &key,
            ObjectBody::from_slice(&encoded),
            &ObjectIntegrity::new(crate::local_backend::chunk_hash(&encoded), length),
        )
        .unwrap();
    assert!(
        file_record_byte_stream(
            store,
            record,
            Some(ByteRange::new(0, 4095).unwrap()),
            ExecutionPools::default_sizes().blocking_io
        )
        .await
        .is_err()
    );
}
