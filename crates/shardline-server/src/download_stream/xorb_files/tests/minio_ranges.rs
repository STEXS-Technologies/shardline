//! Real provider qualification for authenticated member reads and legacy fallback.
use super::*;
use futures_util::{StreamExt, TryStreamExt};
use shardline_protocol::{RepositoryProvider, RepositoryScope, SecretString};
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
        S3ProxyFault::ConditionalRangePreconditionFailedOnce,
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

    let key = crate::xet_adapter::xorb_object_key(&terms[0].hash).unwrap();
    let serialized_length = store.metadata(&key).unwrap().unwrap().length();
    let serialized = store
        .read_range(&key, ByteRange::new(0, serialized_length - 1).unwrap())
        .unwrap();
    for range in [
        ByteRange::new(terms[0].packed_end - 1, terms[0].packed_end + 7).unwrap(),
        ByteRange::new(terms.last().unwrap().packed_end, serialized_length - 1).unwrap(),
    ] {
        let (_, stream) = crate::download_stream::authenticated_range::serialized_stream(
            &store,
            &terms[0].hash,
            range,
            None,
            ExecutionPools::default_sizes().blocking_io,
        )
        .await
        .unwrap()
        .unwrap();
        let chunks = stream.try_collect::<Vec<_>>().await.unwrap();
        let observed = chunks.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(
            observed,
            serialized[range.start() as usize..=range.end_inclusive() as usize]
        );
    }
    let serialized_range = ByteRange::new(0, 4095).unwrap();
    let pool = crate::admission::BoundedPool::new(std::num::NonZeroUsize::MIN);
    let before = proxy.get_requests();
    let before_bytes = proxy.get_response_bytes();
    let (observed_length, mut response) =
        crate::download_stream::authenticated_range::serialized_stream(
            &store,
            &terms[0].hash,
            serialized_range,
            None,
            pool.clone(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed_length, serialized_length);
    let response_bytes = response.next().await.unwrap().unwrap();
    assert_eq!(response_bytes.as_ref(), &serialized[..4096]);
    assert_eq!(
        pool.available_buffer_permits(),
        0,
        "response retains buffer admission"
    );
    let serialized_gets = proxy.get_requests() - before;
    let serialized_backend_bytes = proxy.get_response_bytes() - before_bytes;
    assert!(
        serialized_gets >= 3,
        "suffix, footer, and selected member GETs"
    );
    assert!(
        u64::try_from(serialized_backend_bytes).unwrap() < serialized_length / 10,
        "4 KiB serialized range read {serialized_backend_bytes} of {serialized_length} source bytes"
    );
    eprintln!(
        "native serialized range bytes=4096 source_bytes={serialized_length} backend_bytes={serialized_backend_bytes} gets={serialized_gets}"
    );
    drop(response);
    assert_eq!(
        pool.available_buffer_permits(),
        0,
        "transport bytes still own admission"
    );
    drop(response_bytes);
    assert_eq!(pool.available_buffer_permits(), 1);

    // A provider 412 on the snapshot-bound footer GET must fail before a
    // response stream is returned; a later read should work after the one-shot
    // fault has been consumed.
    proxy.arm();
    let before = proxy.get_requests();
    assert!(
        crate::download_stream::authenticated_range::serialized_stream(
            &store,
            &terms[0].hash,
            serialized_range,
            None,
            pool.clone(),
        )
        .await
        .is_err()
    );
    assert!(proxy.fault_was_injected());
    assert_eq!(pool.available_buffer_permits(), 1);
    assert_eq!(
        proxy.get_requests() - before,
        2,
        "suffix GET and one conditional footer GET; no selected member GET after 412"
    );
    let (_, stream) = crate::download_stream::authenticated_range::serialized_stream(
        &store,
        &terms[0].hash,
        serialized_range,
        None,
        pool.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    let recovered = stream.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(recovered.concat(), serialized[..4096]);
    drop(recovered);
    assert_eq!(pool.available_buffer_permits(), 1);

    let scope = RepositoryScope::new(
        RepositoryProvider::GitHub,
        "range-layout",
        "repo",
        Some("main"),
    )
    .unwrap();
    let mut alternative_record = file(terms.clone());
    alternative_record.repository_scope = Some(scope.clone());
    alternative_record.chunks[0].packed_end -= 1;
    let coverage = crate::record_store::XorbRangeCoverage::from_file_record_members(
        &alternative_record,
        &scope,
        &terms[0].hash,
    )
    .unwrap();
    assert!(coverage.covers(serialized_range));
    let before = proxy.get_requests();
    assert!(
        crate::download_stream::authenticated_range::serialized_stream(
            &store,
            &terms[0].hash,
            serialized_range,
            Some(&coverage),
            pool.clone(),
        )
        .await
        .is_err()
    );
    assert_eq!(
        proxy.get_requests() - before,
        2,
        "layout mismatch may read suffix/footer but cannot fetch the selected member"
    );

    // A tiny range still requires fetching its complete intersecting member.
    // The opt-in planner must cap those provider bytes before issuing that GET.
    let mut large_seed = 4_982_731_u64;
    let oversized_payload: Vec<u8> = (0..8 * 1024 * 1024)
        .map(|_| {
            large_seed ^= large_seed << 13;
            large_seed ^= large_seed >> 7;
            large_seed ^= large_seed << 17;
            large_seed as u8
        })
        .collect();
    let oversized_terms = container(&store, &[&oversized_payload]).await;
    let oversized_member_bytes = oversized_terms[0].packed_end - oversized_terms[0].packed_start;
    assert!(oversized_member_bytes > 8 * 1024 * 1024);
    let oversized_key = crate::xet_adapter::xorb_object_key(&oversized_terms[0].hash).unwrap();
    let oversized_length = store.metadata(&oversized_key).unwrap().unwrap().length();
    let before = proxy.get_requests();
    let before_bytes = proxy.get_response_bytes();
    assert!(
        crate::download_stream::authenticated_range::serialized_stream(
            &store,
            &oversized_terms[0].hash,
            ByteRange::new(0, 4095).unwrap(),
            None,
            pool.clone(),
        )
        .await
        .unwrap()
        .is_none(),
        "planner falls back when the complete selected member exceeds its packed-byte budget"
    );
    assert_eq!(
        proxy.get_requests() - before,
        2,
        "budget rejection must happen before the selected member GET"
    );
    assert!(
        proxy.get_response_bytes() - before_bytes < oversized_member_bytes as usize,
        "only suffix/footer bytes are read before strict fallback"
    );
    assert!(oversized_length > oversized_member_bytes);
    eprintln!(
        "oversized member fallback decoded_bytes={} packed_member_bytes={oversized_member_bytes} gets=2",
        oversized_payload.len()
    );

    qualify_idle_reader(&store, &record, &payload[..4096]).await;
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
    let (observed_length, stream) = crate::download_stream::authenticated_range::serialized_stream(
        &store,
        &terms[0].hash,
        ByteRange::new(0, 4095).unwrap(),
        None,
        pool.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(observed_length, length);
    let chunks = stream.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        chunks.into_iter().flatten().collect::<Vec<_>>(),
        serialized[..4096]
    );
    assert!(
        crate::download_stream::authenticated_range::serialized_stream(
            &store,
            &terms[0].hash,
            ByteRange::new(tail as u64, tail as u64).unwrap(),
            None,
            pool.clone(),
        )
        .await
        .is_err()
    );
    assert!(
        crate::download_stream::prepare_xorb_byte_range(
            &store,
            &key,
            &terms[0].hash,
            Some(length),
            pool.clone(),
        )
        .await
        .is_err()
    );
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
        crate::download_stream::authenticated_range::serialized_stream(
            &store,
            &terms[0].hash,
            ByteRange::new(0, 7).unwrap(),
            None,
            pool,
        )
        .await
        .is_err()
    );
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

async fn qualify_idle_reader(store: &ServerObjectStore, record: &FileRecord, expected: &[u8]) {
    use crate::admission::{BoundedPool, blocking_work};
    use futures_util::StreamExt;
    use std::{num::NonZeroUsize, time::Duration};
    let pool = BoundedPool::new(NonZeroUsize::MIN);
    let mut response = crate::download_stream::authenticated_range::file_stream(
        store.clone(),
        record,
        ByteRange::new(0, 4095).unwrap(),
        pool.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    let first = response.next().await.unwrap().unwrap();
    assert_eq!(first.as_ref(), expected);
    assert_eq!(pool.available_buffer_permits(), 0);
    tokio::time::timeout(Duration::from_secs(5), blocking_work(&pool, || Ok(())))
        .await
        .expect("a paused authenticated response must not occupy execution")
        .unwrap();
    drop(response);
    assert_eq!(
        pool.available_buffer_permits(),
        0,
        "transport still owns bytes"
    );
    let retained = first.slice(0..1);
    drop(first);
    assert_eq!(
        pool.available_buffer_permits(),
        0,
        "slices retain actual admission"
    );
    assert_eq!(retained.as_ref(), &expected[..1]);
    drop(retained);
    assert_eq!(pool.available_buffer_permits(), 1);
}
