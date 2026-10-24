//! Real HTTP request counts for supported individual-chunk representations.
use crate::object_store::ServerObjectStore;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri},
    response::Response,
};
use futures_util::TryStreamExt;
use shardline_index::{FileChunkRecord, StorageRepresentation};
use shardline_protocol::{ByteRange, SecretString};
use shardline_storage::{ObjectStore, S3ObjectStoreConfig};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Provider {
    objects: BTreeMap<String, Bytes>,
    requests: Vec<(Method, String)>,
    deny_body: bool,
    transferred: usize,
}

async fn request(
    State(provider): State<Arc<Mutex<Provider>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let mut provider = provider.lock().unwrap();
    provider
        .requests
        .push((method.clone(), uri.path().to_owned()));
    let Some(bytes) = provider.objects.get(uri.path()) else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    };
    let length = bytes.len();
    if method == Method::HEAD {
        return Response::builder()
            .header("content-length", length)
            .header("etag", "\"fixture\"")
            .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
            .body(Body::empty())
            .unwrap();
    }
    assert_eq!(method, Method::GET);
    if provider.deny_body {
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::empty())
            .unwrap();
    }
    let range = headers.get("range").map(|value| {
        let (start, end) = value
            .to_str()
            .unwrap()
            .strip_prefix("bytes=")
            .unwrap()
            .split_once('-')
            .unwrap();
        if start.is_empty() {
            (
                length.saturating_sub(end.parse::<usize>().unwrap()),
                length.checked_sub(1).unwrap(),
            )
        } else {
            (
                start.parse::<usize>().unwrap(),
                end.parse::<usize>().unwrap(),
            )
        }
    });
    provider.transferred = provider
        .transferred
        .checked_add(range.map_or(length, |(start, end)| {
            end.checked_sub(start).unwrap().checked_add(1).unwrap()
        }))
        .unwrap();
    let bytes = provider.objects.get(uri.path()).unwrap();
    match range {
        Some((start, end)) => Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header("content-range", format!("bytes {start}-{end}/{length}"))
            .header(
                "content-length",
                end.checked_sub(start).unwrap().checked_add(1).unwrap(),
            )
            .header("etag", "\"fixture\"")
            .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
            .body(Body::from(bytes.slice(start..=end)))
            .unwrap(),
        None => Response::builder()
            .header("content-length", length)
            .header("etag", "\"fixture\"")
            .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
            .body(Body::from(bytes.clone()))
            .unwrap(),
    }
}

struct StopServer(tokio::task::JoinHandle<()>);
impl Drop for StopServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_packed_ranges_authenticate_members_without_fetching_unselected_payloads() {
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
    let storage = shardline_test_support::TempStorage::new();
    let local = ServerObjectStore::local(storage.path()).unwrap();
    let mut seed = 736489_u64;
    let payload: Vec<u8> = (0..1536 * 1024)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as u8
        })
        .collect();
    let chunks = super::container(&local, &payload.chunks(512 * 1024).collect::<Vec<_>>()).await;
    let record = super::file(chunks.clone());
    let key = crate::xet_adapter::xorb_object_key(&chunks[0].hash).unwrap();
    let length = local.metadata(&key).unwrap().unwrap().length();
    let encoded = local
        .read_range(&key, ByteRange::new(0, length - 1).unwrap())
        .unwrap();
    let path = format!("/fixture/{}", key.as_str());
    provider
        .lock()
        .unwrap()
        .objects
        .insert(path.clone(), Bytes::copy_from_slice(&encoded));
    assert_eq!(
        super::collect(&store, &record, None).await.unwrap(),
        payload
    );
    assert_eq!(provider.lock().unwrap().requests.len(), 1);
    {
        let mut state = provider.lock().unwrap();
        state.requests.clear();
        state.transferred = 0;
    }
    let range = Some(ByteRange::new(0, 4095).unwrap());
    assert_eq!(
        super::collect(&store, &record, range).await.unwrap(),
        payload[..4096]
    );
    {
        let state = provider.lock().unwrap();
        assert_eq!(
            state.requests.len(),
            3,
            "trailer, authenticated footer, selected member"
        );
        assert!(
            state
                .requests
                .iter()
                .all(|(method, _)| *method == Method::GET)
        );
        assert!(state.transferred < encoded.len() / 2);
    }
    // Unselected corruption is outside this range proof; a full scrub still fails.
    #[allow(deprecated)] // Exercise the older footer bytes, without rewriting storage.
    {
        use shardline_xet_core::xorb_object::{XorbObject, XorbObjectInfoV0};
        let parsed = XorbObject::deserialize(&mut std::io::Cursor::new(&encoded)).unwrap();
        let mut legacy = XorbObjectInfoV0::default();
        legacy.xorb_hash = parsed.info.xorb_hash;
        legacy.num_chunks = u32::try_from(parsed.info.num_chunks).unwrap();
        legacy.chunk_hashes = parsed.info.chunk_hashes;
        legacy.chunk_boundary_offsets = parsed
            .info
            .chunk_boundary_offsets
            .iter()
            .map(|value| u32::try_from(*value).unwrap())
            .collect();
        let mut old_footer = Vec::new();
        legacy.serialize(&mut old_footer).unwrap();
        let footer_length = old_footer.len() as u64;
        old_footer.extend_from_slice(&footer_length.to_le_bytes());
        let payload_end = usize::try_from(chunks.last().unwrap().packed_end).unwrap();
        let mut old_bytes = encoded[..payload_end].to_vec();
        old_bytes.extend_from_slice(&old_footer);
        {
            let mut state = provider.lock().unwrap();
            state
                .objects
                .insert(path.clone(), Bytes::copy_from_slice(&old_bytes));
            state.requests.clear();
        }
        assert_eq!(
            super::collect(&store, &record, range).await.unwrap(),
            payload[..4096]
        );
        assert_eq!(
            provider.lock().unwrap().requests.len(),
            3,
            "legacy footer discovery falls back to a complete-container GET"
        );
        assert_eq!(
            super::collect(&store, &record, None).await.unwrap(),
            payload
        );
        old_bytes[usize::try_from(chunks[2].packed_end - 1).unwrap()] ^= 0x40;
        provider
            .lock()
            .unwrap()
            .objects
            .insert(path.clone(), Bytes::from(old_bytes));
        assert!(
            super::collect(&store, &record, range).await.is_err(),
            "legacy fallback still validates unselected content"
        );
    }
    let mut damaged = encoded.clone();
    damaged[usize::try_from(chunks[2].packed_end - 1).unwrap()] ^= 0x40;
    provider
        .lock()
        .unwrap()
        .objects
        .insert(path.clone(), Bytes::from(damaged));
    assert_eq!(
        super::collect(&store, &record, range).await.unwrap(),
        payload[..4096]
    );
    assert!(super::collect(&store, &record, None).await.is_err());
    // Selected corruption is rejected before stream construction succeeds.
    let mut damaged = encoded.clone();
    damaged[usize::try_from(chunks[0].packed_end - 1).unwrap()] ^= 0x40;
    provider
        .lock()
        .unwrap()
        .objects
        .insert(path.clone(), Bytes::from(damaged));
    assert!(
        crate::download_stream::file_record_byte_stream(
            store.clone(),
            record.clone(),
            range,
            crate::admission::ExecutionPools::default_sizes().blocking_io
        )
        .await
        .is_err()
    );
    provider
        .lock()
        .unwrap()
        .objects
        .insert(path.clone(), Bytes::copy_from_slice(&encoded));
    // Logical terms can reorder and repeat members without changing their commitments.
    let reordered = super::file(vec![
        chunks[2].clone(),
        chunks[0].clone(),
        chunks[2].clone(),
    ]);
    assert_eq!(
        super::collect(&store, &reordered, range).await.unwrap(),
        payload[1024 * 1024..1024 * 1024 + 4096]
    );
    let spanning = Some(ByteRange::new(400 * 1024, 600 * 1024 - 1).unwrap());
    assert_eq!(
        super::collect(&store, &record, spanning).await.unwrap(),
        payload[400 * 1024..600 * 1024]
    );
    // A later corrupt member terminates the response, never a complete success body.
    let mut damaged = encoded.clone();
    damaged[usize::try_from(chunks[1].packed_end - 1).unwrap()] ^= 0x40;
    provider
        .lock()
        .unwrap()
        .objects
        .insert(path.clone(), Bytes::from(damaged));
    let stream = crate::download_stream::file_record_byte_stream(
        store.clone(),
        record.clone(),
        spanning,
        crate::admission::ExecutionPools::default_sizes().blocking_io,
    )
    .await
    .unwrap();
    assert!(stream.try_collect::<Vec<_>>().await.is_err());
    // An attacker-controlled trailer cannot force an unbounded footer fetch.
    let mut damaged = encoded.clone();
    let end = damaged.len();
    damaged[end - 8..].copy_from_slice(&u64::MAX.to_le_bytes());
    {
        let mut state = provider.lock().unwrap();
        state.objects.insert(path.clone(), Bytes::from(damaged));
        state.requests.clear();
    }
    assert!(super::collect(&store, &record, range).await.is_err());
    assert_eq!(provider.lock().unwrap().requests.len(), 1);
    provider
        .lock()
        .unwrap()
        .objects
        .insert(path.clone(), Bytes::copy_from_slice(&encoded));
    let mut wrong_plan = record.clone();
    wrong_plan.chunks[0].packed_end -= 1;
    assert!(super::collect(&store, &wrong_plan, range).await.is_err());
    provider
        .lock()
        .unwrap()
        .objects
        .insert(path, Bytes::copy_from_slice(&encoded));
    let pool = crate::admission::BoundedPool::new(std::num::NonZeroUsize::new(1).unwrap());
    let stream =
        crate::download_stream::file_record_byte_stream(store, record, spanning, pool.clone())
            .await
            .unwrap();
    assert!(
        pool.try_acquire().is_none(),
        "slow-reader buffers remain admitted"
    );
    drop(stream);
    assert!(
        pool.try_acquire().is_some(),
        "cancellation releases admission"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn packed_reads_use_one_get_and_validate_unselected_content() {
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
    let storage = shardline_test_support::TempStorage::new();
    let local = ServerObjectStore::local(storage.path()).unwrap();
    let chunks = super::container(&local, &[b"valid prefix", b"unselected tail"]).await;
    let key = crate::xet_adapter::xorb_object_key(&chunks[0].hash).unwrap();
    use shardline_storage::ObjectStore;
    let length = local.metadata(&key).unwrap().unwrap().length();
    let bytes = local
        .read_range(&key, ByteRange::new(0, length - 1).unwrap())
        .unwrap();
    let path = format!("/fixture/{}", key.as_str());
    provider
        .lock()
        .unwrap()
        .objects
        .insert(path.clone(), Bytes::copy_from_slice(bytes.as_ref()));
    let record = super::file(chunks.clone());
    for range in [None, Some(ByteRange::new(0, 4).unwrap())] {
        provider.lock().unwrap().requests.clear();
        let actual = super::collect(&store, &record, range).await.unwrap();
        assert_eq!(
            actual,
            if range.is_some() {
                b"valid".as_slice()
            } else {
                b"valid prefixunselected tail".as_slice()
            }
        );
        assert_eq!(
            provider.lock().unwrap().requests,
            [(Method::GET, path.clone())]
        );
    }
    // The requested prefix is valid, but corruption elsewhere must still fail
    // while constructing the stream, before a successful HTTP response exists.
    let mut corrupted = bytes.clone();
    corrupted[usize::try_from(chunks[1].packed_end - 1).unwrap()] ^= 0x40;
    provider
        .lock()
        .unwrap()
        .objects
        .insert(path.clone(), Bytes::from(corrupted));
    assert!(
        crate::download_stream::file_record_byte_stream(
            store.clone(),
            record.clone(),
            Some(ByteRange::new(0, 4).unwrap()),
            crate::admission::ExecutionPools::default_sizes().blocking_io,
        )
        .await
        .is_err()
    );
    {
        let mut state = provider.lock().unwrap();
        state
            .objects
            .insert(path.clone(), Bytes::copy_from_slice(bytes.as_ref()));
        state.deny_body = true;
        state.requests.clear();
    }
    assert!(
        crate::download_stream::file_record_byte_stream(
            store,
            record,
            None,
            crate::admission::ExecutionPools::default_sizes().blocking_io,
        )
        .await
        .is_err()
    );
    assert_eq!(
        provider.lock().unwrap().requests,
        // A failed GET now propagates directly, before response construction;
        // it needs neither a HEAD probe nor a fallback to loose chunks.
        [(Method::GET, path)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_chunk_reads_report_bounded_s3_request_cost() {
    for compressed in [false, true] {
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
        let pieces: [&[u8]; 2] = [b"first legacy chunk", b"second legacy chunk"];
        let chunks = pieces
            .iter()
            .map(|bytes| {
                let hash =
                    shardline_index::xet_hash_hex_string(crate::local_backend::chunk_hash(bytes));
                let stored = if compressed {
                    lz4_flex::compress_prepend_size(bytes)
                } else {
                    bytes.to_vec()
                };
                let packed_end = stored.len() as u64;
                let key = crate::chunk_store::chunk_object_key(&hash).unwrap();
                provider
                    .lock()
                    .unwrap()
                    .objects
                    .insert(format!("/fixture/{}", key.as_str()), Bytes::from(stored));
                FileChunkRecord {
                    hash,
                    offset: 0,
                    length: bytes.len() as u64,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end,
                }
            })
            .collect();
        let mut record = super::file(chunks);
        if !compressed {
            record.storage_repr = StorageRepresentation::FixedChunkV1;
        }
        assert_eq!(
            super::collect(&store, &record, None).await.unwrap(),
            pieces.concat()
        );
        let full = provider.lock().unwrap().requests.clone();
        let gets = full
            .iter()
            .filter(|(method, path)| *method == Method::GET && !path.contains("/xorbs/"))
            .count();
        let probes = full
            .iter()
            .filter(|(_, path)| path.contains("/xorbs/"))
            .count();
        assert_eq!(gets, pieces.len());
        assert!(
            full.len() <= 2 * pieces.len(),
            "legacy reads exceed the released HEAD-plus-GET request budget"
        );
        eprintln!(
            "legacy compressed={compressed}: {} HTTP requests, {gets} data GETs, {probes} xorb probes",
            full.len()
        );
        provider.lock().unwrap().requests.clear();
        assert_eq!(
            super::collect(&store, &record, Some(ByteRange::new(0, 3).unwrap()))
                .await
                .unwrap(),
            b"firs"
        );
        let selected = provider.lock().unwrap().requests.clone();
        assert!(selected.len() <= 2);
        assert_eq!(
            selected
                .iter()
                .filter(|(method, path)| *method == Method::GET && !path.contains("/xorbs/"))
                .count(),
            1
        );
        let second_key =
            crate::chunk_store::chunk_object_key(&record.chunks.get(1).unwrap().hash).unwrap();
        assert!(
            selected
                .iter()
                .all(|(_, path)| !path.ends_with(second_key.as_str()))
        );
        eprintln!(
            "legacy compressed={compressed} selected range: {} HTTP requests",
            selected.len()
        );
        let first_key =
            crate::chunk_store::chunk_object_key(&record.chunks.first().unwrap().hash).unwrap();
        let path = format!("/fixture/{}", first_key.as_str());
        let stored = provider.lock().unwrap().objects.get(&path).unwrap().clone();
        provider.lock().unwrap().requests.clear();
        let partial = crate::download_stream::object_byte_range_stream(
            store.clone(),
            first_key.clone(),
            stored.len() as u64,
            ByteRange::new(0, 3).unwrap(),
        )
        .await
        .unwrap()
        .try_fold(Vec::new(), |mut bytes, part| async move {
            bytes.extend_from_slice(&part);
            Ok(bytes)
        })
        .await
        .unwrap();
        assert_eq!(partial.as_slice(), stored.get(..4).unwrap());
        let partial_requests = provider.lock().unwrap().requests.clone();
        assert_eq!(partial_requests.len(), 1);
        assert_eq!(partial_requests.first().unwrap().0, Method::GET);
        {
            let mut provider = provider.lock().unwrap();
            let mut corrupted = provider.objects.get(&path).unwrap().to_vec();
            corrupted.push(0);
            provider
                .objects
                .insert(path.clone(), Bytes::from(corrupted));
        }
        assert!(matches!(
            super::collect(&store, &record, None).await,
            Err(crate::ServerError::ObjectStore(
                crate::error::ObjectStoreError::StoredLengthMismatch
            ))
        ));
        provider.lock().unwrap().objects.remove(&path);
        // A caller must receive the initial storage failure before it can
        // construct successful HTTP headers around the returned stream.
        assert!(matches!(
            crate::download_stream::file_record_byte_stream(
                store.clone(),
                record.clone(),
                None,
                crate::admission::ExecutionPools::default_sizes().blocking_io,
            )
            .await,
            Err(crate::ServerError::NotFound)
        ));
        assert!(matches!(
            super::collect(&store, &record, None).await,
            Err(crate::ServerError::NotFound)
        ));
    }
}
