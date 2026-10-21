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
use shardline_storage::S3ObjectStoreConfig;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Provider {
    objects: BTreeMap<String, Bytes>,
    requests: Vec<(Method, String)>,
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
    let range = headers.get("range").map(|value| {
        let (start, end) = value
            .to_str()
            .unwrap()
            .strip_prefix("bytes=")
            .unwrap()
            .split_once('-')
            .unwrap();
        (
            start.parse::<usize>().unwrap(),
            end.parse::<usize>().unwrap(),
        )
    });
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
async fn packed_xorb_read_uses_one_full_object_get() {
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
    let pieces: [&[u8]; 2] = [b"packed xorb first", b"packed xorb second"];
    let raw_chunks = pieces
        .iter()
        .scan(0_u64, |offset, bytes| {
            let current = *offset;
            *offset = offset.checked_add(bytes.len() as u64)?;
            Some((bytes.to_vec(), current))
        })
        .collect::<Vec<_>>();
    let packed = crate::upload_ingest::xorb_packer::pack_chunks_into_xorb(&raw_chunks).unwrap();
    let key = crate::xet_adapter::xorb_object_key(&packed.xorb_hash_hex).unwrap();
    let serialized = packed.serialized;
    let total_length = u64::try_from(serialized.len()).unwrap();
    let xorb_hash = packed.xorb_hash_hex.clone();
    provider.lock().unwrap().objects.insert(
        format!("/fixture/{}", key.as_str()),
        Bytes::from(serialized.clone()),
    );
    let terms = packed
        .chunk_entries
        .iter()
        .map(|entry| shardline_index::FileChunkRecord {
            hash: packed.xorb_hash_hex.clone(),
            offset: entry.raw_offset,
            length: entry.raw_length,
            range_start: u64::from(entry.chunk_index),
            range_end: u64::from(entry.chunk_index.checked_add(1).unwrap()),
            packed_start: u64::from(entry.packed_offset),
            packed_end: u64::from(
                entry
                    .packed_offset
                    .checked_add(entry.packed_length)
                    .unwrap(),
            ),
        })
        .collect();
    provider.lock().unwrap().requests.clear();

    let bytes = super::collect(&store, &super::file(terms), None)
        .await
        .unwrap();
    assert_eq!(bytes, b"packed xorb firstpacked xorb second");
    let requests = provider.lock().unwrap().requests.clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests.first().unwrap().0, Method::GET);

    // A range covering only the first serialized byte still validates the
    // complete in-memory Xorb before returning a response stream.
    let mut corrupted = serialized;
    let last = corrupted.len().checked_sub(1).unwrap();
    corrupted[last] ^= 1;
    provider
        .lock()
        .unwrap()
        .objects
        .insert(format!("/fixture/{}", key.as_str()), Bytes::from(corrupted));
    provider.lock().unwrap().requests.clear();
    let result = crate::download_stream::validated_xorb_byte_range_stream(
        &store,
        &key,
        &xorb_hash,
        total_length,
        ByteRange::new(0, 0).unwrap(),
        crate::admission::ExecutionPools::default_sizes().blocking_io,
    )
    .await;
    assert!(
        result.is_err(),
        "corruption outside the selected range must fail"
    );
    let requests = provider.lock().unwrap().requests.clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests.first().unwrap().0, Method::GET);
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
            .filter(|(method, _)| *method == Method::GET)
            .count();
        let probes = full
            .iter()
            .filter(|(_, path)| path.contains("/xorbs/"))
            .count();
        // A legacy single-chunk record can be either individually stored or
        // Xorb-backed, including records labeled FixedChunkV1. Probe the Xorb
        // once, then read the individual chunk only when the Xorb is absent.
        let expected_gets = 2 * pieces.len();
        assert_eq!(gets, expected_gets);
        assert!(
            full.len() <= 2 * pieces.len(),
            "legacy reads exceed the one-Xorb-probe plus one-data-GET request budget"
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
                .filter(|(method, _)| *method == Method::GET)
                .count(),
            2
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
