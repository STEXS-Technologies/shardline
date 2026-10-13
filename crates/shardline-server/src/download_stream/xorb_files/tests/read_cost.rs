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
                .filter(|(method, _)| *method == Method::GET)
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
        assert!(matches!(
            super::collect(&store, &record, None).await,
            Err(crate::ServerError::NotFound)
        ));
    }
}
