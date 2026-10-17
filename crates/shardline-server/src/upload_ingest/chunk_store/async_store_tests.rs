use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, Bytes},
    http::{Method, Response, StatusCode},
};
use shardline_protocol::SecretString;
use shardline_storage::S3ObjectStoreConfig;
use tokio::sync::{Notify, mpsc};

use super::{ChunkBuffer, put_if_absent_pooled_chunk_buffer};
use crate::object_store::ServerObjectStore;

#[tokio::test]
async fn only_fully_verified_existing_s3_chunks_can_skip_packing_read_back() {
    use crate::upload_ingest::durable_chunks::{DurableUploadChunks, SharedReadCache};
    use axum::http::HeaderMap;
    use shardline_index::FileChunkRecord;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let raw = Bytes::from(vec![b'a'; 4096]);
    let compressed = Bytes::from(lz4_flex::compress_prepend_size(&raw));
    let corrupt = Bytes::from(lz4_flex::compress_prepend_size(&vec![b'b'; 4096]));
    let mode = Arc::new(AtomicUsize::new(0));
    let ranges = Arc::new(AtomicUsize::new(0));
    let observed_mode = Arc::clone(&mode);
    let observed_ranges = Arc::clone(&ranges);
    let stored = compressed.clone();
    let app = Router::new().fallback(move |method: Method, headers: HeaderMap| {
        let current = observed_mode.load(Ordering::SeqCst);
        let bytes = if current == 2 {
            corrupt.clone()
        } else {
            stored.clone()
        };
        let range = headers.contains_key("range");
        if range {
            observed_ranges.fetch_add(1, Ordering::SeqCst);
        }
        async move {
            if method == Method::PUT {
                return Response::builder()
                    .status(if current == 0 {
                        StatusCode::OK
                    } else {
                        StatusCode::PRECONDITION_FAILED
                    })
                    .header("etag", "\"fixture\"")
                    .body(Body::empty())
                    .unwrap();
            }
            let mut response = Response::builder()
                .status(if range {
                    StatusCode::PARTIAL_CONTENT
                } else {
                    StatusCode::OK
                })
                .header("content-length", bytes.len())
                .header("etag", "\"fixture\"")
                .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT");
            if range {
                response = response.header(
                    "content-range",
                    format!("bytes 0-{}/{}", bytes.len() - 1, bytes.len()),
                );
            }
            response.body(Body::from(bytes)).unwrap()
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = S3ObjectStoreConfig::new("fixture".to_owned(), "us-east-1".to_owned())
        .with_endpoint(Some(endpoint))
        .with_allow_http(true)
        .with_credentials(
            Some(SecretString::from_secret("fixture-access")),
            Some(SecretString::from_secret("fixture-secret")),
            None,
        );
    let store = ServerObjectStore::s3(config.clone()).unwrap();
    let records = [FileChunkRecord {
        hash: shardline_index::xet_hash_hex_string(crate::local_backend::chunk_hash(&raw)),
        offset: 0,
        length: raw.len() as u64,
        range_start: 0,
        range_end: 1,
        packed_start: 0,
        packed_end: compressed.len() as u64,
    }];
    let cache = SharedReadCache::default();
    let (fresh, _) = super::put_if_absent_with_verified_cache(
        &store,
        ChunkBuffer::Pooled(raw.clone()),
        Some(&cache),
    )
    .await
    .unwrap();
    assert!(fresh.inserted);
    let saved = std::mem::take(&mut *cache.lock().unwrap());
    let mut reader = DurableUploadChunks::with_cache(store.clone(), 1, saved);
    assert_eq!(reader.next(&records).await.unwrap().unwrap().0, raw);
    assert_eq!(ranges.load(Ordering::SeqCst), 1);

    mode.store(1, Ordering::SeqCst);
    // A second server starts without the first server's existence hints.
    let store = ServerObjectStore::s3(config).unwrap();
    let (existing, _) = super::put_if_absent_with_verified_cache(
        &store,
        ChunkBuffer::Pooled(raw.clone()),
        Some(&cache),
    )
    .await
    .unwrap();
    assert!(!existing.inserted);
    let saved = std::mem::take(&mut *cache.lock().unwrap());
    // A cache miss would fail: the blackhole has nothing to read back.
    let mut reader = DurableUploadChunks::with_cache(ServerObjectStore::blackhole(), 1, saved);
    assert_eq!(reader.next(&records).await.unwrap().unwrap().0, raw);

    mode.store(2, Ordering::SeqCst);
    assert!(
        super::put_if_absent_with_verified_cache(&store, ChunkBuffer::Pooled(raw), Some(&cache),)
            .await
            .is_err()
    );
    let saved = std::mem::take(&mut *cache.lock().unwrap());
    let mut reader = DurableUploadChunks::with_cache(ServerObjectStore::blackhole(), 1, saved);
    assert!(reader.next(&records).await.is_err());
    server.abort();
}

#[tokio::test]
async fn cancelled_s3_chunk_write_does_not_continue_existing_object_validation() {
    let raw = Bytes::from_static(b"cancelled chunk");
    let compressed = Bytes::from(lz4_flex::compress_prepend_size(&raw));
    let release = Arc::new(Notify::new());
    let (requests, mut received) = mpsc::unbounded_channel();
    let fixture_release = Arc::clone(&release);
    let app = Router::new().fallback(move |method: Method| {
        let requests = requests.clone();
        let release = Arc::clone(&fixture_release);
        let compressed = compressed.clone();
        async move {
            requests.send(method.clone()).unwrap();
            if method == Method::PUT {
                release.notified().await;
                Response::builder()
                    .status(StatusCode::PRECONDITION_FAILED)
                    .body(Body::empty())
                    .unwrap()
            } else {
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-length", compressed.len())
                    .header("etag", "\"fixture\"")
                    .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
                    .body(if method == Method::HEAD {
                        Body::empty()
                    } else {
                        Body::from(compressed)
                    })
                    .unwrap()
            }
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let store = ServerObjectStore::s3(
        S3ObjectStoreConfig::new("fixture".to_owned(), "us-east-1".to_owned())
            .with_endpoint(Some(endpoint))
            .with_allow_http(true)
            .with_credentials(
                Some(SecretString::from_secret("fixture-access")),
                Some(SecretString::from_secret("fixture-secret")),
                None,
            ),
    )
    .unwrap();
    let upload = tokio::spawn(async move {
        put_if_absent_pooled_chunk_buffer(&store, ChunkBuffer::Pooled(raw)).await
    });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), received.recv())
            .await
            .unwrap()
            .unwrap(),
        Method::PUT
    );
    upload.abort();
    assert!(matches!(upload.await, Err(error) if error.is_cancelled()));
    // A detached blocking operation would continue into HEAD/GET after the
    // provider reports a duplicate, despite the request no longer owning it.
    release.notify_one();
    let continued = tokio::time::timeout(Duration::from_millis(200), received.recv()).await;
    server.abort();
    assert!(
        continued.is_err(),
        "cancelled write continued provider validation"
    );
}
