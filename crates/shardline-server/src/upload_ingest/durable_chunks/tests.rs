use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{StatusCode, Uri},
    response::Response,
};
use shardline_protocol::SecretString;
use shardline_storage::S3ObjectStoreConfig;
use tokio::sync::Barrier;

use super::*;

type Fixture = (Arc<HashMap<String, Bytes>>, Arc<Barrier>);

async fn paired_read(State((objects, barrier)): State<Fixture>, uri: Uri) -> Response {
    let bytes = objects.get(uri.path()).unwrap().clone();
    // Neither response is released until both reads have reached the provider.
    // Serial read-back cannot pass this gate.
    barrier.wait().await;
    Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header("content-length", bytes.len())
        .header(
            "content-range",
            format!(
                "bytes 0-{}/{}",
                bytes.len().checked_sub(1).unwrap(),
                bytes.len()
            ),
        )
        .header("etag", "\"fixture\"")
        .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
        .body(Body::from(bytes))
        .unwrap()
}

#[tokio::test]
async fn durable_reads_overlap_at_the_provider_and_preserve_record_order() {
    let raw: [&[u8]; 2] = [b"first durable chunk", b"second durable chunk"];
    let mut objects = HashMap::new();
    let mut records = Vec::new();
    let mut offset = 0;
    for bytes in raw {
        let hash = xet_hash_hex_string(crate::local_backend::chunk_hash(bytes));
        let key = crate::chunk_store::chunk_object_key(&hash).unwrap();
        let compressed = lz4_flex::compress_prepend_size(bytes);
        let packed_end = compressed.len() as u64;
        objects.insert(
            format!("/fixture/{}", key.as_str()),
            Bytes::from(compressed),
        );
        records.push(FileChunkRecord {
            hash,
            offset,
            length: bytes.len() as u64,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end,
        });
        offset += bytes.len() as u64;
    }
    let app = Router::new()
        .fallback(paired_read)
        .with_state((Arc::new(objects), Arc::new(Barrier::new(2))));
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
    let mut reader = DurableUploadChunks::new(store, 2);
    tokio::time::timeout(Duration::from_secs(3), async {
        let first = reader.next(&records).await.unwrap().unwrap();
        let second = reader.next(&records).await.unwrap().unwrap();
        assert_eq!(first, (raw[0].to_vec(), 0));
        assert_eq!(second, (raw[1].to_vec(), raw[0].len() as u64));
        assert!(reader.next(&records).await.unwrap().is_none());
    })
    .await
    .expect("durable reads did not overlap");
    server.abort();
}

#[test]
fn corrupt_size_prefix_is_rejected_before_decompression_allocation() {
    let compressed = u32::MAX.to_le_bytes();
    assert!(matches!(
        decode_and_verify_durable_chunk(&compressed, "unused", 16),
        Err(ServerError::ObjectStore(
            ObjectStoreError::StoredLengthMismatch
        ))
    ));
}

#[tokio::test]
async fn large_durable_chunks_obey_the_read_ahead_byte_budget() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    // One oversized chunk must still work. No second chunk may overlap it,
    // and two 3 MiB chunks must not bypass the 4 MiB read-ahead budget.
    let raw = [
        vec![0; 5 * 1024 * 1024],
        vec![1; 3 * 1024 * 1024],
        vec![2; 3 * 1024 * 1024],
    ];
    let mut objects = HashMap::new();
    let mut records = Vec::new();
    let mut offset = 0;
    for bytes in &raw {
        let hash = xet_hash_hex_string(crate::local_backend::chunk_hash(bytes));
        let key = crate::chunk_store::chunk_object_key(&hash).unwrap();
        let compressed = lz4_flex::compress_prepend_size(bytes);
        records.push(FileChunkRecord {
            hash,
            offset,
            length: bytes.len() as u64,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: compressed.len() as u64,
        });
        objects.insert(
            format!("/fixture/{}", key.as_str()),
            Bytes::from(compressed),
        );
        offset += bytes.len() as u64;
    }
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let observed_peak = Arc::clone(&peak);
    let objects = Arc::new(objects);
    let app = Router::new().fallback(move |uri: Uri| {
        let bytes = objects.get(uri.path()).unwrap().clone();
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);
        async move {
            let count = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(count, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(25)).await;
            active.fetch_sub(1, Ordering::SeqCst);
            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header("content-length", bytes.len())
                .header(
                    "content-range",
                    format!("bytes 0-{}/{}", bytes.len() - 1, bytes.len()),
                )
                .header("etag", "\"fixture\"")
                .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
                .body(Body::from(bytes))
                .unwrap()
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
    let mut reader = DurableUploadChunks::new(store, 32);
    tokio::time::timeout(Duration::from_secs(3), async {
        for (record, expected) in records.iter().zip(&raw) {
            let (actual, offset) = reader.next(&records).await.unwrap().unwrap();
            assert_eq!(&actual, expected);
            assert_eq!(offset, record.offset);
        }
        assert!(reader.next(&records).await.unwrap().is_none());
    })
    .await
    .unwrap();
    server.abort();
    assert_eq!(observed_peak.load(Ordering::SeqCst), 1);
}
