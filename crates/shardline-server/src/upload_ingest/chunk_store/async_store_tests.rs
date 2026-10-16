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
