use std::{sync::mpsc, time::Duration};

use axum::{Router, body::Bytes, http::HeaderMap, routing::put};
use shardline_protocol::SecretString;
use shardline_storage::S3ObjectStoreConfig;

use super::{pack_chunks_into_xorb, store_xorb};
use crate::object_store::ServerObjectStore;

#[test]
fn s3_container_write_yields_to_the_callers_runtime() {
    // The HTTP fixture shares the upload's single-thread runtime. A synchronous
    // bridge would prevent it responding. Use an independent watchdog because
    // that same mistake would also prevent a Tokio timeout from being polled.
    let (finished, completion) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let packed = pack_chunks_into_xorb(&[(b"cooperative write".to_vec(), 0)]).unwrap();
            let expected = Bytes::copy_from_slice(&packed.serialized);
            let app =
                Router::new().fallback_service(put(move |headers: HeaderMap, body: Bytes| {
                    let expected = expected.clone();
                    async move {
                        assert_eq!(headers.get("if-none-match").unwrap(), "*");
                        assert_eq!(body, expected);
                        ([("etag", "\"fixture\"")], "")
                    }
                }));
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
            let inserted = store_xorb(&store, &packed.xorb_hash_hex, &packed.serialized)
                .await
                .unwrap();
            server.abort();
            finished.send(inserted).unwrap();
        });
    });
    assert!(
        completion.recv_timeout(Duration::from_secs(5)).unwrap(),
        "container write did not complete cooperatively"
    );
    worker.join().unwrap();
}
