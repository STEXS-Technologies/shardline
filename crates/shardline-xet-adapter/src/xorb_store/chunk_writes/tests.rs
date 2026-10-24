//! Observe actual conditional HTTP writes, including blocked and rejected work.
use super::*;
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri},
    response::IntoResponse,
};
use shardline_protocol::SecretString;
use shardline_storage::S3ObjectStoreConfig;
use shardline_xet_core::xorb_object::{
    Chunk, CompressionScheme, RawXorbData, SerializedXorbObject,
};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, Semaphore};

#[derive(Default)]
struct Counts {
    active: usize,
    active_bytes: usize,
    peak: usize,
    peak_bytes: usize,
    containers: usize,
}

struct Provider {
    counts: Mutex<Counts>,
    entered: Notify,
    release: Semaphore,
    reject: bool,
}

async fn put(
    State(state): State<Arc<Provider>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    assert_eq!(method, Method::PUT);
    assert_eq!(headers["if-none-match"], "*");
    {
        let mut counts = state.counts.lock().unwrap();
        counts.active = counts.active.checked_add(1).unwrap();
        counts.active_bytes = counts.active_bytes.checked_add(body.len()).unwrap();
        counts.peak = counts.peak.max(counts.active);
        counts.peak_bytes = counts.peak_bytes.max(counts.active_bytes);
        counts.containers = counts
            .containers
            .checked_add(usize::from(uri.path().contains("/xorbs/")))
            .unwrap();
    }
    state.entered.notify_one();
    state.release.acquire().await.unwrap().forget();
    {
        let mut counts = state.counts.lock().unwrap();
        counts.active = counts.active.checked_sub(1).unwrap();
        counts.active_bytes = counts.active_bytes.checked_sub(body.len()).unwrap();
    }
    let status = if state.reject {
        StatusCode::FORBIDDEN
    } else {
        StatusCode::OK
    };
    (status, [("etag", "\"fixture\"")], "")
}

struct Stop(tokio::task::JoinHandle<()>);
impl Drop for Stop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[test]
fn conditional_member_writes_bound_concurrency_bytes_and_failed_publication() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for (size, expected, reject, cancel) in [
            (64 * 1024_u32, 4, false, false),
            (128 * 1024, 2, false, false),
            (512 * 1024, 1, false, false),
            (64 * 1024, 4, true, false),
            (64 * 1024, 4, false, true),
        ] {
            let state = Arc::new(Provider {
                counts: Mutex::new(Counts::default()),
                entered: Notify::new(),
                release: Semaphore::new(0),
                reject,
            });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let app = Router::new().fallback(put).with_state(state.clone());
            let _stop = Stop(tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap()
            }));
            let objects = ServerObjectStore::s3(
                S3ObjectStoreConfig::new("fixture".to_owned(), "us-east-1".to_owned())
                    .with_endpoint(Some(endpoint))
                    .with_allow_http(true)
                    .with_credentials(
                        Some(SecretString::new("fixture".to_owned())),
                        Some(SecretString::new("fixture-secret".to_owned())),
                        None,
                    ),
            )
            .unwrap();
            let members: Vec<_> = (0..8_u8)
                .map(|seed| {
                    let data = vec![seed; usize::try_from(size).unwrap()];
                    Chunk {
                        hash: shardline_xet_core::merklehash::compute_data_hash(&data),
                        data: data.into(),
                    }
                })
                .collect();
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                RawXorbData::from_chunks(&members, vec![0]),
                CompressionScheme::None,
                true,
            )
            .unwrap();
            let task = tokio::spawn(async move {
                if reject || cancel {
                    super::super::store_uploaded_xorb(
                        &objects,
                        &serialized.hash.hex(),
                        &serialized.serialized_data,
                    )
                    .await
                    .map(|_| 0)
                } else {
                    let mut reader = Cursor::new(serialized.serialized_data);
                    let validated = validate_serialized_xorb(
                        &mut reader,
                        parse_xet_hash_hex(&serialized.hash.hex()).unwrap(),
                    )
                    .unwrap();
                    store(&objects, &mut reader, &validated).await
                }
            });
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let entered = state.entered.notified();
                    if state.counts.lock().unwrap().active == expected {
                        break;
                    }
                    entered.await;
                }
            })
            .await
            .expect("bounded writes must reach the HTTP provider concurrently");
            if cancel {
                task.abort();
                assert!(matches!(task.await, Err(error) if error.is_cancelled()));
                state.release.add_permits(16);
            } else {
                state.release.add_permits(16);
                let result = tokio::time::timeout(std::time::Duration::from_secs(5), task)
                    .await
                    .unwrap()
                    .unwrap();
                if reject {
                    assert!(result.is_err());
                } else {
                    assert_eq!(result.unwrap(), u64::from(size.checked_mul(8).unwrap()));
                }
            }
            let counts = state.counts.lock().unwrap();
            assert!(counts.peak <= expected);
            assert!(counts.peak_bytes <= (256 * 1024).max(usize::try_from(size).unwrap()));
            assert_eq!(
                counts.containers, 0,
                "failed or cancelled members cannot publish a container"
            );
        }
    });
}
