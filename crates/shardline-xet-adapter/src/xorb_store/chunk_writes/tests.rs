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

#[tokio::test]
async fn member_persistence_does_not_read_the_source_once_per_frame() {
    struct ObservedReader {
        bytes: Cursor<Vec<u8>>,
        reads: usize,
    }
    impl Read for ObservedReader {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            self.reads += 1;
            self.bytes.read(output)
        }
    }
    impl Seek for ObservedReader {
        fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
            self.bytes.seek(position)
        }
    }
    let members: Vec<_> = (0..128_u8)
        .map(|byte| {
            let data = vec![byte; 1024];
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
    let validated = validate_serialized_xorb(
        &mut Cursor::new(&serialized.serialized_data),
        parse_xet_hash_hex(&serialized.hash.hex()).unwrap(),
    )
    .unwrap();
    let mut reader = ObservedReader {
        bytes: Cursor::new(serialized.serialized_data),
        reads: 0,
    };
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path()).unwrap();
    assert_eq!(
        store(&objects, &mut reader, &validated).await.unwrap(),
        128 * 1024
    );
    eprintln!(
        "source reads={} for {} members",
        reader.reads,
        members.len()
    );
    assert!(
        reader.reads < members.len(),
        "member persistence performed {} source reads for {} members",
        reader.reads,
        members.len(),
    );
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
fn cancelled_queued_local_member_never_publishes() {
    use std::{
        future::Future,
        task::{Context, Waker},
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let data = vec![0x41; 4096];
        let member = Chunk {
            hash: shardline_xet_core::merklehash::compute_data_hash(&data),
            data: data.into(),
        };
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            RawXorbData::from_chunks(&[member], vec![0]),
            CompressionScheme::None,
            true,
        )
        .unwrap();
        let mut reader = Cursor::new(serialized.serialized_data);
        let validated = validate_serialized_xorb(
            &mut reader,
            parse_xet_hash_hex(&serialized.hash.hex()).unwrap(),
        )
        .unwrap();
        let root = tempfile::tempdir().unwrap();
        let objects = ServerObjectStore::local(root.path()).unwrap();
        let key =
            chunk_object_key_local(&xet_hash_hex_string(validated.chunks()[0].hash())).unwrap();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (finish, wait) = std::sync::mpsc::channel();
        let occupied = tokio::task::spawn_blocking(move || {
            entered.send(()).unwrap();
            wait.recv().unwrap();
        });
        started.await.unwrap();
        let admission = Arc::new(Semaphore::new(1));
        let permit = admission.clone().acquire_owned().await.unwrap();
        let resources = Arc::new(Mutex::new(permit));
        let mut queued = Box::pin(store_with_resources(
            &objects,
            &mut reader,
            &validated,
            resources,
        ));
        assert!(
            queued
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(admission.available_permits(), 0);
        drop(queued);
        finish.send(()).unwrap();
        occupied.await.unwrap();
        tokio::task::spawn_blocking(|| ()).await.unwrap();
        assert_eq!(admission.available_permits(), 1);
        assert!(
            shardline_storage::ObjectStore::metadata(&objects, &key)
                .unwrap()
                .is_none(),
            "cancelled queued member was published"
        );
        // Cancellation must not reserve an immutable identity permanently.
        assert_eq!(
            store(&objects, &mut reader, &validated).await.unwrap(),
            4096
        );
        let metadata = shardline_storage::ObjectStore::metadata(&objects, &key)
            .unwrap()
            .unwrap();
        assert_eq!(
            shardline_server_core::read_full_object(&objects, &key, metadata.length()).unwrap(),
            vec![0x41; 4096]
        );
    });
}

#[tokio::test]
async fn changed_member_payload_cannot_poison_its_prevalidated_key() {
    let serialize = |byte: u8| {
        let data = vec![byte; 4096];
        let member = Chunk {
            hash: shardline_xet_core::merklehash::compute_data_hash(&data),
            data: data.into(),
        };
        SerializedXorbObject::from_xorb_with_compression(
            RawXorbData::from_chunks(&[member], vec![0]),
            CompressionScheme::None,
            true,
        )
        .unwrap()
    };
    let original = serialize(0x41);
    let replacement = serialize(0x42);
    assert_eq!(
        original.serialized_data.len(),
        replacement.serialized_data.len()
    );
    let validated = validate_serialized_xorb(
        &mut Cursor::new(&original.serialized_data),
        parse_xet_hash_hex(&original.hash.hex()).unwrap(),
    )
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path()).unwrap();
    let result = store(
        &objects,
        &mut Cursor::new(replacement.serialized_data),
        &validated,
    )
    .await;
    let key = chunk_object_key_local(&xet_hash_hex_string(validated.chunks()[0].hash())).unwrap();
    assert!(
        shardline_storage::ObjectStore::metadata(&objects, &key)
            .unwrap()
            .is_none(),
        "changed payload was persisted under its earlier member identity"
    );
    assert!(matches!(result, Err(XetAdapterError::XorbHashMismatch)));
    // Rejection must leave the original identity usable by an intact retry.
    assert_eq!(
        store(
            &objects,
            &mut Cursor::new(original.serialized_data),
            &validated
        )
        .await
        .unwrap(),
        4096
    );
    let metadata = shardline_storage::ObjectStore::metadata(&objects, &key)
        .unwrap()
        .unwrap();
    assert_eq!(
        shardline_server_core::read_full_object(&objects, &key, metadata.length()).unwrap(),
        vec![0x41; 4096]
    );
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
