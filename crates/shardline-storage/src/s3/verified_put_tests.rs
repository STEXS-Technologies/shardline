#![allow(
    clippy::panic,
    reason = "unexpected requests must fail the fake provider test"
)]

use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use shardline_protocol::{SecretString, ShardlineHash};

use super::{S3ObjectStore, S3ObjectStoreConfig, S3ObjectStoreError};
use crate::{AsyncObjectStore, ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome};

#[derive(Default)]
struct State {
    objects: HashMap<String, Vec<u8>>,
    requests: Vec<String>,
    deny_reads: bool,
    insert_after_missing_read: Option<Vec<u8>>,
}

struct Provider {
    state: Arc<Mutex<State>>,
    address: SocketAddr,
    stopped: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl Provider {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State::default()));
        let stopped = Arc::new(AtomicBool::new(false));
        let provider_state = Arc::clone(&state);
        let provider_stopped = Arc::clone(&stopped);
        let worker = std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                if provider_stopped.load(Ordering::SeqCst) {
                    break;
                }
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 4096];
                let header_end = loop {
                    let read = stream.read(&mut buffer).unwrap();
                    assert!(read > 0);
                    request.extend_from_slice(&buffer[..read]);
                    if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                        break end.checked_add(4).unwrap();
                    }
                };
                let headers = String::from_utf8(request[..header_end].to_vec()).unwrap();
                let mut first = headers.lines().next().unwrap().split_whitespace();
                let method = first.next().unwrap();
                let path = first.next().unwrap();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap_or(0);
                let body_end = header_end.checked_add(length).unwrap();
                while request.len() < body_end {
                    let read = stream.read(&mut buffer).unwrap();
                    assert!(read > 0);
                    request.extend_from_slice(&buffer[..read]);
                }
                let (status, bytes) = {
                    let mut state = provider_state.lock().unwrap();
                    state.requests.push(method.to_owned());
                    match method {
                        "PUT" => {
                            assert!(headers.lines().any(|line| {
                                line.split_once(':').is_some_and(|(name, value)| {
                                    name.eq_ignore_ascii_case("if-none-match")
                                        && value.trim() == "*"
                                })
                            }));
                            if state.objects.contains_key(path) {
                                ("412 Precondition Failed", Vec::new())
                            } else {
                                state.objects.insert(
                                    path.to_owned(),
                                    request[header_end..body_end].to_vec(),
                                );
                                ("200 OK", Vec::new())
                            }
                        }
                        "GET" if state.deny_reads => ("403 Forbidden", Vec::new()),
                        "GET" => state.objects.get(path).cloned().map_or_else(
                            || {
                                if let Some(bytes) = state.insert_after_missing_read.take() {
                                    state.objects.insert(path.to_owned(), bytes);
                                }
                                ("404 Not Found", Vec::new())
                            },
                            |bytes| ("200 OK", bytes),
                        ),
                        other => panic!("unexpected provider operation: {other}"),
                    }
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nETag: \"fixture\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n",
                    bytes.len()
                );
                stream.write_all(response.as_bytes()).unwrap();
                stream.write_all(&bytes).unwrap();
            }
        });
        Self {
            state,
            address,
            stopped,
            worker: Some(worker),
        }
    }

    fn store(&self) -> S3ObjectStore {
        S3ObjectStore::new(
            S3ObjectStoreConfig::new("fixture".to_owned(), "us-east-1".to_owned())
                .with_endpoint(Some(format!("http://{}", self.address)))
                .with_allow_http(true)
                .with_credentials(
                    Some(SecretString::from_secret("fixture-access")),
                    Some(SecretString::from_secret("fixture-secret")),
                    None,
                ),
        )
        .unwrap()
    }

    fn requests(&self) -> Vec<String> {
        self.state.lock().unwrap().requests.clone()
    }
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
        let _wake = TcpStream::connect(self.address);
        self.worker.take().unwrap().join().unwrap();
    }
}

fn integrity(bytes: &[u8]) -> ObjectIntegrity {
    ObjectIntegrity::new(
        ShardlineHash::from_bytes(*blake3::hash(bytes).as_bytes()),
        bytes.len() as u64,
    )
}

#[tokio::test]
async fn concurrent_creates_use_one_put_and_revalidate_every_waiter() {
    let provider = Provider::new();
    let store = provider.store();
    let key = ObjectKey::parse("chunks/concurrent").unwrap();
    let bytes = b"shared upload chunk";
    let expected = integrity(bytes);
    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let store = store.clone();
        let key = key.clone();
        let barrier = Arc::clone(&barrier);
        tasks.spawn(async move {
            barrier.wait().await;
            AsyncObjectStore::put_if_absent(&store, &key, ObjectBody::from_slice(bytes), &expected)
                .await
                .unwrap()
        });
    }
    let mut inserted = 0;
    while let Some(result) = tasks.join_next().await {
        if result.unwrap() == PutOutcome::Inserted {
            inserted += 1;
        }
    }
    assert_eq!(inserted, 1);
    let requests = provider.requests();
    assert_eq!(requests.iter().filter(|method| *method == "PUT").count(), 1);
    assert_eq!(
        requests.iter().filter(|method| *method == "GET").count(),
        15
    );
}

#[tokio::test]
async fn recent_key_is_revalidated_and_conflicting_remote_bytes_are_preserved() {
    let provider = Provider::new();
    let store = provider.store();
    let key = ObjectKey::parse("chunks/recent").unwrap();
    let bytes = b"trusted";
    let expected = integrity(bytes);
    assert_eq!(
        AsyncObjectStore::put_if_absent(&store, &key, ObjectBody::from_slice(bytes), &expected)
            .await
            .unwrap(),
        PutOutcome::Inserted
    );
    assert_eq!(
        AsyncObjectStore::put_if_absent(
            &store.clone(),
            &key,
            ObjectBody::from_slice(bytes),
            &expected
        )
        .await
        .unwrap(),
        PutOutcome::AlreadyExists
    );
    assert_eq!(provider.requests(), ["PUT", "GET"]);
    provider
        .state
        .lock()
        .unwrap()
        .objects
        .insert("/fixture/chunks/recent".to_owned(), b"corrupt".to_vec());
    let error =
        AsyncObjectStore::put_if_absent(&store, &key, ObjectBody::from_slice(bytes), &expected)
            .await
            .unwrap_err();
    assert!(matches!(error, S3ObjectStoreError::ExistingObjectConflict));
    assert_eq!(provider.requests(), ["PUT", "GET", "GET"]);
    assert_eq!(
        provider.state.lock().unwrap().objects["/fixture/chunks/recent"],
        b"corrupt"
    );
}

#[tokio::test]
async fn stale_hint_restores_a_deleted_object_with_conditional_create() {
    let provider = Provider::new();
    let store = provider.store();
    let key = ObjectKey::parse("chunks/deleted").unwrap();
    let bytes = b"restored";
    let expected = integrity(bytes);
    AsyncObjectStore::put_if_absent(&store, &key, ObjectBody::from_slice(bytes), &expected)
        .await
        .unwrap();
    provider.state.lock().unwrap().objects.clear();
    assert_eq!(
        AsyncObjectStore::put_if_absent(&store, &key, ObjectBody::from_slice(bytes), &expected)
            .await
            .unwrap(),
        PutOutcome::Inserted
    );
    assert_eq!(provider.requests(), ["PUT", "GET", "PUT"]);
}

#[tokio::test]
async fn create_racing_a_stale_hint_never_overwrites_conflicting_bytes() {
    let provider = Provider::new();
    let store = provider.store();
    let key = ObjectKey::parse("chunks/raced").unwrap();
    let bytes = b"trusted";
    let expected = integrity(bytes);
    AsyncObjectStore::put_if_absent(&store, &key, ObjectBody::from_slice(bytes), &expected)
        .await
        .unwrap();
    {
        let mut state = provider.state.lock().unwrap();
        state.objects.clear();
        state.insert_after_missing_read = Some(b"racing!".to_vec());
    }
    let error =
        AsyncObjectStore::put_if_absent(&store, &key, ObjectBody::from_slice(bytes), &expected)
            .await
            .unwrap_err();
    assert!(matches!(error, S3ObjectStoreError::ExistingObjectConflict));
    assert_eq!(provider.requests(), ["PUT", "GET", "PUT", "GET"]);
    assert_eq!(
        provider.state.lock().unwrap().objects["/fixture/chunks/raced"],
        b"racing!"
    );
}

#[tokio::test]
async fn failed_hint_probe_preserves_create_only_permissions_and_input_validation() {
    let provider = Provider::new();
    let store = provider.store();
    let key = ObjectKey::parse("chunks/create-only").unwrap();
    let bytes = b"trusted";
    let expected = integrity(bytes);
    ObjectStore::put_if_absent(&store, &key, ObjectBody::from_slice(bytes), &expected).unwrap();
    {
        let mut state = provider.state.lock().unwrap();
        state.objects.clear();
        state.deny_reads = true;
    }
    assert_eq!(
        AsyncObjectStore::put_if_absent(&store, &key, ObjectBody::from_slice(bytes), &expected)
            .await
            .unwrap(),
        PutOutcome::Inserted
    );
    assert_eq!(provider.requests(), ["PUT", "GET", "PUT"]);
    let error = AsyncObjectStore::put_if_absent(
        &store,
        &key,
        ObjectBody::from_slice(b"invalid"),
        &expected,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, S3ObjectStoreError::IntegrityHashMismatch));
    assert_eq!(provider.requests(), ["PUT", "GET", "PUT"]);
}
