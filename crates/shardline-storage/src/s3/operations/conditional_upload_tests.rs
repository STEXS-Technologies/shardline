use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use object_store::{Error as ExternalObjectStoreError, PutPayload, multipart::MultipartStore};
use shardline_protocol::{ByteRange, SecretString, ShardlineHash};

use super::{S3ObjectStore, S3ObjectStoreConfig, S3ObjectStoreError};
use crate::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome};

const SOURCE: &[u8] = b"source bytes";

#[derive(Default)]
struct State {
    requests: usize,
    creates: usize,
    part_puts: usize,
    part_status: Option<&'static str>,
    lose_completion_response: bool,
    destination: Option<Vec<u8>>,
    competing_bytes: Option<Vec<u8>>,
    uploaded: Vec<u8>,
    conditional_completion: bool,
    aborted: bool,
    destination_heads: usize,
    completion_status: Option<&'static str>,
    completion_requests: usize,
    mutate_source_on_create: Option<(std::path::PathBuf, Vec<u8>)>,
}

struct Provider {
    address: SocketAddr,
    state: Arc<Mutex<State>>,
    stopped: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl Provider {
    fn new(competing_bytes: Option<Vec<u8>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State {
            competing_bytes,
            ..State::default()
        }));
        let stopped = Arc::new(AtomicBool::new(false));
        let worker_state = Arc::clone(&state);
        let worker_stopped = Arc::clone(&stopped);
        let worker = std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                if worker_stopped.load(Ordering::SeqCst) {
                    break;
                }
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
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
                let content_length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap_or(0);
                let request_end = header_end.checked_add(content_length).unwrap();
                while request.len() < request_end {
                    let read = stream.read(&mut buffer).unwrap();
                    assert!(read > 0);
                    request.extend_from_slice(&buffer[..read]);
                }
                let conditional = headers.lines().any(|line| {
                    line.split_once(':').is_some_and(|(name, value)| {
                        name.eq_ignore_ascii_case("if-none-match") && value.trim() == "*"
                    })
                });
                if method != "POST" || !path.contains("uploadId=") {
                    assert!(
                        !conditional,
                        "condition must apply only to multipart completion"
                    );
                }
                let mut state = worker_state.lock().unwrap();
                state.requests = state.requests.checked_add(1).unwrap();
                assert!(!path.contains("__tmp"), "unexpected temporary object");
                let (status, body, length) = match method {
                    "HEAD" => {
                        state.destination_heads = state.destination_heads.checked_add(1).unwrap();
                        state.destination.as_ref().map_or_else(
                            || ("404 Not Found", Vec::new(), 0),
                            |bytes| ("200 OK", Vec::new(), bytes.len()),
                        )
                    }
                    "GET" => {
                        let bytes = if path.starts_with("/fixture/source") {
                            Some(SOURCE.to_vec())
                        } else {
                            state.destination.clone()
                        };
                        bytes.map_or_else(
                            || ("404 Not Found", Vec::new(), 0),
                            |bytes| {
                                let length = bytes.len();
                                let status = if headers.to_ascii_lowercase().contains("\r\nrange:")
                                {
                                    "206 Partial Content"
                                } else {
                                    "200 OK"
                                };
                                (status, bytes, length)
                            },
                        )
                    }
                    "PUT" => {
                        state.part_puts = state.part_puts.checked_add(1).unwrap();
                        state.uploaded = request[header_end..request_end].to_vec();
                        (state.part_status.unwrap_or("200 OK"), Vec::new(), 0)
                    }
                    "POST" if !path.contains("uploadId=") => {
                        state.creates = state.creates.checked_add(1).unwrap();
                        if let Some((path, bytes)) = state.mutate_source_on_create.take() {
                            std::fs::write(path, bytes).unwrap();
                        }
                        let body = b"<InitiateMultipartUploadResult><Bucket>fixture</Bucket><Key>destination</Key><UploadId>fixture-id</UploadId></InitiateMultipartUploadResult>".to_vec();
                        let length = body.len();
                        ("200 OK", body, length)
                    }
                    "POST" => {
                        state.completion_requests =
                            state.completion_requests.checked_add(1).unwrap();
                        state.conditional_completion = conditional;
                        // Another node publishes after every existence probe,
                        // immediately before the provider completes this upload.
                        if let Some(bytes) = state.competing_bytes.take() {
                            state.destination = Some(bytes);
                        }
                        state.completion_status.map_or_else(
                            || {
                                if conditional && state.destination.is_some() {
                                    ("412 Precondition Failed", Vec::new(), 0)
                                } else {
                                    state.destination = Some(state.uploaded.clone());
                                    let body = b"<CompleteMultipartUploadResult><ETag>\"fixture\"</ETag></CompleteMultipartUploadResult>".to_vec();
                                    let length = body.len();
                                    ("200 OK", body, length)
                                }
                            },
                            |status| (status, Vec::new(), 0),
                        )
                    }
                    "DELETE" => {
                        state.aborted = true;
                        ("204 No Content", Vec::new(), 0)
                    }
                    _ => ("405 Method Not Allowed", Vec::new(), 0),
                };
                if method == "POST"
                    && path.contains("uploadId=")
                    && status == "200 OK"
                    && state.lose_completion_response
                {
                    state.lose_completion_response = false;
                    // Completion committed, but the response never reaches the
                    // client. Any retry must retain the absence condition.
                    continue;
                }
                drop(state);
                let content_range = if status == "206 Partial Content" {
                    let end = length.checked_sub(1).unwrap();
                    format!("Content-Range: bytes 0-{end}/{length}\r\n")
                } else {
                    String::new()
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {length}\r\n{content_range}ETag: \"fixture\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n"
                );
                stream.write_all(response.as_bytes()).unwrap();
                stream.write_all(&body).unwrap();
            }
        });
        Self {
            address,
            state,
            stopped,
            worker: Some(worker),
        }
    }

    fn store(&self) -> Result<S3ObjectStore, S3ObjectStoreError> {
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
    }

    fn copy(&self) -> Result<PutOutcome, S3ObjectStoreError> {
        let store = self.store()?;
        let source =
            ObjectKey::parse("source").map_err(|_error| S3ObjectStoreError::InvalidListedKey)?;
        let destination = ObjectKey::parse("destination")
            .map_err(|_error| S3ObjectStoreError::InvalidListedKey)?;
        let source_location = store.location_for_key(&source)?;
        let destination_location = store.location_for_key(&destination)?;
        // Exercise the production large-copy fallback with a small payload:
        // byte count does not alter its publication/concurrency protocol.
        store.block_on_result(store.streaming_large_copy(
            &source_location,
            &destination_location,
            &source,
            &destination,
            SOURCE.len() as u64,
        ))
    }
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
        let _wake = TcpStream::connect(self.address);
        self.worker.take().unwrap().join().unwrap();
    }
}

#[test]
fn large_copy_preserves_a_conflicting_concurrent_destination() {
    let winner = b"winner bytes".to_vec();
    let provider = Provider::new(Some(winner.clone()));
    let result = provider.copy();
    let state = provider.state.lock().unwrap();
    assert_eq!(state.destination.as_ref(), Some(&winner));
    assert!(
        matches!(result, Err(S3ObjectStoreError::ExistingObjectConflict)),
        "unexpected copy result: {result:?}"
    );
    assert!(state.conditional_completion);
    assert!(state.aborted);
}

#[test]
fn read_only_store_does_not_initialize_multipart_completion_client() {
    let provider = Provider::new(None);
    provider.state.lock().unwrap().destination = Some(SOURCE.to_vec());
    let store = provider.store().unwrap();
    let source = ObjectKey::parse("destination").unwrap();
    assert!(store.metadata(&source).unwrap().is_some());
    assert!(!store.conditional_completion_inner.is_initialized());
    assert_eq!(provider.state.lock().unwrap().creates, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multipart_completion_client_is_shared_across_concurrent_store_clones() {
    let provider = Provider::new(None);
    let store = provider.store().unwrap();
    let clones: Vec<_> = (0..16).map(|_| store.clone()).collect();
    let clients = futures_util::future::join_all(
        clones
            .iter()
            .map(|store| store.conditional_completion_inner.get()),
    )
    .await;
    let expected = store.conditional_completion_inner.get().await.unwrap();
    for client in clients {
        assert!(std::ptr::eq(expected, client.unwrap()));
    }
    assert_eq!(provider.state.lock().unwrap().requests, 0);
}

#[test]
fn completion_client_failure_precedes_remote_multipart_creation() {
    for file_upload in [false, true] {
        let provider = Provider::new(None);
        let mut store = provider.store().unwrap();
        let invalid = S3ObjectStoreConfig::new("fixture".to_owned(), "us-east-1".to_owned())
            .with_credentials(
                Some(SecretString::from_secret("fixture-access")),
                None,
                None,
            );
        store.conditional_completion_inner = Arc::new(
            super::super::client::ConditionalCompletionClient::new(invalid),
        );
        let source = ObjectKey::parse("source").unwrap();
        let destination = ObjectKey::parse("destination").unwrap();
        let result = if file_upload {
            let file = tempfile::NamedTempFile::new().unwrap();
            std::fs::write(file.path(), SOURCE).unwrap();
            let integrity = ObjectIntegrity::new(
                ShardlineHash::from_bytes(*blake3::hash(SOURCE).as_bytes()),
                SOURCE.len() as u64,
            );
            store.put_file_if_absent(&destination, file.path(), &integrity)
        } else {
            store.block_on_result(store.streaming_large_copy(
                &store.location_for_key(&source).unwrap(),
                &store.location_for_key(&destination).unwrap(),
                &source,
                &destination,
                SOURCE.len() as u64,
            ))
        };
        assert!(matches!(
            result,
            Err(S3ObjectStoreError::IncompleteCredentials)
        ));
        let state = provider.state.lock().unwrap();
        assert_eq!(state.creates, 0);
        assert_eq!(state.part_puts, 0);
        assert_eq!(state.completion_requests, 0);
        assert!(state.destination.is_none());
    }
}

#[test]
fn direct_file_upload_uses_four_requests_and_no_temporary_object() {
    let provider = Provider::new(None);
    let store = provider.store().unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), SOURCE).unwrap();
    let key = ObjectKey::parse("destination").unwrap();
    let integrity = ObjectIntegrity::new(
        ShardlineHash::from_bytes(*blake3::hash(SOURCE).as_bytes()),
        SOURCE.len() as u64,
    );
    assert_eq!(
        store
            .put_file_if_absent(&key, file.path(), &integrity)
            .unwrap(),
        PutOutcome::Inserted
    );
    assert!(store.conditional_completion_inner.is_initialized());
    let state = provider.state.lock().unwrap();
    assert_eq!(state.destination.as_deref(), Some(SOURCE));
    assert_eq!(state.requests, 4);
    assert_eq!(state.creates, 1);
    assert_eq!(state.part_puts, 1);
    assert_eq!(state.completion_requests, 1);
    assert!(state.conditional_completion);
    assert!(!state.aborted);
}

#[test]
fn direct_file_upload_rejects_same_length_source_mutation_and_matching_bad_replay() {
    for exists in [false, true] {
        let provider = Provider::new(None);
        let store = provider.store().unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let integrity = ObjectIntegrity::new(
            ShardlineHash::from_bytes(*blake3::hash(SOURCE).as_bytes()),
            SOURCE.len() as u64,
        );
        let mut changed = SOURCE.to_vec();
        changed[0] ^= 1;
        if exists {
            std::fs::write(file.path(), &changed).unwrap();
            provider.state.lock().unwrap().destination = Some(changed.clone());
        } else {
            std::fs::write(file.path(), SOURCE).unwrap();
            // Change the already-open source only after remote upload
            // creation, beyond caller validation and the existence probe.
            provider.state.lock().unwrap().mutate_source_on_create =
                Some((file.path().to_owned(), changed.clone()));
        }
        let result = store.put_content_addressed_file(
            &ObjectKey::parse("destination").unwrap(),
            file.path(),
            &integrity,
        );
        assert!(result.is_err(), "changed source must not be acknowledged");
        let state = provider.state.lock().unwrap();
        assert_eq!(state.completion_requests, 0);
        assert_eq!(state.destination.as_ref(), exists.then_some(&changed));
        if !exists {
            assert!(state.aborted);
        }
    }
}

#[test]
fn direct_file_upload_preserves_concurrent_writers_and_verifies_replays() {
    for winner in [SOURCE, b"winner bytes".as_slice()] {
        let provider = Provider::new(Some(winner.to_vec()));
        let store = provider.store().unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), SOURCE).unwrap();
        let key = ObjectKey::parse("destination").unwrap();
        let integrity = ObjectIntegrity::new(
            ShardlineHash::from_bytes(*blake3::hash(SOURCE).as_bytes()),
            SOURCE.len() as u64,
        );
        let result = store.put_content_addressed_file(&key, file.path(), &integrity);
        if winner == SOURCE {
            assert_eq!(result.unwrap(), PutOutcome::AlreadyExists);
        } else {
            assert!(matches!(
                result,
                Err(S3ObjectStoreError::ExistingObjectConflict)
            ));
        }
        let state = provider.state.lock().unwrap();
        assert_eq!(state.destination.as_deref(), Some(winner));
        assert!(state.aborted);
        assert!(state.conditional_completion);
    }
}

#[test]
fn direct_file_upload_aborts_parts_and_length_failures_before_publication() {
    for (length, part_status) in [
        (1, None),
        (100, None),
        (SOURCE.len() as u64, Some("403 Forbidden")),
    ] {
        let provider = Provider::new(None);
        provider.state.lock().unwrap().part_status = part_status;
        let store = provider.store().unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), SOURCE).unwrap();
        let location = store
            .location_for_key(&ObjectKey::parse("destination").unwrap())
            .unwrap();
        assert!(
            store
                .stream_file_if_absent(
                    &location,
                    file.path(),
                    &ObjectIntegrity::new(
                        ShardlineHash::from_bytes(*blake3::hash(SOURCE).as_bytes()),
                        length,
                    )
                )
                .is_err()
        );
        let state = provider.state.lock().unwrap();
        assert!(state.destination.is_none());
        assert!(state.aborted);
        assert_eq!(state.completion_requests, 0);
    }
}

#[test]
fn direct_file_upload_does_not_create_remote_state_for_missing_source() {
    let provider = Provider::new(None);
    let store = provider.store().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let location = store
        .location_for_key(&ObjectKey::parse("destination").unwrap())
        .unwrap();
    assert!(
        store
            .stream_file_if_absent(
                &location,
                &dir.path().join("missing"),
                &ObjectIntegrity::new(ShardlineHash::from_bytes(*blake3::hash(&[]).as_bytes()), 0,)
            )
            .is_err()
    );
    assert_eq!(provider.state.lock().unwrap().requests, 0);
}

#[test]
fn direct_file_upload_recovers_a_lost_completion_reply_without_overwrite() {
    let provider = Provider::new(None);
    provider.state.lock().unwrap().lose_completion_response = true;
    let store = provider.store().unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), SOURCE).unwrap();
    let key = ObjectKey::parse("destination").unwrap();
    let integrity = ObjectIntegrity::new(
        ShardlineHash::from_bytes(*blake3::hash(SOURCE).as_bytes()),
        SOURCE.len() as u64,
    );
    assert_eq!(
        store
            .put_file_if_absent(&key, file.path(), &integrity)
            .unwrap(),
        PutOutcome::AlreadyExists
    );
    let state = provider.state.lock().unwrap();
    assert_eq!(state.destination.as_deref(), Some(SOURCE));
    assert_eq!(state.creates, 1);
    assert_eq!(state.completion_requests, 2);
    assert!(state.conditional_completion);
}

#[test]
fn large_copy_accepts_an_identical_concurrent_destination() {
    let provider = Provider::new(Some(SOURCE.to_vec()));
    assert_eq!(provider.copy().unwrap(), PutOutcome::AlreadyExists);
    let state = provider.state.lock().unwrap();
    assert_eq!(state.destination.as_deref(), Some(SOURCE));
    assert!(state.conditional_completion);
    assert!(state.aborted);
}

#[test]
fn large_copy_publishes_when_destination_stays_absent() {
    let provider = Provider::new(None);
    assert_eq!(provider.copy().unwrap(), PutOutcome::Inserted);
    let state = provider.state.lock().unwrap();
    assert_eq!(state.destination.as_deref(), Some(SOURCE));
    assert!(state.conditional_completion);
    assert!(!state.aborted);
    assert_eq!(
        state.destination_heads, 1,
        "successful copy needs no final HEAD"
    );
}

#[test]
fn large_copy_aborts_failed_completion_without_unconditional_retry() {
    for status in ["409 Conflict", "403 Forbidden"] {
        let provider = Provider::new(None);
        provider.state.lock().unwrap().completion_status = Some(status);
        assert!(provider.copy().is_err());
        let state = provider.state.lock().unwrap();
        assert!(state.destination.is_none());
        assert!(state.conditional_completion);
        assert!(state.aborted);
        assert_eq!(state.completion_requests, 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_large_copy_completion_is_conditional_and_signed() {
    use shardline_test_support::DockerLocalStack;

    // This contract uses tiny payloads to exercise conditional multipart wire
    // behavior, not disk durability or throughput. Keep its disposable provider
    // independent of the host volume's free-space reserve.
    let Some(stack) = DockerLocalStack::builder()
        .with_minio_tmpfs()
        .start()
        .unwrap()
    else {
        eprintln!("skipping conditional multipart provider test: Docker is unavailable");
        return;
    };
    let prefix = stack.unique_s3_key_prefix("conditional-large-copy");
    let raw = stack.s3_raw_config(Some(&prefix)).unwrap();
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new(raw.bucket, raw.region)
            .with_endpoint(raw.endpoint)
            .with_credentials(
                raw.access_key.map(SecretString::new),
                raw.secret_key.map(SecretString::new),
                raw.session_token.map(SecretString::new),
            )
            .with_key_prefix(raw.key_prefix.as_deref())
            .with_allow_http(raw.allow_http),
    )
    .unwrap();
    let source = ObjectKey::parse("source").unwrap();
    let destination = ObjectKey::parse("destination").unwrap();
    let integrity = ObjectIntegrity::new(
        ShardlineHash::from_bytes(*blake3::hash(SOURCE).as_bytes()),
        SOURCE.len() as u64,
    );
    store
        .put_if_absent(&source, ObjectBody::from_slice(SOURCE), &integrity)
        .unwrap();
    let src = store.location_for_key(&source).unwrap();
    let dst = store.location_for_key(&destination).unwrap();
    assert_eq!(
        store
            .streaming_large_copy(&src, &dst, &source, &destination, SOURCE.len() as u64,)
            .await
            .unwrap(),
        PutOutcome::Inserted
    );
    let source_range = ByteRange::new(0, SOURCE.len() as u64 - 1).unwrap();
    assert_eq!(
        store.read_range(&destination, source_range).unwrap(),
        SOURCE
    );

    let race_key = ObjectKey::parse("race-destination").unwrap();
    let race_location = store.location_for_key(&race_key).unwrap();
    let upload_id = store.inner.create_multipart(&race_location).await.unwrap();
    let part = store
        .inner
        .put_part(
            &race_location,
            &upload_id,
            0,
            PutPayload::from_bytes(bytes::Bytes::from_static(SOURCE)),
        )
        .await
        .unwrap();
    let winner = b"winner bytes";
    assert!(
        store.metadata(&race_key).unwrap().is_none(),
        "uncompleted parts must not publish an object"
    );
    let winner_integrity = ObjectIntegrity::new(
        ShardlineHash::from_bytes(*blake3::hash(winner).as_bytes()),
        winner.len() as u64,
    );
    store
        .put_if_absent(&race_key, ObjectBody::from_slice(winner), &winner_integrity)
        .unwrap();
    let completion = store
        .conditional_completion_inner
        .get()
        .await
        .unwrap()
        .complete_multipart(&race_location, &upload_id, vec![part])
        .await;
    assert!(matches!(
        completion,
        Err(ExternalObjectStoreError::Precondition { .. })
    ));
    store
        .inner
        .abort_multipart(&race_location, &upload_id)
        .await
        .unwrap();
    let range = ByteRange::new(0, winner.len() as u64 - 1).unwrap();
    assert_eq!(store.read_range(&race_key, range).unwrap(), winner);
    for key in [&source, &destination, &race_key] {
        store.delete_if_present(key).unwrap();
    }
    eprintln!("MinIO conditional multipart completion and competing-writer checks executed");
}
