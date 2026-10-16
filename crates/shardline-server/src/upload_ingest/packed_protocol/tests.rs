use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    body::Bytes,
    extract::DefaultBodyLimit,
    http::{HeaderMap, Method, StatusCode, Uri},
    response::Response,
};
use shardline_protocol::SecretString;
use shardline_storage::S3ObjectStoreConfig;

use super::*;
use crate::upload_ingest::FileUploadIngestor;

#[derive(Default)]
struct ProviderState {
    objects: HashMap<String, Bytes>,
    requests: Vec<(Method, String)>,
}

struct Provider {
    state: Arc<Mutex<ProviderState>>,
    objects: ServerObjectStore,
    task: tokio::task::JoinHandle<()>,
}

impl Provider {
    fn s3_store(&self) -> &S3ObjectStore {
        match &self.objects {
            ServerObjectStore::S3(store) => Some(store),
            ServerObjectStore::Local(_) | ServerObjectStore::Blackhole => None,
        }
        .expect("test provider must use S3 storage")
    }

    // 0: ordinary storage; 1: deny containers; 2: corrupt containers;
    // 3: corrupt every new object, including the fallback chunks;
    // 4: commit containers but return a retryable error instead of success.
    async fn new(mode: u8) -> Self {
        let state = Arc::new(Mutex::new(ProviderState::default()));
        let observed = state.clone();
        let app = Router::new()
            .fallback(
                move |method: Method, uri: Uri, headers: HeaderMap, bytes: Bytes| {
                    let observed = observed.clone();
                    async move {
                        let mut state = observed.lock().unwrap();
                        let path = uri.path().to_owned();
                        state.requests.push((method.clone(), path.clone()));
                        let mut response = Response::builder()
                            .header("etag", "\"fixture\"")
                            .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT");
                        if method == Method::PUT {
                            if mode == 1 && path.contains("/xorbs/") {
                                return response
                                    .status(StatusCode::FORBIDDEN)
                                    .body(axum::body::Body::empty())
                                    .unwrap();
                            }
                            if state.objects.contains_key(&path) {
                                return response
                                    .status(StatusCode::PRECONDITION_FAILED)
                                    .body(axum::body::Body::empty())
                                    .unwrap();
                            }
                            let bytes = if mode == 3 || (mode == 2 && path.contains("/xorbs/")) {
                                let mut bad = bytes.to_vec();
                                if let Some(byte) = bad.first_mut() {
                                    *byte ^= 1;
                                }
                                Bytes::from(bad)
                            } else {
                                bytes
                            };
                            let ambiguous_commit = mode == 4 && path.contains("/xorbs/");
                            state.objects.insert(path, bytes);
                            if ambiguous_commit {
                                return response
                                    .status(StatusCode::SERVICE_UNAVAILABLE)
                                    .body(axum::body::Body::empty())
                                    .unwrap();
                            }
                            return response
                                .status(StatusCode::OK)
                                .body(axum::body::Body::empty())
                                .unwrap();
                        }
                        if method == Method::HEAD {
                            return match state.objects.get(&path) {
                                Some(bytes) => response
                                    .header("content-length", bytes.len())
                                    .status(StatusCode::OK),
                                None => response.status(StatusCode::NOT_FOUND),
                            }
                            .body(axum::body::Body::empty())
                            .unwrap();
                        }
                        assert_eq!(method, Method::GET);
                        let Some(bytes) = state.objects.get(&path) else {
                            return response
                                .status(StatusCode::NOT_FOUND)
                                .body(axum::body::Body::empty())
                                .unwrap();
                        };
                        if let Some(range) = headers.get("range") {
                            let range = range.to_str().unwrap().strip_prefix("bytes=").unwrap();
                            let (start, end) = range.split_once('-').unwrap();
                            let start: usize = start.parse().unwrap();
                            let end: usize = end.parse().unwrap();
                            response = response.status(StatusCode::PARTIAL_CONTENT).header(
                                "content-range",
                                format!("bytes {start}-{end}/{}", bytes.len()),
                            );
                            return response
                                .body(axum::body::Body::from(bytes.slice(start..=end)))
                                .unwrap();
                        }
                        response
                            .status(StatusCode::OK)
                            .body(axum::body::Body::from(bytes.clone()))
                            .unwrap()
                    }
                },
            )
            .layer(DefaultBodyLimit::max(128 * 1024 * 1024));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let objects = ServerObjectStore::s3(
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
        Self {
            state,
            objects,
            task,
        }
    }

    async fn ingest(
        &self,
        bytes: &[u8],
        direct: bool,
        frame: usize,
    ) -> Result<
        (
            shardline_index::FileRecord,
            crate::model::UploadFileResponse,
        ),
        ServerError,
    > {
        let mut ingestor = FileUploadIngestor::new_with_parallelism(
            NonZeroUsize::new(64 * 1024).unwrap(),
            false,
            NonZeroUsize::new(8).unwrap(),
        );
        if direct {
            ingestor = ingestor.with_packed_protocol_storage(
                &self.objects,
                &crate::admission::ExecutionPools::default_sizes().hashing,
            );
        }
        for part in bytes.chunks(frame) {
            ingestor
                .ingest_body_chunk(&self.objects, &Bytes::copy_from_slice(part))
                .await?;
        }
        ingestor.finish(&self.objects, "identity", None, None).await
    }
}

mod admission;
mod digest;
mod lost_ack;

impl Drop for Provider {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn empty_upload_matches_legacy_without_storage_requests() {
    let legacy = Provider::new(0).await;
    let direct = Provider::new(0).await;
    let (expected, _) = legacy.ingest(&[], false, 4093).await.unwrap();
    let (actual, _) = direct.ingest(&[], true, 7919).await.unwrap();
    assert_eq!(actual, expected);
    assert!(direct.state.lock().unwrap().requests.is_empty());
}

#[tokio::test]
async fn chunk_count_limit_flushes_before_adding_another_chunk() {
    let provider = Provider::new(0).await;
    let mut upload = PackedProtocolUpload::new(
        provider.s3_store().clone(),
        crate::admission::ExecutionPools::default_sizes().hashing,
    );
    for offset in 0..=MAX_XORB_CHUNKS {
        upload.push(b"x", offset as u64).await.unwrap();
        assert!(upload.pending.len() <= MAX_XORB_CHUNKS);
    }
    assert_eq!(upload.pending.len(), 1);
    let result = upload.finish().await.unwrap();
    assert_eq!(result.records.len(), MAX_XORB_CHUNKS + 1);
    assert_eq!(result.records[0].range_start, 0);
    assert_eq!(
        result.records[MAX_XORB_CHUNKS - 1].range_end as usize,
        MAX_XORB_CHUNKS
    );
    assert_eq!(result.records[MAX_XORB_CHUNKS].range_start, 0);
    assert_ne!(result.records[0].hash, result.records[MAX_XORB_CHUNKS].hash);
}

#[tokio::test]
async fn direct_packing_matches_legacy_records_without_individual_writes() {
    let bytes: Vec<_> = (0..256 * 1024).map(|index| (index % 251) as u8).collect();
    let legacy = Provider::new(0).await;
    let (expected, _) = legacy.ingest(&bytes, false, 4093).await.unwrap();
    let direct = Provider::new(0).await;
    let (actual, _) = direct.ingest(&bytes, true, 7919).await.unwrap();
    assert_eq!(actual, expected);
    let state = direct.state.lock().unwrap();
    assert!(state.objects.keys().all(|path| path.contains("/xorbs/")));
    assert_eq!(
        state
            .requests
            .iter()
            .map(|(method, _)| method.clone())
            .collect::<Vec<_>>(),
        [Method::PUT, Method::GET]
    );
}

#[tokio::test]
async fn container_failure_falls_back_to_verified_individual_chunks() {
    let bytes = vec![b'a'; 100 * 1024];
    for mode in [1, 2] {
        let provider = Provider::new(mode).await;
        let (record, _) = provider.ingest(&bytes, true, 16384).await.unwrap();
        let mut reader =
            DurableUploadChunks::with_cache(provider.objects.clone(), 8, ReadCache::default());
        let mut actual = Vec::new();
        while let Some((chunk, _)) = reader.next(&record.chunks).await.unwrap() {
            actual.extend(chunk);
        }
        assert_eq!(actual, bytes);
    }
}

#[tokio::test]
async fn corrupt_container_and_fallback_never_produce_a_publishable_record() {
    let provider = Provider::new(3).await;
    assert!(
        provider
            .ingest(&vec![b'a'; 100 * 1024], true, 16384)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn bounded_batches_keep_original_container_boundaries_and_release_raw_input() {
    let provider = Provider::new(0).await;
    let mut upload = PackedProtocolUpload::new(
        provider.s3_store().clone(),
        crate::admission::BoundedPool::new(NonZeroUsize::new(1).unwrap()),
    );
    let chunk = vec![b'a'; 1024 * 1024];
    for index in 0..33 {
        upload.push(&chunk, index * 1024 * 1024).await.unwrap();
        assert!(upload.pending_bytes <= MAX_SERVER_XORB_RAW_BYTES);
        assert!(upload.pending.len() <= MAX_XORB_CHUNKS);
    }
    assert_eq!(upload.pending.len(), 1);
    let result = upload.finish().await.unwrap();
    assert_eq!(result.records.len(), 33);
    let expected_first = crate::upload_ingest::xorb_packer::pack_chunks_into_xorb(
        &(0..32)
            .map(|index| (chunk.clone(), index * 1024 * 1024))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    assert_eq!(result.records[0].hash, expected_first.xorb_hash_hex);
    assert_eq!(result.records[31].hash, expected_first.xorb_hash_hex);
    assert_ne!(result.records[32].hash, expected_first.xorb_hash_hex);
}
