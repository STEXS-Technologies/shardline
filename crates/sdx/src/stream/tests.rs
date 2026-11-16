use std::sync::atomic::AtomicU64;
use std::time::Duration;

use serde_json::json;
use tokio::runtime::Runtime;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

use super::*;
use crate::{auth::Auth, reconstruction, transfer::TransferClient};

const FILE_ID: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const XORB_HASH: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const READ_TOKEN: &str = "read-token";

/// Serializes chunk payloads with the pinned upstream serializer.
fn serialize_payload(chunks: &[&[u8]]) -> Vec<u8> {
    use xet_core_structures::xorb_object::{CompressionScheme, serialize_chunk};
    let mut payload = Vec::new();
    for chunk in chunks {
        serialize_chunk(chunk, &mut payload, CompressionScheme::None).unwrap();
    }
    payload
}

fn v2_response_body(
    offset: u64,
    terms: serde_json::Value,
    xorbs: serde_json::Value,
) -> serde_json::Value {
    let mut body = serde_json::Map::new();
    body.insert(
        "offset_into_first_range".to_owned(),
        serde_json::Value::from(offset),
    );
    body.insert("terms".to_owned(), terms);
    body.insert("xorbs".to_owned(), xorbs);
    serde_json::Value::Object(body)
}

/// Small prefetch limits so unit tests exercise multiple term blocks
/// without huge JSON responses.
fn test_limits() -> StreamLimits {
    StreamLimits {
        min_reconstruction_fetch_size: 4096,
        max_reconstruction_fetch_size: 4096,
        min_prefetch_buffer: 8192,
        ..StreamLimits::default()
    }
}

/// Shared dedicated runtime for the streaming tests.
///
/// Creating/dropping a `tokio::runtime::Runtime` inside an async test
/// context panics ("cannot drop a runtime in a context where blocking is
/// not allowed"), so a single runtime is created once and shared across
/// all tests; it is dropped at process exit on a clean thread.
fn test_runtime() -> Arc<Runtime> {
    static RUNTIME: std::sync::OnceLock<Arc<Runtime>> = std::sync::OnceLock::new();
    RUNTIME
        .get_or_init(|| Arc::new(Runtime::new().unwrap()))
        .clone()
}

fn test_stream_context(server: &MockServer, buffer_cap: u64) -> StreamContext {
    StreamContext {
        transfer: TransferClient::new(reqwest::Client::new()),
        api_base: server.uri(),
        buffer_semaphore: Arc::new(BufferSemaphore::new(buffer_cap, buffer_cap, buffer_cap)),
        active_downloads: Arc::new(AtomicU64::new(0)),
        download_permits: Arc::new(Semaphore::new(DEFAULT_DOWNLOAD_CONCURRENCY)),
        limits: test_limits(),
        chunk_cache: None,
        xorb_fetch_count: Arc::new(AtomicU64::new(0)),
        tokens: test_tokens(server),
        retry_policy: RetryPolicy::default(),
        blocking_runtime: test_runtime(),
    }
}

/// Builds a token service used only as a retry-context carrier in stream
/// tests (its `read_token` is never exercised because tests never 401).
fn test_tokens(server: &MockServer) -> TokenService {
    let auth = Auth::new(
        &server.uri(),
        crate::RepositoryId {
            provider: "github".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: "main".to_owned(),
        },
    )
    .unwrap()
    .with_token(READ_TOKEN.to_owned());
    auth.build().unwrap()
}

/// Mounts a 200 reconstruction response for `file_hash` scoped to exactly
/// the `Range` header `bytes=start-end`; every other range gets 416
/// (past-EOF), which the prefetch loop treats as end-of-stream.
async fn reconstruction_mock(
    server: &MockServer,
    start: u64,
    end: u64,
    offset: u64,
    terms: serde_json::Value,
    xorbs: serde_json::Value,
) {
    Mock::given(method("GET"))
        .and(path(format!("/v2/reconstructions/{FILE_ID}")))
        .and(header("authorization", format!("Bearer {READ_TOKEN}")))
        .and(header("range", format!("bytes={start}-{end}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(v2_response_body(offset, terms, xorbs)),
        )
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/v2/reconstructions/{FILE_ID}")))
        .and(header("authorization", format!("Bearer {READ_TOKEN}")))
        .respond_with(ResponseTemplate::new(416).set_body_json(json!({"error": "past eof"})))
        .mount(server)
        .await;
}

async fn xorb_range_mock(server: &MockServer, start: u64, end: u64, body: Vec<u8>) {
    Mock::given(method("GET"))
        .and(path(format!("/transfer/xorb/default/{XORB_HASH}")))
        .and(header("authorization", format!("Bearer {READ_TOKEN}")))
        .and(header("range", format!("bytes={start}-{end}")))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header(
                    "Content-Range",
                    format!("bytes {start}-{end}/{}", body.len()),
                )
                .set_body_raw(body, "application/octet-stream"),
        )
        .mount(server)
        .await;
}

/// Responds to actual arbitrary single-range HTTP requests. Coalescing
/// tests must check network behavior, rather than a copied normalizer.
async fn xorb_payload_mock(server: &MockServer, payload: Vec<u8>) {
    Mock::given(method("GET"))
        .and(path(format!("/transfer/xorb/default/{XORB_HASH}")))
        .respond_with(move |request: &wiremock::Request| {
            let range = request.headers.get("range").unwrap().to_str().unwrap();
            let (start, end) = range
                .strip_prefix("bytes=")
                .unwrap()
                .split_once('-')
                .unwrap();
            let start: usize = start.parse().unwrap();
            let end: usize = end.parse().unwrap();
            ResponseTemplate::new(206)
                .insert_header(
                    "Content-Range",
                    format!("bytes {start}-{end}/{}", payload.len()),
                )
                .set_body_bytes(payload[start..=end].to_vec())
        })
        .mount(server)
        .await;
}

async fn adjacent_chunk_fixture(
    server: &MockServer,
    count: usize,
    size: usize,
    order: &[usize],
    requested: Option<Range<u64>>,
) -> Vec<u8> {
    let chunks: Vec<Vec<u8>> = (0..count)
        .map(|i| vec![u8::try_from(i % 251).unwrap(); size])
        .collect();
    let serialized_size = serialize_payload(&[&chunks[0]]).len();
    let payload = serialize_payload(&chunks.iter().map(Vec::as_slice).collect::<Vec<_>>());
    let expected: Vec<u8> = order
        .iter()
        .flat_map(|i| chunks[*i].iter().copied())
        .collect();
    let terms: serde_json::Value = order
            .iter()
            .map(
                |i| json!({"hash": XORB_HASH,"unpacked_length":size,"range":{"start":i,"end":i.saturating_add(1)}}),
            )
            .collect();
    let ranges: serde_json::Value = (0..count).map(|i| json!({"chunks":{"start":i,"end":i.saturating_add(1)},"bytes":{"start":i.saturating_mul(serialized_size),"end":i.saturating_add(1).saturating_mul(serialized_size).saturating_sub(1)}})).collect();
    let start = requested.as_ref().map_or(0, |range| range.start);
    let end = requested
        .as_ref()
        .map_or(4095, |range| range.end.saturating_sub(1));
    reconstruction_mock(server,start,end,start,terms,json!({XORB_HASH:[{"url":format!("{}/transfer/xorb/default/{XORB_HASH}",server.uri()),"ranges":ranges}]})).await;
    xorb_payload_mock(server, payload).await;
    requested.map_or_else(
        || expected.clone(),
        |range| expected[range.start as usize..range.end as usize].to_vec(),
    )
}

#[tokio::test]
async fn streaming_coalesces_actual_http_requests_with_partial_slices() {
    let server = MockServer::start().await;
    let range = 3..507;
    let expected = adjacent_chunk_fixture(
        &server,
        64,
        8,
        &(0..64).collect::<Vec<_>>(),
        Some(range.clone()),
    )
    .await;
    let ctx = test_stream_context(&server, 256);
    let (output, _) = tokio::time::timeout(
        Duration::from_secs(3),
        drain(make_stream(ctx.clone(), Some(range))),
    )
    .await
    .unwrap();
    assert_eq!(output, expected);
    let requests = server.received_requests().await.unwrap();
    let fetches = requests
        .iter()
        .filter(|request| request.url.path().starts_with("/transfer/xorb/"))
        .count();
    assert_eq!(
        fetches, 2,
        "64 tiny descriptors should become two bounded block fetches"
    );
    assert_eq!(ctx.xorb_fetch_count.load(Ordering::Relaxed), 2);
    assert_eq!(ctx.buffer_semaphore.available_permits(), 256);
}

#[tokio::test]
async fn streaming_gapped_and_repeated_ranges_do_not_pin_old_blocks() {
    let server = MockServer::start().await;
    let expected = adjacent_chunk_fixture(&server, 6, 8, &[0, 1, 4, 5, 0, 1], None).await;
    let ctx = test_stream_context(&server, 16);
    let (output, _) = tokio::time::timeout(
        Duration::from_secs(3),
        drain(make_stream(ctx.clone(), None)),
    )
    .await
    .unwrap();
    assert_eq!(output, expected);
    assert_eq!(ctx.xorb_fetch_count.load(Ordering::Relaxed), 3);
    assert_eq!(ctx.buffer_semaphore.available_permits(), 16);
}

#[tokio::test]
async fn streaming_tiny_terms_bound_queued_work_until_consumer_advances() {
    let server = MockServer::start().await;
    let expected =
        adjacent_chunk_fixture(&server, 512, 1, &(0..512).collect::<Vec<_>>(), None).await;
    let ctx = test_stream_context(&server, 4096);
    let mut stream = make_stream(ctx.clone(), None);
    stream.start();
    tokio::time::timeout(Duration::from_secs(3), async {
        while stream
            .run_state
            .total_bytes_scheduled
            .load(Ordering::Relaxed)
            < MAX_BUFFERED_TERMS as u64
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        stream
            .run_state
            .total_bytes_scheduled
            .load(Ordering::Relaxed),
        MAX_BUFFERED_TERMS as u64
    );
    assert_eq!(
        ctx.buffer_semaphore.total_permits() - ctx.buffer_semaphore.available_permits(),
        128
    );
    let (output, _) = tokio::time::timeout(Duration::from_secs(3), drain(stream))
        .await
        .unwrap();
    assert_eq!(output, expected);
    assert_eq!(ctx.xorb_fetch_count.load(Ordering::Relaxed), 4);
    // MockServer::drop synchronously polls a Tokio RwLock. A fully drained
    // cooperative budget can defer its wake onto this same runtime, which
    // cannot advance while Drop is blocking. Force that condition, then
    // make only fixture teardown unconstrained.
    while tokio::task::coop::has_budget_remaining() {
        tokio::task::consume_budget().await;
    }
    assert!(!tokio::task::coop::has_budget_remaining());
    tokio::task::unconstrained(async { drop(server) }).await;
}

#[tokio::test]
async fn cancelling_stream_releases_fetch_and_buffer_without_detached_tasks() {
    let server = MockServer::start().await;
    let payload = serialize_payload(&[b"payload"]);
    reconstruction_mock(&server,0,4095,0,json!([{"hash":XORB_HASH,"unpacked_length":7,"range":{"start":0,"end":1}}]),
            json!({XORB_HASH:[{"url":format!("{}/transfer/xorb/default/{XORB_HASH}",server.uri()),"ranges":[{"chunks":{"start":0,"end":1},"bytes":{"start":0,"end":payload.len()-1}}]}]})).await;
    Mock::given(method("GET"))
        .and(path(format!("/transfer/xorb/default/{XORB_HASH}")))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header(
                    "Content-Range",
                    format!("bytes 0-{}/{}", payload.len() - 1, payload.len()),
                )
                .set_body_bytes(payload)
                .set_delay(Duration::from_secs(2)),
        )
        .mount(&server)
        .await;
    let ctx = test_stream_context(&server, 128);
    let mut stream = make_stream(ctx.clone(), None);
    stream.start();
    tokio::time::timeout(Duration::from_secs(1), async {
        while ctx.download_permits.available_permits() == DEFAULT_DOWNLOAD_CONCURRENCY {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    drop(stream);
    tokio::time::timeout(Duration::from_millis(250), async {
        while ctx.download_permits.available_permits() != DEFAULT_DOWNLOAD_CONCURRENCY
            || ctx.buffer_semaphore.available_permits() != 128
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("cancelled stream left an untracked fetch holding permits");
    assert_eq!(ctx.xorb_fetch_count.load(Ordering::Relaxed), 0);
}

fn make_stream(ctx: StreamContext, range: Option<Range<u64>>) -> DownloadStream {
    let mut reconstructor = FileReconstructor::new(ctx, FILE_ID.to_owned(), READ_TOKEN.to_owned());
    if let Some(range) = range {
        reconstructor = reconstructor.with_byte_range(range);
    }
    reconstructor.reconstruct_to_stream()
}

fn make_unordered_stream(ctx: StreamContext, range: Option<Range<u64>>) -> UnorderedDownloadStream {
    let mut reconstructor = FileReconstructor::new(ctx, FILE_ID.to_owned(), READ_TOKEN.to_owned());
    if let Some(range) = range {
        reconstructor = reconstructor.with_byte_range(range);
    }
    reconstructor.reconstruct_to_unordered_stream()
}

async fn drain(mut stream: DownloadStream) -> (Vec<u8>, Vec<u64>) {
    let mut out = Vec::new();
    let mut sizes = Vec::new();
    while let Some(chunk) = stream.next().await.unwrap() {
        sizes.push(u64::try_from(chunk.len()).unwrap());
        out.extend_from_slice(&chunk);
    }
    (out, sizes)
}

// ── BufferSemaphore ─────────────────────────────────────────────────────

#[tokio::test]
async fn buffer_semaphore_acquire_release_bounds_in_flight() {
    let sem = Arc::new(BufferSemaphore::new(100, 100, 100));
    assert_eq!(sem.total_permits(), 100);
    assert_eq!(sem.available_permits(), 100);

    let mut p1 = sem.acquire_many(60).await.unwrap();
    assert!(p1.split(0).is_none());
    assert!(p1.split(61).is_none());
    assert_eq!(p1.num_permits(), 60);
    let split = p1.split(60).unwrap();
    assert_eq!(p1.num_permits(), 0);
    assert_eq!(split.num_permits(), 60);
    assert_eq!(sem.available_permits(), 40);
    let p2 = sem.acquire_many(40).await.unwrap();
    assert_eq!(sem.available_permits(), 0);
    drop(p1);
    assert_eq!(sem.available_permits(), 0);
    drop(split);
    assert_eq!(sem.available_permits(), 60);
    drop(p2);
    assert_eq!(sem.available_permits(), 100);
}

#[test]
fn buffer_semaphore_scales_with_active_download_target() {
    let base = 100u64;
    let perfile = 50u64;
    let limit = 250u64;
    let sem = Arc::new(BufferSemaphore::new(base, base, limit));
    assert_eq!(sem.total_permits(), base);

    // One active download: target = base + perfile.
    let seed = sem.increment_permits_to_target(base + perfile);
    assert!(seed.is_some());
    assert_eq!(sem.total_permits(), 150);

    // Splitting the virtual permit gives immediate (queue-bypassing) access.
    let mut seed = seed.unwrap();
    assert!(seed.split(0).is_none());
    assert!(seed.split(51).is_none());
    assert_eq!(seed.num_permits(), 50);
    let term = seed.split(40).unwrap();
    assert_eq!(term.num_permits(), 40);
    assert_eq!(sem.total_permits(), 150);
    drop(term);
    // Dropping the virtual remainder enters the semaphore.
    drop(seed);
    assert_eq!(sem.available_permits(), 150);

    // Two active downloads: target = base + 2*perfile = 200.
    let _seed2 = sem.increment_permits_to_target(base + perfile.saturating_mul(2));
    assert_eq!(sem.total_permits(), 200);

    // Shrink back to base on exit.
    assert!(sem.decrement_permits_to_target(base).is_some());
    assert_eq!(sem.total_permits(), 100);
}

#[test]
fn buffer_semaphore_clamps_to_max_and_min() {
    let sem = Arc::new(BufferSemaphore::new(50, 50, 100));
    let _ = sem.increment_permits_to_target(10_000);
    assert_eq!(sem.total_permits(), 100);
    assert!(sem.increment_permits_to_target(100).is_none());
    let _ = sem.decrement_permits_to_target(0);
    assert_eq!(sem.total_permits(), 50);
}

#[tokio::test]
async fn buffer_semaphore_large_capacity_uses_scaling_basis() {
    // 8 GiB exceeds u32::MAX, so a basis > 1 is required; acquisitions of
    // arbitrarily large term sizes must still succeed.
    let sem = Arc::new(BufferSemaphore::new(
        DEFAULT_DOWNLOAD_BUFFER_SIZE,
        DEFAULT_DOWNLOAD_BUFFER_SIZE,
        DEFAULT_DOWNLOAD_BUFFER_LIMIT,
    ));
    // ceil(8 GiB / basis) must fit u32::MAX: basis = 4.
    assert_eq!(sem.basis, 4);
    let p = sem
        .acquire_many(DEFAULT_DOWNLOAD_BUFFER_SIZE)
        .await
        .unwrap();
    assert!(p.num_permits() >= DEFAULT_DOWNLOAD_BUFFER_SIZE);
    drop(p);
    assert_eq!(sem.available_permits(), DEFAULT_DOWNLOAD_BUFFER_SIZE);
}

// ── RunState ────────────────────────────────────────────────────────────

#[tokio::test]
async fn run_state_error_surfaces_via_check_error() {
    let state = RunState::new(CancellationToken::new());
    assert!(state.check_error().is_ok());
    state.set_error(SdxError::StreamInternal("boom".to_owned()));
    let error = state.check_error().unwrap_err();
    assert!(matches!(error, SdxError::StreamInternal(_)));
    // Cancellation is triggered by the error.
    state.cancelled().await;
}

#[test]
fn background_checks_preserve_first_typed_error_for_consumer() {
    let state = RunState::new(CancellationToken::new());
    state.set_error(SdxError::Transfer(TransferError::InvalidResponse(
        "original".to_owned(),
    )));
    assert!(state.check_background_error().is_err());
    assert!(state.check_run_state().is_err());
    assert!(
        matches!(state.check_error(), Err(SdxError::Transfer(TransferError::InvalidResponse(message))) if message == "original")
    );
    assert!(state.check_error().is_ok());
}

#[tokio::test]
async fn run_state_cancel_returns_cancelled() {
    let state = RunState::new(CancellationToken::new());
    state.cancel();
    assert!(matches!(state.check_run_state(), Err(RunError::Cancelled)));
}

#[tokio::test]
async fn run_state_tracks_scheduled_and_delivered_bytes() {
    let state = RunState::new(CancellationToken::new());
    state.record_new_term(100);
    state.record_new_term(50);
    assert_eq!(state.total_bytes_scheduled(), 150);
    state.report_bytes_written(75);
    assert_eq!(state.total_bytes_delivered(), 75);
}

// ── SequentialWriter (unit) ─────────────────────────────────────────────

fn immediate_future(data: Bytes) -> DataFuture {
    Box::pin(async move { Ok(data) })
}

fn delayed_future(data: Bytes, delay: Duration) -> DataFuture {
    Box::pin(async move {
        tokio::time::sleep(delay).await;
        Ok(data)
    })
}

#[tokio::test]
async fn sequential_writer_streaming_orders_chunks() {
    let state = RunState::new(CancellationToken::new());
    let (mut writer, mut receiver) = SequentialWriter::new_streaming(state);

    writer
        .set_next_term_data_source(
            0..5,
            None,
            delayed_future(Bytes::from("Hello"), Duration::from_millis(50)),
        )
        .await
        .unwrap();
    writer
        .set_next_term_data_source(
            5..6,
            None,
            delayed_future(Bytes::from(" "), Duration::from_millis(10)),
        )
        .await
        .unwrap();
    writer
        .set_next_term_data_source(6..11, None, immediate_future(Bytes::from("World")))
        .await
        .unwrap();
    let total = writer.finish().await.unwrap();
    assert_eq!(total, 11);

    let mut out = Vec::new();
    while let Some(item) = receiver.recv().await {
        match item {
            SequentialRetrievalItem::Data { receiver, permit } => {
                let data = receiver.await.unwrap();
                out.extend_from_slice(&data);
                drop(permit);
            }
            SequentialRetrievalItem::Finish => break,
        }
    }
    assert_eq!(&out, b"Hello World");
}

struct CooperativeSink {
    always_interrupted: bool,
    interrupt_once: bool,
    permanent_error: bool,
    flush_error: bool,
    gate: Option<std::sync::mpsc::Receiver<()>>,
    started: Option<oneshot::Sender<()>>,
    dropped: Option<oneshot::Sender<()>>,
    written: Arc<AtomicU64>,
}

impl Write for CooperativeSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Some(started) = self.started.take() {
            let _sent = started.send(());
        }
        if let Some(gate) = self.gate.take() {
            gate.recv().map_err(std::io::Error::other)?;
        }
        if self.always_interrupted || std::mem::take(&mut self.interrupt_once) {
            return Err(std::io::ErrorKind::Interrupted.into());
        }
        if self.permanent_error {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "owned sink failure",
            ));
        }
        let count = bytes.len().min(3);
        self.written
            .fetch_add(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.flush_error {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "owned flush failure",
            ))
        } else {
            Ok(())
        }
    }
}

impl Drop for CooperativeSink {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _sent = dropped.send(());
        }
    }
}

#[tokio::test]
async fn cooperative_writer_cancelled_finish_stops_retries_and_releases_permits() {
    for vectorized in [false, true] {
        for gated in [false, true] {
            let state = RunState::new(CancellationToken::new());
            let semaphore = Arc::new(BufferSemaphore::new(64, 64, 64));
            let written = Arc::new(AtomicU64::new(0));
            let (started_tx, started_rx) = oneshot::channel();
            let (dropped_tx, dropped_rx) = oneshot::channel();
            let (release_tx, gate_rx) = std::sync::mpsc::channel();
            let sink = CooperativeSink {
                always_interrupted: !gated,
                interrupt_once: false,
                permanent_error: false,
                flush_error: false,
                gate: gated.then_some(gate_rx),
                started: Some(started_tx),
                dropped: Some(dropped_tx),
                written: written.clone(),
            };
            let mut writer = SequentialWriter::new(sink, vectorized, state.clone());
            writer
                .set_next_term_data_source(
                    0..64,
                    Some(semaphore.acquire_many(64).await.unwrap()),
                    immediate_future(Bytes::from(vec![7u8; 64])),
                )
                .await
                .unwrap();
            let finishing = tokio::spawn(writer.finish());
            tokio::time::timeout(Duration::from_secs(5), started_rx)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(semaphore.available_permits(), 0);
            finishing.abort();
            assert!(finishing.await.unwrap_err().is_cancelled());
            assert!(state.is_cancelled());
            if gated {
                release_tx.send(()).unwrap();
            }
            tokio::time::timeout(Duration::from_secs(5), dropped_rx)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(written.load(Ordering::Relaxed), if gated { 3 } else { 0 });
            assert_eq!(semaphore.available_permits(), 64);
        }
    }
}

#[tokio::test]
async fn cooperative_writer_preserves_short_writes_interrupted_and_typed_errors() {
    for vectorized in [false, true] {
        for (permanent_error, flush_error) in [(false, false), (true, false), (false, true)] {
            let state = RunState::new(CancellationToken::new());
            let written = Arc::new(AtomicU64::new(0));
            let sink = CooperativeSink {
                always_interrupted: false,
                interrupt_once: true,
                permanent_error,
                flush_error,
                gate: None,
                started: None,
                dropped: None,
                written: written.clone(),
            };
            let mut writer = SequentialWriter::new(sink, vectorized, state.clone());
            writer
                .set_next_term_data_source(
                    0..64,
                    None,
                    immediate_future(Bytes::from(vec![7u8; 64])),
                )
                .await
                .unwrap();
            let result = writer.finish().await;
            if permanent_error || flush_error {
                assert!(result.is_err());
                assert!(
                    matches!(state.check_error(), Err(SdxError::Io(error)) if error.kind() == std::io::ErrorKind::BrokenPipe && error.to_string() == if permanent_error { "owned sink failure" } else { "owned flush failure" })
                );
            } else {
                assert_eq!(result.unwrap(), 64);
                assert_eq!(written.load(Ordering::Relaxed), 64);
                assert!(!state.is_cancelled());
            }
        }
    }
}

#[tokio::test]
async fn sequential_writer_rejects_non_sequential_range() {
    let state = RunState::new(CancellationToken::new());
    let (mut writer, _receiver) = SequentialWriter::new_streaming(state);
    writer
        .set_next_term_data_source(0..5, None, immediate_future(Bytes::from("Hello")))
        .await
        .unwrap();
    let result = writer
        .set_next_term_data_source(10..15, None, immediate_future(Bytes::from("World")))
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn sequential_writer_reports_size_mismatch() {
    let state = RunState::new(CancellationToken::new());
    let (mut writer, _receiver) = SequentialWriter::new_streaming(state);
    writer
        .set_next_term_data_source(0..10, None, immediate_future(Bytes::from("Hi")))
        .await
        .unwrap();
    let result = writer.finish().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn sequential_writer_releases_permit_after_write() {
    let sem = Arc::new(BufferSemaphore::new(10, 10, 10));
    let state = RunState::new(CancellationToken::new());
    let (mut writer, mut receiver) = SequentialWriter::new_streaming(state);

    let permit1 = sem.acquire_many(5).await.unwrap();
    let permit2 = sem.acquire_many(5).await.unwrap();
    assert_eq!(sem.available_permits(), 0);

    writer
        .set_next_term_data_source(0..5, Some(permit1), immediate_future(Bytes::from("Hello")))
        .await
        .unwrap();
    writer
        .set_next_term_data_source(5..6, Some(permit2), immediate_future(Bytes::from(" ")))
        .await
        .unwrap();
    writer.finish().await.unwrap();

    // Permits are only released after the consumer pulls the bytes.
    assert_eq!(sem.available_permits(), 0);
    while let Some(item) = receiver.recv().await {
        match item {
            SequentialRetrievalItem::Data { receiver, permit } => {
                let _ = receiver.await.unwrap();
                drop(permit);
            }
            SequentialRetrievalItem::Finish => break,
        }
    }
    assert_eq!(sem.available_permits(), 10);
}

#[tokio::test]
async fn sequential_writer_background_thread_writes_and_flushes() {
    let state = RunState::new(CancellationToken::new());
    let mut writer = SequentialWriter::new(Vec::new(), false, state);
    writer
        .set_next_term_data_source(0..5, None, immediate_future(Bytes::from("Hello")))
        .await
        .unwrap();
    writer
        .set_next_term_data_source(5..11, None, immediate_future(Bytes::from(" World")))
        .await
        .unwrap();
    let total = writer.finish().await.unwrap();
    assert_eq!(total, 11);
}

// ── UnorderedWriter (unit) ──────────────────────────────────────────────

#[tokio::test]
async fn unordered_writer_yields_completion_order_with_offsets() {
    let state = RunState::new(CancellationToken::new());
    let (mut writer, mut receiver, _progress) = UnorderedWriter::new_streaming(state);

    writer
        .set_next_term_data_source(
            0..5,
            None,
            delayed_future(Bytes::from("Hello"), Duration::from_millis(80)),
        )
        .await
        .unwrap();
    writer
        .set_next_term_data_source(
            5..6,
            None,
            delayed_future(Bytes::from(" "), Duration::from_millis(40)),
        )
        .await
        .unwrap();
    writer
        .set_next_term_data_source(6..11, None, immediate_future(Bytes::from("World")))
        .await
        .unwrap();
    writer.finish().await.unwrap();

    let mut items = Vec::new();
    while let Some(term) = receiver.recv().await {
        items.push((term.byte_range.start, term.data.to_vec()));
        drop(term.permit);
    }
    items.sort_by_key(|(offset, _)| *offset);
    let out: Vec<u8> = items.into_iter().flat_map(|(_, data)| data).collect();
    assert_eq!(&out, b"Hello World");
}

// ── Full-stream wiremock tests ──────────────────────────────────────────

#[tokio::test]
async fn download_stream_yields_chunks_in_order() {
    let server = MockServer::start().await;
    let chunk_a = vec![7u8; 64];
    let chunk_b = vec![9u8; 64];
    let chunk_c = vec![3u8; 64];
    let payload = serialize_payload(&[&chunk_a, &chunk_b, &chunk_c]);

    reconstruction_mock(
            &server,
            0,
            4095,
            0,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}},
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 1, "end": 2}},
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 2, "end": 3}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 3}, "bytes": {"start": 0, "end": payload.len() - 1}}
                    ]
                }]
            }),
        )
        .await;
    xorb_range_mock(&server, 0, (payload.len() - 1) as u64, payload).await;

    let ctx = test_stream_context(&server, 1_048_576);
    let (out, _sizes) = drain(make_stream(ctx, None)).await;
    let mut expected = chunk_a.clone();
    expected.extend_from_slice(&chunk_b);
    expected.extend_from_slice(&chunk_c);
    assert_eq!(out, expected);
}

async fn assert_reservation_retry_after_failed_init(cancel: bool) {
    let server = MockServer::start().await;
    let decoded = vec![7; 128];
    let payload = serialize_payload(&[&decoded]);
    let response = ResponseTemplate::new(206)
        .insert_header(
            "Content-Range",
            format!(
                "bytes 0-{}/{}",
                payload.len().saturating_sub(1),
                payload.len()
            ),
        )
        .set_body_bytes(vec![0; payload.len()]);
    Mock::given(method("GET"))
        .respond_with(if cancel {
            response.set_delay(Duration::from_secs(5))
        } else {
            response
        })
        .mount(&server)
        .await;
    let mut ctx = test_stream_context(&server, 4096);
    let block = Arc::new(XorbBlock {
        hash: XORB_HASH.to_owned(),
        chunk_range: (0, 1),
        url: server.uri(),
        bytes: ByteRange::new(0, (payload.len().saturating_sub(1)) as u64),
        data: OnceCell::new(),
        decoded_budget: 128,
        reservation: Mutex::new(None),
        reservation_started: AtomicBool::new(false),
        publication_gate: None,
    });
    let loader_block = Arc::clone(&block);
    let loader_context = ctx.clone();
    let loader = tokio::spawn(async move {
        loader_block
            .retrieve_data(&loader_context, READ_TOKEN)
            .await
    });
    if cancel {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if !server.received_requests().await.unwrap().is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(block.reservation_started.load(Ordering::Acquire));
        assert_eq!(
            ctx.buffer_semaphore.available_permits(),
            ctx.buffer_semaphore.total_permits().saturating_sub(128)
        );
        loader.abort();
        assert!(matches!(loader.await, Err(error) if error.is_cancelled()));
    } else {
        assert!(loader.await.unwrap().is_err());
    }
    assert!(block.data.get().is_none());
    assert!(!block.reservation_started.load(Ordering::Acquire));
    assert!(block.reservation.lock().unwrap().is_none());
    assert_eq!(
        ctx.buffer_semaphore.available_permits(),
        ctx.buffer_semaphore.total_permits()
    );
    let directory = tempfile::tempdir().unwrap();
    let cache = Arc::new(ChunkCache::new(directory.path(), 1024).unwrap());
    cache
        .put(XORB_HASH, (0, 1), &[0, 128], &decoded)
        .await
        .unwrap();
    ctx.chunk_cache = Some(cache);
    let data = block.clone().retrieve_data(&ctx, READ_TOKEN).await.unwrap();
    assert_eq!(data.data.as_ref(), decoded.as_slice());
    assert!(block.reservation_started.load(Ordering::Acquire));
    assert_eq!(
        ctx.buffer_semaphore.available_permits(),
        ctx.buffer_semaphore.total_permits().saturating_sub(128)
    );
    drop(data);
    drop(block);
    assert_eq!(
        ctx.buffer_semaphore.available_permits(),
        ctx.buffer_semaphore.total_permits()
    );
}

#[tokio::test]
async fn error_initialization_releases_reservation_and_allows_retry() {
    assert_reservation_retry_after_failed_init(false).await;
}

#[tokio::test]
async fn cancelled_initialization_releases_reservation_and_allows_retry() {
    assert_reservation_retry_after_failed_init(true).await;
}

async fn assert_reservation_publication(cache_hit: bool) {
    let server = MockServer::start().await;
    let decoded = vec![7; 128];
    let payload = serialize_payload(&[&decoded]);
    let mut ctx = test_stream_context(&server, 4096);
    let directory = tempfile::tempdir().unwrap();
    if cache_hit {
        let cache = Arc::new(ChunkCache::new(directory.path(), 1024).unwrap());
        cache
            .put(XORB_HASH, (0, 1), &[0, 128], &decoded)
            .await
            .unwrap();
        ctx.chunk_cache = Some(cache);
    } else {
        xorb_range_mock(
            &server,
            0,
            (payload.len().saturating_sub(1)) as u64,
            payload.clone(),
        )
        .await;
    }
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let block = Arc::new(XorbBlock {
        hash: XORB_HASH.to_owned(),
        chunk_range: (0, 1),
        url: format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
        bytes: ByteRange::new(0, (payload.len().saturating_sub(1)) as u64),
        data: OnceCell::new(),
        decoded_budget: 128,
        reservation: Mutex::new(None),
        reservation_started: AtomicBool::new(false),
        publication_gate: Some(Arc::new(PublicationGate {
            ready: Mutex::new(Some(ready_tx)),
            release: Mutex::new(release_rx),
        })),
    });
    block.reserve(&ctx, &mut None).await.unwrap();
    let loader_block = Arc::clone(&block);
    let loader_context = ctx.clone();
    let loader = tokio::spawn(async move {
        loader_block
            .retrieve_data(&loader_context, READ_TOKEN)
            .await
    });
    ready_rx.await.unwrap();
    assert!(block.data.get().is_none());
    block.reserve(&ctx, &mut None).await.unwrap();
    let used = ctx
        .buffer_semaphore
        .total_permits()
        .saturating_sub(ctx.buffer_semaphore.available_permits());
    let unused_reservation = block.reservation.lock().unwrap().is_some();
    release_tx.send(()).unwrap();
    let data = loader.await.unwrap().unwrap();
    assert_eq!(data.data.as_ref(), decoded.as_slice());
    assert_eq!(
        used, 128,
        "one backing block must retain exactly one reservation before publication"
    );
    assert!(!unused_reservation);
    assert_eq!(
        ctx.xorb_fetch_count.load(Ordering::Relaxed),
        u64::from(!cache_hit)
    );
    drop(data);
    drop(block);
    assert_eq!(
        ctx.buffer_semaphore.available_permits(),
        ctx.buffer_semaphore.total_permits()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_hit_reservation_publication_keeps_single_permit() {
    assert_reservation_publication(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn network_reservation_publication_keeps_single_permit() {
    assert_reservation_publication(false).await;
}

#[tokio::test]
async fn download_stream_with_chunk_cache_serves_second_download_from_disk() {
    let server = MockServer::start().await;
    let chunk_a = vec![7u8; 64];
    let chunk_b = vec![9u8; 64];
    let payload = serialize_payload(&[&chunk_a, &chunk_b]);

    reconstruction_mock(
            &server,
            0,
            4095,
            0,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}},
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 1, "end": 2}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 2}, "bytes": {"start": 0, "end": payload.len() - 1}}
                    ]
                }]
            }),
        )
        .await;
    xorb_range_mock(&server, 0, (payload.len() - 1) as u64, payload).await;

    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(crate::cache::ChunkCache::new(dir.path().to_path_buf(), 1 << 20).unwrap());
    let mut ctx = test_stream_context(&server, 1_048_576);
    ctx.chunk_cache = Some(cache.clone());
    let mut expected = chunk_a.clone();
    expected.extend_from_slice(&chunk_b);

    // First download: cache miss, one network fetch.
    let (first, _) = drain(make_stream(ctx.clone(), None)).await;
    assert_eq!(first, expected);
    assert_eq!(ctx.xorb_fetch_count.load(Ordering::Relaxed), 1);
    // The cache put is best-effort; wait for it to land. Generous
    // budget (5 s) under slow instrumentation.
    for _ in 0..500 {
        if cache.entry_count().await.unwrap() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(cache.entry_count().await.unwrap(), 1);

    // Second download: cache hit, no additional network fetch.
    let (second, _) = drain(make_stream(ctx.clone(), None)).await;
    assert_eq!(second, expected);
    assert_eq!(ctx.xorb_fetch_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn streaming_xorb_rejects_full_body_for_nonzero_range() {
    let server = MockServer::start().await;
    let first = serialize_payload(&[b"AAAA"]);
    let second = serialize_payload(&[b"BBBB"]);
    let mut full = first.clone();
    full.extend_from_slice(&second);
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(full.clone()))
        .mount(&server)
        .await;
    let block = Arc::new(XorbBlock {
        hash: XORB_HASH.to_owned(),
        chunk_range: (1, 2),
        url: server.uri(),
        bytes: ByteRange::new(first.len() as u64, full.len() as u64 - 1),
        data: OnceCell::new(),
        decoded_budget: 1_048_576,
        reservation: Mutex::new(None),
        reservation_started: AtomicBool::new(false),
        #[cfg(test)]
        publication_gate: None,
    });
    let result = block
        .clone()
        .retrieve_data(&test_stream_context(&server, 1_048_576), READ_TOKEN)
        .await;
    assert!(matches!(
        result,
        Err(SdxError::Transfer(TransferError::InvalidResponse(_)))
    ));
    assert!(block.data.get().is_none());
}

fn queued_download_stream(
    receiver: UnboundedReceiver<SequentialRetrievalItem>,
    run_state: Arc<RunState>,
) -> DownloadStream {
    DownloadStream {
        receiver,
        pending_data: None,
        finished: false,
        run_state,
        start_signal: None,
        #[cfg(not(target_family = "wasm"))]
        runtime: test_runtime(),
    }
}

#[tokio::test]
async fn timed_out_next_preserves_queued_and_active_data() {
    let state = RunState::new(CancellationToken::new());
    let semaphore = Arc::new(BufferSemaphore::new(1, 1, 1));
    let (tx, rx) = unbounded_channel();
    let mut stream = queued_download_stream(rx, state);
    let deadline = std::time::Duration::from_millis(1);
    // No item has been queued: cancelling the queue wait loses nothing.
    assert!(tokio::time::timeout(deadline, stream.next()).await.is_err());
    let (data_tx, data_rx) = oneshot::channel();
    tx.send(SequentialRetrievalItem::Data {
        receiver: data_rx,
        permit: Some(semaphore.acquire_many(1).await.unwrap()),
    })
    .unwrap();
    tx.send(SequentialRetrievalItem::Finish).unwrap();
    // The item is dequeued but its bytes have not arrived. Repeated
    // consumer timeouts must retain both the item and its buffer permit.
    for _ in 0..2 {
        assert!(tokio::time::timeout(deadline, stream.next()).await.is_err());
        assert_eq!(semaphore.available_permits(), 0);
        assert!(!data_tx.is_closed());
    }
    let expected = Bytes::from_static(b"preserved download chunk");
    data_tx.send(expected.clone()).unwrap();
    assert_eq!(stream.next().await.unwrap(), Some(expected));
    assert_eq!(semaphore.available_permits(), 1);
    assert!(stream.next().await.unwrap().is_none());
}

#[tokio::test]
async fn timed_out_next_preserves_background_error() {
    let state = RunState::new(CancellationToken::new());
    let (tx, rx) = unbounded_channel();
    let (data_tx, data_rx) = oneshot::channel();
    tx.send(SequentialRetrievalItem::Data {
        receiver: data_rx,
        permit: None,
    })
    .unwrap();
    tx.send(SequentialRetrievalItem::Finish).unwrap();
    let mut stream = queued_download_stream(rx, state.clone());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(1), stream.next())
            .await
            .is_err()
    );
    // A queued Finish cannot bypass the still-pending term while the
    // producer has not yet published its failure.
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(1), stream.next())
            .await
            .is_err()
    );
    state.set_error(SdxError::StreamInternal(
        "delayed producer error".to_owned(),
    ));
    drop(data_tx);
    assert!(
        matches!(stream.next().await, Err(SdxError::StreamInternal(message))
            if message == "delayed producer error")
    );
    assert!(stream.next().await.unwrap().is_none());
}

#[tokio::test]
async fn cancel_and_drop_release_timed_out_pending_data() {
    for cancel in [true, false] {
        let state = RunState::new(CancellationToken::new());
        let semaphore = Arc::new(BufferSemaphore::new(1, 1, 1));
        let (tx, rx) = unbounded_channel();
        let (data_tx, data_rx) = oneshot::channel();
        tx.send(SequentialRetrievalItem::Data {
            receiver: data_rx,
            permit: Some(semaphore.acquire_many(1).await.unwrap()),
        })
        .unwrap();
        let mut stream = queued_download_stream(rx, state);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(1), stream.next())
                .await
                .is_err()
        );
        if cancel {
            stream.cancel();
            // Cancellation releases pending data while the stream is alive.
            assert_eq!(semaphore.available_permits(), 1);
            assert!(data_tx.is_closed());
            assert!(stream.next().await.unwrap().is_none());
        }
        drop(stream);
        assert_eq!(semaphore.available_permits(), 1);
        assert!(data_tx.is_closed());
    }
}

#[tokio::test]
async fn cancelled_stream_with_closed_data_sender_finishes_cleanly() {
    let state = RunState::new(CancellationToken::new());
    let semaphore = Arc::new(BufferSemaphore::new(1, 1, 1));
    let permit = semaphore.acquire_many(1).await.unwrap();
    let (data_tx, data_rx) = oneshot::channel();
    let (tx, rx) = unbounded_channel();
    tx.send(SequentialRetrievalItem::Data {
        receiver: data_rx,
        permit: Some(permit),
    })
    .unwrap();
    // Both select branches are ready: producer shutdown dropped the
    // sender, and external cancellation was already requested.
    state.cancel();
    drop(data_tx);
    let mut stream = DownloadStream {
        receiver: rx,
        pending_data: None,
        finished: false,
        run_state: state,
        start_signal: None,
        #[cfg(not(target_family = "wasm"))]
        runtime: test_runtime(),
    };
    assert!(stream.next().await.unwrap().is_none());
    assert_eq!(semaphore.available_permits(), 1);
}

#[tokio::test]
async fn download_stream_reports_incompatible_full_body_response() {
    let server = MockServer::start().await;
    let first = serialize_payload(&[b"AAAA"]);
    let mut full = first.clone();
    full.extend_from_slice(&serialize_payload(&[b"BBBB"]));
    reconstruction_mock(&server, 0, 3, 0,
            json!([{ "hash": XORB_HASH, "unpacked_length": 4, "range": {"start": 1, "end": 2}}]),
            json!({XORB_HASH: [{ "url": format!("{}/xorb", server.uri()),
                "ranges": [{"chunks": {"start": 1, "end": 2}, "bytes": {"start": first.len(), "end": full.len() - 1}}]
            }]})).await;
    Mock::given(method("GET"))
        .and(path("/xorb"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(full))
        .mount(&server)
        .await;
    let mut stream = make_stream(test_stream_context(&server, 1_048_576), Some(0..4));
    let result = stream.next().await;
    assert!(
        matches!(
            result,
            Err(SdxError::Transfer(TransferError::InvalidResponse(_)))
        ),
        "unexpected stream result: {result:?}"
    );
}

#[tokio::test]
async fn streaming_xorb_rejects_unexpected_chunk_count_before_caching() {
    let server = MockServer::start().await;
    let full = serialize_payload(&[b"AAAA", b"BBBB"]);
    let range = ByteRange::new(0, full.len() as u64 - 1);
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header(
                    "Content-Range",
                    format!("bytes 0-{}/{}", range.end, full.len()),
                )
                .set_body_bytes(full),
        )
        .mount(&server)
        .await;
    let block = Arc::new(XorbBlock {
        hash: XORB_HASH.to_owned(),
        chunk_range: (1, 2),
        url: server.uri(),
        bytes: range,
        data: OnceCell::new(),
        decoded_budget: 1_048_576,
        reservation: Mutex::new(None),
        reservation_started: AtomicBool::new(false),
        #[cfg(test)]
        publication_gate: None,
    });
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(crate::cache::ChunkCache::new(dir.path().to_path_buf(), 1 << 20).unwrap());
    let mut ctx = test_stream_context(&server, 1_048_576);
    ctx.chunk_cache = Some(cache.clone());
    let result = block.retrieve_data(&ctx, READ_TOKEN).await;
    assert!(matches!(
        result,
        Err(SdxError::FetchChunkCountMismatch {
            expected: 1,
            actual: 2,
            ..
        })
    ));
    assert_eq!(cache.entry_count().await.unwrap(), 0);
}

#[tokio::test]
async fn download_stream_accepts_exact_full_body_response() {
    let server = MockServer::start().await;
    let payload = serialize_payload(&[b"AAAA", b"BBBB"]);
    reconstruction_mock(&server, 0, 4095, 0,
            json!([{ "hash": XORB_HASH, "unpacked_length": 8, "range": {"start": 0, "end": 2}}]),
            json!({XORB_HASH: [{ "url": format!("{}/xorb", server.uri()),
                "ranges": [{"chunks": {"start": 0, "end": 2}, "bytes": {"start": 0, "end": payload.len() - 1}}]
            }]})).await;
    Mock::given(method("GET"))
        .and(path("/xorb"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
        .mount(&server)
        .await;
    let (out, _) = drain(make_stream(test_stream_context(&server, 1_048_576), None)).await;
    assert_eq!(out, b"AAAABBBB");
}

#[tokio::test]
async fn download_stream_eof_after_last_chunk() {
    let server = MockServer::start().await;
    let chunk = vec![5u8; 64];
    let payload = serialize_payload(&[&chunk]);

    reconstruction_mock(
            &server,
            0,
            4095,
            0,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": payload.len() - 1}}
                    ]
                }]
            }),
        )
        .await;
    xorb_range_mock(&server, 0, (payload.len() - 1) as u64, payload).await;

    let ctx = test_stream_context(&server, 1_048_576);
    let mut stream = make_stream(ctx, None);
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first, chunk);
    // EOF at the end.
    assert!(stream.next().await.unwrap().is_none());
    assert!(stream.next().await.unwrap().is_none());
}

async fn pending_prefetch_task() -> (
    AbortOnDropHandle<Result<Option<TermBlock>, SdxError>>,
    oneshot::Receiver<()>,
) {
    let (started_tx, started_rx) = oneshot::channel();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let _guard = ExitGuard::new(move || {
            let _sent = dropped_tx.send(());
        });
        started_tx.send(()).unwrap();
        std::future::pending::<Result<Option<TermBlock>, SdxError>>().await
    });
    started_rx.await.unwrap();
    (AbortOnDropHandle::new(handle), dropped_rx)
}

#[tokio::test]
async fn dropping_term_manager_aborts_queued_prefetch() {
    let server = MockServer::start().await;
    let mut manager = ReconstructionTermManager::new(
        test_stream_context(&server, 1_048_576),
        FILE_ID.to_owned(),
        READ_TOKEN.to_owned(),
        0..0,
    );
    let (handle, dropped) = pending_prefetch_task().await;
    manager.prefetch_queue.push_back(handle);
    drop(manager);
    tokio::time::timeout(std::time::Duration::from_secs(1), dropped)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn cancelling_next_file_terms_aborts_actively_popped_prefetch() {
    let server = MockServer::start().await;
    let mut manager = ReconstructionTermManager::new(
        test_stream_context(&server, 1_048_576),
        FILE_ID.to_owned(),
        READ_TOKEN.to_owned(),
        0..0,
    );
    let (handle, dropped) = pending_prefetch_task().await;
    manager.prefetch_queue.push_back(handle);
    let mut next = Box::pin(manager.next_file_terms());
    std::future::poll_fn(|cx| {
        assert!(next.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    // Cancel the awaiting operation while keeping the manager alive:
    // aborting only the remaining queue would miss this popped handle.
    drop(next);
    assert!(manager.prefetch_queue.is_empty());
    tokio::time::timeout(std::time::Duration::from_secs(1), dropped)
        .await
        .unwrap()
        .unwrap();
    drop(manager);
}

#[tokio::test]
async fn download_stream_empty_file_yields_immediate_eof() {
    let server = MockServer::start().await;
    reconstruction_mock(&server, 0, 4095, 0, json!([]), json!({})).await;

    let ctx = test_stream_context(&server, 1_048_576);
    let (out, _sizes) = drain(make_stream(ctx, None)).await;
    assert!(out.is_empty());
}

#[tokio::test]
async fn download_stream_byte_range_honors_start_offset() {
    let server = MockServer::start().await;
    let chunk = vec![7u8; 64];
    let payload = serialize_payload(&[&chunk]);

    // The requested range 16..64 is clamped into a single prefetch block,
    // so the only wire request is `bytes=16-63`.
    reconstruction_mock(
            &server,
            16,
            63,
            16,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": payload.len() - 1}}
                    ]
                }]
            }),
        )
        .await;
    xorb_range_mock(&server, 0, (payload.len() - 1) as u64, payload).await;

    let ctx = test_stream_context(&server, 1_048_576);
    let (out, _sizes) = drain(make_stream(ctx, Some(16..64))).await;
    assert_eq!(out, chunk[16..64]);
}

#[tokio::test]
async fn cancelled_retained_stream_discards_queue_and_releases_shared_budget() {
    for unordered in [false, true] {
        for mode in 0..4 {
            let server = MockServer::start().await;
            let chunk = vec![7u8; 64];
            let payload = serialize_payload(&[&chunk]);
            reconstruction_mock(
                &server,
                0,
                4095,
                0,
                json!([
                    {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}},
                    {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}}
                ]),
                json!({XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [{"chunks": {"start": 0, "end": 1},
                        "bytes": {"start": 0, "end": payload.len().saturating_sub(1)}}]
                }]}),
            )
            .await;
            xorb_payload_mock(&server, payload).await;
            let ctx = test_stream_context(&server, 64);
            let state;
            if unordered {
                let mut stream = make_unordered_stream(ctx.clone(), None);
                assert_eq!(stream.next().await.unwrap().unwrap().1, chunk);
                tokio::time::timeout(Duration::from_secs(5), async {
                    while stream.receiver.is_empty() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(ctx.buffer_semaphore.available_permits(), 0);
                state = stream.run_state.clone();
                match mode {
                    1 => stream.cancel(),
                    2 => state.cancel(),
                    3 => state.set_error(SdxError::Transfer(TransferError::NotFound(
                        "owned queued failure".to_owned(),
                    ))),
                    _ => {}
                }
                let result = stream.next().await;
                if mode == 0 {
                    assert_eq!(result.unwrap().unwrap().1, chunk);
                } else if mode == 3 {
                    assert!(
                        matches!(result, Err(SdxError::Transfer(TransferError::NotFound(message)))
                            if message == "owned queued failure")
                    );
                } else {
                    assert!(result.unwrap().is_none());
                }
                assert!(stream.next().await.unwrap().is_none());
                assert!(stream.receiver.is_empty());
                tokio::time::timeout(Duration::from_secs(5), async {
                    while ctx.buffer_semaphore.available_permits() != 64 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                let mut sibling = make_unordered_stream(ctx.clone(), None);
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(5), sibling.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap()
                        .1,
                    chunk
                );
                assert_eq!(sibling.next().await.unwrap().unwrap().1, chunk);
                assert!(sibling.next().await.unwrap().is_none());
                // Keep the original handle alive throughout sibling recovery.
                assert!(stream.next().await.unwrap().is_none());
            } else {
                let mut stream = make_stream(ctx.clone(), None);
                assert_eq!(stream.next().await.unwrap().unwrap(), chunk);
                tokio::time::timeout(Duration::from_secs(5), async {
                    while stream.receiver.is_empty() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(ctx.buffer_semaphore.available_permits(), 0);
                state = stream.run_state.clone();
                match mode {
                    1 => stream.cancel(),
                    2 => state.cancel(),
                    3 => state.set_error(SdxError::Transfer(TransferError::NotFound(
                        "owned queued failure".to_owned(),
                    ))),
                    _ => {}
                }
                let result = stream.next().await;
                if mode == 0 {
                    assert_eq!(result.unwrap().unwrap(), chunk);
                } else if mode == 3 {
                    assert!(
                        matches!(result, Err(SdxError::Transfer(TransferError::NotFound(message)))
                            if message == "owned queued failure")
                    );
                } else {
                    assert!(result.unwrap().is_none());
                }
                assert!(stream.next().await.unwrap().is_none());
                assert!(stream.receiver.is_empty());
                tokio::time::timeout(Duration::from_secs(5), async {
                    while ctx.buffer_semaphore.available_permits() != 64 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                let mut sibling = make_stream(ctx.clone(), None);
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(5), sibling.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap(),
                    chunk
                );
                assert_eq!(sibling.next().await.unwrap().unwrap(), chunk);
                assert!(sibling.next().await.unwrap().is_none());
                assert!(stream.next().await.unwrap().is_none());
            }
            assert_eq!(
                state.total_bytes_delivered(),
                if mode == 0 { 128 } else { 64 }
            );
        }
    }
}

#[tokio::test]
async fn download_stream_buffer_semaphore_bounds_in_flight_bytes() {
    let server = MockServer::start().await;
    // 32 chunks × 64 bytes = 2 KiB, with a 256-byte buffer cap: only a few
    // terms may be buffered at once, yet the stream must complete.
    let chunks: Vec<Vec<u8>> = (0..32u8).map(|i| vec![i; 64]).collect();
    let payload = serialize_payload(
        chunks
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<_>>()
            .as_slice(),
    );

    let terms: serde_json::Value = chunks
        .iter()
        .enumerate()
        .map(|(i, chunk)| {
            json!({
                "hash": XORB_HASH,
                "unpacked_length": chunk.len(),
                "range": {"start": i as u64, "end": i as u64 + 1}
            })
        })
        .collect();
    let serialized_chunk_length = serialize_payload(&[chunks[0].as_slice()]).len() as u64;
    let ranges: serde_json::Value = (0..32u64)
            .map(|i| {
                json!({
                    "chunks": {"start": i, "end": i + 1},
                    "bytes": {"start": i * serialized_chunk_length, "end": (i + 1) * serialized_chunk_length - 1}
                })
            })
            .collect();

    reconstruction_mock(
        &server,
        0,
        4095,
        0,
        terms,
        json!({
            XORB_HASH: [{
                "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                "ranges": ranges
            }]
        }),
    )
    .await;
    xorb_payload_mock(&server, payload).await;

    let cap = 256u64;
    let ctx = test_stream_context(&server, cap);
    let mut stream = make_unordered_stream(ctx.clone(), None);
    let mut expected = Vec::new();
    for chunk in &chunks {
        expected.extend_from_slice(chunk);
    }
    // Start without consuming: completed terms retain their backing block
    // permits, so the producer must fill and then wait on this finite cap.
    stream.start();
    tokio::time::timeout(Duration::from_secs(5), async {
        while ctx.buffer_semaphore.available_permits() != 0 || stream.receiver.is_empty() {
            stream.run_state.check_background_error().unwrap();
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("producer did not fill the buffer before consumption");
    let peak_held = cap.saturating_sub(ctx.buffer_semaphore.available_permits());
    assert_eq!(peak_held, cap);
    assert!(peak_held > 0);

    let initial = stream.receiver.try_recv().unwrap();
    {
        let backing = initial
            .permit
            .as_ref()
            .unwrap()
            ._backing_block
            .as_ref()
            .unwrap();
        let decoded = backing.data.get().unwrap();
        let decoded_bytes = u64::try_from(decoded.data.len()).unwrap();
        assert!(decoded_bytes > 0);
        assert!(decoded_bytes <= decoded._buffer_permit.num_permits());
        assert!(decoded._buffer_permit.num_permits() <= cap);
    }
    // The completed item is still retained. A further reservation cannot
    // succeed until consumption releases its backing block's capacity.
    let mut waiting = Box::pin(ctx.buffer_semaphore.acquire_many(1));
    assert!(futures_util::poll!(&mut waiting).is_pending());
    drop(waiting);

    let mut received = std::collections::BTreeMap::new();
    let (offset, chunk) = stream.process_term(initial).unwrap().unwrap();
    assert!(received.insert(offset, chunk).is_none());
    while let Some((offset, chunk)) = stream.next().await.unwrap() {
        assert!(received.insert(offset, chunk).is_none());
        let available = ctx.buffer_semaphore.available_permits();
        let held = cap
            .checked_sub(available)
            .expect("buffer capacity exceeded its cap");
        assert!(held <= cap);
    }
    assert_eq!(received.len(), chunks.len());
    let actual: Vec<u8> = received
        .into_values()
        .flat_map(|chunk| chunk.to_vec())
        .collect();
    assert_eq!(actual, expected);
    // After full consumption the semaphore is back to capacity.
    assert_eq!(ctx.buffer_semaphore.available_permits(), cap);
}

#[tokio::test]
async fn download_unordered_stream_yields_completion_order() {
    let server = MockServer::start().await;
    let chunk_a = vec![7u8; 64];
    let chunk_b = vec![9u8; 64];
    let payload_a = serialize_payload(&[&chunk_a]);
    let payload_b = serialize_payload(&[&chunk_b]);
    let split = payload_a.len() as u64;
    let final_byte = split + payload_b.len() as u64 - 1;

    reconstruction_mock(
        &server,
        0,
        4095,
        0,
        json!([
            {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}},
            {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 1, "end": 2}}
        ]),
        json!({
            XORB_HASH: [{
                "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                "ranges": [
                    {"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": split - 1}},
                    {"chunks": {"start": 1, "end": 2}, "bytes": {"start": split, "end": final_byte}}
                ]
            }]
        }),
    )
    .await;
    let mut payload = payload_a;
    payload.extend_from_slice(&payload_b);
    xorb_payload_mock(&server, payload).await;

    let ctx = test_stream_context(&server, 1_048_576);
    let mut stream = make_unordered_stream(ctx, None);
    let mut expected = chunk_a.clone();
    expected.extend_from_slice(&chunk_b);
    let mut pieces: Vec<(u64, Vec<u8>)> = Vec::new();
    while let Some((offset, chunk)) = stream.next().await.unwrap() {
        pieces.push((offset, chunk.to_vec()));
    }
    // The stream yields completion order (not file order): reassemble by
    // offset to verify byte-identity.
    pieces.sort_by_key(|(offset, _)| *offset);
    let mut out = Vec::new();
    let offsets: Vec<u64> = pieces.iter().map(|(offset, _)| *offset).collect();
    for (_, data) in pieces {
        out.extend_from_slice(&data);
    }
    assert_eq!(offsets, vec![0, 64]);
    assert_eq!(out, expected);
}

#[tokio::test]
async fn download_stream_cancel_stops_promptly() {
    let server = MockServer::start().await;
    let chunk_a = vec![7u8; 64];
    let chunk_b = vec![9u8; 64];
    let payload = serialize_payload(&[&chunk_a, &chunk_b]);

    reconstruction_mock(
            &server,
            0,
            4095,
            0,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}},
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 1, "end": 2}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 2}, "bytes": {"start": 0, "end": payload.len() - 1}}
                    ]
                }]
            }),
        )
        .await;
    xorb_range_mock(&server, 0, (payload.len() - 1) as u64, payload).await;

    let ctx = test_stream_context(&server, 1_048_576);
    let mut stream = make_stream(ctx, None);
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first, chunk_a);
    stream.cancel();
    // After cancel, subsequent calls return Ok(None) and stay finished.
    assert!(stream.next().await.unwrap().is_none());
    assert!(stream.next().await.unwrap().is_none());
}

#[tokio::test]
async fn download_stream_drop_cancels_background_task() {
    let server = MockServer::start().await;
    let chunk = vec![5u8; 64];
    let payload = serialize_payload(&[&chunk]);

    reconstruction_mock(
            &server,
            0,
            4095,
            0,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": payload.len() - 1}}
                    ]
                }]
            }),
        )
        .await;
    xorb_range_mock(&server, 0, (payload.len() - 1) as u64, payload).await;

    let ctx = test_stream_context(&server, 1_048_576);
    let mut stream = make_stream(ctx, None);
    // Pull one chunk, then drop mid-stream: the background task must be
    // cancelled rather than leaking.
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first, chunk);
    drop(stream);
    // Allow any in-flight task to observe the cancellation.
    tokio::task::yield_now().await;
}

async fn blocking_stream_fixture() -> (MockServer, StreamContext) {
    let server = MockServer::start().await;
    let payload = serialize_payload(&[&[5u8; 64]]);
    let last_byte = u64::try_from(payload.len())
        .unwrap()
        .checked_sub(1)
        .unwrap();
    reconstruction_mock(
        &server,
        0,
        4095,
        0,
        json!([{"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}}]),
        json!({XORB_HASH: [{
            "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
            "ranges": [{"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": last_byte}}]
        }]}),
    )
    .await;
    xorb_range_mock(&server, 0, last_byte, payload).await;
    let context = test_stream_context(&server, 1_048_576);
    (server, context)
}

async fn check_blocking_streams_inside_runtime() {
    let (_server, context) = blocking_stream_fixture().await;
    let mut ordered = make_stream(context.clone(), None);
    let mut output = Vec::new();
    while let Some(bytes) = ordered.blocking_next().unwrap() {
        output.extend_from_slice(&bytes);
    }
    assert_eq!(output, vec![5u8; 64]);
    assert!(ordered.blocking_next().unwrap().is_none());

    let mut unordered = make_unordered_stream(context.clone(), None);
    let (offset, bytes) = unordered.blocking_next().unwrap().unwrap();
    assert_eq!(offset, 0);
    assert_eq!(&bytes[..], &[5u8; 64]);
    assert!(unordered.blocking_next().unwrap().is_none());

    let mut ordered_cancelled = make_stream(context.clone(), None);
    ordered_cancelled.cancel();
    assert!(ordered_cancelled.blocking_next().unwrap().is_none());
    let mut unordered_cancelled = make_unordered_stream(context, None);
    unordered_cancelled.cancel();
    assert!(unordered_cancelled.blocking_next().unwrap().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_next_works_inside_current_thread_runtime() {
    check_blocking_streams_inside_runtime().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_next_works_inside_multi_thread_runtime() {
    check_blocking_streams_inside_runtime().await;
}

#[test]
fn unordered_blocking_next_works_from_plain_thread() {
    let runtime = Runtime::new().unwrap();
    let (server, mut stream) = runtime.block_on(async {
        let (server, context) = blocking_stream_fixture().await;
        (server, make_unordered_stream(context, None))
    });
    drop(runtime);
    let (offset, bytes) = stream.blocking_next().unwrap().unwrap();
    assert_eq!(offset, 0);
    assert_eq!(&bytes[..], &[5u8; 64]);
    assert!(stream.blocking_next().unwrap().is_none());
    drop(server);
}

#[test]
fn blocking_next_works_from_plain_thread() {
    let rt = Runtime::new().unwrap();
    let (server, mut stream) = rt.block_on(async {
        let (server, context) = blocking_stream_fixture().await;
        (server, make_stream(context, None))
    });
    drop(rt);

    // Consume on a plain (non-async) thread via the dedicated runtime.
    let handle = std::thread::spawn(move || {
        let mut out = Vec::new();
        while let Some(chunk) = stream.blocking_next().unwrap() {
            out.extend_from_slice(&chunk);
        }
        (out, stream)
    });
    let (out, _stream) = handle.join().unwrap();
    assert_eq!(out, vec![5u8; 64]);
    drop(server);
}

#[tokio::test]
async fn download_stream_matches_reconstruct_output() {
    // Use two separate mock servers: the M2a `reconstruct` path sends no
    // Range header, while the streaming path sends `bytes=0-4095` (with a
    // 416 fallback), so the mock sets must not overlap.
    let full_server = MockServer::start().await;
    let stream_server = MockServer::start().await;
    let chunk_a = vec![7u8; 64];
    let chunk_b = vec![9u8; 64];
    let payload = serialize_payload(&[&chunk_a, &chunk_b]);

    let terms = json!([
        {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}},
        {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 1, "end": 2}}
    ]);
    let full_xorbs = json!({
        XORB_HASH: [{
            "url": format!("{}/transfer/xorb/default/{XORB_HASH}", full_server.uri()),
            "ranges": [
                {"chunks": {"start": 0, "end": 2}, "bytes": {"start": 0, "end": payload.len() - 1}}
            ]
        }]
    });
    let stream_xorbs = json!({
        XORB_HASH: [{
            "url": format!("{}/transfer/xorb/default/{XORB_HASH}", stream_server.uri()),
            "ranges": [
                {"chunks": {"start": 0, "end": 2}, "bytes": {"start": 0, "end": payload.len() - 1}}
            ]
        }]
    });

    // Full-file reconstruction (no Range header) via the M2a path.
    Mock::given(method("GET"))
        .and(path(format!("/v2/reconstructions/{FILE_ID}")))
        .and(header("authorization", format!("Bearer {READ_TOKEN}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(v2_response_body(
            0,
            terms.clone(),
            full_xorbs,
        )))
        .mount(&full_server)
        .await;
    xorb_range_mock(&full_server, 0, (payload.len() - 1) as u64, payload.clone()).await;

    // Streaming path uses the ranged request; map the prefetch range.
    reconstruction_mock(&stream_server, 0, 4095, 0, terms, stream_xorbs).await;
    xorb_range_mock(
        &stream_server,
        0,
        (payload.len() - 1) as u64,
        payload.clone(),
    )
    .await;

    let mut expected = chunk_a.clone();
    expected.extend_from_slice(&chunk_b);

    // M2a sequential reconstruction.
    let transfer = TransferClient::new(reqwest::Client::new());
    let file = reconstruction::reconstruct(
        &transfer,
        None,
        &full_server.uri(),
        READ_TOKEN,
        FILE_ID,
        None,
    )
    .await
    .unwrap();
    assert_eq!(file.data, expected);

    // Streaming path.
    let ctx = test_stream_context(&stream_server, 1_048_576);
    let (out, _sizes) = drain(make_stream(ctx, None)).await;
    assert_eq!(out, expected);
}

#[tokio::test]
async fn download_stream_surfaces_background_errors() {
    let server = MockServer::start().await;
    let chunk = vec![5u8; 64];
    let payload = serialize_payload(&[&chunk]);

    reconstruction_mock(
            &server,
            0,
            4095,
            0,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": payload.len() - 1}}
                    ]
                }]
            }),
        )
        .await;
    // No xorb mock: the fetch 404s, and the error must surface via the
    // stream's check_error at an item boundary.
    let _ = payload;

    let ctx = test_stream_context(&server, 1_048_576);
    let mut stream = make_stream(ctx, None);
    // Drain the stream, capturing whether `next()` surfaced the background
    // error directly. The xorb is deliberately un-mocked so the ranged fetch
    // 404s; that failure must surface through the stream at an item boundary
    // and never be silently swallowed. The background task starts paused and
    // auto-starts on the first `next()`; under slow/instrumented execution
    // the first `next()` can race the 404 (returning a derived
    // data-pipeline error, or `Ok(None)` before the root error is observed),
    // so the deterministic check is on the shared `RunState`: the
    // reconstruction task re-sets the root error on exit, so drain to EOF,
    // wait for it to be recorded, then surface it via `check_error`.
    let mut surfaced: Option<SdxError> = None;
    loop {
        match stream.next().await {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(error) => {
                surfaced = Some(error);
                break;
            }
        }
    }
    // If `next()` did not surface the root NotFound directly, allow the
    // background task to re-set it in the run state, then surface it via
    // `check_error` at the terminal boundary.
    if !matches!(
        surfaced,
        Some(SdxError::Transfer(TransferError::NotFound(_)))
    ) {
        for _ in 0..500 {
            if stream.run_state.error_message().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        surfaced = stream.run_state.check_error().err();
    }
    assert!(
        matches!(
            surfaced,
            Some(SdxError::Transfer(TransferError::NotFound(_)))
        ),
        "background xorb fetch failure was swallowed or surfaced a wrong error; got {surfaced:?}"
    );
}
