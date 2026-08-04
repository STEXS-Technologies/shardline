//! Stream-group layer (M2b2, `docs/SDX_PLAN.md` §4.4.3).
//!
//! Mirrors `hf-xet-1.5.4/src/xet_session/` (`session.rs`,
//! `download_stream_group.rs`, `download_stream_handle.rs`, `task_runtime.rs`),
//! download side only:
//!
//! - [`XetStreamGroup`] owns shared session state (the client's
//!   `DownloadSessionInner`, and through it the dedicated blocking runtime and
//!   the byte-denominated buffer semaphore) and manages multiple concurrent
//!   streams with **abort-all** and per-stream **status** probes
//!   ([`XetTaskState`]). Create one via
//!   [`crate::XetClient::new_download_stream_group`].
//! - Each group holds its own root [`CancellationToken`]; every stream is
//!   created with a **child** token (mirror `TaskRuntime::child`) and
//!   registered in a weak-reference map, so dropping the stream unregisters it
//!   and `abort()` cancels the whole subtree.
//! - Streams are the M2b1 [`DownloadStream`] / [`UnorderedDownloadStream`]
//!   wrapped as [`GroupedDownloadStream`] / [`GroupedUnorderedDownloadStream`]:
//!   spawned paused, auto-start on the first `next()`, unregister on `Drop`.
//!
//! # Blocking / async-runtime trap (upstream, `docs/SDX_PLAN.md` §4.4.3)
//!
//! Upstream `xet-data` `blocking_next()` **panics inside an async runtime**
//! (upstream asserts no-runtime context). sdx's
//! [`GroupedDownloadStream::blocking_next`] runs the async `next()` future on
//! the client-owned dedicated runtime (M2b1's bridge), so group streams block
//! safely from plain CLI threads — but calling `blocking_next()` from within
//! an async context still blocks that executor thread until a chunk arrives;
//! prefer `next()` there. `blocking_next()` is compiled only on non-wasm
//! targets (wasm has no multi-thread runtime).
//!
//! # Out of scope (M3)
//!
//! The upload/commit side (`upload_stream_handle.rs`, `upload_commit.rs`,
//! `XetUploadCommit`) is M3 and intentionally **not** implemented here; this
//! module is download-only.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use bytes::Bytes;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{
    error::SdxError,
    session::DownloadSessionInner,
    stream::{DownloadStream, RunState, UnorderedDownloadStream},
};

/// Per-stream task state for the group status probe.
///
/// Mirrors hf-xet `XetTaskState` (`task_runtime.rs`), adapted to the CLI's
/// vocabulary (`docs/SDX_PLAN.md` §4.4.3): `queued/in_progress/completed/
/// failed/cancelled`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum XetTaskState {
    /// Stream created but not yet started (no `next()`/`start()` yet).
    Queued,
    /// Stream started and reconstruction is in flight.
    InProgress,
    /// Stream finished normally (all scheduled bytes delivered).
    Completed,
    /// A background task failed; the error message is attached.
    Failed(String),
    /// The stream (or its group) was cancelled/aborted.
    Cancelled,
}

/// Group-unique stream id source.
static NEXT_GROUP_ID: AtomicU64 = AtomicU64::new(0);

/// A session-level group managing concurrent downloads with abort-all and
/// per-stream status.
///
/// Clone is cheap: all clones share the underlying group state. Groups are
/// independent of each other; each has its own cancellation tree.
#[derive(Clone)]
pub struct XetStreamGroup {
    inner: Arc<XetStreamGroupInner>,
}

/// All shared state owned by one [`XetStreamGroup`].
struct XetStreamGroupInner {
    session: Arc<DownloadSessionInner>,
    id: u64,
    /// Root of this group's cancellation tree; `abort()` cancels it, which
    /// propagates to every stream's child token.
    token: CancellationToken,
    aborted: AtomicBool,
    /// Weak references so dropping a stream unregisters it immediately
    /// (mirror `XetSession::active_download_stream_groups`).
    active: Mutex<HashMap<u64, Weak<StreamRegistration>>>,
    next_id: AtomicU64,
}

/// Per-stream bookkeeping shared between the group and the stream wrapper.
struct StreamRegistration {
    id: u64,
    run_state: Arc<RunState>,
    /// The paused task's start signal, so `abort()` can wake it promptly.
    start_signal: Option<Arc<Notify>>,
    started: AtomicBool,
    finished: AtomicBool,
}

impl StreamRegistration {
    fn new(id: u64, stream: &DownloadStream) -> Arc<Self> {
        Arc::new(Self {
            id,
            run_state: stream.run_state(),
            start_signal: stream.pending_start_signal(),
            started: AtomicBool::new(false),
            finished: AtomicBool::new(false),
        })
    }

    fn new_unordered(id: u64, stream: &UnorderedDownloadStream) -> Arc<Self> {
        Arc::new(Self {
            id,
            run_state: stream.run_state(),
            start_signal: stream.pending_start_signal(),
            started: AtomicBool::new(false),
            finished: AtomicBool::new(false),
        })
    }

    /// Cancels this stream only (never the group or its siblings).
    fn cancel(&self) {
        self.run_state.cancel();
        if let Some(signal) = &self.start_signal {
            signal.notify_one();
        }
    }

    /// Computes this stream's status snapshot.
    fn state(&self, group_aborted: bool) -> XetTaskState {
        if group_aborted || self.run_state.is_cancelled() {
            return XetTaskState::Cancelled;
        }
        if let Some(message) = self.run_state.error_message() {
            return XetTaskState::Failed(message);
        }
        let scheduled = self.run_state.total_bytes_scheduled();
        let delivered = self.run_state.total_bytes_delivered();
        if self.finished.load(Ordering::Relaxed) || (scheduled > 0 && delivered >= scheduled) {
            return XetTaskState::Completed;
        }
        if self.started.load(Ordering::Relaxed) {
            return XetTaskState::InProgress;
        }
        XetTaskState::Queued
    }
}

impl XetStreamGroupInner {
    /// Registers a stream; streams created after an abort start cancelled.
    fn register(&self, registration: &Arc<StreamRegistration>) {
        if self.aborted.load(Ordering::Relaxed) {
            registration.cancel();
        }
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        active.insert(registration.id, Arc::downgrade(registration));
    }

    /// Unregisters a stream by id (called from the wrapper's `Drop`).
    fn unregister(&self, id: u64) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        active.remove(&id);
    }
}

impl XetStreamGroup {
    /// Creates a group over the client's shared session state.
    pub(crate) fn new(session: Arc<DownloadSessionInner>) -> Self {
        Self {
            inner: Arc::new(XetStreamGroupInner {
                session,
                id: NEXT_GROUP_ID.fetch_add(1, Ordering::Relaxed),
                token: CancellationToken::new(),
                aborted: AtomicBool::new(false),
                active: Mutex::new(HashMap::new()),
                next_id: AtomicU64::new(0),
            }),
        }
    }

    /// Returns the unique group id.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    /// Returns `true` after [`abort`](Self::abort) has been called.
    #[must_use]
    pub fn is_aborted(&self) -> bool {
        self.inner.aborted.load(Ordering::Relaxed)
    }

    /// Creates a sequential streaming download of `file_id` (optionally
    /// restricted to `range`), registered with this group.
    ///
    /// The background reconstruction task is spawned paused and auto-starts on
    /// the first [`GroupedDownloadStream::next`] /
    /// [`GroupedDownloadStream::blocking_next`]; dropping the stream cancels
    /// it and unregisters it from the group. Aborting the group stops the
    /// stream promptly.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when `file_id` is malformed, `range` is inverted,
    /// token issuance fails, or the blocking runtime cannot be resolved.
    pub async fn download_stream(
        &self,
        file_id: &str,
        range: Option<Range<u64>>,
    ) -> Result<GroupedDownloadStream, SdxError> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let stream_token = self.inner.token.child_token();
        let mut reconstructor = self
            .inner
            .session
            .build_reconstructor(file_id, range)
            .await?;
        reconstructor = reconstructor.with_cancellation_token(stream_token.clone());
        let stream = reconstructor.reconstruct_to_stream();
        let registration = StreamRegistration::new(id, &stream);
        self.register(&registration);
        Ok(GroupedDownloadStream::new(
            stream,
            id,
            registration,
            Arc::downgrade(&self.inner),
        ))
    }

    /// Creates an unordered streaming download of `file_id` (optionally
    /// restricted to `range`), registered with this group.
    ///
    /// See [`download_stream`](Self::download_stream) for the lifecycle.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when `file_id` is malformed, `range` is inverted,
    /// token issuance fails, or the blocking runtime cannot be resolved.
    pub async fn download_unordered_stream(
        &self,
        file_id: &str,
        range: Option<Range<u64>>,
    ) -> Result<GroupedUnorderedDownloadStream, SdxError> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let stream_token = self.inner.token.child_token();
        let mut reconstructor = self
            .inner
            .session
            .build_reconstructor(file_id, range)
            .await?;
        reconstructor = reconstructor.with_cancellation_token(stream_token.clone());
        let stream = reconstructor.reconstruct_to_unordered_stream();
        let registration = StreamRegistration::new_unordered(id, &stream);
        self.register(&registration);
        Ok(GroupedUnorderedDownloadStream::new(
            stream,
            id,
            registration,
            Arc::downgrade(&self.inner),
        ))
    }

    /// Aborts every active stream in this group and cancels the group's
    /// cancellation subtree.
    ///
    /// Subsequent `next()`/`blocking_next()` on any stream created by this
    /// group return `Ok(None)` promptly; the group status probe reports every
    /// stream as [`XetTaskState::Cancelled`]. Streams created after the abort
    /// start already-cancelled.
    pub fn abort(&self) {
        self.inner.aborted.store(true, Ordering::Relaxed);
        self.inner.token.cancel();
        let active = self
            .inner
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for weak in active.values() {
            if let Some(registration) = weak.upgrade() {
                registration.cancel();
            }
        }
    }

    /// Returns a snapshot of every live stream's status as `(stream_id,
    /// state)` pairs, ordered by stream id.
    #[must_use]
    pub fn status(&self) -> Vec<(u64, XetTaskState)> {
        let aborted = self.inner.aborted.load(Ordering::Relaxed);
        let active = self
            .inner
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut states = Vec::with_capacity(active.len());
        for (id, weak) in active.iter() {
            let Some(registration) = weak.upgrade() else {
                continue;
            };
            states.push((*id, registration.state(aborted)));
        }
        states.sort_by_key(|(id, _)| *id);
        states
    }

    /// Number of live (not-yet-dropped) streams registered with this group.
    #[must_use]
    pub fn active_stream_count(&self) -> usize {
        let active = self
            .inner
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        active
            .values()
            .filter(|weak| weak.upgrade().is_some())
            .count()
    }

    /// Registers a stream; streams created after an abort start cancelled.
    fn register(&self, registration: &Arc<StreamRegistration>) {
        self.inner.register(registration);
    }
}

/// A group-scoped sequential streaming download handle.
///
/// Wraps the M2b1 [`DownloadStream`] with group registration: `Drop`
/// unregisters the stream, [`cancel`](Self::cancel) stops only this stream,
/// and [`task_id`](Self::task_id) identifies it in the group status probe.
pub struct GroupedDownloadStream {
    inner: DownloadStream,
    id: u64,
    registration: Arc<StreamRegistration>,
    group: Weak<XetStreamGroupInner>,
}

impl GroupedDownloadStream {
    const fn new(
        inner: DownloadStream,
        id: u64,
        registration: Arc<StreamRegistration>,
        group: Weak<XetStreamGroupInner>,
    ) -> Self {
        Self {
            inner,
            id,
            registration,
            group,
        }
    }

    /// Returns the unique stream id within its group.
    #[must_use]
    pub const fn task_id(&self) -> u64 {
        self.id
    }

    /// Unblocks the reconstruction task so it begins producing data.
    ///
    /// If already started, this is a no-op. Called automatically on the first
    /// [`next`](Self::next) / [`blocking_next`](Self::blocking_next). This
    /// method is non-async and does not require a tokio runtime context.
    pub fn start(&mut self) {
        self.registration.started.store(true, Ordering::Relaxed);
        self.inner.start();
    }

    /// Returns the next chunk of downloaded data asynchronously.
    ///
    /// Returns `Ok(None)` when the download is complete or cancelled.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when a background reconstruction task failed.
    pub async fn next(&mut self) -> Result<Option<Bytes>, SdxError> {
        self.registration.started.store(true, Ordering::Relaxed);
        let result = self.inner.next().await;
        if result.as_ref().is_ok_and(Option::is_none) {
            self.registration.finished.store(true, Ordering::Relaxed);
        }
        result
    }

    /// Returns the next chunk of downloaded data, blocking the current thread
    /// until data is available.
    ///
    /// Returns `Ok(None)` when the download is complete or cancelled.
    ///
    /// # Runtime requirements
    ///
    /// Runs the async [`next`](Self::next) on the client-owned dedicated
    /// runtime, so it works from plain CLI threads and from
    /// [`tokio::task::spawn_blocking`] **without** panicking (upstream
    /// `xet-data` panics inside an async runtime). Calling it from within an
    /// async context is supported but blocks that executor thread — prefer
    /// [`next`](Self::next) there.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when a background reconstruction task failed.
    #[cfg(not(target_family = "wasm"))]
    pub fn blocking_next(&mut self) -> Result<Option<Bytes>, SdxError> {
        self.registration.started.store(true, Ordering::Relaxed);
        let result = self.inner.blocking_next();
        if result.as_ref().is_ok_and(Option::is_none) {
            self.registration.finished.store(true, Ordering::Relaxed);
        }
        result
    }

    /// Cancels this stream only (the group and sibling streams continue).
    ///
    /// Subsequent calls to [`next`](Self::next) /
    /// [`blocking_next`](Self::blocking_next) return `Ok(None)`.
    pub fn cancel(&mut self) {
        self.registration.cancel();
        self.inner.cancel();
    }
}

impl Drop for GroupedDownloadStream {
    fn drop(&mut self) {
        if let Some(group) = self.group.upgrade() {
            group.unregister(self.id);
        }
    }
}

/// A group-scoped unordered streaming download handle.
///
/// Yields `(file_offset, Bytes)` pairs in completion order; the lifecycle
/// matches [`GroupedDownloadStream`].
pub struct GroupedUnorderedDownloadStream {
    inner: UnorderedDownloadStream,
    id: u64,
    registration: Arc<StreamRegistration>,
    group: Weak<XetStreamGroupInner>,
}

impl GroupedUnorderedDownloadStream {
    const fn new(
        inner: UnorderedDownloadStream,
        id: u64,
        registration: Arc<StreamRegistration>,
        group: Weak<XetStreamGroupInner>,
    ) -> Self {
        Self {
            inner,
            id,
            registration,
            group,
        }
    }

    /// Returns the unique stream id within its group.
    #[must_use]
    pub const fn task_id(&self) -> u64 {
        self.id
    }

    /// Unblocks the reconstruction task so it begins producing data.
    ///
    /// If already started, this is a no-op. Called automatically on the first
    /// [`next`](Self::next) / [`blocking_next`](Self::blocking_next).
    pub fn start(&mut self) {
        self.registration.started.store(true, Ordering::Relaxed);
        self.inner.start();
    }

    /// Returns the next `(file_offset, chunk)` pair asynchronously.
    ///
    /// Returns `Ok(None)` when the download is complete or cancelled.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when a background reconstruction task failed.
    pub async fn next(&mut self) -> Result<Option<(u64, Bytes)>, SdxError> {
        self.registration.started.store(true, Ordering::Relaxed);
        let result = self.inner.next().await;
        if result.as_ref().is_ok_and(Option::is_none) {
            self.registration.finished.store(true, Ordering::Relaxed);
        }
        result
    }

    /// Returns the next `(file_offset, chunk)` pair, blocking the current
    /// thread until data is available.
    ///
    /// Returns `Ok(None)` when the download is complete or cancelled.
    ///
    /// # Runtime requirements
    ///
    /// See [`GroupedDownloadStream::blocking_next`] for the dedicated-runtime
    /// bridge and the upstream panic-in-async-runtime documentation.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when a background reconstruction task failed.
    #[cfg(not(target_family = "wasm"))]
    pub fn blocking_next(&mut self) -> Result<Option<(u64, Bytes)>, SdxError> {
        self.registration.started.store(true, Ordering::Relaxed);
        let result = self.inner.blocking_next();
        if result.as_ref().is_ok_and(Option::is_none) {
            self.registration.finished.store(true, Ordering::Relaxed);
        }
        result
    }

    /// Cancels this stream only (the group and sibling streams continue).
    ///
    /// Subsequent calls to [`next`](Self::next) /
    /// [`blocking_next`](Self::blocking_next) return `Ok(None)`.
    pub fn cancel(&mut self) {
        self.registration.cancel();
        self.inner.cancel();
    }
}

impl Drop for GroupedUnorderedDownloadStream {
    fn drop(&mut self) {
        if let Some(group) = self.group.upgrade() {
            group.unregister(self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use tokio::time::timeout;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path},
    };

    use super::*;
    use crate::{Auth, RepositoryId, XetClientBuilder};

    const FILE_ID: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    const XORB_HASH: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const READ_TOKEN: &str = "read-token";

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

    fn test_limits() -> crate::StreamLimits {
        crate::StreamLimits {
            min_reconstruction_fetch_size: 4096,
            max_reconstruction_fetch_size: 4096,
            min_prefetch_buffer: 8192,
            ..crate::StreamLimits::default()
        }
    }

    /// Mounts the token route and builds a client pointed at `server`.
    async fn client(server: &MockServer) -> crate::XetClient {
        Mock::given(method("GET"))
            .and(path("/api/github/team/assets/xet-read-token/main"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "casUrl": server.uri(),
                "exp": 4_000_000_000u64,
                "accessToken": READ_TOKEN,
            })))
            .mount(server)
            .await;
        let auth = Auth::new(
            &server.uri(),
            RepositoryId {
                provider: "github".to_owned(),
                owner: "team".to_owned(),
                repo: "assets".to_owned(),
                revision: "main".to_owned(),
            },
        )
        .unwrap()
        .with_api_key("bootstrap".to_owned())
        .with_subject("user".to_owned());
        let port = server.uri().split(':').next_back().unwrap().to_owned();
        XetClientBuilder::new()
            .endpoint(format!("xet://127.0.0.1:{port}/github/team/assets/main"))
            .auth(auth)
            .with_stream_limits(test_limits())
            .build()
            .unwrap()
    }

    /// Mounts a 200 reconstruction response scoped to `bytes=0-4095` plus a
    /// 416 fallback, and a 206 xorb range response (optionally delayed).
    async fn mocks(server: &MockServer, delay: Option<Duration>) {
        let payload = serialize_payload(&[&[7u8; 64], &[9u8; 64]]);
        Mock::given(method("GET"))
            .and(path(format!("/v2/reconstructions/{FILE_ID}")))
            .and(header("authorization", format!("Bearer {READ_TOKEN}")))
            .and(header("range", "bytes=0-4095"))
            .respond_with(ResponseTemplate::new(200).set_body_json(v2_response_body(
                0,
                json!([
                    {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}},
                    {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 1, "end": 2}}
                ]),
                json!({
                    XORB_HASH: [{
                        "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                        "ranges": [
                            {"chunks": {"start": 0, "end": 2}, "bytes": {"start": 0, "end": 200}}
                        ]
                    }]
                }),
            )))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/v2/reconstructions/{FILE_ID}")))
            .and(header("authorization", format!("Bearer {READ_TOKEN}")))
            .respond_with(ResponseTemplate::new(416).set_body_json(json!({"error": "past eof"})))
            .mount(server)
            .await;
        let mut template = ResponseTemplate::new(206)
            .insert_header("Content-Range", format!("bytes 0-200/{}", payload.len()))
            .set_body_raw(payload, "application/octet-stream");
        if let Some(delay) = delay {
            template = template.set_delay(delay);
        }
        Mock::given(method("GET"))
            .and(path(format!("/transfer/xorb/default/{XORB_HASH}")))
            .and(header("authorization", format!("Bearer {READ_TOKEN}")))
            .and(header("range", "bytes=0-200"))
            .respond_with(template)
            .mount(server)
            .await;
    }

    async fn drain(stream: &mut GroupedDownloadStream) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await.unwrap() {
            out.extend_from_slice(&chunk);
        }
        out
    }

    #[tokio::test]
    async fn status_transitions_queued_to_in_progress_to_completed() {
        let server = MockServer::start().await;
        mocks(&server, None).await;
        let group = client(&server).await.new_download_stream_group();

        let mut stream = group.download_stream(FILE_ID, None).await.unwrap();
        let id = stream.task_id();
        assert_eq!(group.active_stream_count(), 1);
        assert!(group.status().contains(&(id, XetTaskState::Queued)));

        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk.as_ref(), vec![7u8; 64]);
        assert!(group.status().contains(&(id, XetTaskState::InProgress)));

        // The first chunk was consumed above; the rest is the second chunk.
        let out = drain(&mut stream).await;
        assert_eq!(out, vec![9u8; 64]);
        assert!(group.status().contains(&(id, XetTaskState::Completed)));
    }

    #[tokio::test]
    async fn abort_mid_stream_stops_promptly() {
        let server = MockServer::start().await;
        mocks(&server, Some(Duration::from_secs(30))).await;
        let group = client(&server).await.new_download_stream_group();

        let mut stream = group.download_stream(FILE_ID, None).await.unwrap();
        let id = stream.task_id();
        // The xorb fetch is delayed, so next() blocks until the group aborts.
        let abort = {
            let group = group.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                group.abort();
            })
        };
        let next = timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("abort must stop the stream promptly")
            .unwrap();
        assert!(next.is_none());
        abort.await.unwrap();
        assert!(group.is_aborted());
        assert!(group.status().contains(&(id, XetTaskState::Cancelled)));
    }

    #[tokio::test]
    async fn abort_cancels_queued_streams_too() {
        let server = MockServer::start().await;
        mocks(&server, Some(Duration::from_secs(30))).await;
        let group = client(&server).await.new_download_stream_group();

        // A queued (never started) stream alongside a running one.
        let mut queued = group.download_stream(FILE_ID, None).await.unwrap();
        let queued_id = queued.task_id();
        let mut running = group.download_stream(FILE_ID, None).await.unwrap();
        let running_id = running.task_id();

        group.abort();
        let next = timeout(Duration::from_secs(5), running.next())
            .await
            .expect("abort must stop the running stream")
            .unwrap();
        assert!(next.is_none());
        let next = timeout(Duration::from_secs(5), queued.next())
            .await
            .expect("abort must stop the queued stream")
            .unwrap();
        assert!(next.is_none());

        let status = group.status();
        assert!(status.contains(&(queued_id, XetTaskState::Cancelled)));
        assert!(status.contains(&(running_id, XetTaskState::Cancelled)));
    }

    #[tokio::test]
    async fn drop_unregisters_stream_from_group() {
        let server = MockServer::start().await;
        mocks(&server, None).await;
        let group = client(&server).await.new_download_stream_group();

        let stream = group.download_stream(FILE_ID, None).await.unwrap();
        assert_eq!(group.active_stream_count(), 1);
        drop(stream);
        assert_eq!(group.active_stream_count(), 0);
        assert!(group.status().is_empty());
    }

    #[tokio::test]
    async fn two_concurrent_streams_are_byte_identical() {
        let server = MockServer::start().await;
        mocks(&server, None).await;
        let group = client(&server).await.new_download_stream_group();
        let expected = [vec![7u8; 64], vec![9u8; 64]].concat();

        let mut a = group.download_stream(FILE_ID, None).await.unwrap();
        let mut b = group
            .download_unordered_stream(FILE_ID, None)
            .await
            .unwrap();
        let task_a = tokio::spawn(async move { drain(&mut a).await });
        let task_b = tokio::spawn(async move {
            let mut pieces = Vec::new();
            while let Some((offset, chunk)) = b.next().await.unwrap() {
                pieces.push((offset, chunk));
            }
            pieces.sort_by_key(|(offset, _)| *offset);
            let mut out = Vec::new();
            for (_, chunk) in pieces {
                out.extend_from_slice(&chunk);
            }
            out
        });
        let out_a = task_a.await.unwrap();
        let out_b = task_b.await.unwrap();
        assert_eq!(out_a, expected);
        assert_eq!(out_b, expected);
        assert_eq!(group.active_stream_count(), 0);
    }

    #[tokio::test]
    async fn stream_cancel_does_not_cancel_siblings() {
        let server = MockServer::start().await;
        mocks(&server, None).await;
        let group = client(&server).await.new_download_stream_group();
        let expected = [vec![7u8; 64], vec![9u8; 64]].concat();

        let mut a = group.download_stream(FILE_ID, None).await.unwrap();
        let mut b = group.download_stream(FILE_ID, None).await.unwrap();
        a.cancel();
        assert!(a.next().await.unwrap().is_none());
        // b is untouched and still completes.
        let out = drain(&mut b).await;
        assert_eq!(out, expected);
    }

    #[test]
    fn xet_task_state_is_comparable() {
        assert_eq!(XetTaskState::Queued, XetTaskState::Queued);
        assert_ne!(XetTaskState::Queued, XetTaskState::InProgress);
        assert_ne!(
            XetTaskState::Failed("x".to_owned()),
            XetTaskState::Failed("y".to_owned())
        );
    }
}
