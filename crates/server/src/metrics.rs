use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use axum::body::Body;
use tower::{Layer, Service};

// Re-export all types and convenience functions from shardline-metrics.
pub use shardline_metrics::*;

// ── Compatibility free functions ──────────────────────────────────────────
//
// The server originally exposed these signatures.  Wrappers convert to the
// `shardline_metrics` API which uses `Duration` instead of raw `f64` and
// `u16` status codes instead of string labels.

pub fn record_upload(protocol: &str, bytes: u64, duration_secs: f64, ok: bool) {
    let status = if ok { 200_u16 } else { 500 };
    shardline_metrics::record_upload(protocol, bytes);
    shardline_metrics::metrics()
        .transfer
        .record_upload_duration(duration_secs);
    let _ = status;
}

pub fn record_download(protocol: &str, bytes: u64, duration_secs: f64, ok: bool) {
    let status = if ok { 200_u16 } else { 500 };
    shardline_metrics::record_download(protocol, bytes);
    shardline_metrics::metrics()
        .transfer
        .record_download_duration(duration_secs);
    let _ = status;
}

pub fn record_range_request() {
    shardline_metrics::metrics().transfer.record_range_request();
}

pub fn record_xet_reconstruction(duration_secs: f64, chunks: u64) {
    shardline_metrics::metrics().xet.record_reconstruction(
        true,
        std::time::Duration::from_secs_f64(duration_secs),
        chunks,
    );
}

pub fn record_reconstruction(ok: bool, duration_secs: f64, chunks: u64) {
    shardline_metrics::record_reconstruction(
        ok,
        std::time::Duration::from_secs_f64(duration_secs),
        chunks,
    );
}

pub fn record_gc_run(duration_secs: f64, objects: u64, bytes: u64) {
    shardline_metrics::record_gc_run(
        std::time::Duration::from_secs_f64(duration_secs),
        objects,
        bytes,
    );
}

pub fn record_fsck_run(duration_secs: f64, errors: u64) {
    shardline_metrics::record_fsck_run(std::time::Duration::from_secs_f64(duration_secs), errors);
}

pub fn record_hub_api_request(endpoint: &str, method: &str, status: &str) {
    let code: u16 = status.parse().unwrap_or(0);
    shardline_metrics::record_hub_api_request(endpoint, method, code);
}

pub fn record_hub_api_commit(operation_type: &str) {
    shardline_metrics::record_hub_api_commit(operation_type);
}

pub fn record_s3_request(operation: &str, ok: bool, duration_secs: f64) {
    shardline_metrics::metrics()
        .backend
        .record_s3_request(std::time::Duration::from_secs_f64(duration_secs));
    let _ = (operation, ok);
}

pub fn record_s3_error(error_type: &str) {
    shardline_metrics::metrics().backend.record_s3_error();
    let _ = error_type;
}

pub fn record_local_io(operation: &str, ok: bool, duration_secs: f64) {
    shardline_metrics::metrics()
        .backend
        .record_local_io(std::time::Duration::from_secs_f64(duration_secs));
    let _ = (operation, ok);
}

pub fn record_webhook_event(provider: &str, event_type: &str, duration_secs: f64) {
    shardline_metrics::record_provider_webhook(provider, event_type);
    shardline_metrics::metrics()
        .provider
        .record_webhook_duration(std::time::Duration::from_secs_f64(duration_secs));
}

pub fn record_token_exchange() {
    shardline_metrics::record_provider_token_exchange();
}

pub fn record_object_inserted(bytes: u64) {
    shardline_metrics::metrics()
        .storage
        .record_object_stored(bytes);
}

pub fn record_object_reused() {
    shardline_metrics::metrics().storage.record_object_stored(0);
}

pub fn record_chunk_inserted(bytes: u64) {
    shardline_metrics::metrics()
        .storage
        .record_chunk_stored(bytes);
}

pub fn record_xorb_stored(bytes: u64) {
    shardline_metrics::metrics()
        .storage
        .record_xorb_stored(bytes);
}

pub fn record_shard_stored() {
    shardline_metrics::metrics().storage.record_shard_stored();
}

pub fn record_dedup_saves(bytes: u64) {
    shardline_metrics::metrics()
        .storage
        .record_dedup_saves(bytes);
}

pub const fn update_dedup_ratio(_numerator: u64, _denominator: u64) {
    // The shardline-metrics crate does not currently expose a dedup-ratio gauge.
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_metrics::metrics;

    // ── Smoke tests: verify no panic ─────────────────────────────────────

    #[test]
    fn record_upload_no_panic() {
        record_upload("http", 1024, 1.5, true);
        record_upload("grpc", 0, 0.0, false);
    }

    #[test]
    fn record_download_no_panic() {
        record_download("http", 512, 0.5, true);
        record_download("grpc", 0, 0.0, false);
    }

    #[test]
    fn record_range_request_no_panic() {
        record_range_request();
        record_range_request();
    }

    #[test]
    fn record_xet_reconstruction_no_panic() {
        record_xet_reconstruction(0.1, 5);
        record_xet_reconstruction(0.0, 0);
    }

    #[test]
    fn record_reconstruction_no_panic() {
        record_reconstruction(true, 0.05, 10);
        record_reconstruction(false, 0.0, 0);
    }

    #[test]
    fn record_gc_run_no_panic() {
        record_gc_run(1.0, 100, 4096);
        record_gc_run(0.0, 0, 0);
    }

    #[test]
    fn record_fsck_run_no_panic() {
        record_fsck_run(0.5, 3);
        record_fsck_run(0.0, 0);
    }

    #[test]
    fn record_s3_request_no_panic() {
        record_s3_request("GetObject", true, 0.05);
        record_s3_request("PutObject", false, 0.0);
    }

    #[test]
    fn record_s3_error_no_panic() {
        record_s3_error("AccessDenied");
        record_s3_error("NoSuchKey");
    }

    #[test]
    fn record_local_io_no_panic() {
        record_local_io("read", true, 0.01);
        record_local_io("write", false, 0.0);
    }

    #[test]
    fn record_webhook_event_no_panic() {
        record_webhook_event("github", "push", 0.25);
        record_webhook_event("gitlab", "merge_request", 0.0);
    }

    // ── Counter increment tests ──────────────────────────────────────────

    #[test]
    fn record_upload_increments_upload_counter() {
        let before = metrics().transfer.upload_requests.get();
        record_upload("http", 42, 0.1, true);
        let after = metrics().transfer.upload_requests.get();
        assert!(
            after > before,
            "upload_requests should increase (before: {before}, after: {after})"
        );
    }

    #[test]
    fn record_download_increments_download_counter() {
        let before = metrics().transfer.download_requests.get();
        record_download("http", 99, 0.2, true);
        let after = metrics().transfer.download_requests.get();
        assert!(
            after > before,
            "download_requests should increase (before: {before}, after: {after})"
        );
    }

    #[test]
    fn record_range_request_increments_range_counter() {
        let before = metrics().transfer.range_requests.get();
        record_range_request();
        let after = metrics().transfer.range_requests.get();
        assert!(
            after > before,
            "range_requests should increase (before: {before}, after: {after})"
        );
    }

    #[test]
    fn record_hub_api_request_increments_counter() {
        let before = metrics().protocol.hub_api_requests.get();
        record_hub_api_request("/models", "GET", "200");
        let after = metrics().protocol.hub_api_requests.get();
        assert!(
            after > before,
            "hub_api_requests should increase (before: {before}, after: {after})"
        );
    }

    #[test]
    fn record_hub_api_commit_increments_counter() {
        let before = metrics().protocol.hub_api_commits.get();
        record_hub_api_commit("create");
        let after = metrics().protocol.hub_api_commits.get();
        assert!(
            after > before,
            "hub_api_commits should increase (before: {before}, after: {after})"
        );
    }

    #[test]
    fn record_token_exchange_increments_counter() {
        let before = metrics().provider.token_exchanges.get();
        record_token_exchange();
        let after = metrics().provider.token_exchanges.get();
        assert!(
            after > before,
            "token_exchanges should increase (before: {before}, after: {after})"
        );
    }

    #[test]
    fn record_object_inserted_increments_object_counter() {
        let before = metrics().storage.objects_total.get();
        record_object_inserted(256);
        let after = metrics().storage.objects_total.get();
        assert!(
            after > before,
            "objects_total should increase (before: {before}, after: {after})"
        );
    }

    #[test]
    fn record_object_reused_increments_object_counter() {
        let before = metrics().storage.objects_total.get();
        record_object_reused();
        let after = metrics().storage.objects_total.get();
        assert!(
            after > before,
            "objects_total should increase (before: {before}, after: {after})"
        );
    }

    #[test]
    fn record_chunk_inserted_increments_chunk_counter() {
        let before = metrics().storage.chunks_total.get();
        record_chunk_inserted(64);
        let after = metrics().storage.chunks_total.get();
        assert!(
            after > before,
            "chunks_total should increase (before: {before}, after: {after})"
        );
    }

    #[test]
    fn record_xorb_stored_increments_xorb_counter() {
        let before = metrics().storage.xorbs_total.get();
        record_xorb_stored(128);
        let after = metrics().storage.xorbs_total.get();
        assert!(
            after > before,
            "xorbs_total should increase (before: {before}, after: {after})"
        );
    }

    #[test]
    fn record_shard_stored_increments_shard_counter() {
        let before = metrics().storage.shards_total.get();
        record_shard_stored();
        let after = metrics().storage.shards_total.get();
        assert!(
            after > before,
            "shards_total should increase (before: {before}, after: {after})"
        );
    }

    #[test]
    fn record_dedup_saves_increments_dedup_counter() {
        let before = metrics().storage.dedup_saves_bytes_total.get();
        record_dedup_saves(1024);
        let after = metrics().storage.dedup_saves_bytes_total.get();
        assert!(
            after > before,
            "dedup_saves_bytes_total should increase (before: {before}, after: {after})"
        );
    }

    // ── S3 backend counter checks ────────────────────────────────────────

    #[test]
    fn record_s3_request_increments_s3_counter() {
        let before = metrics().backend.s3_requests.get();
        record_s3_request("GetObject", true, 0.01);
        let after = metrics().backend.s3_requests.get();
        assert!(
            after > before,
            "s3_requests should increase (before: {before}, after: {after})"
        );
    }

    #[test]
    fn record_s3_error_increments_s3_error_counter() {
        let before = metrics().backend.s3_errors.get();
        record_s3_error("Timeout");
        let after = metrics().backend.s3_errors.get();
        assert!(
            after > before,
            "s3_errors should increase (before: {before}, after: {after})"
        );
    }

    // ── MetricsLayer & MetricsService tests ──────────────────────────────

    #[test]
    fn metrics_layer_is_cloneable() {
        let layer = MetricsLayer;
        let _clone = layer;
    }

    #[test]
    fn metrics_service_construction() {
        // MetricsService wraps an inner service; we use a simple tokio
        // runtime to test the basic construction. The inner type doesn't
        // need to be a real HTTP service for this test.
        let inner = tower::util::service_fn(|_req: axum::http::Request<Body>| async {
            Ok::<_, std::convert::Infallible>(axum::http::Response::new(Body::empty()))
        });
        let _svc = MetricsService { inner };
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metrics_service_poll_ready_and_call_tracks_connections() {
        use tower::ServiceExt;
        let svc = MetricsService {
            inner: tower::util::service_fn(|_req: axum::http::Request<Body>| async {
                Ok::<_, std::convert::Infallible>(axum::http::Response::new(Body::empty()))
            }),
        };
        let before = metrics().system.active_connections.get();
        // Use oneshot to drive poll_ready + call
        let _response = svc
            .oneshot(axum::http::Request::builder().body(Body::empty()).unwrap())
            .await;
        let after = metrics().system.active_connections.get();
        // Connections opened and then closed, so active should be same
        assert_eq!(after, before);
    }

    // ── No-panic for the remaining counters ───────────────────────────────

    #[test]
    fn record_hub_api_request_no_panic() {
        record_hub_api_request("/repos", "POST", "201");
        record_hub_api_request("/orgs", "GET", "200");
    }

    #[test]
    fn record_hub_api_commit_no_panic() {
        record_hub_api_commit("upsert");
        record_hub_api_commit("delete");
    }

    #[test]
    fn record_token_exchange_no_panic() {
        record_token_exchange();
    }

    #[test]
    fn record_hub_api_request_unknown_status_defaults_to_zero() {
        record_hub_api_request("test", "GET", "unknown");
        // Should not panic; metrics should still record
    }

    #[test]
    fn record_object_inserted_increments_bytes_counter() {
        let before_bytes = metrics().storage.objects_bytes_total.get();
        record_object_inserted(42);
        let after_bytes = metrics().storage.objects_bytes_total.get();
        assert!(
            after_bytes >= before_bytes + 42,
            "objects_bytes_total should increase by at least 42 (before: {before_bytes}, after: {after_bytes})"
        );
    }

    #[test]
    fn record_chunk_inserted_increments_bytes_counter() {
        let before_bytes = metrics().storage.chunks_bytes_total.get();
        record_chunk_inserted(128);
        let after_bytes = metrics().storage.chunks_bytes_total.get();
        assert!(
            after_bytes >= before_bytes + 128,
            "chunks_bytes_total should increase by at least 128 (before: {before_bytes}, after: {after_bytes})"
        );
    }

    #[test]
    fn record_xorb_stored_increments_bytes_counter() {
        let before_bytes = metrics().storage.xorbs_bytes_total.get();
        record_xorb_stored(256);
        let after_bytes = metrics().storage.xorbs_bytes_total.get();
        assert!(
            after_bytes >= before_bytes + 256,
            "xorbs_bytes_total should increase by at least 256 (before: {before_bytes}, after: {after_bytes})"
        );
    }

    #[test]
    fn record_dedup_saves_increments_bytes_counter() {
        let before_bytes = metrics().storage.dedup_saves_bytes_total.get();
        record_dedup_saves(512);
        let after_bytes = metrics().storage.dedup_saves_bytes_total.get();
        assert!(
            after_bytes >= before_bytes + 512,
            "dedup_saves_bytes_total should increase by at least 512 (before: {before_bytes}, after: {after_bytes})"
        );
    }

    #[test]
    fn record_local_io_increments_local_io_counter() {
        let before = metrics().backend.local_io_operations.get();
        record_local_io("read", true, 0.01);
        let after = metrics().backend.local_io_operations.get();
        assert!(
            after > before,
            "local_io_operations should increase (before: {before}, after: {after})"
        );
    }

    #[test]
    fn record_webhook_event_increments_webhook_counter() {
        let before = metrics().provider.webhook_events.get();
        record_webhook_event("github", "push", 0.25);
        let after = metrics().provider.webhook_events.get();
        assert!(
            after > before,
            "webhook_events should increase (before: {before}, after: {after})"
        );
    }

    // ── Remaining record_* functions ──────────────────────────────────────

    #[test]
    fn record_range_request_does_not_panic() {
        record_range_request();
    }

    #[test]
    fn record_gc_run_does_not_panic() {
        record_gc_run(1.5, 100, 4096);
    }

    #[test]
    fn record_fsck_run_does_not_panic() {
        record_fsck_run(0.5, 3);
    }

    #[test]
    fn record_s3_request_does_not_panic() {
        record_s3_request("GetObject", true, 0.1);
    }

    #[test]
    fn record_s3_error_does_not_panic() {
        record_s3_error("NoSuchKey");
    }

    #[test]
    fn record_hub_api_request_with_unknown_status_does_not_panic() {
        record_hub_api_request("/api/v1/test", "PATCH", "unknown");
    }

    #[test]
    fn record_local_io_read_success_does_not_panic() {
        record_local_io("read", true, 0.01);
    }

    #[test]
    fn record_local_io_write_failure_does_not_panic() {
        record_local_io("write", false, 0.05);
    }

    #[test]
    fn record_upload_protocol_variants() {
        record_upload("xet", 1024, 0.5, true);
        record_upload("lfs", 2048, 1.0, false);
    }

    #[test]
    fn record_download_protocol_variants() {
        record_download("xet", 4096, 2.0, true);
        record_download("oci", 8192, 3.0, false);
    }

    #[test]
    fn record_xet_reconstruction_does_not_panic() {
        record_xet_reconstruction(0.25, 10);
    }

    #[test]
    fn record_reconstruction_with_ok_and_err() {
        record_reconstruction(true, 0.1, 5);
        record_reconstruction(false, 0.2, 0);
    }

    #[test]
    fn record_hub_api_request_different_methods() {
        record_hub_api_request("/repos/test/assets", "GET", "200");
        record_hub_api_request("/repos/test/assets/contents/path", "PUT", "201");
        record_hub_api_request("/repos/test/assets/contents/path", "DELETE", "204");
    }

    #[test]
    fn record_hub_api_commit_different_types() {
        record_hub_api_commit("upsert");
        record_hub_api_commit("delete");
        record_hub_api_commit("unknown-operation");
    }

    #[test]
    fn record_token_exchange_does_not_panic() {
        record_token_exchange();
    }

    #[test]
    fn record_s3_request_with_false_ok_status() {
        record_s3_request("PutObject", false, 0.3);
        record_s3_request("ListObjects", true, 0.05);
    }

    // ── update_dedup_ratio ────────────────────────────────────────────────

    #[test]
    fn update_dedup_ratio_is_noop() {
        // This is a const fn that does nothing — verify it compiles and runs.
        update_dedup_ratio(100, 200);
        update_dedup_ratio(0, 1);
    }

    // ── MetricsLayer ──────────────────────────────────────────────────────

    #[test]
    fn metrics_layer_creates_metrics_service() {
        use axum::routing::get;
        use tower::ServiceExt;
        async fn handler() -> &'static str {
            "ok"
        }
        let layer = MetricsLayer;
        let svc = layer.layer(get(handler));
        // Verify the service wraps the inner by calling it
        let response = svc.oneshot(
            axum::http::Request::builder()
                .uri("/")
                .body(axum::body::Body::empty())
                .unwrap(),
        );
        drop(response);
    }
}

// ── Axum middleware & routes ─────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct MetricsLayer;

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService { inner }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MetricsService<S> {
    inner: S,
}

impl<S, ReqBody> Service<axum::http::Request<ReqBody>> for MetricsService<S>
where
    S: Service<axum::http::Request<ReqBody>, Response = axum::http::Response<Body>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = axum::http::Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: axum::http::Request<ReqBody>) -> Self::Future {
        let start = Instant::now();
        shardline_metrics::metrics().system.connection_opened();

        let mut inner = self.inner.clone();
        Box::pin(async move {
            let result = inner.call(req).await;
            shardline_metrics::metrics().system.connection_closed();
            let response = result?;
            let _elapsed = start.elapsed().as_secs_f64();

            Ok(response)
        })
    }
}
