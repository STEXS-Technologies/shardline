use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use axum::{Router, body::Body, response::IntoResponse, routing::get};
use prometheus::{Encoder, TextEncoder};
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
    shardline_metrics::metrics()
        .xet
        .record_reconstruction(true, std::time::Duration::from_secs_f64(duration_secs), chunks);
}

pub fn record_reconstruction(ok: bool, duration_secs: f64, chunks: u64) {
    shardline_metrics::record_reconstruction(ok, std::time::Duration::from_secs_f64(duration_secs), chunks);
}

pub fn record_gc_run(duration_secs: f64, objects: u64, bytes: u64) {
    shardline_metrics::record_gc_run(std::time::Duration::from_secs_f64(duration_secs), objects, bytes);
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
    shardline_metrics::metrics().storage.record_object_stored(bytes);
}

pub fn record_object_reused() {
    shardline_metrics::metrics().storage.record_object_stored(0);
}

pub fn record_chunk_inserted(bytes: u64) {
    shardline_metrics::metrics().storage.record_chunk_stored(bytes);
}

pub fn record_xorb_stored(bytes: u64) {
    shardline_metrics::metrics().storage.record_xorb_stored(bytes);
}

pub fn record_shard_stored() {
    shardline_metrics::metrics().storage.record_shard_stored();
}

pub fn record_dedup_saves(bytes: u64) {
    shardline_metrics::metrics().storage.record_dedup_saves(bytes);
}

pub const fn update_dedup_ratio(_numerator: u64, _denominator: u64) {
    // The shardline-metrics crate does not currently expose a dedup-ratio gauge.
}

// ── Axum middleware & routes ─────────────────────────────────────────────

#[must_use]
pub(crate) fn metrics_routes<S: Clone + Send + Sync + 'static>() -> Router<S> {
    Router::new().route("/metrics", get(prometheus_handler))
}

async fn prometheus_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = shardline_metrics::registry().gather();
    let mut buffer = Vec::new();
    if encoder.encode(&metric_families, &mut buffer).is_err() {
        return axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    (
        [(
            axum::http::header::CONTENT_TYPE,
            encoder.format_type(),
        )],
        buffer,
    )
        .into_response()
}

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
