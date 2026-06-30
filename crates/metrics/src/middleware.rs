use std::task::{Context, Poll};
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, Response};
use tower::{Layer, Service};

use crate::CasMetrics;

#[derive(Clone)]
pub struct MetricsLayer {
    metrics: std::sync::Arc<CasMetrics>,
}

impl MetricsLayer {
    #[must_use]
    pub const fn new(metrics: std::sync::Arc<CasMetrics>) -> Self {
        Self { metrics }
    }
}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService { inner, metrics: self.metrics.clone() }
    }
}

#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
    metrics: std::sync::Arc<CasMetrics>,
}

impl<S> Service<Request<Body>> for MetricsService<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = futures_util::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let start = Instant::now();
        let method = req.method().to_string();
        let path = req.uri().path().to_owned();

        self.metrics.system.connection_opened();

        let mut inner = self.inner.clone();
        let metrics = self.metrics.clone();

        Box::pin(async move {
            let response = inner.call(req).await?;
            let status = response.status().as_u16();
            let elapsed = start.elapsed();

            metrics.transfer.record_upload_duration(elapsed.as_secs_f64());
            metrics.transfer.record_download_duration(elapsed.as_secs_f64());

            metrics.system.connection_closed();

            tracing::debug!(method, path, status, duration_ms = elapsed.as_millis() as u64, "http request");

            Ok(response)
        })
    }
}
