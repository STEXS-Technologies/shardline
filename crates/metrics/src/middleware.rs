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
        MetricsService {
            inner,
            metrics: self.metrics.clone(),
        }
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

            metrics
                .transfer
                .record_upload_duration(elapsed.as_secs_f64());
            metrics
                .transfer
                .record_download_duration(elapsed.as_secs_f64());

            metrics.system.connection_closed();

            tracing::debug!(
                method,
                path,
                status,
                duration_ms = elapsed.as_millis() as u64,
                "http request"
            );

            Ok(response)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::Request;
    use prometheus::Registry;
    use tower::Layer;

    use super::{MetricsLayer, MetricsService};
    use crate::CasMetrics;

    #[test]
    fn metrics_layer_and_service_are_clone() {
        let registry = Registry::new();
        let metrics = Arc::new(CasMetrics::new(&registry));
        let layer = MetricsLayer::new(metrics);
        let _cloned_layer = layer.clone();

        let svc = tower::service_fn(|_req: Request<Body>| {
            async {
                Ok::<_, std::convert::Infallible>(
                    axum::response::Response::builder()
                        .status(200)
                        .body(Body::empty())
                        .unwrap(),
                )
            }
        });
        let metrics_svc: MetricsService<_> = layer.layer(svc);
        let _cloned_svc = metrics_svc;
    }

    #[test]
    fn metrics_layer_new_stores_metrics_reference() {
        let registry = Registry::new();
        let metrics = Arc::new(CasMetrics::new(&registry));
        let layer = MetricsLayer::new(metrics);
        // The layer should be usable to wrap a service.
        let svc = tower::service_fn(|_req: Request<Body>| {
            async {
                Ok::<_, std::convert::Infallible>(
                    axum::response::Response::builder()
                        .status(200)
                        .body(Body::empty())
                        .unwrap(),
                )
            }
        });
        let _wrapped: MetricsService<_> = layer.layer(svc);
    }

    #[tokio::test]
    async fn metrics_service_call_records_metrics() {
        use std::sync::Arc;
        use axum::body::Body;
        use axum::http::Request;
        use prometheus::{Encoder, Registry, TextEncoder};
        use tower::Service;
        use tower::ServiceExt;

        let registry = Registry::new();
        let metrics = Arc::new(CasMetrics::new(&registry));
        let layer = MetricsLayer::new(metrics);

        let svc = tower::service_fn(|_req: Request<Body>| {
            async {
                Ok::<_, std::convert::Infallible>(
                    axum::response::Response::builder()
                        .status(200)
                        .body(Body::empty())
                        .unwrap(),
                )
            }
        });
        let mut wrapped = layer.layer(svc);

        let req = Request::get("/health").body(Body::empty()).unwrap();
        let response = wrapped.ready().await.unwrap().call(req).await.unwrap();
        assert_eq!(response.status(), 200);

        // Verify metrics were recorded
        let encoder = TextEncoder::new();
        let families = registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&families, &mut buffer).unwrap();
        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("shardline_active_connections"));
        assert!(output.contains("shardline_upload_duration_seconds_count"));
        assert!(output.contains("shardline_download_duration_seconds_count"));
    }

    #[tokio::test]
    async fn metrics_service_call_with_error_status_still_records() {
        use std::sync::Arc;
        use axum::body::Body;
        use axum::http::Request;
        use prometheus::{Encoder, Registry, TextEncoder};
        use tower::Service;
        use tower::ServiceExt;

        let registry = Registry::new();
        let metrics = Arc::new(CasMetrics::new(&registry));
        let layer = MetricsLayer::new(metrics);

        let svc = tower::service_fn(|_req: Request<Body>| {
            async {
                Ok::<_, std::convert::Infallible>(
                    axum::response::Response::builder()
                        .status(404)
                        .body(Body::empty())
                        .unwrap(),
                )
            }
        });
        let mut wrapped = layer.layer(svc);

        let req = Request::get("/missing").body(Body::empty()).unwrap();
        let response = wrapped.ready().await.unwrap().call(req).await.unwrap();
        assert_eq!(response.status(), 404);

        // Metrics still recorded
        let encoder = TextEncoder::new();
        let families = registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&families, &mut buffer).unwrap();
        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("shardline_active_connections"));
    }

    #[tokio::test]
    async fn metrics_service_call_with_server_error_still_records() {
        use std::sync::Arc;
        use axum::body::Body;
        use axum::http::Request;
        use prometheus::{Encoder, Registry, TextEncoder};
        use tower::Service;
        use tower::ServiceExt;

        let registry = Registry::new();
        let metrics = Arc::new(CasMetrics::new(&registry));
        let layer = MetricsLayer::new(metrics);

        let svc = tower::service_fn(|_req: Request<Body>| {
            async {
                Ok::<_, std::convert::Infallible>(
                    axum::response::Response::builder()
                        .status(500)
                        .body(Body::empty())
                        .unwrap(),
                )
            }
        });
        let mut wrapped = layer.layer(svc);

        let req = Request::get("/error").body(Body::empty()).unwrap();
        let response = wrapped.ready().await.unwrap().call(req).await.unwrap();
        assert_eq!(response.status(), 500);

        let encoder = TextEncoder::new();
        let families = registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&families, &mut buffer).unwrap();
        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("shardline_active_connections"));
        assert!(output.contains("shardline_upload_duration_seconds_count"));
        assert!(output.contains("shardline_download_duration_seconds_count"));
    }
}
