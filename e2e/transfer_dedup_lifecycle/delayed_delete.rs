//! Hold an actual S3 DELETE before MinIO applies it, without changing its reply.
use super::*;
use axum::{Router, body::Body, extract::State, http::Request, response::IntoResponse};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Semaphore;

#[derive(Clone)]
struct Proxy {
    backend: String,
    key: String,
    arrived: Arc<Semaphore>,
    release: Arc<Semaphore>,
    applied: Arc<AtomicUsize>,
    client: Client,
}

async fn forward(State(proxy): State<Proxy>, request: Request<Body>) -> axum::response::Response {
    let (parts, body) = request.into_parts();
    let Ok(bytes) = axum::body::to_bytes(body, 1024 * 1024).await else {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    };
    let target = (parts.method == axum::http::Method::DELETE
        && parts.uri.path().ends_with(&format!("/{}", proxy.key)))
        || (parts.method == axum::http::Method::POST
            && parts.uri.query().is_some_and(|query| {
                query
                    .split('&')
                    .any(|part| part.split('=').next() == Some("delete"))
            })
            && String::from_utf8_lossy(&bytes).contains(&format!("<Key>{}</Key>", proxy.key)));
    if target {
        proxy.arrived.add_permits(1);
        let Ok(permit) = proxy.release.acquire().await else {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        };
        permit.forget();
    }
    // Preserve the signed Host and request path; only the transport destination changes.
    let Ok(response) = proxy
        .client
        .request(parts.method, format!("{}{}", proxy.backend, parts.uri))
        .headers(parts.headers)
        .body(bytes)
        .send()
        .await
    else {
        return StatusCode::BAD_GATEWAY.into_response();
    };
    let status = response.status();
    let headers = response.headers().clone();
    let Ok(body) = response.bytes().await else {
        return StatusCode::BAD_GATEWAY.into_response();
    };
    if target && status.is_success() {
        proxy.applied.fetch_add(1, Ordering::SeqCst);
    }
    (status, headers, body).into_response()
}

pub(super) struct Attempt {
    proxy: tokio::task::JoinHandle<()>,
    request: tokio::task::JoinHandle<
        Result<shardline_storage::DeleteOutcome, shardline_server_core::ServerObjectStoreError>,
    >,
    release: Arc<Semaphore>,
    applied: Arc<AtomicUsize>,
}

impl Drop for Attempt {
    fn drop(&mut self) {
        self.release.close();
        self.request.abort();
        self.proxy.abort();
    }
}

impl Attempt {
    pub(super) async fn start(
        config: &S3ObjectStoreConfig,
        backend: String,
        key: &ObjectKey,
    ) -> Result<Self, Error> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let arrived = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let applied = Arc::new(AtomicUsize::new(0));
        let physical_key = config.key_prefix().map_or_else(
            || key.as_str().to_owned(),
            |prefix| format!("{prefix}/{}", key.as_str()),
        );
        let state = Proxy {
            backend,
            key: physical_key,
            arrived: Arc::clone(&arrived),
            release: Arc::clone(&release),
            applied: Arc::clone(&applied),
            client: Client::builder().timeout(Duration::from_secs(10)).build()?,
        };
        let store = ServerObjectStore::s3(config.clone().with_endpoint(Some(endpoint)))?;
        let key = key.clone();
        let proxy = tokio::spawn(async move {
            axum::serve(listener, Router::new().fallback(forward).with_state(state))
                .await
                .unwrap();
        });
        let request =
            tokio::spawn(async move { AsyncObjectStore::delete_if_present(&store, &key).await });
        let attempt = Self {
            proxy,
            request,
            release,
            applied,
        };
        tokio::time::timeout(Duration::from_secs(10), arrived.acquire())
            .await??
            .forget();
        assert_eq!(attempt.applied.load(Ordering::SeqCst), 0);
        Ok(attempt)
    }

    pub(super) async fn complete(mut self) -> Result<(), Error> {
        self.release.add_permits(1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), &mut self.request).await???,
            shardline_storage::DeleteOutcome::Deleted
        );
        assert_eq!(
            self.applied.load(Ordering::SeqCst),
            1,
            "exactly one backend mutation"
        );
        Ok(())
    }
}
