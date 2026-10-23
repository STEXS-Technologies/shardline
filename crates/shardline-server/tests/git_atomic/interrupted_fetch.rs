//! Break one shallow-fetch response mid-pack and verify the client can retry.
use super::*;
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::Request,
    response::Response,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Clone)]
struct Proxy {
    backend: String,
    client: Client,
    interrupted: Arc<AtomicBool>,
    upload_pack_requests: Arc<AtomicUsize>,
}

async fn forward(State(proxy): State<Proxy>, request: Request<Body>) -> Response {
    let (parts, body) = request.into_parts();
    let upload_pack =
        parts.method == axum::http::Method::POST && parts.uri.path().ends_with("/git-upload-pack");
    if upload_pack {
        proxy.upload_pack_requests.fetch_add(1, Ordering::SeqCst);
    }
    let body = axum::body::to_bytes(body, 16 * 1024 * 1024).await.unwrap();
    let upstream = proxy
        .client
        .request(parts.method, format!("{}{}", proxy.backend, parts.uri))
        .headers(parts.headers)
        .body(body)
        .send()
        .await
        .unwrap();
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let bytes = upstream.bytes().await.unwrap();
    if upload_pack && !proxy.interrupted.swap(true, Ordering::SeqCst) {
        assert!(
            bytes.len() > 1,
            "upload-pack response was unexpectedly empty"
        );
        let keep = bytes.len() / 2;
        let partial = Bytes::copy_from_slice(&bytes[..keep]);
        let mut response = Response::new(Body::from_stream(futures_util::stream::iter([
            Ok(partial),
            Err(std::io::Error::other(
                "injected interrupted Git pack response",
            )),
        ])));
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        return response;
    }
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

pub(super) async fn fail_once_then_retry(directory: &Path, backend: &str, repository: &str) {
    let interrupted = Arc::new(AtomicBool::new(false));
    let upload_pack_requests = Arc::new(AtomicUsize::new(0));
    let proxy = Proxy {
        backend: backend.to_owned(),
        client: Client::new(),
        interrupted: Arc::clone(&interrupted),
        upload_pack_requests: Arc::clone(&upload_pack_requests),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/models/{repository}",
        listener.local_addr().unwrap()
    );
    let router = axum::Router::new().fallback(forward).with_state(proxy);
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let updated = git(directory, &["remote", "set-url", "origin", &endpoint]).await;
    assert!(updated.status.success());
    let fetched = git(directory, &["fetch", "--deepen=1", "origin", "main"]).await;
    assert!(
        interrupted.load(Ordering::SeqCst),
        "proxy did not cut upload-pack"
    );
    assert!(
        fetched.status.success(),
        "Git did not recover from the interrupted pack response: {}",
        String::from_utf8_lossy(&fetched.stderr)
    );
    assert!(
        upload_pack_requests.load(Ordering::SeqCst) >= 2,
        "Git did not retry upload-pack after the interrupted response"
    );
    success(directory, &["fsck", "--strict"]).await;
    assert_eq!(
        success(directory, &["rev-list", "--count", "HEAD"])
            .await
            .stdout,
        b"4\n"
    );
    let backend_endpoint = format!("{backend}/models/{repository}");
    let restored = git(
        directory,
        &["remote", "set-url", "origin", &backend_endpoint],
    )
    .await;
    assert!(restored.status.success());
    task.abort();
    let _stopped = task.await;
}
