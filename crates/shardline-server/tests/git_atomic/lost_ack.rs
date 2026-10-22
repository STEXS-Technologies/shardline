//! Drop the client response only after the real server commits both refs.
use super::*;
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::Request,
    response::Response,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Clone)]
struct Proxy {
    backend: String,
    client: Client,
    committed: Arc<AtomicBool>,
}

async fn forward(State(proxy): State<Proxy>, request: Request<Body>) -> Response {
    let (parts, body) = request.into_parts();
    let publication =
        parts.method == axum::http::Method::POST && parts.uri.path().ends_with("/git-receive-pack");
    let body = axum::body::to_bytes(body, 8 * 1024 * 1024).await.unwrap();
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
    if publication && !proxy.committed.load(Ordering::SeqCst) {
        assert_eq!(status, StatusCode::OK);
        let report = String::from_utf8_lossy(&bytes);
        assert!(report.contains("ok refs/heads/main"), "{report}");
        assert!(report.contains("ok refs/heads/peer"), "{report}");
        proxy.committed.store(true, Ordering::SeqCst);
        return Response::new(Body::from_stream(futures_util::stream::iter([
            Ok(Bytes::from_static(b"000")),
            Err(std::io::Error::other(
                "injected lost Git publication status",
            )),
        ])));
    }
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

pub(super) async fn commit_without_delivering_status(
    backend: &str,
    repository: &str,
    checkout: &Path,
) {
    let committed = Arc::new(AtomicBool::new(false));
    let proxy = Proxy {
        backend: backend.to_owned(),
        client: Client::new(),
        committed: Arc::clone(&committed),
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
    let output = git(
        checkout,
        &["push", "--atomic", &endpoint, "HEAD:main", "HEAD:peer"],
    )
    .await;
    task.abort();
    let _stopped = task.await;
    assert!(
        committed.load(Ordering::SeqCst),
        "proxy never observed successful durable publication"
    );
    assert!(
        !output.status.success(),
        "native Git unexpectedly received the dropped status"
    );
    let head = success(checkout, &["rev-parse", "HEAD"]).await.stdout;
    let head = std::str::from_utf8(&head).unwrap().trim();
    for reference in ["refs/heads/main", "refs/heads/peer"] {
        let advertised = success(checkout, &["ls-remote", "origin", reference]).await;
        assert_eq!(
            std::str::from_utf8(&advertised.stdout).unwrap(),
            format!("{head}\t{reference}\n")
        );
    }
    // The parent then retries through the ordinary endpoint. Git discovers
    // both committed heads instead of replaying a stale ref move.
}
