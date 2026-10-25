//! Real TCP peers that accept a health probe without sending response headers.
use std::time::Duration;

use tokio::{net::TcpListener, time::timeout};

#[tokio::test]
async fn health_probe_rejects_an_unresponsive_peer_within_its_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (accepted, mut observed) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (_connection, _) = listener.accept().await.unwrap();
        let _ = accepted.send(());
        std::future::pending::<()>().await;
    });
    let result = timeout(
        Duration::from_secs(12),
        super::support::wait_for_health(&base),
    )
    .await;
    peer.abort();
    let _ = peer.await;
    assert!(
        observed.try_recv().is_ok(),
        "probe must reach its TCP fixture"
    );
    assert!(result.is_ok(), "health probe exceeded its own deadline");
    assert!(result.unwrap().is_err(), "silent peer cannot be healthy");
}

#[tokio::test]
async fn health_probe_accepts_a_healthy_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = axum::Router::new().route("/healthz", axum::routing::get(|| async { "ok" }));
    let peer = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let result = timeout(
        Duration::from_secs(2),
        super::support::wait_for_health(&base),
    )
    .await;
    peer.abort();
    let _ = peer.await;
    assert!(result.unwrap().is_ok());
}
