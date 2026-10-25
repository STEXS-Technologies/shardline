//! Deployment startup deadlines must also bound silent HTTP peers.
use std::time::Duration;

use reqwest::Client;
use tokio::{net::TcpListener, time::timeout};

#[tokio::test]
async fn deployment_health_probe_bounds_a_silent_peer() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (accepted, mut observed) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (_connection, _) = listener.accept().await.unwrap();
        let _ = accepted.send(());
        std::future::pending::<()>().await;
    });
    let result = timeout(
        Duration::from_secs(42),
        super::wait_for_health(&Client::new(), &base),
    )
    .await;
    peer.abort();
    assert!(peer.await.unwrap_err().is_cancelled());
    assert!(observed.try_recv().is_ok(), "probe must reach the fixture");
    assert!(result.is_ok(), "deployment probe exceeded its own deadline");
    assert!(result.unwrap().is_err(), "silent peer cannot be healthy");
}

#[tokio::test]
async fn deployment_health_probe_accepts_a_healthy_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = axum::Router::new().route("/healthz", axum::routing::get(|| async { "ok" }));
    let peer = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let result = timeout(
        Duration::from_secs(2),
        super::wait_for_health(&Client::new(), &base),
    )
    .await;
    peer.abort();
    assert!(peer.await.unwrap_err().is_cancelled());
    assert!(result.unwrap().is_ok());
}
