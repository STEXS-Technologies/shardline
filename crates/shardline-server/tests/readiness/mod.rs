//! Shared startup qualification for real HTTP chaos fixtures.
use std::time::Duration;

pub(super) async fn wait_for_health(
    client: &reqwest::Client,
    base: &str,
) -> Result<(), &'static str> {
    wait_for_endpoint(client, base, "healthz").await
}

pub(super) async fn wait_for_endpoint(
    client: &reqwest::Client,
    base: &str,
    endpoint: &str,
) -> Result<(), &'static str> {
    let probe = async {
        loop {
            if let Ok(response) = client
                .get(format!("{base}/{endpoint}"))
                .timeout(Duration::from_secs(1))
                .send()
                .await
                && response.status().is_success()
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), probe)
        .await
        .unwrap_or(Err(
            "chaos fixture did not become healthy before its deadline",
        ))
}

#[tokio::test]
async fn error_response_does_not_qualify_but_recovery_does() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let healthy = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(tokio::sync::Notify::new());
    let status = Arc::clone(&healthy);
    let request = Arc::clone(&observed);
    let app = axum::Router::new().route(
        "/healthz",
        axum::routing::get(move || {
            let healthy = Arc::clone(&status);
            let observed = Arc::clone(&request);
            async move {
                observed.notify_one();
                if healthy.load(Ordering::SeqCst) {
                    axum::http::StatusCode::OK
                } else {
                    axum::http::StatusCode::SERVICE_UNAVAILABLE
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let probe = tokio::spawn(async move { wait_for_health(&reqwest::Client::new(), &base).await });
    tokio::time::timeout(Duration::from_secs(2), observed.notified())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let premature = probe.is_finished();
    healthy.store(true, Ordering::SeqCst);
    let result = tokio::time::timeout(Duration::from_secs(2), probe).await;
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    assert!(!premature, "HTTP error must not qualify as healthy");
    assert!(result.unwrap().unwrap().is_ok());
}

#[tokio::test]
async fn silent_peer_cannot_outlive_the_probe_deadline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (accepted, observed) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        let _ = accepted.send(());
        std::future::pending::<()>().await;
    });
    let result = tokio::time::timeout(
        Duration::from_secs(12),
        wait_for_health(&reqwest::Client::new(), &base),
    )
    .await;
    peer.abort();
    assert!(peer.await.unwrap_err().is_cancelled());
    observed.await.unwrap();
    assert!(result.is_ok(), "probe must enforce its own deadline");
    assert!(result.unwrap().is_err(), "silent peer must not qualify");
}
