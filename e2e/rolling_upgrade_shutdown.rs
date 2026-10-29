//! A stopped deployment requires a closed listener, not merely broken HTTP.
use std::time::Duration;
use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};

#[tokio::test]
async fn silent_status_headers_cannot_stall_upgrade_qualification() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (_connection, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let result = timeout(
        super::PROBE_TIMEOUT + Duration::from_secs(2),
        super::ready_response(&reqwest::Client::new(), &base),
    )
    .await;
    peer.abort();
    assert!(peer.await.unwrap_err().is_cancelled());
    assert!(result.is_ok(), "probe must enforce its own deadline");
    let error = result.unwrap().unwrap_err();
    assert!(error.downcast_ref::<reqwest::Error>().unwrap().is_timeout());
}

#[tokio::test]
async fn stalled_status_json_cannot_stall_upgrade_qualification() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = axum::Router::new()
        .route("/readyz", axum::routing::get(|| async { "ok" }))
        .route(
            "/api/v1/status",
            axum::routing::get(|| async {
                axum::body::Body::from_stream(futures_util::stream::pending::<
                    Result<axum::body::Bytes, std::io::Error>,
                >())
            }),
        );
    let peer = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let result = timeout(
        super::PROBE_TIMEOUT + Duration::from_secs(2),
        super::ready_response(&reqwest::Client::new(), &base),
    )
    .await;
    peer.abort();
    assert!(peer.await.unwrap_err().is_cancelled());
    assert!(result.is_ok(), "JSON body must share the request deadline");
    let error = result.unwrap().unwrap_err();
    assert!(error.downcast_ref::<reqwest::Error>().unwrap().is_timeout());
}

#[tokio::test]
async fn malformed_http_does_not_prove_listener_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        loop {
            let (mut connection, _) = listener.accept().await.unwrap();
            let _ = connection.write_all(b"invalid HTTP\r\n\r\n").await;
        }
    });
    let result = timeout(Duration::from_secs(12), super::wait_for_server_down(&base)).await;
    peer.abort();
    assert!(peer.await.unwrap_err().is_cancelled());
    assert!(result.is_ok(), "shutdown probe must have its own deadline");
    assert!(result.unwrap().is_err(), "an open listener is not stopped");
}

#[tokio::test]
async fn silent_http_does_not_prove_listener_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (_connection, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let result = timeout(Duration::from_secs(12), super::wait_for_server_down(&base)).await;
    peer.abort();
    assert!(peer.await.unwrap_err().is_cancelled());
    assert!(
        result.is_ok(),
        "silent peer cannot stall shutdown qualification"
    );
    assert!(result.unwrap().is_err(), "an open listener is not stopped");
}

#[tokio::test]
async fn refused_listener_proves_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    timeout(Duration::from_secs(2), super::wait_for_server_down(&base))
        .await
        .unwrap()
        .unwrap();
}
