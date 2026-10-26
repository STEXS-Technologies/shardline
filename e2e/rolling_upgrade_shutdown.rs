//! A stopped deployment requires a closed listener, not merely broken HTTP.
use std::time::Duration;
use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};

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
