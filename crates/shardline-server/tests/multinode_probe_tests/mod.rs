//! Shutdown means a refused fresh connection, not a broken HTTP response.
use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn malformed_http_from_a_live_listener_does_not_qualify_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let base = format!("http://{address}");
    let observed = Arc::new(AtomicUsize::new(0));
    let connections = Arc::clone(&observed);
    let peer = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            connections.fetch_add(1, Ordering::SeqCst);
            let _ = socket.write_all(b"not-http\r\n\r\n").await;
        }
    });
    let result = tokio::time::timeout(
        Duration::from_secs(12),
        wait_for_server_down(&Client::new(), &base),
    )
    .await;
    peer.abort();
    assert!(peer.await.unwrap_err().is_cancelled());
    assert!(
        observed.load(Ordering::SeqCst) > 0,
        "probe must reach live listener"
    );
    assert!(
        result.is_ok(),
        "shutdown probe must enforce its own deadline"
    );
    assert!(
        result.unwrap().is_err(),
        "malformed HTTP is not a stopped listener"
    );
    tokio::time::timeout(
        Duration::from_secs(2),
        wait_for_server_down(&Client::new(), &base),
    )
    .await
    .unwrap()
    .unwrap();
}
