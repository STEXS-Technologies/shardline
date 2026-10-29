use super::*;
use futures_util::FutureExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const IDLE: Duration = Duration::from_secs(10);

#[tokio::test(start_paused = true)]
async fn blocked_write_expires_and_connection_stays_failed() {
    let (inner, _peer) = tokio::io::duplex(1);
    let mut writer = IdleWrite::new(inner, IDLE);
    writer.write_all(b"a").await.unwrap();
    assert_eq!(
        writer.write_all(b"b").await.unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(
        writer.flush().await.unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(
        writer.read_u8().await.unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
}

#[tokio::test(start_paused = true)]
async fn progressing_writes_can_run_longer_than_idle_limit() {
    let (inner, mut peer) = tokio::io::duplex(1);
    let mut writer = IdleWrite::new(inner, IDLE);
    writer.write_all(b"a").await.unwrap();
    for _ in 0..4 {
        assert!(writer.write_all(b"a").now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(6)).await;
        assert_eq!(peer.read_u8().await.unwrap(), b'a');
        writer.write_all(b"a").await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn unused_keepalive_connection_has_no_write_deadline() {
    let (inner, _peer) = tokio::io::duplex(1);
    let mut writer = IdleWrite::new(inner, IDLE);
    tokio::time::advance(Duration::from_secs(100)).await;
    writer.write_all(b"a").await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn flush_empty_writes_and_incoming_bytes_do_not_hide_write_stall() {
    let (inner, mut peer) = tokio::io::duplex(1);
    let mut writer = IdleWrite::new(inner, IDLE);
    writer.write_all(b"a").await.unwrap();
    assert!(writer.write_all(b"b").now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(6)).await;
    writer.flush().await.unwrap();
    // Some AsyncWrite implementations return Pending even for an empty write
    // when full. Neither that result nor Ready(0) is byte progress.
    if let Some(result) = writer.write(&[]).now_or_never() {
        assert_eq!(result.unwrap(), 0);
    }
    peer.write_all(b"x").await.unwrap();
    assert_eq!(writer.read_u8().await.unwrap(), b'x');
    tokio::time::advance(Duration::from_secs(4)).await;
    assert_eq!(
        writer.write_all(b"b").await.unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
}

#[tokio::test(start_paused = true)]
async fn vectored_writes_preserve_bytes_and_also_expire_when_stalled() {
    let (inner, mut peer) = tokio::io::duplex(2);
    let vectored = inner.is_write_vectored();
    let mut writer = IdleWrite::new(inner, IDLE);
    assert_eq!(writer.is_write_vectored(), vectored);
    let slices = [io::IoSlice::new(b"a"), io::IoSlice::new(b"b")];
    assert_eq!(writer.write_vectored(&slices).await.unwrap(), 2);
    let mut bytes = [0; 2];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"ab");
    writer.write_all(b"ab").await.unwrap();
    assert_eq!(
        writer.write_vectored(&slices).await.unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
}

struct PendingFlush;
impl AsyncRead for PendingFlush {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}
impl AsyncWrite for PendingFlush {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

#[tokio::test(start_paused = true)]
async fn pending_flush_and_shutdown_are_bounded() {
    let mut flushing = IdleWrite::new(PendingFlush, IDLE);
    assert_eq!(
        flushing.flush().await.unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    let mut closing = IdleWrite::new(PendingFlush, IDLE);
    assert_eq!(
        closing.shutdown().await.unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
}

#[tokio::test]
async fn nonreading_tcp_client_releases_transport_owned_buffer_admission() {
    use axum::{body::Body, response::Response};
    use futures_util::stream;
    use std::sync::{Arc, Mutex};

    let pool = crate::admission::BoundedPool::new(std::num::NonZeroUsize::MIN);
    let bytes = crate::admission::retain_bytes(
        vec![0x7f; 32 * 1024 * 1024],
        pool.acquire_buffer().await.unwrap(),
    );
    let source = Arc::new(Mutex::new(Some(bytes)));
    let route_source = source.clone();
    let app = axum::Router::new().route(
        "/stream",
        axum::routing::get(move || {
            let bytes = route_source.lock().unwrap().take().unwrap();
            async move {
                Response::new(Body::from_stream(stream::once(async move {
                    Ok::<_, io::Error>(bytes)
                })))
            }
        }),
    );
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let listener = WriteIdleListener::new(tcp, Duration::from_millis(250));
    assert_eq!(listener.local_addr().unwrap(), address);
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    client
        .write_all(b"GET /stream HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while source.lock().unwrap().is_some() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        pool.available_buffer_permits(),
        0,
        "the actual transport still owns the emitted buffer"
    );
    // Keep the client socket open and never read. Only closing the server-side
    // stalled connection may release this charge; dropping its producer cannot.
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.available_buffer_permits() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    drop(client);
    server.abort();
    let _ = server.await;
}
