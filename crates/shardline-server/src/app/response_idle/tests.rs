use super::*;
use futures_util::{StreamExt, stream};
use std::sync::atomic::{AtomicBool, Ordering};

const IDLE: Duration = Duration::from_secs(10);

struct DropProbe(Arc<AtomicBool>);
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn pending_body(dropped: Arc<AtomicBool>) -> Body {
    let guard = DropProbe(dropped);
    Body::from_stream(stream::unfold(guard, |guard| async move {
        std::future::pending::<()>().await;
        Some((Ok::<_, std::io::Error>(Bytes::new()), guard))
    }))
}

async fn frame(body: &mut IdleBody) -> Option<Result<Frame<Bytes>, axum::Error>> {
    futures_util::future::poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await
}

#[tokio::test(start_paused = true)]
async fn unpolled_response_releases_producer_and_reports_truncation() {
    let dropped = Arc::new(AtomicBool::new(false));
    let mut body = IdleBody::new(pending_body(dropped.clone()), IDLE);
    tokio::time::advance(IDLE).await;
    tokio::task::yield_now().await;
    assert!(
        dropped.load(Ordering::SeqCst),
        "cleanup must not depend on a body poll"
    );
    assert!(
        !body.is_end_stream(),
        "expiry must surface an error before EOF"
    );
    assert!(frame(&mut body).await.unwrap().is_err());
    assert!(frame(&mut body).await.is_none());
    assert!(body.is_end_stream());
}

#[tokio::test(start_paused = true)]
async fn pending_provider_is_woken_and_cancelled_at_idle_deadline() {
    let dropped = Arc::new(AtomicBool::new(false));
    let mut body = IdleBody::new(pending_body(dropped.clone()), IDLE);
    assert!(frame(&mut body).await.unwrap().is_err());
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn continuous_progress_allows_a_transfer_longer_than_idle_limit() {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let stream = stream::unfold(receiver, |mut receiver| async move {
        receiver
            .recv()
            .await
            .map(|bytes| (Ok::<_, std::io::Error>(bytes), receiver))
    });
    let mut body = IdleBody::new(Body::from_stream(stream), IDLE);
    for _ in 0..4 {
        tokio::time::advance(Duration::from_secs(6)).await;
        sender.send(Bytes::from_static(b"progress")).await.unwrap();
        assert_eq!(
            frame(&mut body)
                .await
                .unwrap()
                .unwrap()
                .into_data()
                .unwrap(),
            "progress"
        );
    }
    drop(sender);
    assert!(frame(&mut body).await.is_none());
}

#[tokio::test(start_paused = true)]
async fn empty_frames_do_not_keep_an_idle_producer_alive() {
    let body = Body::from_stream(stream::repeat_with(
        || Ok::<_, std::io::Error>(Bytes::new()),
    ));
    let mut body = IdleBody::new(body, IDLE);
    tokio::time::advance(Duration::from_secs(6)).await;
    assert!(
        frame(&mut body)
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .is_empty()
    );
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(frame(&mut body).await.unwrap().is_err());
}

#[tokio::test(start_paused = true)]
async fn cancellation_releases_producer_without_waiting_for_timeout() {
    let dropped = Arc::new(AtomicBool::new(false));
    let body = IdleBody::new(pending_body(dropped.clone()), IDLE);
    let weak = Arc::downgrade(&body.shared);
    drop(body);
    assert!(dropped.load(Ordering::SeqCst));
    assert!(weak.upgrade().is_none());
}

#[tokio::test(start_paused = true)]
async fn timeout_does_not_release_transport_owned_buffer_charges() {
    let pool = crate::admission::BoundedPool::new(std::num::NonZeroUsize::MIN);
    let bytes = crate::admission::retain_bytes(vec![7; 128], pool.acquire_buffer().await.unwrap());
    let dropped = Arc::new(AtomicBool::new(false));
    let source = stream::once(async move { Ok::<_, axum::Error>(bytes) })
        .chain(pending_body(dropped.clone()).into_data_stream());
    let mut body = IdleBody::new(Body::from_stream(source), IDLE);
    let held = frame(&mut body)
        .await
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    let slice = held.slice(1..2);
    tokio::time::advance(IDLE).await;
    tokio::task::yield_now().await;
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(pool.available_buffer_permits(), 0);
    drop(held);
    assert_eq!(pool.available_buffer_permits(), 0);
    drop(slice);
    assert_eq!(pool.available_buffer_permits(), 1);
}

struct TrailerBody(Option<Frame<Bytes>>);
impl HttpBody for TrailerBody {
    type Data = Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        Poll::Ready(self.0.take().map(Ok))
    }
}

#[tokio::test(start_paused = true)]
async fn response_headers_extensions_and_trailers_are_preserved() {
    let mut trailers = axum::http::HeaderMap::new();
    trailers.insert("x-checksum", "verified".parse().unwrap());
    let mut response = Response::builder()
        .status(206)
        .header("etag", "identity")
        .body(Body::new(TrailerBody(Some(Frame::trailers(
            trailers.clone(),
        )))))
        .unwrap();
    response.extensions_mut().insert(42_u32);
    let response = wrap(response, IDLE);
    assert_eq!(response.status(), 206);
    assert_eq!(response.headers()["etag"], "identity");
    assert_eq!(response.extensions().get::<u32>(), Some(&42));
    let mut body = response.into_body();
    let received = futures_util::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.into_trailers().unwrap(), trailers);
}

#[tokio::test]
async fn real_http_nonreading_client_does_not_retain_response_producer() {
    use tokio::io::AsyncWriteExt;
    let dropped = Arc::new(AtomicBool::new(false));
    let observed = dropped.clone();
    let app = axum::Router::new().route(
        "/stream",
        axum::routing::get(move || {
            let guard = DropProbe(observed.clone());
            async move {
                let source = stream::unfold(guard, |guard| async move {
                    Some((
                        Ok::<_, std::io::Error>(Bytes::from_static(&[0x7f; 64 * 1024])),
                        guard,
                    ))
                });
                wrap(
                    Response::new(Body::from_stream(source)),
                    Duration::from_millis(200),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    client
        .write_all(b"GET /stream HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    // Never read headers or body. The watchdog must clean up even once the
    // kernel send buffer fills and Hyper stops polling the body.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    drop(client);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn real_http_stalled_source_fails_instead_of_completing_partial_download() {
    let dropped = Arc::new(AtomicBool::new(false));
    let observed = dropped.clone();
    let app = axum::Router::new().route(
        "/stream",
        axum::routing::get(move || {
            let pending = pending_body(observed.clone());
            async move {
                let source =
                    stream::once(async { Ok::<_, axum::Error>(Bytes::from_static(b"prefix")) })
                        .chain(pending.into_data_stream());
                let response = Response::builder()
                    .header("content-length", "1024")
                    .body(Body::from_stream(source))
                    .unwrap();
                wrap(response, Duration::from_millis(100))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let response = reqwest::Client::new()
        .get(format!("http://{address}/stream"))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    assert!(
        tokio::time::timeout(Duration::from_secs(5), response.bytes())
            .await
            .unwrap()
            .is_err()
    );
    assert!(dropped.load(Ordering::SeqCst));
    server.abort();
    let _ = server.await;
}
