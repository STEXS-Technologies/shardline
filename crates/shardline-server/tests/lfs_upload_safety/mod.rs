//! Real HTTP disconnects must not publish incomplete LFS objects.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn boundary_body(chunk: bytes::Bytes, extra: bool) -> reqwest::Body {
    let frames = (0..64).map(move |_| Ok::<_, std::io::Error>(chunk.clone()));
    let tail = extra.then(|| Ok(bytes::Bytes::from_static(b"x")));
    reqwest::Body::wrap_stream(futures_util::stream::iter(frames.chain(tail)))
}

#[tokio::test]
async fn streamed_lfs_size_boundary_preserves_published_objects() {
    use futures_util::StreamExt;

    let chunk = bytes::Bytes::from(vec![73_u8; 1024 * 1024]);
    let mut digest = Sha256::new();
    for _ in 0..64 {
        digest.update(&chunk);
    }
    let oid = hex::encode(digest.clone().finalize());
    digest.update(b"x");
    let oversized_oid = hex::encode(digest.finalize());
    for (frontend, prefix, rejected) in [
        (
            ServerFrontend::Lfs,
            "/v1/lfs/objects",
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        (ServerFrontend::Hub, "/lfs/objects", StatusCode::BAD_REQUEST),
    ] {
        let root = tempfile::tempdir().unwrap();
        let server = start(root.path(), frontend, true, None).await;
        let credential = token("writer", "size-safety", TokenScope::Write);
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .unwrap();
        let url = format!("{}{prefix}/{oid}", server.base);
        let oversized_url = format!("{}{prefix}/{oversized_oid}", server.base);
        let uploaded = client
            .put(&url)
            .bearer_auth(&credential)
            .body(boundary_body(chunk.clone(), false))
            .send()
            .await
            .unwrap();
        assert!(uploaded.status().is_success());
        // No Content-Length: the server must enforce the actual streamed bytes,
        // including on its duplicate-object verification path.
        for target in [&oversized_url, &url] {
            let response = client
                .put(target)
                .bearer_auth(&credential)
                .body(boundary_body(chunk.clone(), true))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), rejected);
        }
        assert_eq!(
            client
                .get(&oversized_url)
                .bearer_auth(&credential)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        let response = client
            .get(&url)
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut stream = response.bytes_stream();
        let mut actual = Sha256::new();
        let mut length = 0;
        while let Some(frame) = stream.next().await {
            let frame = frame.unwrap();
            length += frame.len();
            actual.update(frame);
        }
        assert_eq!(length, 64 * 1024 * 1024);
        assert_eq!(hex::encode(actual.finalize()), oid);
        server.stop().await;
    }
}

#[tokio::test]
async fn disconnected_lfs_upload_can_retry_without_corrupting_published_bytes() {
    for (frontend, prefix) in [
        (ServerFrontend::Lfs, "/v1/lfs/objects"),
        (ServerFrontend::Hub, "/lfs/objects"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let server = start(root.path(), frontend, true, None).await;
        let credential = token("writer", "upload-safety", TokenScope::Write);
        let payload: Vec<u8> = (0..131_072).map(|offset| (offset % 251) as u8).collect();
        let oid = hex::encode(Sha256::digest(&payload));
        let path = format!("{prefix}/{oid}");
        let address = server.base.strip_prefix("http://").unwrap();
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        socket
            .write_all(
                format!(
                    "PUT {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {credential}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    payload.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        socket.write_all(&payload[..65_536]).await.unwrap();
        // Half-close the request while retaining the response side. Waiting for
        // EOF makes the failure observation independent of scheduling sleeps.
        socket.shutdown().await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            socket.read_to_end(&mut response),
        )
        .await
        .expect("incomplete HTTP request did not terminate")
        .unwrap();
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap();
        let url = format!("{}{path}", server.base);
        assert_eq!(
            client
                .get(&url)
                .bearer_auth(&credential)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert!(
            client
                .put(&url)
                .bearer_auth(&credential)
                .body(payload.clone())
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
        let mut damaged = payload.clone();
        damaged[0] ^= 1;
        assert!(
            !client
                .put(&url)
                .bearer_auth(&credential)
                .body(damaged)
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
        server.stop().await;
        let restarted = start(root.path(), frontend, true, None).await;
        let downloaded = client
            .get(format!("{}{path}", restarted.base))
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap();
        assert_eq!(downloaded.status(), StatusCode::OK);
        assert_eq!(
            downloaded.bytes().await.unwrap().as_ref(),
            payload.as_slice()
        );
        let other = token("other", "other-upload-safety", TokenScope::Write);
        assert!(
            !client
                .get(format!("{}{path}", restarted.base))
                .bearer_auth(other)
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
        restarted.stop().await;
    }
}
