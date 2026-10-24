//! Real HTTP disconnects must not publish incomplete LFS objects.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
