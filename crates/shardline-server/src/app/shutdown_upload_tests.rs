//! A cutover must preserve uploads already admitted before listener shutdown.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use sha2::{Digest, Sha256};
use shardline_protocol::{
    RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, TokenSigner,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admitted_upload_finishes_during_drain_and_reads_after_restart() {
    tokio::time::timeout(Duration::from_secs(30), exercise())
        .await
        .expect("upload drain and restart exceeded its deadline");
}

async fn exercise() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let key = b"0123456789abcdef0123456789abcdef";
    let config = ServerConfig::new(
        address,
        format!("http://{address}"),
        root.path().to_owned(),
        std::num::NonZeroUsize::new(65_536).unwrap(),
    )
    .with_deployment_mode(crate::DeploymentMode::Insecure)
    .with_server_frontends([crate::ServerFrontend::Lfs])
    .unwrap()
    .with_token_signing_key(key.to_vec())
    .unwrap();
    let repository =
        RepositoryScope::new(RepositoryProvider::Generic, "upgrade", "assets", None).unwrap();
    let claims = TokenClaims::new(
        "shardline",
        "uploader",
        TokenScope::Write,
        repository,
        u64::MAX,
    )
    .unwrap();
    let token = TokenSigner::new(key).unwrap().sign(&claims).unwrap();
    let payload = vec![0x5a; 1024 * 1024];
    let oid = format!("{:x}", Sha256::digest(&payload));
    let path = format!("/v1/lfs/objects/{oid}");
    let (shutdown, stop) = oneshot::channel();
    let mut server = tokio::spawn(serve_with_listener_until(config.clone(), listener, async {
        let _ = stop.await;
    }));
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    loop {
        if client
            .get(format!("http://{address}/healthz"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            break;
        }
        assert!(!server.is_finished(), "server stopped before readiness");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let connection = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut connection = BufReader::new(connection);
    connection.get_mut().write_all(format!(
        "PUT {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {token}\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
        payload.len(),
    ).as_bytes()).await.unwrap();
    // Hyper sends Continue when the authorized handler polls the body. This
    // establishes admission without a timing assumption or a synthetic route.
    let mut status = String::new();
    connection.read_line(&mut status).await.unwrap();
    assert!(
        status.starts_with("HTTP/1.1 100"),
        "upload was not admitted: {status}"
    );
    loop {
        let mut line = String::new();
        connection.read_line(&mut line).await.unwrap();
        if line == "\r\n" {
            break;
        }
        assert!(!line.is_empty(), "Continue headers were truncated");
    }
    shutdown.send(()).unwrap();
    // Wait for listener closure, not a fixed delay, while the upload stays open.
    loop {
        if tokio::net::TcpStream::connect(address).await.is_err() {
            break;
        }
        assert!(
            !server.is_finished(),
            "shutdown discarded an admitted upload"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !server.is_finished(),
        "shutdown completed before the upload body"
    );
    connection.get_mut().write_all(&payload).await.unwrap();
    let mut response = Vec::new();
    connection.read_to_end(&mut response).await.unwrap();
    let status =
        std::str::from_utf8(response.split(|byte| *byte == b'\n').next().unwrap()).unwrap();
    assert!(
        status.starts_with("HTTP/1.1 200") || status.starts_with("HTTP/1.1 201"),
        "draining upload failed: {status}"
    );
    (&mut server).await.unwrap().unwrap();

    let listener = TcpListener::bind(address).await.unwrap();
    let (shutdown, stop) = oneshot::channel();
    let restarted = tokio::spawn(serve_with_listener_until(config, listener, async {
        let _ = stop.await;
    }));
    // No replay may rewrite the object before the post-restart read.
    let response = client
        .get(format!("http://{address}{path}"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(response.bytes().await.unwrap().as_ref(), payload.as_slice());
    shutdown.send(()).unwrap();
    restarted.await.unwrap().unwrap();
}
