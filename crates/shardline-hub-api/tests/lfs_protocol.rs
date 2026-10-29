#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Error responses exposed by the Hub's LFS routes must follow the LFS API
//! media type and body contract, including failures raised before a handler.

mod support;

use axum::{body::Body, http::Request};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use http_body_util::BodyExt;
use sha2::{Digest, Sha256};
use shardline_index::hub::HubRepoType;
use shardline_server_core::ServerObjectStore;
use std::{collections::HashSet, time::Duration};
use tokio::{io::AsyncWriteExt, net::TcpListener};
use tower::ServiceExt;

use support::common;

#[tokio::test]
async fn malformed_batch_body_returns_git_lfs_error_contract() {
    let test = common::setup();
    test.state()
        .store
        .create_repo(HubRepoType::Model, "alice/lfs-errors", false)
        .unwrap();

    let response = test
        .app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/alice/lfs-errors.git/info/lfs/objects/batch")
                .header("content-type", "application/vnd.git-lfs+json")
                .body(Body::from("{ malformed"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/vnd.git-lfs+json"
    );
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        body.get("message")
            .and_then(serde_json::Value::as_str)
            .is_some()
    );
    assert!(body.get("error").is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn object_store_failure_returns_git_lfs_error_contract() {
    let test = common::setup();
    test.state()
        .store
        .create_repo(HubRepoType::Model, "alice/lfs-errors", false)
        .unwrap();
    let local_root = match &test.state().object_store {
        ServerObjectStore::Local(local_store) => Some(local_store.root()),
        ServerObjectStore::S3(_) | ServerObjectStore::Blackhole => None,
    };
    assert!(local_root.is_some(), "test setup uses local object storage");
    let root = local_root.unwrap().to_path_buf();
    let moved_root = root.with_file_name("lfs-real");
    std::fs::rename(&root, &moved_root).unwrap();
    std::os::unix::fs::symlink(&moved_root, &root).unwrap();

    let response = test
        .app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/alice/lfs-errors.git/info/lfs/objects/batch")
                .header("content-type", "application/vnd.git-lfs+json")
                .body(Body::from(
                    serde_json::json!({
                        "operation": "upload",
                        "ref": { "name": "refs/heads/main" },
                        "objects": [{
                            "oid": "e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0",
                            "size": 1000
                        }],
                        "transfers": ["basic"],
                        "hash_algo": "sha256"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/vnd.git-lfs+json"
    );
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        body.get("message")
            .and_then(serde_json::Value::as_str)
            .is_some()
    );
    assert!(body.get("error").is_none());
}

#[tokio::test]
async fn cancelled_http_upload_discards_partial_filesystem_staging() {
    let test = common::setup();
    let local_root = test
        .state()
        .object_store
        .local_root()
        .expect("test setup uses local object storage")
        .to_path_buf();
    let staging_directory = local_root.parent().unwrap().to_path_buf();
    let existing: HashSet<_> = std::fs::read_dir(&staging_directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();

    // Force the request beyond the in-memory staging threshold, then leave its
    // body open as if the client disconnected while streaming the upload.
    let first_chunk = Bytes::from(vec![b'x'; 256 * 1024 + 1]);
    let body_stream = stream::iter([Ok::<_, std::io::Error>(first_chunk)]).chain(stream::pending());
    let oid = hex::encode(Sha256::digest(b"complete upload that will never arrive"));
    let request = Request::builder()
        .method("PUT")
        .uri(format!("/lfs/objects/{oid}"))
        .body(Body::from_stream(body_stream))
        .unwrap();
    let request_task = tokio::spawn(async move { test.app().oneshot(request).await });

    let staged_path = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let new_file = std::fs::read_dir(&staging_directory)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .find(|path| !existing.contains(path) && path.is_file());
            if let Some(path) = new_file {
                break path;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("upload handler should create a staging file after reading its first chunk");
    assert!(staged_path.exists());

    request_task.abort();
    assert!(request_task.await.unwrap_err().is_cancelled());
    assert!(
        !staged_path.exists(),
        "cancelling the HTTP request must remove its incomplete staging file"
    );
}

#[tokio::test]
async fn tcp_disconnect_discards_partial_filesystem_staging() {
    let test = common::setup();
    let local_root = test
        .state()
        .object_store
        .local_root()
        .expect("test setup uses local object storage")
        .to_path_buf();
    let staging_directory = local_root.parent().unwrap().to_path_buf();
    let existing: HashSet<_> = std::fs::read_dir(&staging_directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = test.app();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // Announce a larger body than we send, then close the TCP connection after
    // the first chunk has forced file-backed staging.
    let oid = hex::encode(Sha256::digest(
        b"full body that the disconnected client never sends",
    ));
    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    client
        .write_all(
            format!(
                "PUT /lfs/objects/{oid} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2097152\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    client.write_all(&vec![b'x'; 256 * 1024 + 1]).await.unwrap();
    client.flush().await.unwrap();

    let staged_path = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let new_file = std::fs::read_dir(&staging_directory)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .find(|path| !existing.contains(path) && path.is_file());
            if let Some(path) = new_file {
                break path;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server should stage the first body chunk before client disconnect");
    assert!(staged_path.exists());
    drop(client);

    tokio::time::timeout(Duration::from_secs(5), async {
        while staged_path.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("TCP disconnect must cancel upload and remove partial staging file");

    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
