//! Shared concurrent workloads for the PostgreSQL and local-metadata S3 suites.

use super::http_contracts::oci::{oci_manifest_json, oci_upload_blob_oneshot};
use super::{TestServer, sha256_hex};
use tower::ServiceExt;

pub(crate) async fn lfs_upload_same_oid(server: &TestServer, content: &[u8]) {
    let oid = sha256_hex(content);
    let url = server.url(&format!("/v1/lfs/objects/{oid}"));
    let token = server.auth_header().to_owned();
    let mut handles = Vec::new();
    for _ in 0..5 {
        let url = url.clone();
        let token = token.clone();
        let data = content.to_vec();
        handles.push(tokio::spawn(async move {
            reqwest::Client::new()
                .put(url)
                .header("Authorization", format!("Bearer {token}"))
                .header("Content-Type", "application/octet-stream")
                .body(data)
                .send()
                .await
                .unwrap()
                .status()
        }));
    }
    for (i, handle) in handles.into_iter().enumerate() {
        let status = handle.await.unwrap();
        assert!(status.is_success(), "concurrent PUT {i} returned {status}");
    }
    let response = reqwest::Client::new()
        .get(url)
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.unwrap().as_ref(), content);
}

pub(crate) async fn oci_manifest_reads(
    app: &axum::Router,
    token: &str,
    repo: &str,
    tag: &str,
    layer: &[u8],
) {
    let config = b"{}";
    let config_digest = oci_upload_blob_oneshot(app, token, repo, config).await;
    let layer_digest = oci_upload_blob_oneshot(app, token, repo, layer).await;
    let manifest = oci_manifest_json(&config_digest, &layer_digest, config.len(), layer.len());
    let uri = format!("/v2/{repo}/manifests/{tag}");
    let request = axum::http::Request::builder()
        .method("PUT")
        .uri(&uri)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(axum::body::Body::from(manifest.clone()))
        .unwrap();
    assert_eq!(app.clone().oneshot(request).await.unwrap().status(), 201);

    // Spawn every reader before awaiting any response.
    let mut readers = Vec::new();
    for _ in 0..3 {
        let app = app.clone();
        let token = token.to_owned();
        let uri = uri.clone();
        readers.push(tokio::spawn(async move {
            let request = axum::http::Request::builder()
                .method("GET")
                .uri(uri)
                .header("Authorization", format!("Bearer {token}"))
                .body(axum::body::Body::empty())
                .unwrap();
            app.oneshot(request).await.unwrap()
        }));
    }
    for (i, reader) in readers.into_iter().enumerate() {
        let response = reader.await.unwrap();
        assert_eq!(response.status(), 200, "concurrent manifest GET {i}");
        let body = axum::body::to_bytes(response.into_body(), manifest.len())
            .await
            .unwrap();
        assert_eq!(
            body.as_ref(),
            manifest.as_bytes(),
            "concurrent manifest GET {i}"
        );
    }
}
