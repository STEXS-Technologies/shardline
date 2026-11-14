//! Bazel action-cache and content-addressed storage contracts for each deployment.

use super::super::{TestServer, sha256_hex};

fn request(
    server: &TestServer,
    client: &reqwest::Client,
    method: reqwest::Method,
    cache: &str,
    hash: &str,
) -> reqwest::RequestBuilder {
    client
        .request(
            method,
            server.url(&format!("/v1/bazel/cache/{cache}/{hash}")),
        )
        .header("Authorization", format!("Bearer {}", server.auth_header()))
}

async fn put(
    server: &TestServer,
    client: &reqwest::Client,
    cache: &str,
    hash: &str,
    content: &[u8],
) -> reqwest::Response {
    request(server, client, reqwest::Method::PUT, cache, hash)
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap()
}

pub(crate) async fn ac_put_and_get(server: &TestServer, content: &[u8]) {
    let client = reqwest::Client::new();
    let hash = sha256_hex(content);
    let response = put(server, &client, "ac", &hash, content).await;
    assert!(
        response.status().is_success(),
        "AC PUT: {}",
        response.status()
    );
    let response = request(server, &client, reqwest::Method::GET, "ac", &hash)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.unwrap().as_ref(), content);
}

pub(crate) async fn head_existing(server: &TestServer, content: &[u8]) {
    let client = reqwest::Client::new();
    let hash = sha256_hex(content);
    let response = put(server, &client, "cas", &hash, content).await;
    assert!(
        response.status().is_success(),
        "CAS PUT: {}",
        response.status()
    );
    let response = request(server, &client, reqwest::Method::HEAD, "cas", &hash)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let length = response
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(length, u64::try_from(content.len()).unwrap());
}

pub(crate) async fn head_missing(server: &TestServer, hash: &str) {
    let response = request(
        server,
        &reqwest::Client::new(),
        reqwest::Method::HEAD,
        "cas",
        hash,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), 404);
}

pub(crate) async fn cas_hash_mismatch(server: &TestServer, expected: &[u8], actual: &[u8]) {
    let hash = sha256_hex(expected);
    let response = put(server, &reqwest::Client::new(), "cas", &hash, actual).await;
    assert_eq!(response.status(), 400);
}
