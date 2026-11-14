//! The same LFS byte and batch contracts run against each deployment's server.

use super::super::{TestServer, sha256_hex};

fn object_request(
    server: &TestServer,
    client: &reqwest::Client,
    method: reqwest::Method,
    oid: &str,
) -> reqwest::RequestBuilder {
    client
        .request(method, server.url(&format!("/v1/lfs/objects/{oid}")))
        .header("Authorization", format!("Bearer {}", server.auth_header()))
}

async fn put_object(server: &TestServer, client: &reqwest::Client, content: &[u8]) -> String {
    let oid = sha256_hex(content);
    let response = object_request(server, client, reqwest::Method::PUT, &oid)
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "LFS PUT: {}",
        response.status()
    );
    oid
}

async fn batch(
    server: &TestServer,
    client: &reqwest::Client,
    operation: &str,
    oid: &str,
    size: u64,
) -> serde_json::Value {
    let response = client
        .post(server.url("/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {}", server.auth_header()))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({
            "operation": operation,
            "objects": [{"oid": oid, "size": size}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    response.json().await.unwrap()
}

pub(crate) async fn batch_upload_existing(server: &TestServer, content: &[u8]) {
    let client = reqwest::Client::new();
    let oid = put_object(server, &client, content).await;
    let response = batch(
        server,
        &client,
        "upload",
        &oid,
        u64::try_from(content.len()).unwrap(),
    )
    .await;
    let object = &response["objects"][0];
    assert_eq!(object["oid"], oid);
    assert!(
        object["actions"]
            .as_object()
            .is_none_or(|actions| actions.is_empty()),
        "existing object should not have upload actions: {object:?}"
    );
}

pub(crate) async fn batch_download_missing(server: &TestServer, oid: &str, size: u64) {
    let response = batch(server, &reqwest::Client::new(), "download", oid, size).await;
    let object = &response["objects"][0];
    assert_eq!(object["oid"], oid);
    assert!(
        object.get("error").is_some(),
        "missing object should have error: {object:?}"
    );
}

pub(crate) async fn head_object(server: &TestServer, content: &[u8]) {
    let client = reqwest::Client::new();
    let oid = put_object(server, &client, content).await;
    let response = object_request(server, &client, reqwest::Method::HEAD, &oid)
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

pub(crate) async fn patch_object(server: &TestServer, first: &[u8], second: &[u8]) {
    let client = reqwest::Client::new();
    let full = [first, second].concat();
    let oid = sha256_hex(&full);
    let total = u64::try_from(full.len()).unwrap();
    let split = u64::try_from(first.len()).unwrap();
    let last = total.checked_sub(1).unwrap();
    for (range, chunk) in [
        (
            format!("bytes 0-{}/{}", split.checked_sub(1).unwrap(), total),
            first,
        ),
        (format!("bytes {split}-{last}/{total}"), second),
    ] {
        let response = object_request(server, &client, reqwest::Method::PATCH, &oid)
            .header("Content-Type", "application/octet-stream")
            .header("Content-Range", &range)
            .body(chunk.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200, "PATCH {range} failed");
    }
    let response = object_request(server, &client, reqwest::Method::GET, &oid)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.unwrap().as_ref(), full.as_slice());
}
