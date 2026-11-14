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
    batch_request(
        server,
        client,
        serde_json::json!({
            "operation": operation,
            "objects": [{"oid": oid, "size": size}]
        }),
    )
    .await
}

async fn batch_request(
    server: &TestServer,
    client: &reqwest::Client,
    request: serde_json::Value,
) -> serde_json::Value {
    let response = client
        .post(server.url("/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {}", server.auth_header()))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&request)
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

async fn verify(server: &TestServer, client: &reqwest::Client, oid: &str, status: u16) {
    let response = object_request(
        server,
        client,
        reqwest::Method::POST,
        &format!("{oid}/verify"),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), status, "LFS verify {oid}");
}

pub(crate) async fn verify_existing(server: &TestServer, content: &[u8]) {
    let client = reqwest::Client::new();
    let oid = put_object(server, &client, content).await;
    verify(server, &client, &oid, 200).await;
}

pub(crate) async fn verify_missing(server: &TestServer, oid: &str) {
    verify(server, &reqwest::Client::new(), oid, 404).await;
}

pub(crate) async fn batch_mixed(
    server: &TestServer,
    existing: &[u8],
    missing_oid: &str,
    missing_size: u64,
) {
    let client = reqwest::Client::new();
    let existing_oid = put_object(server, &client, existing).await;
    let response = batch_request(
        server,
        &client,
        serde_json::json!({
            "operation": "download",
            "objects": [
                {"oid": existing_oid, "size": u64::try_from(existing.len()).unwrap()},
                {"oid": missing_oid, "size": missing_size}
            ]
        }),
    )
    .await;
    let objects = response["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 2);
    assert_eq!(objects[0]["oid"], existing_oid);
    assert_eq!(objects[1]["oid"], missing_oid);
    assert!(
        objects[0]["error"].is_null(),
        "existing object should have no error: {:?}",
        objects[0]
    );
    assert!(
        objects[1].get("error").is_some(),
        "missing object should have error: {:?}",
        objects[1]
    );
}

pub(crate) async fn upload_empty(server: &TestServer) {
    let client = reqwest::Client::new();
    let oid = put_object(server, &client, b"").await;
    let response = object_request(server, &client, reqwest::Method::GET, &oid)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.bytes().await.unwrap().is_empty());
}
