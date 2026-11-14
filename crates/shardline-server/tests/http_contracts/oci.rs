use super::super::sha256_hex;
use tower::ServiceExt;

pub(crate) async fn oci_upload_blob_oneshot(
    app: &axum::Router,
    token: &str,
    repo: &str,
    data: &[u8],
) -> String {
    let digest = sha256_hex(data);
    let uri = format!("/v2/{repo}/blobs/uploads/?digest=sha256:{digest}");
    let req = axum::http::Request::builder()
        .method("POST")
        .uri(&uri)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::from(data.to_vec()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 201, "blob upload: status={}", resp.status());
    digest
}

pub(crate) fn oci_manifest_json(
    config_digest: &str,
    layer_digest: &str,
    config_size: usize,
    layer_size: usize,
) -> String {
    serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": config_size,
            "digest": format!("sha256:{config_digest}")
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "size": layer_size,
                "digest": format!("sha256:{layer_digest}")
            }
        ]
    })
    .to_string()
}

const MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";

async fn publish_manifest(
    app: &axum::Router,
    token: &str,
    repo: &str,
    tag: &str,
    layer: &[u8],
) -> (String, String) {
    let config = b"{}";
    let config_digest = oci_upload_blob_oneshot(app, token, repo, config).await;
    let layer_digest = oci_upload_blob_oneshot(app, token, repo, layer).await;
    let manifest = oci_manifest_json(&config_digest, &layer_digest, config.len(), layer.len());
    let digest = sha256_hex(manifest.as_bytes());
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("PUT")
                .uri(format!("/v2/{repo}/manifests/{tag}"))
                .header("Authorization", format!("Bearer {token}"))
                .header("Content-Type", MANIFEST_MEDIA_TYPE)
                .body(axum::body::Body::from(manifest))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    assert_eq!(
        response.headers().get("Docker-Content-Digest").unwrap(),
        format!("sha256:{digest}").as_str()
    );
    (digest, config_digest)
}

async fn get_manifest(
    app: &axum::Router,
    token: &str,
    repo: &str,
    reference: &str,
) -> serde_json::Value {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri(format!("/v2/{repo}/manifests/{reference}"))
                .header("Authorization", format!("Bearer {token}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap()
}

fn assert_manifest(manifest: &serde_json::Value, config_digest: &str) {
    assert_eq!(manifest["schemaVersion"], 2);
    assert_eq!(manifest["mediaType"], MANIFEST_MEDIA_TYPE);
    assert_eq!(
        manifest["config"]["digest"],
        format!("sha256:{config_digest}")
    );
}

pub(crate) async fn manifest_by_tag(
    app: &axum::Router,
    token: &str,
    repo: &str,
    tag: &str,
    layer: &[u8],
) {
    let (_digest, config_digest) = publish_manifest(app, token, repo, tag, layer).await;
    assert_manifest(&get_manifest(app, token, repo, tag).await, &config_digest);
}

pub(crate) async fn manifest_by_digest(
    app: &axum::Router,
    token: &str,
    repo: &str,
    tag: &str,
    layer: &[u8],
) {
    let (digest, config_digest) = publish_manifest(app, token, repo, tag, layer).await;
    assert_manifest(
        &get_manifest(app, token, repo, &format!("sha256:{digest}")).await,
        &config_digest,
    );
}

pub(crate) async fn tags_list(
    app: &axum::Router,
    token: &str,
    repo: &str,
    tag: &str,
    layer: &[u8],
) {
    publish_manifest(app, token, repo, tag, layer).await;
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri(format!("/v2/{repo}/tags/list"))
                .header("Authorization", format!("Bearer {token}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let list: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(list["name"], repo);
    let tags = list["tags"].as_array().unwrap();
    assert!(
        tags.iter().any(|value| value == tag),
        "missing {tag}: {tags:?}"
    );
}

pub(crate) async fn blob_head(app: &axum::Router, token: &str, repo: &str, content: &[u8]) {
    let digest = oci_upload_blob_oneshot(app, token, repo, content).await;
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("HEAD")
                .uri(format!("/v2/{repo}/blobs/sha256:{digest}"))
                .header("Authorization", format!("Bearer {token}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
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
    assert_eq!(
        response.headers().get("Docker-Content-Digest").unwrap(),
        format!("sha256:{digest}").as_str()
    );
}

pub(crate) async fn upload_invalid_digest_algorithm(
    app: &axum::Router,
    token: &str,
    repo: &str,
    content: &[u8],
) {
    let req = axum::http::Request::builder()
        .method("POST")
        .uri(&format!(
            "/v2/{repo}/blobs/uploads/?digest=sha512:{}",
            sha256_hex(content)
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::from(content.to_vec()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 400);
}

pub(crate) async fn upload_body_hash_mismatch(
    app: &axum::Router,
    token: &str,
    repo: &str,
    expected: &[u8],
    actual: &[u8],
) {
    let wrong_digest = sha256_hex(expected);

    let req = axum::http::Request::builder()
        .method("POST")
        .uri(&format!(
            "/v2/{repo}/blobs/uploads/?digest=sha256:{wrong_digest}"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::from(actual.to_vec()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 400);
}

pub(crate) async fn patch_nonexistent_session(app: &axum::Router, token: &str, repo: &str) {
    let req = axum::http::Request::builder()
        .method("PATCH")
        .uri(&format!(
            "/v2/{repo}/blobs/uploads/00000000-0000-0000-0000-000000000000"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .header("Content-Range", "0-63/64")
        .body(axum::body::Body::from(b"data".to_vec()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8_lossy(&body_bytes);
    assert_eq!(
        status, 404,
        "PATCH non-existent session: status={} body={}",
        status, body_str
    );
}

pub(crate) async fn put_finalize_without_digest(app: &axum::Router, token: &str, repo: &str) {
    let req = axum::http::Request::builder()
        .method("PUT")
        .uri(&format!(
            "/v2/{repo}/blobs/uploads/00000000-0000-0000-0000-000000000000"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 400);
}

pub(crate) async fn get_nonexistent_blob(app: &axum::Router, token: &str, repo: &str) {
    let req = axum::http::Request::builder()
        .method("GET")
        .uri(&format!("/v2/{repo}/blobs/sha256:{}", "a".repeat(64)))
        .header("Authorization", format!("Bearer {token}"))
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 404);
}

pub(crate) async fn delete_nonexistent_manifest(app: &axum::Router, token: &str, repo: &str) {
    let req = axum::http::Request::builder()
        .method("DELETE")
        .uri(&format!("/v2/{repo}/manifests/nonexistent-tag"))
        .header("Authorization", format!("Bearer {token}"))
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 404);
}

pub(crate) async fn push_manifest_with_nonexistent_blob(
    app: &axum::Router,
    token: &str,
    repo: &str,
) {
    let manifest_body = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": 0,
            "digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "size": 0,
            "digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        }]
    })
    .to_string();

    let req = axum::http::Request::builder()
        .method("PUT")
        .uri(&format!("/v2/{repo}/manifests/nonexistent-blob-ref"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(axum::body::Body::from(manifest_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 400);
}
