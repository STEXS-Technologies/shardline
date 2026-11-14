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
