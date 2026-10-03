#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects
)]
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use shardline_protocol::{
    RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, TokenSigner,
};
use shardline_server::{DeploymentMode, ServerConfig, ServerFrontend, app};
use std::{num::NonZeroUsize, sync::Arc};
use tower::ServiceExt;
#[tokio::test]
async fn all_routes_count_handler_lifetime_and_selected_download_payload() {
    let root = tempfile::tempdir().unwrap();
    let key = b"0123456789abcdef0123456789abcdef";
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".into(),
        root.path().into(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_frontends([ServerFrontend::S3, ServerFrontend::Lfs])
    .unwrap()
    .with_deployment_mode(DeploymentMode::Insecure)
    .with_token_signing_key(key.to_vec())
    .unwrap();
    let repo = RepositoryScope::new(RepositoryProvider::Generic, "audit", "bucket", None).unwrap();
    let token = TokenSigner::new(key)
        .unwrap()
        .sign(&TokenClaims::new("shardline", "test", TokenScope::Write, repo, u64::MAX).unwrap())
        .unwrap();
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={token}/20261002/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef"
    );
    let router = app::router(config).await.unwrap();
    assert_eq!(
        router
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/audit.bucket")
                    .header("authorization", &auth)
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
        200
    );
    let m = shardline_metrics::metrics();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let wait = release.clone();
    let body = Body::from_stream(futures_util::stream::once(async move {
        tx.send(()).unwrap();
        wait.notified().await;
        Ok::<_, std::io::Error>("0123456789")
    }));
    let request = Request::builder()
        .method("PUT")
        .uri("/audit.bucket/source")
        .header("authorization", &auth)
        .body(body)
        .unwrap();
    let task = tokio::spawn(router.clone().oneshot(request));
    rx.await.unwrap();
    assert_eq!(m.system.active_connections.get(), 1);
    release.notify_one();
    assert_eq!(task.await.unwrap().unwrap().status(), 200);
    let range_before = m.transfer.download_bytes.get();
    let range_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/audit.bucket/source")
                .header("authorization", &auth)
                .header("range", "bytes=2-3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(range_response.status(), 206);
    assert_eq!(m.system.active_connections.get(), 0);
    assert_eq!(m.transfer.download_bytes.get() - range_before, 2);
    assert_eq!(
        m.transfer.download_duration.get_sample_sum().to_bits(),
        0.0_f64.to_bits()
    );
    let range_bytes = to_bytes(range_response.into_body(), 4096).await.unwrap();
    assert_eq!(range_bytes.as_ref(), b"23");
    assert_eq!(m.transfer.download_bytes.get() - range_before, 2);
    let full_before = m.transfer.download_bytes.get();
    let full_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/audit.bucket/source")
                .header("authorization", &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(full_response.status(), 200);
    drop(full_response);
    assert_eq!(m.transfer.download_bytes.get() - full_before, 10);

    use sha2::Digest;
    let oid = hex::encode(sha2::Sha256::digest(b"0123456789"));
    let uri = format!("/v1/lfs/objects/{oid}");
    let bearer = format!("Bearer {token}");
    let uploaded = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(&uri)
                .header("authorization", &bearer)
                .body(Body::from("0123456789"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(uploaded.status(), 200);
    let lfs_before = m.transfer.download_bytes.get();
    let selected = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(&uri)
                .header("authorization", &bearer)
                .header("range", "bytes=2-3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(selected.status(), 206);
    let lfs_bytes = to_bytes(selected.into_body(), 4096).await.unwrap();
    assert_eq!(lfs_bytes.as_ref(), b"23");
    assert_eq!(m.transfer.download_bytes.get() - lfs_before, 2);

    for timed_out in [false, true] {
        let (pending_tx, pending_rx) = tokio::sync::oneshot::channel();
        let never = Arc::new(tokio::sync::Notify::new());
        let pending_wait = never.clone();
        let pending_body = Body::from_stream(futures_util::stream::once(async move {
            pending_tx.send(()).unwrap();
            pending_wait.notified().await;
            Ok::<_, std::io::Error>("pending")
        }));
        let pending_request = Request::builder()
            .method("PUT")
            .uri("/audit.bucket/pending")
            .header("authorization", &auth)
            .body(pending_body)
            .unwrap();
        let pending_task = tokio::spawn(router.clone().oneshot(pending_request));
        pending_rx.await.unwrap();
        assert_eq!(m.system.active_connections.get(), 1);
        if timed_out {
            tokio::time::pause();
            tokio::time::advance(std::time::Duration::from_secs(301)).await;
            let pending_response = pending_task.await.unwrap().unwrap();
            use axum::response::IntoResponse;
            assert_eq!(
                pending_response.status(),
                shardline_server::ServerError::RequestTimedOut
                    .into_response()
                    .status()
            );
            assert!(
                pending_response
                    .headers()
                    .contains_key("x-content-type-options")
            );
            tokio::time::resume();
        } else {
            pending_task.abort();
            assert!(pending_task.await.unwrap_err().is_cancelled());
        }
        assert_eq!(m.system.active_connections.get(), 0);
    }
    let ready = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ready.status(), 200);
    assert_eq!(m.system.active_connections.get(), 0);
    tokio::fs::remove_file(root.path().join("metadata.sqlite3"))
        .await
        .unwrap();
    tokio::fs::create_dir(root.path().join("metadata.sqlite3"))
        .await
        .unwrap();
    let degraded_ready = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(degraded_ready.status(), 503);
    assert_eq!(m.system.active_connections.get(), 0);
    let scrape = router
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let text = String::from_utf8(
        to_bytes(scrape.into_body(), 1024 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(
        text.lines()
            .any(|line| line == "shardline_active_connections 1")
    );
    assert_eq!(m.system.active_connections.get(), 0);
}
