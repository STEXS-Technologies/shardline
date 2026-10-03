#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::num::{NonZeroU64, NonZeroUsize};

use axum::http::StatusCode;
use base64::{Engine, engine::general_purpose::STANDARD};
use shardline_server::{DeploymentMode, ServerConfig, ServerFrontend, app};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};

const KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
const BOOTSTRAP: &str = "bootstrap-key-16bytes";

fn basic(subject: &str) -> String {
    format!("Basic {}", STANDARD.encode(format!("{subject}:password")))
}

async fn assert_subject(response: reqwest::Response, expected: &str) {
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    let token = body
        .get("accessToken")
        .and_then(serde_json::Value::as_str)
        .unwrap();
    let provider = LocalHmacProvider::new(KEY).unwrap();
    let claims = provider.verify_token(token).unwrap();
    assert_eq!(claims.subject(), expected);
    assert_eq!(claims.repository().owner(), "team");
    assert_eq!(claims.repository().name(), "assets");
}

async fn assert_denied(response: reqwest::Response, expected: StatusCode) {
    assert_eq!(response.status(), expected);
    assert!(!response.text().await.unwrap().contains("\"accessToken\""));
}

#[tokio::test]
async fn selected_subject_sources_reject_controls_before_normalization() {
    let root = tempfile::tempdir().unwrap();
    let catalog = root.path().join("providers.json");
    std::fs::write(&catalog, r#"{"providers":[{"kind":"github","integration_subject":"github-app","webhook_secret":"testsecret","repositories":[{"owner":"team","name":"assets","visibility":"private","default_revision":"main","clone_url":"https://github.example/team/assets.git","read_subjects":["user"],"write_subjects":["user"]}]}]}"#).unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".into(),
        root.path().into(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_frontends([ServerFrontend::Xet])
    .unwrap()
    .with_deployment_mode(DeploymentMode::Insecure)
    .with_token_signing_key(KEY.to_vec())
    .unwrap()
    .with_provider_runtime(
        catalog,
        BOOTSTRAP.as_bytes().to_vec(),
        "provider".into(),
        NonZeroU64::new(300).unwrap(),
    )
    .unwrap();
    let router = app::router(config).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/api/github/team/assets/xet-read-token/main",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = reqwest::Client::new();
    for ordinary in ["user", " user "] {
        assert_subject(
            client
                .get(&url)
                .header("x-shardline-provider-key", BOOTSTRAP)
                .query(&[("subject", ordinary)])
                .send()
                .await
                .unwrap(),
            "user",
        )
        .await;
        assert_subject(
            client
                .get(&url)
                .header("x-shardline-provider-key", BOOTSTRAP)
                .header("authorization", basic(ordinary))
                .send()
                .await
                .unwrap(),
            "user",
        )
        .await;
    }
    for malformed in ["\nuser\n", "\ruser\r", "\tuser\t", "\n", "user\0"] {
        assert_denied(
            client
                .get(&url)
                .header("x-shardline-provider-key", BOOTSTRAP)
                .header("x-shardline-provider-subject", "user")
                .query(&[("subject", malformed)])
                .send()
                .await
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_denied(
            client
                .get(&url)
                .header("x-shardline-provider-key", BOOTSTRAP)
                .header("authorization", basic(malformed))
                .send()
                .await
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    // Unselected lower-priority inputs remain irrelevant to a valid query.
    assert_subject(
        client
            .get(&url)
            .header("x-shardline-provider-key", BOOTSTRAP)
            .query(&[("subject", "user")])
            .header("x-shardline-provider-subject", "bad\tvalue")
            .header("authorization", basic("\nmalformed\n"))
            .send()
            .await
            .unwrap(),
        "user",
    )
    .await;
    // Blank ordinary spaces still fall through to the subject header; the header
    // still outranks a different Basic username.
    assert_subject(
        client
            .get(&url)
            .header("x-shardline-provider-key", BOOTSTRAP)
            .query(&[("subject", "   ")])
            .header("x-shardline-provider-subject", "user")
            .header("authorization", basic("denied"))
            .send()
            .await
            .unwrap(),
        "user",
    )
    .await;
    assert_denied(
        client
            .get(&url)
            .header("x-shardline-provider-key", BOOTSTRAP)
            .query(&[("subject", "denied")])
            .header("x-shardline-provider-subject", "user")
            .header("authorization", basic("user"))
            .send()
            .await
            .unwrap(),
        StatusCode::FORBIDDEN,
    )
    .await;
    assert_denied(
        client
            .get(&url)
            .query(&[("subject", "user")])
            .send()
            .await
            .unwrap(),
        StatusCode::UNAUTHORIZED,
    )
    .await;
    assert_denied(
        client
            .get(&url)
            .header("x-shardline-provider-key", "invalid-key")
            .query(&[("subject", "\nuser\n")])
            .send()
            .await
            .unwrap(),
        StatusCode::FORBIDDEN,
    )
    .await;
    server.abort();
}
