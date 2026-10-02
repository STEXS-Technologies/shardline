#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::num::{NonZeroU64, NonZeroUsize};

use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use base64::{Engine, engine::general_purpose::STANDARD};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{DeploymentMode, ServerConfig, ServerFrontend, app};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};

const KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

fn token(scope: TokenScope) -> String {
    let provider = LocalHmacProvider::new(KEY).unwrap();
    let repository =
        RepositoryScope::new(RepositoryProvider::Generic, "team", "assets", None).unwrap();
    let claims = TokenClaims::new("shardline", "credentials", scope, repository, u64::MAX).unwrap();
    provider.mint_token(&claims).unwrap()
}

fn sigv4(token: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential={token}/20261002/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=deadbeef"
    )
}

async fn serve(
    frontend: ServerFrontend,
) -> (String, tempfile::TempDir, tokio::task::JoinHandle<()>) {
    let root = tempfile::tempdir().unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".into(),
        root.path().into(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_frontends([frontend])
    .unwrap()
    .with_deployment_mode(DeploymentMode::Insecure)
    .with_token_signing_key(KEY.to_vec())
    .unwrap();
    let router = app::router(config).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (base, root, task)
}

fn authorization(values: &[&str]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for value in values {
        headers.append(header::AUTHORIZATION, HeaderValue::from_str(value).unwrap());
    }
    headers
}

#[tokio::test]
async fn s3_credential_bridge_rejects_repeated_authorization_without_writes() {
    let (base, _root, server) = serve(ServerFrontend::S3).await;
    let client = reqwest::Client::new();
    let write = token(TokenScope::Write);
    let bearer = format!("Bearer {write}");
    let signed = sigv4(&write);
    let read = format!("Bearer {}", token(TokenScope::Read));
    let invalid = "Bearer invalid-token";
    for value in [&signed, &bearer] {
        let response = client
            .put(format!("{base}/team.assets/control"))
            .headers(authorization(&[value]))
            .body("control")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let cases = [
        (&signed[..], invalid),
        (invalid, &signed[..]),
        (&bearer[..], invalid),
        (invalid, &bearer[..]),
        (&bearer[..], &bearer[..]),
        (&bearer[..], &read[..]),
        (&read[..], &bearer[..]),
    ];
    let mut failures = Vec::new();
    for (index, (first, second)) in cases.into_iter().enumerate() {
        let url = format!("{base}/team.assets/repeated-{index}");
        let response = client
            .put(&url)
            .headers(authorization(&[first, second]))
            .body("must not be written")
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        let stored = client
            .get(&url)
            .headers(authorization(&[&bearer]))
            .send()
            .await
            .unwrap()
            .status();
        println!("S3 repeated credentials case {index}: PUT={status}, GET={stored}");
        if status != StatusCode::FORBIDDEN
            || !body.contains("AccessDenied")
            || stored != StatusCode::NOT_FOUND
        {
            failures.push((index, status, stored));
        }
    }
    server.abort();
    assert!(
        failures.is_empty(),
        "ambiguous credentials must be rejected before writes: {failures:?}"
    );
}

#[tokio::test]
async fn oci_token_exchange_rejects_repeated_bootstrap_credentials_with_challenge() {
    let (base, _root, server) = serve(ServerFrontend::Oci).await;
    let client = reqwest::Client::new();
    let token = token(TokenScope::Write);
    let bearer = format!("Bearer {token}");
    let basic = format!("Basic {}", STANDARD.encode(format!("user:{token}")));
    let invalid = "Bearer invalid-token";
    let url = format!("{base}/v2/token?service=shardline&scope=repository:team/assets:pull,push");
    for value in [&bearer, &basic] {
        let response = client
            .get(&url)
            .headers(authorization(&[value]))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let cases = [
        (&bearer[..], invalid),
        (invalid, &bearer[..]),
        (&basic[..], invalid),
        (invalid, &basic[..]),
        (&basic[..], &basic[..]),
        (&basic[..], &bearer[..]),
        (&bearer[..], &basic[..]),
    ];
    let mut failures = Vec::new();
    for (index, (first, second)) in cases.into_iter().enumerate() {
        let response = client
            .get(&url)
            .headers(authorization(&[first, second]))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let challenge = response
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let body = response.text().await.unwrap();
        println!(
            "OCI repeated credentials case {index}: token exchange={status}, issued_token={}",
            body.contains("access_token")
        );
        if status != StatusCode::UNAUTHORIZED
            || !challenge.starts_with("Bearer ")
            || body.contains("access_token")
        {
            failures.push((index, status));
        }
    }
    server.abort();
    assert!(
        failures.is_empty(),
        "ambiguous bootstrap credentials must not mint tokens: {failures:?}"
    );
}

#[tokio::test]
async fn s3_security_token_fallback_rejects_repeated_values_without_writes() {
    let (base, _root, server) = serve(ServerFrontend::S3).await;
    let client = reqwest::Client::new();
    let valid = token(TokenScope::Write);
    let bearer = format!("Bearer {valid}");
    let fallback = "x-amz-security-token";
    let invalid = "invalid-token";
    let fallback_control_response = client
        .put(format!("{base}/team.assets/fallback-control"))
        .header(fallback, &valid)
        .body("control")
        .send()
        .await
        .unwrap();
    assert_eq!(fallback_control_response.status(), StatusCode::OK);
    // A selected Authorization credential takes precedence over an unused
    // security-token fallback, including malformed/multiple fallback values.
    let mut ignored = authorization(&[&bearer]);
    ignored.append(fallback, HeaderValue::from_static(invalid));
    ignored.append(fallback, HeaderValue::from_static("another-invalid-token"));
    let authorization_priority_response = client
        .put(format!("{base}/team.assets/authorization-priority"))
        .headers(ignored)
        .body("control")
        .send()
        .await
        .unwrap();
    assert_eq!(authorization_priority_response.status(), StatusCode::OK);
    // The existing malformed Authorization -> singleton fallback behavior is
    // deliberate; guard changes must not turn that precedence into an error.
    let malformed_fallback_response = client
        .put(format!(
            "{base}/team.assets/malformed-authorization-fallback"
        ))
        .header(header::AUTHORIZATION, "unsupported-credentials")
        .header(fallback, &valid)
        .body("control")
        .send()
        .await
        .unwrap();
    assert_eq!(malformed_fallback_response.status(), StatusCode::OK);
    let mut failures = Vec::new();
    for (index, (first, second)) in [
        (&valid[..], invalid),
        (invalid, &valid[..]),
        (&valid[..], &valid[..]),
    ]
    .into_iter()
    .enumerate()
    {
        let url = format!("{base}/team.assets/repeated-fallback-{index}");
        let mut headers = HeaderMap::new();
        headers.append(fallback, HeaderValue::from_str(first).unwrap());
        headers.append(fallback, HeaderValue::from_str(second).unwrap());
        let response = client
            .put(&url)
            .headers(headers)
            .body("must not be written")
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        let stored = client
            .get(&url)
            .headers(authorization(&[&bearer]))
            .send()
            .await
            .unwrap()
            .status();
        println!("S3 repeated fallback case {index}: PUT={status}, GET={stored}");
        if status != StatusCode::FORBIDDEN
            || !body.contains("AccessDenied")
            || stored != StatusCode::NOT_FOUND
        {
            failures.push((index, status, stored));
        }
    }
    server.abort();
    assert!(
        failures.is_empty(),
        "ambiguous fallback credentials must be rejected before writes: {failures:?}"
    );
}

#[tokio::test]
async fn provider_token_issuer_rejects_repeated_bootstrap_keys_without_minting() {
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
        b"bootstrap-key-16bytes".to_vec(),
        "provider".into(),
        NonZeroU64::new(300).unwrap(),
    )
    .unwrap();
    let router = app::router(config).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/v1/providers/github/tokens",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = reqwest::Client::new();
    let payload =
        r#"{"subject":"user","owner":"team","repo":"assets","revision":"main","scope":"Write"}"#;
    let valid_response = client
        .post(&url)
        .header("x-shardline-provider-key", "bootstrap-key-16bytes")
        .header(header::CONTENT_TYPE, "application/json")
        .body(payload)
        .send()
        .await
        .unwrap();
    assert_eq!(valid_response.status(), StatusCode::OK);
    let issued: serde_json::Value = valid_response.json().await.unwrap();
    let minted = issued
        .get("token")
        .and_then(serde_json::Value::as_str)
        .unwrap();
    let provider = LocalHmacProvider::new(KEY).unwrap();
    let verified = provider.verify_token(minted).unwrap();
    assert_eq!(verified.subject(), "user");
    assert_eq!(verified.repository().owner(), "team");
    assert_eq!(verified.repository().name(), "assets");
    assert_eq!(verified.scope(), TokenScope::Write);
    for values in [
        vec![],
        vec!["invalid-key"],
        vec!["bootstrap-key-16bytes", "invalid-key"],
        vec!["invalid-key", "bootstrap-key-16bytes"],
        vec!["bootstrap-key-16bytes", "bootstrap-key-16bytes"],
    ] {
        let mut headers = HeaderMap::new();
        for value in &values {
            headers.append(
                "x-shardline-provider-key",
                HeaderValue::from_str(value).unwrap(),
            );
        }
        let response = client
            .post(&url)
            .headers(headers)
            .header(header::CONTENT_TYPE, "application/json")
            .body(payload)
            .send()
            .await
            .unwrap();
        let expected = if values.is_empty() {
            StatusCode::UNAUTHORIZED
        } else {
            StatusCode::FORBIDDEN
        };
        assert_eq!(
            response.status(),
            expected,
            "credentials count={}",
            values.len()
        );
        assert!(!response.text().await.unwrap().contains("\"token\""));
    }
    server.abort();
}
