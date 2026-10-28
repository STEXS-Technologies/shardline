//! An explicitly selected live-forge campaign must never pass without fixtures.
use std::process::Command;

#[tokio::test]
async fn identity_lookup_errors_do_not_disclose_provider_urls_or_credentials() {
    use super::*;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/denied", get(|| async { StatusCode::UNAUTHORIZED }))
        .route("/invalid", get(|| async { "not a JSON identity" }));
    let peer = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let storage = tempfile::tempdir().unwrap();
    let mut results = Vec::new();
    for (path, expected) in [
        ("denied", StatusCode::UNAUTHORIZED),
        ("invalid", StatusCode::BAD_GATEWAY),
    ] {
        let config = LiveProviderConfig {
            provider: "github".to_owned(),
            owner: "private-owner".to_owned(),
            repo: "private-repository".to_owned(),
            subject: "private-actor".to_owned(),
            seed: 0,
            api_user_url: format!("{base}/{path}?fixture-secret=private-query-token"),
            clone_url: "https://example.invalid/private-repository".to_owned(),
            auth_remote_url: "https://private-user:private-token@example.invalid/repo".to_owned(),
            lfs_auth_token: "private-token".to_owned(),
        };
        let state = BridgeState {
            shardline_api_base_url: base.clone(),
            bridge_base_url: base.clone(),
            provider_key: "private-provider-key".to_owned(),
            client: live_http_client().unwrap(),
            storage_root: storage.path().to_owned(),
            requests: Arc::new(Mutex::new(Vec::new())),
            providers: Arc::new(HashMap::from([("github".to_owned(), config)])),
        };
        let result = resolve_provider_subject(&state, "github", "private-token").await;
        results.push((expected, result));
    }
    peer.abort();
    assert!(peer.await.unwrap_err().is_cancelled());
    for (expected, result) in results {
        let (status, diagnostic) = result.expect_err("invalid identity cannot authenticate");
        assert_eq!(status, expected);
        for secret in [
            base.as_str(),
            "private-query-token",
            "private-token",
            "private-provider-key",
            "private-user",
        ] {
            assert!(
                !diagnostic.contains(secret),
                "provider diagnostic disclosed a secret"
            );
        }
    }
}

#[tokio::test]
async fn identity_lookup_bounds_a_silent_provider() {
    assert_identity_deadline(false).await;
}

#[tokio::test]
async fn identity_lookup_bounds_a_stalled_response_body() {
    assert_identity_deadline(true).await;
}

async fn assert_identity_deadline(send_headers: bool) {
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (accepted, mut observed) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut connection, _) = listener.accept().await.unwrap();
        if send_headers {
            connection
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1024\r\n\r\n{\"login\":")
                .await
                .unwrap();
        }
        let _ = accepted.send(());
        // Keep the socket alive without completing headers or the JSON body.
        std::future::pending::<()>().await;
        drop(connection);
    });
    let result = tokio::time::timeout(
        Duration::from_secs(40),
        super::resolve_live_subject(
            &super::live_http_client().unwrap(),
            &base,
            "github",
            "local-fixture-token",
        ),
    )
    .await;
    peer.abort();
    assert!(peer.await.unwrap_err().is_cancelled());
    assert!(observed.try_recv().is_ok(), "lookup must reach the fixture");
    let error = result
        .expect("identity lookup exceeded its own HTTP deadline")
        .expect_err("an incomplete identity response cannot authenticate a user");
    assert!(
        error
            .downcast_ref::<reqwest::Error>()
            .is_some_and(reqwest::Error::is_timeout),
        "the HTTP deadline must cover both headers and body"
    );
}

#[test]
fn explicit_live_campaign_without_fixtures_fails() {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args([
        "--exact",
        "live_provider_bridge_supports_private_github_and_gitea_repositories",
        "--ignored",
    ]);
    for name in [
        "SHARDLINE_LIVE_GITHUB_OWNER",
        "SHARDLINE_LIVE_GITHUB_REPO",
        "SHARDLINE_LIVE_GITHUB_TOKEN",
        "SHARDLINE_LIVE_GITEA_BASE_URL",
        "SHARDLINE_LIVE_GITEA_OWNER",
        "SHARDLINE_LIVE_GITEA_REPO",
        "SHARDLINE_LIVE_GITEA_TOKEN",
    ] {
        command.env_remove(name);
    }
    let output = shardline_test_support::native_client_output(
        &mut command,
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    assert!(!output.status.success());
    let diagnostic = String::from_utf8_lossy(&output.stdout);
    assert!(
        diagnostic.contains("missing live-provider fixture variable")
            || diagnostic.contains("explicit live-provider qualification requires"),
        "{diagnostic}"
    );
}

#[test]
fn command_failure_diagnostics_do_not_disclose_provider_credentials() {
    let mut output = shardline_test_support::native_client_output(
        Command::new(std::env::current_exe().unwrap()).arg("--list"),
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    output.stdout = b"https://private-user:private-token@example.invalid/repository".to_vec();
    output.stderr = b"Authorization: Bearer private-token".to_vec();
    let diagnostic = super::command_error("git", &output).to_string();
    assert!(diagnostic.contains("git failed"));
    assert!(!diagnostic.contains("private-user"));
    assert!(!diagnostic.contains("private-token"));
    assert!(!diagnostic.contains("example.invalid"));
}
