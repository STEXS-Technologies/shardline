#![allow(clippy::unwrap_used)]

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{AuthProvider, LocalHmacProvider};
use tower::ServiceExt;

fn fixture() -> (tempfile::TempDir, super::HubState, String) {
    let (root, mut state) = super::tests::make_test_state();
    state
        .store
        .create_repo(
            shardline_index::hub::HubRepoType::Model,
            "alice/repo",
            false,
        )
        .unwrap();
    let provider = LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap();
    let scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "alice", "repo", Some("main")).unwrap();
    let token = provider
        .mint_token(
            &TokenClaims::new("local", "subject", TokenScope::Write, scope, u64::MAX).unwrap(),
        )
        .unwrap();
    state.auth = Some(crate::auth::HubAuth::new(Box::new(provider)));
    (root, state, token)
}

fn repository_token() -> String {
    let provider = LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap();
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "alice", "repo", None).unwrap();
    provider
        .mint_token(
            &TokenClaims::new("local", "subject", TokenScope::Write, scope, u64::MAX).unwrap(),
        )
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repository_deletion_requires_repository_wide_scope() {
    for typed in [false, true] {
        let (_root, state, branch_token) = fixture();
        let url = if typed {
            "/api/models/alice/repo"
        } else {
            "/api/repos/delete"
        };
        for (token, expected) in [
            (branch_token, StatusCode::FORBIDDEN),
            (repository_token(), StatusCode::NO_CONTENT),
        ] {
            let response = crate::hub_routes(state.clone(), true)
                .oneshot(
                    Request::builder()
                        .method("DELETE")
                        .uri(url)
                        .header("authorization", format!("Bearer {token}"))
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"name":"alice/repo","type":"model"}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            assert_eq!(
                state.store.get_repo("alice/repo").unwrap().is_some(),
                expected == StatusCode::FORBIDDEN
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webhook_configuration_requires_repository_wide_scope() {
    let (_root, state, branch_token) = fixture();
    let url = "/api/models/alice/repo/webhooks";
    let registration = r#"{"url":"https://example.com/hook","events":["push"]}"#;
    let response = crate::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(url)
                .header("authorization", format!("Bearer {branch_token}"))
                .header("content-type", "application/json")
                .body(Body::from(registration))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(state.store.list_webhooks("alice/repo").unwrap().is_empty());

    let broad = repository_token();
    let response = crate::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(url)
                .header("authorization", format!("Bearer {broad}"))
                .header("content-type", "application/json")
                .body(Body::from(registration))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
    let id = body["id"].as_str().unwrap();
    for method in ["GET", "DELETE"] {
        let target = if method == "GET" {
            url.to_owned()
        } else {
            format!("{url}/{id}")
        };
        let response = crate::hub_routes(state.clone(), true)
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(&target)
                    .header("authorization", format!("Bearer {branch_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(state.store.list_webhooks("alice/repo").unwrap().len(), 1);
        let response = crate::hub_routes(state.clone(), true)
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(&target)
                    .header("authorization", format!("Bearer {broad}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if method == "GET" {
                StatusCode::OK
            } else {
                StatusCode::NO_CONTENT
            }
        );
    }
    assert!(state.store.list_webhooks("alice/repo").unwrap().is_empty());
}
