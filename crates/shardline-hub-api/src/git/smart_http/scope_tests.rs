#![allow(clippy::unwrap_used)]

use crate::{auth::HubAuth, routes::HubState};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use sha2::Digest;
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{AuthProvider, AuthorizedRepository, LocalHmacProvider};
use tower::ServiceExt;

fn fixture() -> (tempfile::TempDir, HubState, String, String) {
    let (root, mut state) = super::tests::make_hub_state();
    state
        .store
        .create_repo(
            shardline_index::hub::HubRepoType::Model,
            "alice/repo",
            false,
        )
        .unwrap();
    let initial = state
        .store
        .resolve_revision("alice/repo", "main")
        .unwrap()
        .unwrap();
    let side = "b".repeat(64);
    state.store.store_files(&side, &[]).unwrap();
    state
        .store
        .create_revision(
            "alice/repo",
            Some(&initial),
            &side,
            "private-side",
            "private message",
        )
        .unwrap();
    let provider = LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap();
    let scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "alice", "repo", Some("main")).unwrap();
    let claims = TokenClaims::new("local", "subject", TokenScope::Write, scope, u64::MAX).unwrap();
    let token = provider.mint_token(&claims).unwrap();
    state.auth = Some(HubAuth::new(Box::new(provider)));
    (root, state, token, side)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn main_token_cannot_discover_sibling_refs() {
    let (_root, state, token, _side) = fixture();
    let response = crate::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .uri("/models/alice/repo/info/refs?service=git-upload-pack")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("refs/heads/main"), "{text}");
    assert!(!text.contains("refs/heads/private-side"), "{text}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn main_token_cannot_delete_sibling_ref() {
    let (_root, state, token, side) = fixture();
    let public = super::public_commit_id(
        &state,
        "alice/repo",
        &side,
        &AuthorizedRepository::anonymous_full_access(),
    )
    .await
    .unwrap();
    let command = crate::git::pktline::encode_line(&format!(
        "{public} {} refs/heads/private-side\0report-status\n",
        "0".repeat(40)
    ))
    .unwrap();
    let response = crate::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/models/alice/repo/git-receive-pack")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(format!("{command}0000")))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        state
            .store
            .resolve_revision("alice/repo", "private-side")
            .unwrap(),
        Some(side)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_delete_batch_has_no_partial_publication() {
    let (_root, state, token, side) = fixture();
    let main = state
        .store
        .resolve_revision("alice/repo", "main")
        .unwrap()
        .unwrap();
    let mut commands = String::new();
    for (index, (internal, name)) in [(&main, "main"), (&side, "private-side")]
        .into_iter()
        .enumerate()
    {
        let public = super::public_commit_id(
            &state,
            "alice/repo",
            internal,
            &AuthorizedRepository::anonymous_full_access(),
        )
        .await
        .unwrap();
        let capabilities = if index == 0 { "\0report-status" } else { "" };
        commands.push_str(
            &crate::git::pktline::encode_line(&format!(
                "{public} {} refs/heads/{name}{capabilities}\n",
                "0".repeat(40)
            ))
            .unwrap(),
        );
    }
    commands.push_str("0000");
    let response = crate::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/models/alice/repo/git-receive-pack")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(commands))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        state.store.resolve_revision("alice/repo", "main").unwrap(),
        Some(main)
    );
    assert_eq!(
        state
            .store
            .resolve_revision("alice/repo", "private-side")
            .unwrap(),
        Some(side)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn main_token_cannot_fetch_guessed_sibling_commit() {
    let (_root, state, token, side) = fixture();
    let public = super::public_commit_id(
        &state,
        "alice/repo",
        &side,
        &AuthorizedRepository::anonymous_full_access(),
    )
    .await
    .unwrap();
    let want = crate::git::pktline::encode_line(&format!("want {public}\n")).unwrap();
    let done = crate::git::pktline::encode_line("done\n").unwrap();
    let response = crate::hub_routes(state, true)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/models/alice/repo/git-upload-pack")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(format!("{want}0000{done}")))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(!response.status().is_success());
    assert!(!response.status().is_server_error());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ancestry_rejects_a_sibling_traversal_root() {
    let (_root, state, token, side) = fixture();
    let provider = LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap();
    let reader =
        AuthorizedRepository::verify_and_authorize(&provider, &token, TokenScope::Read).unwrap();
    assert!(matches!(
        super::reachable_commits(&state, "alice/repo", &side, &reader).await,
        Err(crate::error::HubApiError::Forbidden)
    ));
    let head = state
        .store
        .resolve_revision("alice/repo", "main")
        .unwrap()
        .unwrap();
    let visible = super::reachable_commits(&state, "alice/repo", &head, &reader)
        .await
        .unwrap();
    assert!(visible.contains(&head));
    assert!(!visible.contains(&side));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn main_token_v2_lists_only_permitted_refs() {
    let (_root, state, token, _) = fixture();
    let command = crate::git::pktline::encode_line("command=ls-refs\n").unwrap();
    let symrefs = crate::git::pktline::encode_line("symrefs\n").unwrap();
    let response = crate::hub_routes(state, true)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/models/alice/repo/git-upload-pack")
                .header("authorization", format!("Bearer {token}"))
                .header("git-protocol", "version=2")
                .body(Body::from(format!("{command}0001{symrefs}0000")))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("refs/heads/main"), "{text}");
    assert!(!text.contains("private-side"), "{text}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stock_git_scoped_discovery_and_clone() {
    stock_git_payload_flow(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_scoped_reader_downloads_repository_wide_inline_commit() {
    let (_root, state, token, _) = fixture();
    let provider = LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap();
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "alice", "repo", None).unwrap();
    let claims = TokenClaims::new("local", "subject", TokenScope::Write, scope, u64::MAX).unwrap();
    let writer = provider.mint_token(&claims).unwrap();
    let capability =
        AuthorizedRepository::verify_and_authorize(&provider, &writer, TokenScope::Write).unwrap();
    let parent = state
        .store
        .resolve_revision("alice/repo", "main")
        .unwrap()
        .unwrap();
    let _commit = crate::routes::apply_commit(
        &state,
        "alice/repo",
        &parent,
        &crate::commit::ParsedCommit {
            message: "Broad writer inline file".to_owned(),
            parent_commit: None,
            instructions: vec![
                crate::commit::CommitInstruction::InlineFile {
                    path: "payload.txt".to_owned(),
                    content: b"authorized inline content".to_vec(),
                },
                crate::commit::CommitInstruction::InlineFile {
                    path: "README.md".to_owned(),
                    content: card_bytes().to_vec(),
                },
            ],
        },
        &capability,
    )
    .await
    .unwrap();
    let reader =
        AuthorizedRepository::verify_and_authorize(&provider, &token, TokenScope::Read).unwrap();
    let projection =
        super::projection::project_history_async(state.clone(), "alice/repo".to_owned(), reader)
            .await
            .unwrap();
    assert!(
        projection
            .objects
            .iter()
            .any(|object| object.data == b"authorized inline content")
    );
    assert_path_metadata(
        &state,
        &token,
        &hex::encode(
            crate::git::pack::GitObject::blob(b"authorized inline content".to_vec()).sha1(),
        ),
    )
    .await;
    assert_card_metadata(&state, &token).await;
    let response = crate::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .uri("/alice/repo/resolve/main/payload.txt")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
        b"authorized inline content"
    );
    for method in ["GET", "HEAD"] {
        let response = crate::hub_routes(state.clone(), true)
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri("/alice/repo/resolve/main/payload.txt")
                    .header("authorization", format!("Bearer {token}"))
                    .header("range", "bytes=0-9")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if method == "GET" {
                StatusCode::PARTIAL_CONTENT
            } else {
                StatusCode::OK
            }
        );
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(
            body.as_ref(),
            if method == "GET" {
                &b"authorized"[..]
            } else {
                &[][..]
            }
        );
    }
    let response = crate::hub_routes(state, true)
        .oneshot(
            Request::builder()
                .uri("/alice/repo/resolve/private-side/payload.txt")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn basic_lfs_scoped_reader_downloads_repository_wide_publication() {
    let (_root, state, token, _) = fixture();
    let provider = LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap();
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "alice", "repo", None).unwrap();
    let claims = TokenClaims::new("local", "subject", TokenScope::Write, scope, u64::MAX).unwrap();
    let writer = provider.mint_token(&claims).unwrap();
    let capability =
        AuthorizedRepository::verify_and_authorize(&provider, &writer, TokenScope::Write).unwrap();
    let content = b"published basic LFS bytes";
    let oid = hex::encode(sha2::Sha256::digest(content));
    let response = crate::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/lfs/objects/{oid}"))
                .header("authorization", format!("Bearer {writer}"))
                .body(Body::from(content.as_slice()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());
    let parent = state
        .store
        .resolve_revision("alice/repo", "main")
        .unwrap()
        .unwrap();
    let _commit = crate::routes::apply_commit(
        &state,
        "alice/repo",
        &parent,
        &crate::commit::ParsedCommit {
            message: "Publish basic LFS pointer".to_owned(),
            parent_commit: None,
            instructions: vec![crate::commit::CommitInstruction::LfsPointer {
                path: "weights.bin".to_owned(),
                oid: oid.clone(),
                size: content.len() as u64,
            }],
        },
        &capability,
    )
    .await
    .unwrap();
    // The object remains authorized through immutable history after deletion
    // from the current branch; physical presence alone is never the proof.
    let current = state
        .store
        .resolve_revision("alice/repo", "main")
        .unwrap()
        .unwrap();
    let _deletion = crate::routes::apply_commit(
        &state,
        "alice/repo",
        &current,
        &crate::commit::ParsedCommit {
            message: "Remove current pointer".to_owned(),
            parent_commit: None,
            instructions: vec![crate::commit::CommitInstruction::Delete {
                path: "weights.bin".to_owned(),
            }],
        },
        &capability,
    )
    .await
    .unwrap();
    let response = crate::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .uri(format!("/lfs/objects/{oid}"))
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
        content
    );
    let response = crate::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/alice/repo.git/info/lfs/objects/batch")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"operation":"download", "transfers":["basic"],
            "objects":[{"oid":oid,"size":content.len()}]})
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
    assert!(
        body["objects"][0]["actions"]["download"]["href"].is_string(),
        "{body}"
    );
    let response = crate::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .uri(format!("/lfs/objects/{oid}"))
                .header("authorization", format!("Bearer {token}"))
                .header("range", "bytes=0-8")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
        b"published"
    );
    let side_scope = RepositoryScope::new(
        RepositoryProvider::GitHub,
        "alice",
        "repo",
        Some("private-side"),
    )
    .unwrap();
    let side_claims =
        TokenClaims::new("local", "subject", TokenScope::Read, side_scope, u64::MAX).unwrap();
    let side_token = provider.mint_token(&side_claims).unwrap();
    let response = crate::hub_routes(state, true).oneshot(Request::builder()
        .method("POST").uri("/alice/repo.git/info/lfs/objects/batch")
        .header("authorization", format!("Bearer {side_token}"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::json!({"operation":"download", "objects":[{"oid":oid,"size":content.len()}]}).to_string()))
        .unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
    assert_eq!(body["objects"][0]["error"]["code"], 404);
    assert!(body["objects"][0]["actions"].is_null());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stock_git_scoped_reader_of_repository_wide_writes() {
    stock_git_payload_flow(true).await;
}

async fn stock_git_payload_flow(broad_writer: bool) {
    let (_root, state, token, _) = fixture();
    let provider = LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap();
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "alice", "repo", None).unwrap();
    let claims = TokenClaims::new("local", "subject", TokenScope::Write, scope, u64::MAX).unwrap();
    let writer = provider.mint_token(&claims).unwrap();
    let reader =
        AuthorizedRepository::verify_and_authorize(&provider, &token, TokenScope::Read).unwrap();
    let verification_state = state.clone();
    let runtime = tokio::runtime::Handle::current();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, crate::hub_routes(state, true))
            .await
            .unwrap();
    });
    let result = tokio::task::spawn_blocking(move || {
        let destination = tempfile::tempdir().unwrap();
        let url = format!("http://{address}/models/alice/repo");
        for version in ["0", "1", "2"] {
            let run = |args: &[&str]| {
                let credential = if broad_writer && args.contains(&"push") {
                    &writer
                } else {
                    &token
                };
                std::process::Command::new("git")
                    .arg("-c")
                    .arg(format!("protocol.version={version}"))
                    .env("GIT_CONFIG_COUNT", "1")
                    .env("GIT_CONFIG_KEY_0", "http.extraHeader")
                    .env(
                        "GIT_CONFIG_VALUE_0",
                        format!("Authorization: Bearer {credential}"),
                    )
                    .env("GIT_TERMINAL_PROMPT", "0")
                    .env("GIT_AUTHOR_NAME", "Scope Test")
                    .env("GIT_AUTHOR_EMAIL", "scope@example.invalid")
                    .env("GIT_COMMITTER_NAME", "Scope Test")
                    .env("GIT_COMMITTER_EMAIL", "scope@example.invalid")
                    .env("GIT_HTTP_LOW_SPEED_LIMIT", "1")
                    .env("GIT_HTTP_LOW_SPEED_TIME", "10")
                    .args(args)
                    .current_dir(destination.path())
                    .output()
                    .unwrap()
            };
            let listed = run(&["ls-remote", &url]);
            assert!(
                listed.status.success(),
                "{}",
                String::from_utf8_lossy(&listed.stderr)
            );
            let refs = String::from_utf8_lossy(&listed.stdout);
            assert!(refs.contains("refs/heads/main"), "{refs}");
            assert!(!refs.contains("private-side"), "{refs}");
            let cloned = run(&["clone", &url, &format!("clone-{version}")]);
            assert!(
                cloned.status.success(),
                "{}",
                String::from_utf8_lossy(&cloned.stderr)
            );
            let checked = run(&["-C", &format!("clone-{version}"), "fsck", "--strict"]);
            assert!(
                checked.status.success(),
                "{}",
                String::from_utf8_lossy(&checked.stderr)
            );
            let checkout = format!("clone-{version}");
            let expected = format!("nonempty Git payload for protocol {version}\n");
            std::fs::write(
                destination.path().join(&checkout).join("payload.txt"),
                &expected,
            )
            .unwrap();
            std::fs::write(
                destination.path().join(&checkout).join("README.md"),
                card_bytes(),
            )
            .unwrap();
            for arguments in [
                vec!["-C", &checkout, "add", "payload.txt", "README.md"],
                vec!["-C", &checkout, "commit", "-m", "Publish scoped payload"],
                vec!["-C", &checkout, "push", "origin", "HEAD:refs/heads/main"],
            ] {
                let output = run(&arguments);
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            let fresh = format!("fresh-{version}");
            let history = runtime
                .block_on(crate::revision_access::visible_commits(
                    &verification_state,
                    "alice/repo",
                    &reader,
                ))
                .unwrap()
                .unwrap();
            assert!(history.len() >= 2);
            let hashed = run(&["-C", &checkout, "hash-object", "payload.txt"]);
            assert!(hashed.status.success());
            runtime.block_on(assert_path_metadata(
                &verification_state,
                &token,
                String::from_utf8_lossy(&hashed.stdout).trim(),
            ));
            runtime.block_on(assert_card_metadata(&verification_state, &token));
            let output = run(&["clone", &url, &fresh]);
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                std::fs::read_to_string(destination.path().join(&fresh).join("payload.txt"))
                    .unwrap(),
                expected
            );
            let output = run(&["-C", &fresh, "fsck", "--strict"]);
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let updated = format!("{expected}incremental update\n");
            std::fs::write(
                destination.path().join(&fresh).join("payload.txt"),
                &updated,
            )
            .unwrap();
            for arguments in [
                vec!["-C", &fresh, "add", "payload.txt"],
                vec!["-C", &fresh, "commit", "-m", "Incremental scoped payload"],
                vec!["-C", &fresh, "push", "origin", "HEAD:refs/heads/main"],
                vec!["-C", &checkout, "pull", "--ff-only"],
                vec!["-C", &checkout, "fsck", "--strict"],
            ] {
                let output = run(&arguments);
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            assert_eq!(
                std::fs::read_to_string(destination.path().join(&checkout).join("payload.txt"))
                    .unwrap(),
                updated
            );
        }
    })
    .await;
    server.abort();
    result.unwrap();
}

async fn assert_path_metadata(state: &HubState, token: &str, expected_oid: &str) {
    let auth = AuthorizedRepository::verify_and_authorize(
        state.auth.as_ref().unwrap().provider(),
        token,
        TokenScope::Read,
    )
    .unwrap();
    let revision = state
        .store
        .resolve_revision("alice/repo", "main")
        .unwrap()
        .unwrap();
    let files = state.store.get_files(&revision).unwrap();
    let metadata = super::path_oids_sync(
        state,
        "alice/repo",
        &revision,
        &files,
        &["payload.txt".to_owned()],
        &auth,
    )
    .unwrap();
    assert_eq!(metadata["payload.txt"].oid, expected_oid);
    let response = crate::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/alice/repo/paths-info/main")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"paths":["payload.txt"],"expand":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 8192).await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body[0]["oid"], expected_oid);
    assert!(body[0]["lastCommit"]["id"].is_string(), "{body}");
    let response = crate::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/alice/repo/paths-info/private-side")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"paths":["payload.txt"],"expand":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

const fn card_bytes() -> &'static [u8] {
    b"---\nlanguage: en\nlicense: mit\n---\n# Authorized model card\n"
}

async fn assert_card_metadata(state: &HubState, token: &str) {
    for path in [
        "/api/models/alice/repo",
        "/api/models/alice/repo/revision/main",
    ] {
        let response = crate::hub_routes(state.clone(), true)
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(body["card_data"]["license"], "mit", "{body}");
    }
    let response = crate::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .uri("/api/models/alice/repo/modelcard")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), 8192).await.unwrap().as_ref(),
        card_bytes()
    );
}
