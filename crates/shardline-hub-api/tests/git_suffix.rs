#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Git discovery, HEAD and LFS must resolve aliases without widening credentials.
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use shardline_hub_api::{auth::HubAuth, hub_routes, routes::HubState};
use shardline_index::{
    LocalIndexStore,
    hub::{BoxedHubStore, HubRepoType},
};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{AuthProvider, LocalHmacProvider, ServerObjectStore};
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
use tower::ServiceExt;

mod history_pages;
mod snapshot_v2;

fn token(name: &str, revision: Option<&str>) -> String {
    LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!")
        .unwrap()
        .mint_token(
            &TokenClaims::new(
                "local",
                "alice",
                TokenScope::Write,
                RepositoryScope::new(RepositoryProvider::Generic, "alice", name, revision).unwrap(),
                u64::MAX,
            )
            .unwrap(),
        )
        .unwrap()
}

#[tokio::test]
async fn tag_token_cannot_fetch_a_guessed_parent_commit() {
    let root = tempfile::tempdir().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_owned()).unwrap());
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    snapshot_contract(root, store, objects).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker, stock Git and SHARDLINE_BIN built from the current tree"]
async fn postgres_and_minio_snapshot_tokens_are_confined() {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()
        .unwrap()
        .expect("Docker is required");
    let database = services.postgres_url().unwrap();
    let binary = std::env::var("SHARDLINE_BIN").expect("SHARDLINE_BIN must name the current CLI");
    let migrated = shardline_test_support::native_client_output(
        std::process::Command::new(binary).args([
            "db",
            "migrate",
            "up",
            "--database-url",
            &database,
        ]),
        std::time::Duration::from_secs(120),
    )
    .unwrap();
    assert!(
        migrated.status.success(),
        "disposable snapshot database migration failed"
    );
    let pool = sqlx::PgPool::connect(&database).await.unwrap();
    let store = BoxedHubStore::from_store(shardline_index::PostgresIndexStore::new(pool.clone()));
    let prefix = services.unique_s3_key_prefix("git-snapshot");
    let raw = services.s3_raw_config(Some(&prefix)).unwrap();
    let objects = ServerObjectStore::s3(
        shardline_storage::S3ObjectStoreConfig::new(raw.bucket, raw.region)
            .with_endpoint(raw.endpoint)
            .with_allow_http(true)
            .with_key_prefix(Some(&prefix))
            .with_credentials(
                raw.access_key.map(shardline_protocol::SecretString::new),
                raw.secret_key.map(shardline_protocol::SecretString::new),
                None,
            ),
    )
    .unwrap();
    snapshot_contract(tempfile::tempdir().unwrap(), store, objects).await;
    pool.close().await;
}

async fn snapshot_contract(
    _root: tempfile::TempDir,
    store: BoxedHubStore,
    objects: ServerObjectStore,
) {
    let repo = "alice/assets";
    store.create_repo(HubRepoType::Model, repo, true).unwrap();
    let parent = "a".repeat(64);
    let head = "b".repeat(64);
    let storage_scope =
        RepositoryScope::new(RepositoryProvider::Generic, "alice", "assets", None).unwrap();
    let namespace = shardline_server_core::protocol_support::scope_namespace(Some(&storage_scope));
    for (revision, payload) in [
        (&parent, b"private parent payload".as_slice()),
        (&head, b"released payload".as_slice()),
    ] {
        let hash = blake3::hash(payload);
        objects
            .put_if_absent(
                &ObjectKey::parse(&format!(
                    "protocols/lfs/{namespace}/objects/{}",
                    hash.to_hex()
                ))
                .unwrap(),
                ObjectBody::from_slice(payload),
                &ObjectIntegrity::new(
                    shardline_protocol::ShardlineHash::from_bytes(*hash.as_bytes()),
                    payload.len() as u64,
                ),
            )
            .unwrap();
        store
            .store_files(
                revision,
                &[shardline_index::hub::HubFileEntry {
                    path: "payload.txt".into(),
                    size: payload.len() as u64,
                    sha: hash.to_hex().to_string(),
                    is_lfs: false,
                }],
            )
            .unwrap();
    }
    for revision in [&parent, &head] {
        let mut files = store.get_files(revision).unwrap();
        files.push(shardline_index::hub::HubFileEntry {
            path: "stable.txt".into(),
            size: 16,
            sha: blake3::hash(b"released payload").to_hex().to_string(),
            is_lfs: false,
        });
        store.store_files(revision, &files).unwrap();
    }
    store
        .create_revision(repo, None, &parent, "main", "parent")
        .unwrap();
    store
        .create_revision(repo, Some(&parent), &head, "main", "head")
        .unwrap();
    store
        .create_revision_if_absent(repo, Some(&head), &head, "refs/tags/release", "release")
        .unwrap();
    store
        .create_revision_if_absent(repo, Some(&parent), &parent, "older", "older")
        .unwrap();
    let app = hub_routes(
        HubState {
            store,
            object_store: objects,
            auth: Some(HubAuth::new(Box::new(
                LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap(),
            ))),
            http_client: None,
            webhook_secret_cipher: None,
            public_base_url: "http://localhost".into(),
        },
        false,
    );
    let (status, refs) = request(
        &app,
        "/models/alice/assets/info/refs?service=git-upload-pack",
        &token("assets", None),
        false,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{refs}");
    for revision in [None, Some("refs/tags/release"), Some(head.as_str())] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/models/alice/assets/info/refs?service=git-upload-pack")
                    .header("Git-Protocol", "version=2")
                    .header(
                        "Authorization",
                        format!("Bearer {}", token("assets", revision)),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let discovery = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap();
        assert_eq!(
            discovery.starts_with(b"000eversion 2\n"),
            revision.is_none(),
            "only restricted snapshots require legacy negotiation"
        );
        if revision.is_some() {
            assert!(discovery.starts_with(b"001e# service=git-upload-pack\n"));
            assert!(!String::from_utf8_lossy(&discovery).contains("refs/heads/older"));
        }
    }
    let at = refs.find(" refs/heads/older").unwrap();
    let oid = &refs[at.checked_sub(40).unwrap()..at];
    for (scope, revision) in [
        ("refs/tags/release", "refs%2Ftags%2Frelease"),
        (head.as_str(), head.as_str()),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/models/alice/assets/paths-info/{revision}"))
                    .header(
                        "Authorization",
                        format!("Bearer {}", token("assets", Some(scope))),
                    )
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        r#"{"paths":["stable.txt","payload.txt"],"expand":true}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap();
        let metadata: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(
            metadata
                .get(0)
                .unwrap()
                .get("lastCommit")
                .unwrap()
                .is_null(),
            "snapshot must not expose an older commit: {metadata}"
        );
        assert_eq!(
            metadata
                .get(1)
                .unwrap()
                .get("lastCommit")
                .unwrap()
                .get("title")
                .unwrap(),
            "head"
        );
        let (history_status, body) = request(
            &app,
            &format!("/api/models/alice/assets/commits/{revision}"),
            &token("assets", Some(scope)),
            false,
        )
        .await;
        assert_eq!(history_status, StatusCode::OK, "{body}");
        let history: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(history.as_array().unwrap().len(), 1, "{history}");
        assert_eq!(history.get(0).unwrap().get("title").unwrap(), "head");
    }
    let want = format!("want {oid}\n");
    let body = format!(
        "{:04x}{want}00000009done\n",
        want.len().checked_add(4).unwrap()
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/models/alice/assets/git-upload-pack")
                .header(
                    "Authorization",
                    format!("Bearer {}", token("assets", Some("refs/tags/release"))),
                )
                .header("Content-Type", "application/x-git-upload-pack-request")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    use sha1::{Digest, Sha1};
    let secret_blob = hex::encode(Sha1::digest(b"blob 22\0private parent payload"));
    let blob_want = format!("want {secret_blob}\n");
    let blob_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/models/alice/assets/git-upload-pack")
                .header(
                    "Authorization",
                    format!("Bearer {}", token("assets", Some("refs/tags/release"))),
                )
                .body(Body::from(format!(
                    "{:04x}{blob_want}00000009done\n",
                    blob_want.len().checked_add(4).unwrap()
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(blob_response.status(), StatusCode::FORBIDDEN);
    snapshot_v2::deny_hidden_objects(&app, &head, oid, &secret_blob).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/models/alice/assets",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let work = tempfile::tempdir().unwrap();
    let path = work.path().to_owned();
    let credential = token("assets", Some("refs/tags/release"));
    let parent_oid = oid.to_owned();
    tokio::task::spawn_blocking(move || {
        for protocol in ["0", "1", "2"] {
            let checkout = path.join(protocol);
            let git_with_credential = |authorization: &str, arguments: &[&str]| {
                shardline_test_support::native_client_output(
                    std::process::Command::new("git")
                        .args(["-c", &format!("protocol.version={protocol}")])
                        .args(arguments)
                        .env("GIT_CONFIG_NOSYSTEM", "1")
                        .env("GIT_CONFIG_GLOBAL", "/dev/null")
                        .env("GIT_CONFIG_COUNT", "1")
                        .env("GIT_CONFIG_KEY_0", "http.extraHeader")
                        .env(
                            "GIT_CONFIG_VALUE_0",
                            format!("Authorization: Bearer {authorization}"),
                        )
                        .env("GIT_TERMINAL_PROMPT", "0"),
                    std::time::Duration::from_secs(30),
                )
                .unwrap()
            };
            let git = |arguments: &[&str]| git_with_credential(&credential, arguments);
            let output = git(&[
                "clone",
                "--branch",
                "release",
                "--single-branch",
                &url,
                checkout.to_str().unwrap(),
            ]);
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let filtered = path.join(format!("filtered-{protocol}"));
            let filtered_output = git(&[
                "clone",
                "--filter=blob:none",
                "--branch",
                "release",
                "--single-branch",
                &url,
                filtered.to_str().unwrap(),
            ]);
            assert!(
                filtered_output.status.success(),
                "{}",
                String::from_utf8_lossy(&filtered_output.stderr)
            );
            assert_eq!(
                std::fs::read(filtered.join("payload.txt")).unwrap(),
                b"released payload"
            );
            assert!(
                git(&["-C", filtered.to_str().unwrap(), "fsck", "--strict"])
                    .status
                    .success()
            );
            let directory = checkout.to_str().unwrap();
            assert_eq!(
                std::fs::read(checkout.join("payload.txt")).unwrap(),
                b"released payload"
            );
            assert_eq!(
                git(&["-C", directory, "rev-list", "--count", "HEAD"]).stdout,
                b"1\n"
            );
            let tip = git(&["-C", directory, "rev-parse", "HEAD"]);
            let tip = String::from_utf8(tip.stdout).unwrap();
            let pinned = token("assets", Some(&head));
            let pinned_checkout = path.join(format!("pinned-{protocol}"));
            let pinned_directory = pinned_checkout.to_str().unwrap();
            let fetched =
                git_with_credential(&pinned, &["clone", "--no-tags", &url, pinned_directory]);
            assert!(
                fetched.status.success(),
                "{}",
                String::from_utf8_lossy(&fetched.stderr)
            );
            assert_eq!(
                git(&["-C", pinned_directory, "rev-list", "--count", "HEAD"]).stdout,
                b"1\n"
            );
            assert_eq!(
                git(&["-C", pinned_directory, "rev-parse", "HEAD"]).stdout,
                tip.as_bytes()
            );
            assert!(
                !git_with_credential(
                    &pinned,
                    &["-C", pinned_directory, "fetch", &url, &parent_oid]
                )
                .status
                .success()
            );
            assert!(git(&["-C", directory, "fsck", "--strict"]).status.success());
            assert!(
                !git(&[
                    "-C",
                    directory,
                    "cat-file",
                    "-e",
                    &format!("{parent_oid}^{{commit}}")
                ])
                .status
                .success()
            );
            let _unshallow = git(&["-C", directory, "fetch", "--unshallow", "origin"]);
            assert_eq!(
                git(&["-C", directory, "rev-list", "--count", "HEAD"]).stdout,
                b"1\n"
            );
        }
    })
    .await
    .unwrap();
    server.abort();
}

async fn request(app: &Router, path: &str, token: &str, batch: bool) -> (StatusCode, String) {
    let mut request = Request::builder()
        .uri(path)
        .header("Authorization", format!("Bearer {token}"));
    let body = if batch {
        request = request
            .method("POST")
            .header("Content-Type", "application/json");
        Body::from(
            serde_json::json!({
                "operation":"upload", "transfers":["basic"], "ref":{"name":"main"},
                "objects":[{"oid":"00".repeat(32), "size":12}]
            })
            .to_string(),
        )
    } else {
        Body::empty()
    };
    let response = app
        .clone()
        .oneshot(request.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let response_body = axum::body::to_bytes(response.into_body(), 65536)
        .await
        .unwrap();
    (status, String::from_utf8(response_body.to_vec()).unwrap())
}

async fn batch_request(
    app: &Router,
    path: &str,
    credential: &str,
    operation: &str,
    transfer: &str,
    revision: Option<&str>,
) -> StatusCode {
    let mut body = serde_json::json!({
        "operation":operation, "transfers":[transfer],
        "objects":[{"oid":"00".repeat(32), "size":12}]
    });
    if let Some(revision) = revision {
        body.as_object_mut()
            .unwrap()
            .insert("ref".to_owned(), serde_json::json!({"name":revision}));
    }
    app.clone()
        .oneshot(
            Request::builder()
                .uri(path)
                .method("POST")
                .header("Authorization", format!("Bearer {credential}"))
                .header("Content-Type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn head_and_lfs_preserve_git_suffix_aliases_literal_names_and_scope() {
    let root = tempfile::tempdir().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_owned()).unwrap());
    for (name, sha) in [("assets", "a".repeat(64)), ("assets.git", "b".repeat(64))] {
        let id = format!("alice/{name}");
        store.create_repo(HubRepoType::Model, &id, true).unwrap();
        let ancestor = if name == "assets" { "e" } else { "f" }.repeat(64);
        store
            .create_revision(&id, None, &ancestor, "main", "older fixture")
            .unwrap();
        store
            .create_revision(&id, Some(&ancestor), &sha, "main", "fixture")
            .unwrap();
    }
    let app = hub_routes(
        HubState {
            store,
            object_store: ServerObjectStore::local(root.path().join("objects")).unwrap(),
            auth: Some(HubAuth::new(Box::new(
                LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap(),
            ))),
            http_client: None,
            webhook_secret_cipher: None,
            public_base_url: "http://localhost".into(),
        },
        false,
    );
    for (name, path, sha) in [
        ("assets", "assets.git", "a".repeat(64)),
        ("assets.git", "assets.git", "b".repeat(64)),
        ("assets.git", "assets.git.git", "b".repeat(64)),
    ] {
        let credential = token(name, None);
        for prefix in ["", "/models"] {
            let head = format!("{prefix}/alice/{path}/HEAD");
            let (status, body) = request(&app, &head, &credential, false).await;
            assert_eq!(status, StatusCode::OK);
            assert!(
                body.contains(&sha),
                "HEAD must retain the exact authorized repository"
            );
            let lfs = format!("{prefix}/alice/{path}/info/lfs/objects/batch");
            assert_eq!(
                request(&app, &lfs, &credential, true).await.0,
                StatusCode::OK
            );
            let unrelated = token("other", None);
            assert_eq!(
                request(&app, &head, &unrelated, false).await.0,
                StatusCode::FORBIDDEN
            );
            assert_eq!(
                request(&app, &lfs, &unrelated, true).await.0,
                StatusCode::FORBIDDEN
            );
            // A branch-only credential must not learn another branch's HEAD.
            assert_eq!(
                request(&app, &head, &token(name, Some("private")), false)
                    .await
                    .0,
                StatusCode::FORBIDDEN
            );
            assert_eq!(
                request(&app, &head, &token(name, Some("main")), false)
                    .await
                    .0,
                StatusCode::OK
            );
            assert_eq!(
                request(&app, &lfs, &token(name, Some("private")), true)
                    .await
                    .0,
                StatusCode::FORBIDDEN,
                "a branch-only credential cannot request upload actions for another branch"
            );
            for (operation, transfer) in [
                ("upload", "basic"),
                ("upload", "xet"),
                ("verify", "basic"),
                ("download", "basic"),
            ] {
                assert_eq!(
                    batch_request(
                        &app,
                        &lfs,
                        &token(name, Some("private")),
                        operation,
                        transfer,
                        Some("main")
                    )
                    .await,
                    StatusCode::FORBIDDEN
                );
                assert_eq!(
                    batch_request(
                        &app,
                        &lfs,
                        &token(name, Some("main")),
                        operation,
                        transfer,
                        Some("refs/heads/main")
                    )
                    .await,
                    StatusCode::OK
                );
                assert_eq!(
                    batch_request(
                        &app,
                        &lfs,
                        &token(name, Some("private")),
                        operation,
                        transfer,
                        None
                    )
                    .await,
                    StatusCode::OK,
                    "omitted refs must retain the existing credential scope"
                );
            }
            assert_eq!(
                batch_request(
                    &app,
                    &lfs,
                    &token(name, Some("main")),
                    "download",
                    "basic",
                    Some(&sha)
                )
                .await,
                StatusCode::OK,
                "branch readers retain immutable checkout access"
            );
            let ancestor = if name == "assets" { "e" } else { "f" }.repeat(64);
            assert_eq!(
                batch_request(
                    &app,
                    &lfs,
                    &token(name, Some("main")),
                    "download",
                    "basic",
                    Some(&ancestor)
                )
                .await,
                StatusCode::OK,
                "branch readers retain access to an older immutable ancestor"
            );
            assert_eq!(
                batch_request(
                    &app,
                    &lfs,
                    &token(name, Some(&sha)),
                    "download",
                    "basic",
                    Some(&ancestor)
                )
                .await,
                StatusCode::FORBIDDEN,
                "a pinned credential does not gain branch ancestry rights"
            );
            assert_eq!(
                request(&app, &lfs, &token(name, Some("main")), true)
                    .await
                    .0,
                StatusCode::OK
            );
        }
    }
}
