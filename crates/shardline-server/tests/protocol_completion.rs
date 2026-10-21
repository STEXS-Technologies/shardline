//! HTTP and native-client qualification for LFS locks and OCI Distribution.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{ServerConfig, ServerFrontend, app};
use shardline_server_core::{AuthProvider, LocalHmacProvider};
use std::{num::NonZeroUsize, path::Path};

const KEY: &[u8] = b"protocol-completion-signing-key-32!";
const INDEX: &str = "application/vnd.oci.image.index.v1+json";

struct Runtime {
    base: String,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn start(
    root: &Path,
    frontend: ServerFrontend,
    auth: bool,
    database: Option<&str>,
) -> Runtime {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let mut config = ServerConfig::new(
        listener.local_addr().unwrap(),
        base.clone(),
        root.to_owned(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_frontends(vec![frontend])
    .unwrap()
    .with_reconstruction_cache_disabled();
    if auth {
        config = config.with_token_signing_key(KEY.to_vec()).unwrap();
    }
    if let Some(database) = database {
        let pool = sqlx::PgPool::connect(database).await.unwrap();
        shardline_server::apply_database_migrations(&pool)
            .await
            .unwrap();
        pool.close().await;
        config = config.with_index_postgres_url(database.to_owned()).unwrap();
    }
    let router = app::router(config).await.unwrap();
    Runtime {
        base,
        task: tokio::spawn(async move { axum::serve(listener, router).await }),
    }
}

fn token(subject: &str, repository: &str, scope: TokenScope) -> String {
    revision_token(subject, repository, scope, None)
}

fn revision_token(
    subject: &str,
    repository: &str,
    scope: TokenScope,
    revision: Option<&str>,
) -> String {
    let repository =
        RepositoryScope::new(RepositoryProvider::Generic, repository, "locks", revision).unwrap();
    let claims = TokenClaims::new("completion", subject, scope, repository, u64::MAX).unwrap();
    LocalHmacProvider::new(KEY)
        .unwrap()
        .mint_token(&claims)
        .unwrap()
}

async fn exercise_locks(database: Option<&str>) {
    let root = tempfile::tempdir().unwrap();
    let repository = format!(
        "locks-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
    let alice = token("alice", &repository, TokenScope::Write);
    let bob = token("bob", &repository, TokenScope::Write);
    let other = token("alice", &format!("{repository}-other"), TokenScope::Write);
    let read = token("alice", &repository, TokenScope::Read);
    let client = Client::new();
    let server = start(root.path(), ServerFrontend::Lfs, true, database).await;
    let mut url = format!("{}/v1/lfs/locks", server.base);
    let created = client
        .post(&url)
        .bearer_auth(&alice)
        .json(&json!({"path":"models/weights.bin"}))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    assert_eq!(
        created.headers()["content-type"],
        "application/vnd.git-lfs+json"
    );
    let created: Value = created.json().await.unwrap();
    let id = created["lock"]["id"].as_str().unwrap();
    assert_eq!(created["lock"]["owner"]["name"], "alice");
    let duplicate = client
        .post(&url)
        .bearer_auth(&bob)
        .json(&json!({"path":"models/weights.bin"}))
        .send()
        .await
        .unwrap();
    assert_eq!(duplicate.status(), StatusCode::CONFLICT);
    assert_eq!(duplicate.json::<Value>().await.unwrap()["lock"]["id"], id);
    assert_eq!(
        client
            .post(&url)
            .bearer_auth(&read)
            .json(&json!({"path":"readonly"}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .post(&url)
            .bearer_auth(&alice)
            .json(&json!({"path":"../escape"}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let verify: Value = client
        .post(format!("{url}/verify"))
        .bearer_auth(&bob)
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(verify["ours"].as_array().unwrap().is_empty());
    assert_eq!(verify["theirs"][0]["id"], id);
    let unlock = format!("{url}/{id}/unlock");
    assert_eq!(
        client
            .post(&unlock)
            .bearer_auth(&bob)
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .post(&unlock)
            .bearer_auth(&other)
            .json(&json!({"force":true}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    drop(server);
    let restarted = start(root.path(), ServerFrontend::Lfs, true, database).await;
    url = format!("{}/v1/lfs/locks", restarted.base);
    let listed: Value = client
        .get(&url)
        .bearer_auth(&alice)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["locks"][0], created["lock"]);
    assert_eq!(
        client
            .post(format!("{url}/{id}/unlock"))
            .bearer_auth(&bob)
            .json(&json!({"force":true}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let mut tasks = tokio::task::JoinSet::new();
    let other_root = tempfile::tempdir().unwrap();
    let other_server = start(
        if database.is_some() {
            other_root.path()
        } else {
            root.path()
        },
        ServerFrontend::Lfs,
        true,
        database,
    )
    .await;
    let other_url = format!("{}/v1/lfs/locks", other_server.base);
    for index in 0..16 {
        let client = client.clone();
        let alice = alice.clone();
        let url = if index % 2 == 0 {
            url.clone()
        } else {
            other_url.clone()
        };
        tasks.spawn(async move {
            client
                .post(url)
                .bearer_auth(alice)
                .json(&json!({"path":"race.bin"}))
                .send()
                .await
                .unwrap()
                .status()
        });
    }
    let mut winners = 0_u32;
    while let Some(result) = tasks.join_next().await {
        let status = result.unwrap();
        if status == StatusCode::CREATED {
            winners = winners.saturating_add(1);
        } else {
            assert_eq!(status, StatusCode::CONFLICT);
        }
    }
    assert_eq!(winners, 1);
    for path in ["page/a", "page/b"] {
        assert_eq!(
            client
                .post(&url)
                .bearer_auth(&alice)
                .json(&json!({"path":path}))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
    }
    let mut cursor: Option<String> = None;
    let mut ids = std::collections::BTreeSet::new();
    loop {
        let mut request = client
            .get(&url)
            .bearer_auth(&alice)
            .query(&[("limit", "1")]);
        if let Some(cursor) = &cursor {
            request = request.query(&[("cursor", cursor)]);
        }
        let page: Value = request.send().await.unwrap().json().await.unwrap();
        for lock in page["locks"].as_array().unwrap() {
            assert!(ids.insert(lock["id"].as_str().unwrap().to_owned()));
        }
        cursor = page
            .get("next_cursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(ids.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lfs_locks_are_exclusive_owned_scoped_and_durable_on_sqlite() {
    exercise_locks(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn lfs_locks_are_exclusive_owned_scoped_and_durable_on_postgres() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    exercise_locks(Some(&database)).await;
}

async fn publish(client: &Client, base: &str, repository: &str, document: &Value) -> String {
    let bytes = serde_json::to_vec(document).unwrap();
    let digest = format!("sha256:{}", hex::encode(Sha256::digest(&bytes)));
    let response = client
        .put(format!("{base}/v2/{repository}/manifests/{digest}"))
        .header("content-type", INDEX)
        .body(bytes)
        .send()
        .await
        .unwrap();
    let status = response.status();
    if let Some(subject) = document.get("subject") {
        assert_eq!(
            response.headers()["OCI-Subject"],
            subject["digest"].as_str().unwrap()
        );
    }
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        response.text().await.unwrap()
    );
    digest
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oci_referrers_filter_paginate_survive_restart_and_respect_deletion() {
    exercise_referrers(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn oci_referrers_on_postgres() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    exercise_referrers(Some(&database)).await;
}

async fn exercise_referrers(database: Option<&str>) {
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path(), ServerFrontend::Oci, false, database).await;
    let repository = format!(
        "completion-{}/artifacts",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
    let client = Client::new();
    let subject = format!("sha256:{}", "a".repeat(64));
    let mut digests = Vec::new();
    for artifact_type in ["application/example.signature", "application/example.sbom"] {
        let document = json!({"schemaVersion":2,"mediaType":INDEX,"manifests":[],"artifactType":artifact_type,"annotations":{"org.example.kind":artifact_type},"subject":{"mediaType":INDEX,"digest":subject,"size":123}});
        digests.push(publish(&client, &server.base, &repository, &document).await);
    }
    let mut url = format!("{}/v2/{repository}/referrers/{subject}", server.base);
    let first = client.get(&url).query(&[("n", "1")]).send().await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers()["content-type"], INDEX);
    let link = first.headers()["link"].to_str().unwrap().to_owned();
    let first: Value = first.json().await.unwrap();
    assert_eq!(first["manifests"].as_array().unwrap().len(), 1);
    let next = link.split_once('<').unwrap().1.split_once('>').unwrap().0;
    let second: Value = client
        .get(format!("{}{next}", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_ne!(
        first["manifests"][0]["digest"],
        second["manifests"][0]["digest"]
    );
    let filtered = client
        .get(&url)
        .query(&[("artifactType", "application/example.signature")])
        .send()
        .await
        .unwrap();
    assert_eq!(filtered.headers()["OCI-Filters-Applied"], "artifactType");
    let filtered: Value = filtered.json().await.unwrap();
    assert_eq!(filtered["manifests"].as_array().unwrap().len(), 1);
    let other: Value = client
        .get(format!(
            "{}/v2/other/artifacts/referrers/{subject}",
            server.base
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(other["manifests"].as_array().unwrap().is_empty());
    drop(server);
    let restarted = start(root.path(), ServerFrontend::Oci, false, database).await;
    url = format!("{}/v2/{repository}/referrers/{subject}", restarted.base);
    let listed: Value = client.get(&url).send().await.unwrap().json().await.unwrap();
    assert_eq!(listed["manifests"].as_array().unwrap().len(), 2);
    for digest in digests {
        assert_eq!(
            client
                .delete(format!(
                    "{}/v2/{repository}/manifests/{digest}",
                    restarted.base
                ))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::ACCEPTED
        );
    }
    let after_delete: Value = client.get(&url).send().await.unwrap().json().await.unwrap();
    assert!(after_delete["manifests"].as_array().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires OCI_CONFORMANCE_BIN from distribution-spec v1.1.1"]
async fn official_oci_distribution_conformance() {
    let executable = std::env::var("OCI_CONFORMANCE_BIN").expect("OCI_CONFORMANCE_BIN is required");
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path(), ServerFrontend::Oci, false, None).await;
    let mut command = std::process::Command::new(executable);
    command
        .current_dir(root.path())
        .arg("-ginkgo.timeout=5m")
        .arg("-ginkgo.no-color")
        .env("OCI_ROOT_URL", &server.base)
        .env("OCI_NAMESPACE", "conformance/main")
        .env("OCI_CROSSMOUNT_NAMESPACE", "conformance/other")
        .env("OCI_TEST_PULL", "1")
        .env("OCI_TEST_PUSH", "1")
        .env("OCI_TEST_CONTENT_DISCOVERY", "1")
        .env("OCI_TEST_CONTENT_MANAGEMENT", "1")
        .env("OCI_DELETE_MANIFEST_BEFORE_BLOBS", "1")
        .env("OCI_AUTOMATIC_CROSSMOUNT", "1")
        .env("OCI_REPORT_DIR", "none");
    let output = tokio::task::spawn_blocking(move || command.output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "official OCI suite failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("{}", String::from_utf8_lossy(&output.stdout));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oci_errors_distinguish_missing_blobs_manifests_and_dependencies() {
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path(), ServerFrontend::Oci, false, None).await;
    let client = Client::new();
    let missing = format!("sha256:{}", "b".repeat(64));
    for (path, code) in [
        (format!("blobs/{missing}"), "BLOB_UNKNOWN"),
        (
            "manifests/.INVALID_MANIFEST_NAME".to_owned(),
            "MANIFEST_UNKNOWN",
        ),
    ] {
        let response = client
            .get(format!("{}/v2/errors/test/{path}", server.base))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let error: Value = response.json().await.unwrap();
        assert_eq!(error["errors"][0]["code"], code);
    }
    for (document, code) in [
        (
            json!({"schemaVersion":2,"mediaType":INDEX,"manifests":[{"mediaType":INDEX,"digest":missing,"size":1}]}),
            "MANIFEST_BLOB_UNKNOWN",
        ),
        (
            json!({"schemaVersion":1,"mediaType":INDEX,"manifests":[]}),
            "MANIFEST_INVALID",
        ),
    ] {
        let response = client
            .put(format!("{}/v2/errors/test/manifests/latest", server.base))
            .header("content-type", INDEX)
            .json(&document)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: Value = response.json().await.unwrap();
        assert_eq!(error["errors"][0]["code"], code);
    }
    let upload = client
        .post(format!("{}/v2/errors/test/blobs/uploads/", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::ACCEPTED);
    let location = upload.headers()["location"].to_str().unwrap();
    let session_url = reqwest::Url::parse(&server.base)
        .unwrap()
        .join(location)
        .unwrap();
    assert_eq!(
        client
            .delete(session_url.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    let missing_session = client.get(session_url).send().await.unwrap();
    assert_eq!(missing_session.status(), StatusCode::NOT_FOUND);
    let error: Value = missing_session.json().await.unwrap();
    assert_eq!(error["errors"][0]["code"], "BLOB_UPLOAD_UNKNOWN");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native git-lfs"]
async fn native_git_lfs_lock_list_verify_and_unlock() {
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path(), ServerFrontend::Lfs, true, None).await;
    let worktree = tempfile::tempdir().unwrap();
    let credential = revision_token(
        "native-client",
        "native-lfs-locks",
        TokenScope::Write,
        Some("main"),
    );
    async fn git(directory: &Path, args: &[&str]) -> Vec<u8> {
        let directory = directory.to_owned();
        let args: Vec<_> = args.iter().map(|arg| (*arg).to_owned()).collect();
        let output = tokio::task::spawn_blocking(move || {
            std::process::Command::new("git")
                .arg("-C")
                .arg(directory)
                .args(args)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .output()
        })
        .await
        .unwrap()
        .expect("git and git-lfs must be installed");
        assert!(
            output.status.success(),
            "native git command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }
    git(worktree.path(), &["init"]).await;
    git(worktree.path(), &["branch", "-M", "main"]).await;
    std::fs::write(worktree.path().join("model.bin"), b"native locking fixture").unwrap();
    git(worktree.path(), &["add", "model.bin"]).await;
    git(
        worktree.path(),
        &[
            "-c",
            "user.name=Protocol Test",
            "-c",
            "user.email=protocol@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "locking fixture",
        ],
    )
    .await;
    git(
        worktree.path(),
        &[
            "remote",
            "add",
            "origin",
            "https://example.invalid/test/repository.git",
        ],
    )
    .await;
    git(
        worktree.path(),
        &["config", "lfs.url", &format!("{}/v1/lfs", server.base)],
    )
    .await;
    git(
        worktree.path(),
        &[
            "config",
            "http.extraHeader",
            &format!("Authorization: Bearer {credential}"),
        ],
    )
    .await;
    git(worktree.path(), &["config", "lfs.locksverify", "true"]).await;
    let locked: Value = serde_json::from_slice(
        &git(worktree.path(), &["lfs", "lock", "model.bin", "--json"]).await,
    )
    .unwrap();
    let locked = &locked[0];
    assert_eq!(locked["path"], "model.bin");
    let listed: Value =
        serde_json::from_slice(&git(worktree.path(), &["lfs", "locks", "--json"]).await).unwrap();
    assert_eq!(listed[0]["id"], locked["id"]);
    git(worktree.path(), &["lfs", "locks", "--verify", "--json"]).await;
    git(worktree.path(), &["lfs", "unlock", "model.bin", "--json"]).await;
    let after_unlock: Value =
        serde_json::from_slice(&git(worktree.path(), &["lfs", "locks", "--json"]).await).unwrap();
    assert!(after_unlock.as_array().unwrap().is_empty());

    // Exercise actual transfer with locking verification enabled, then discard
    // the client's cache so a successful checkout must download from Shardline.
    let origin = tempfile::tempdir().unwrap();
    git(origin.path(), &["init", "--bare"]).await;
    git(
        worktree.path(),
        &[
            "remote",
            "set-url",
            "origin",
            origin.path().to_str().unwrap(),
        ],
    )
    .await;
    git(worktree.path(), &["lfs", "install", "--local"]).await;
    git(worktree.path(), &["lfs", "track", "*.bin"]).await;
    git(worktree.path(), &["add", ".gitattributes", "model.bin"]).await;
    git(
        worktree.path(),
        &[
            "-c",
            "user.name=Protocol Test",
            "-c",
            "user.email=protocol@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "LFS transfer fixture",
        ],
    )
    .await;
    git(worktree.path(), &["push", "origin", "main"]).await;
    std::fs::remove_dir_all(worktree.path().join(".git/lfs/objects")).unwrap();
    std::fs::remove_file(worktree.path().join("model.bin")).unwrap();
    git(worktree.path(), &["lfs", "fetch", "--all", "origin"]).await;
    git(worktree.path(), &["lfs", "checkout"]).await;
    assert_eq!(
        std::fs::read(worktree.path().join("model.bin")).unwrap(),
        b"native locking fixture"
    );
}
