#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
//! Real Git client coverage for repositories updated through the Hub NDJSON API.
#[path = "support/common.rs"]
mod common;

use axum::{body::Body, http::Request};
use base64::{Engine, engine::general_purpose::STANDARD};
use tower::ServiceExt;

async fn commit(test: &common::HubTestContext, content: &[u8]) {
    let rows = [
        serde_json::json!({"header": {"message": "upload"}}),
        serde_json::json!({"file": {"path": "nested/hello.txt", "content": STANDARD.encode(content)}}),
        serde_json::json!({"file": {"path": "foo.bar", "content": STANDARD.encode(b"root file\n")}}),
        serde_json::json!({"file": {"path": "foo/child.txt", "content": STANDARD.encode(b"directory child\n")}}),
        serde_json::json!({"file": {"path": "z-last.txt", "content": STANDARD.encode(b"last root file\n")}}),
    ];
    let body = rows
        .iter()
        .map(serde_json::Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    let response = test
        .app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/alice/git-cli/commit/main")
                .header("content-type", "application/x-ndjson")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
}

fn git(args: &[&str]) -> std::process::Output {
    std::process::Command::new("git")
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_HTTP_LOW_SPEED_LIMIT", "1")
        .env("GIT_HTTP_LOW_SPEED_TIME", "10")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .expect("installed Git client")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_cli_clone_and_fetch_ndjson_revisions() {
    let test = common::setup();
    test.state()
        .store
        .create_repo(
            shardline_index::hub::HubRepoType::Model,
            "alice/git-cli",
            false,
        )
        .unwrap();
    commit(&test, b"first real contents\n").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = test.app();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let work = tempfile::tempdir().unwrap();
    let clone_path = work.path().join("clone");
    let clone_dir = clone_path.to_str().unwrap();
    let url = format!("http://{addr}/models/alice/git-cli");
    let advertised_first = git(&["ls-remote", &url]);
    assert!(advertised_first.status.success());
    assert_eq!(
        advertised_first.stdout,
        git(&["ls-remote", &url]).stdout,
        "immutable advertisement must be stable"
    );
    let cloned = git(&["clone", &url, clone_dir]);
    assert!(
        cloned.status.success(),
        "clone: {}",
        String::from_utf8_lossy(&cloned.stderr)
    );
    assert_eq!(
        std::fs::read(clone_path.join("nested/hello.txt")).unwrap(),
        b"first real contents\n"
    );
    assert_eq!(
        std::fs::read(clone_path.join("foo.bar")).unwrap(),
        b"root file\n"
    );
    assert_eq!(
        std::fs::read(clone_path.join("foo/child.txt")).unwrap(),
        b"directory child\n"
    );
    assert_eq!(
        std::fs::read(clone_path.join("z-last.txt")).unwrap(),
        b"last root file\n"
    );
    let first = git(&["-C", clone_dir, "rev-parse", "HEAD"]);
    commit(&test, b"second real contents\n").await;
    let fetched = git(&["-C", clone_dir, "fetch", "origin"]);
    assert!(
        fetched.status.success(),
        "fetch: {}",
        String::from_utf8_lossy(&fetched.stderr)
    );
    let ancestor = String::from_utf8(first.stdout).unwrap();
    assert!(
        git(&[
            "-C",
            clone_dir,
            "merge-base",
            "--is-ancestor",
            ancestor.trim(),
            "origin/main"
        ])
        .status
        .success()
    );
    let shown = git(&["-C", clone_dir, "show", "origin/main:nested/hello.txt"]);
    assert!(shown.status.success());
    assert_eq!(shown.stdout, b"second real contents\n");
    let checked = git(&["-C", clone_dir, "fsck", "--full", "--strict"]);
    assert!(
        checked.status.success(),
        "fsck: {}",
        String::from_utf8_lossy(&checked.stderr)
    );
    assert!(
        git(&["-C", clone_dir, "reset", "--hard", "origin/main"])
            .status
            .success()
    );
    std::fs::write(clone_path.join("nested/hello.txt"), b"from Git push\n").unwrap();
    assert!(git(&["-C", clone_dir, "add", "."]).status.success());
    assert!(
        git(&[
            "-C",
            clone_dir,
            "-c",
            "user.name=Git Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "Git-origin"
        ])
        .status
        .success()
    );
    let pushed_head = git(&["-C", clone_dir, "rev-parse", "HEAD"]);
    let pushed = git(&["-C", clone_dir, "push", "origin", "HEAD:main"]);
    assert!(
        pushed.status.success(),
        "push: {}",
        String::from_utf8_lossy(&pushed.stderr)
    );
    let second_path = work.path().join("clone-git");
    let second_dir = second_path.to_str().unwrap();
    let recloned = git(&["clone", &url, second_dir]);
    assert!(
        recloned.status.success(),
        "reclone: {}",
        String::from_utf8_lossy(&recloned.stderr)
    );
    assert_eq!(
        git(&["-C", second_dir, "rev-parse", "HEAD"]).stdout,
        pushed_head.stdout,
        "preserve original Git commit identity"
    );
    assert_eq!(
        std::fs::read(second_path.join("nested/hello.txt")).unwrap(),
        b"from Git push\n"
    );
    assert!(
        git(&["-C", second_dir, "fsck", "--full", "--strict"])
            .status
            .success()
    );
    let main_refs_before = git(&["ls-remote", &url, "refs/heads/main"]);
    let tagged = git(&[
        "-C",
        clone_dir,
        "push",
        "origin",
        "HEAD:refs/tags/v1",
        "HEAD:refs/heads/same-head",
    ]);
    assert!(
        tagged.status.success(),
        "same-commit refs: {}",
        String::from_utf8_lossy(&tagged.stderr)
    );
    assert_eq!(
        main_refs_before.stdout,
        git(&["ls-remote", &url, "refs/heads/main"]).stdout,
        "other refs must not change main identity"
    );
    let all_refs = git(&["ls-remote", &url]);
    let head_sha = String::from_utf8(pushed_head.stdout.clone()).unwrap();
    let advertised = String::from_utf8(all_refs.stdout).unwrap();
    assert!(
        advertised
            .lines()
            .any(|line| line == format!("{}\trefs/tags/v1", head_sha.trim()))
    );
    assert!(
        advertised
            .lines()
            .any(|line| line == format!("{}\trefs/heads/same-head", head_sha.trim()))
    );
    let mut large = vec![0u8; 256 * 1024];
    for (index, byte) in large.iter_mut().enumerate() {
        *byte = u8::try_from(index % 251).unwrap();
    }
    std::fs::write(clone_path.join("large.bin"), &large).unwrap();
    assert!(git(&["-C", clone_dir, "add", "large.bin"]).status.success());
    assert!(
        git(&[
            "-C",
            clone_dir,
            "-c",
            "user.name=Git Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "large blob"
        ])
        .status
        .success()
    );
    let large_push = git(&["-C", clone_dir, "push", "origin", "HEAD:main"]);
    assert!(
        large_push.status.success(),
        "large push: {}",
        String::from_utf8_lossy(&large_push.stderr)
    );
    large[100_000..100_016].copy_from_slice(b"edited delta row");
    std::fs::write(clone_path.join("large.bin"), &large).unwrap();
    assert!(git(&["-C", clone_dir, "add", "large.bin"]).status.success());
    assert!(
        git(&[
            "-C",
            clone_dir,
            "-c",
            "user.name=Git Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "delta edit"
        ])
        .status
        .success()
    );
    let delta_push = git(&["-C", clone_dir, "push", "origin", "HEAD:main"]);
    assert!(
        delta_push.status.success(),
        "delta push: {}",
        String::from_utf8_lossy(&delta_push.stderr)
    );
    let mixed_parent = git(&["-C", clone_dir, "rev-parse", "HEAD"]);
    // An NDJSON edit based on a Git-origin revision must retain exact ancestry.
    commit(&test, b"NDJSON after Git push\n").await;
    let mixed_fetch = git(&["-C", clone_dir, "fetch", "origin"]);
    assert!(
        mixed_fetch.status.success(),
        "mixed fetch: {}",
        String::from_utf8_lossy(&mixed_fetch.stderr)
    );
    let parent = git(&["-C", clone_dir, "rev-parse", "origin/main^"]);
    assert_eq!(parent.stdout, mixed_parent.stdout);
    let large_shown = git(&["-C", clone_dir, "show", "origin/main:large.bin"]);
    assert_eq!(large_shown.stdout, large);
    assert!(
        git(&["-C", clone_dir, "fsck", "--full", "--strict"])
            .status
            .success()
    );
    let archived_sha = String::from_utf8(pushed_head.stdout).unwrap();
    let archived_key = shardline_storage::ObjectKey::parse(&format!(
        "protocols/hub/git/global/{}/{}",
        blake3::hash(b"alice/git-cli").to_hex(),
        archived_sha.trim()
    ))
    .unwrap();
    use shardline_storage::ObjectStore;
    assert!(test.state().object_store.contains(&archived_key).unwrap());
    let deleted = test
        .app()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/models/alice/git-cli")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deleted.status(), axum::http::StatusCode::NO_CONTENT);
    assert!(
        git(&["ls-remote", &url]).stdout.is_empty(),
        "deleted refs must not be advertised"
    );
    let want = shardline_hub_api::git::pktline::encode_line(&format!(
        "want {} side-band-64k\n",
        archived_sha.trim()
    ))
    .unwrap();
    let rejected = test
        .app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/models/alice/git-cli/git-upload-pack")
                .body(Body::from(format!("{want}0000")))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        rejected.status(),
        axum::http::StatusCode::BAD_REQUEST,
        "retained archives do not authorize deleted revisions"
    );
    assert!(
        test.state().object_store.contains(&archived_key).unwrap(),
        "Hub deletion retains archived bytes like inline CAS"
    );
    server.abort();
}
