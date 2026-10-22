#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
//! Real Git client coverage for repositories updated through the Hub NDJSON API.
mod support;
use support::common;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::{
    body::{Body, to_bytes},
    http::Request,
    middleware::{self, Next},
};
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
        .env("GIT_TRACE_PACKET", "1")
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
    // Count actual upload-pack HTTP response bytes seen by native Git. Compare
    // incremental and cold fetches at the same head, rather than local pack
    // sizes (Git can repack those independently of the server response).
    let transferred = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&transferred);
    let app = test.app().layer(middleware::from_fn(
        move |request: Request<Body>, next: Next| {
            let observed = Arc::clone(&observed);
            async move {
                let upload_pack = request.uri().path().ends_with("/git-upload-pack");
                let response = next.run(request).await;
                if !upload_pack {
                    return response;
                }
                let (parts, body) = response.into_parts();
                let bytes = to_bytes(body, 16 * 1024 * 1024).await.unwrap();
                observed.fetch_add(bytes.len(), Ordering::Relaxed);
                axum::response::Response::from_parts(parts, Body::from(bytes))
            }
        },
    ));
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
    transferred.store(0, Ordering::Relaxed);
    let fetched = git(&["-C", clone_dir, "fetch", "origin"]);
    assert!(
        fetched.status.success(),
        "fetch: {}",
        String::from_utf8_lossy(&fetched.stderr)
    );
    let incremental_bytes = transferred.swap(0, Ordering::Relaxed);
    let cold_path = work.path().join("cold-same-head");
    let cold_dir = cold_path.to_str().unwrap();
    let cold = git(&["clone", &url, cold_dir]);
    assert!(
        cold.status.success(),
        "cold clone: {}",
        String::from_utf8_lossy(&cold.stderr)
    );
    let cold_bytes = transferred.load(Ordering::Relaxed);
    let shallow_path = work.path().join("shallow");
    let shallow_dir = shallow_path.to_str().unwrap();
    for args in [
        vec![
            "clone",
            "--depth=1",
            "--no-tags",
            "--single-branch",
            &url,
            shallow_dir,
        ],
        vec!["-C", shallow_dir, "fsck", "--strict"],
    ] {
        let result = git(&args);
        assert!(
            result.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    assert_eq!(
        git(&["-C", shallow_dir, "rev-list", "--count", "HEAD"]).stdout,
        b"1\n"
    );
    assert_eq!(
        git(&["-C", shallow_dir, "rev-parse", "--is-shallow-repository"]).stdout,
        b"true\n"
    );
    let deepen = git(&["-C", shallow_dir, "fetch", "--deepen=1", "origin", "main"]);
    assert!(
        deepen.status.success(),
        "deepen: {}",
        String::from_utf8_lossy(&deepen.stderr)
    );
    assert_eq!(
        git(&["-C", shallow_dir, "rev-list", "--count", "HEAD"]).stdout,
        b"2\n"
    );
    let complete = git(&["-C", shallow_dir, "fetch", "--unshallow", "origin", "main"]);
    assert!(
        complete.status.success(),
        "unshallow: {}",
        String::from_utf8_lossy(&complete.stderr)
    );
    assert_eq!(
        git(&["-C", shallow_dir, "rev-parse", "--is-shallow-repository"]).stdout,
        b"false\n"
    );
    assert_eq!(
        git(&["-C", shallow_dir, "rev-list", "HEAD"]).stdout,
        git(&["-C", cold_dir, "rev-list", "HEAD"]).stdout
    );
    assert!(
        git(&["-C", shallow_dir, "fsck", "--strict"])
            .status
            .success()
    );
    eprintln!("upload-pack response bytes: incremental={incremental_bytes}, cold={cold_bytes}");
    assert!(incremental_bytes > 0);
    assert!(
        incremental_bytes < cold_bytes,
        "native incremental fetch transferred {incremental_bytes} bytes versus {cold_bytes} for a cold clone at the same head"
    );
    assert_eq!(
        git(&["-C", clone_dir, "rev-parse", "origin/main"]).stdout,
        git(&["-C", cold_dir, "rev-parse", "HEAD"]).stdout
    );
    assert_eq!(
        std::fs::read(cold_path.join("nested/hello.txt")).unwrap(),
        b"second real contents\n"
    );
    assert!(
        git(&["-C", cold_dir, "fsck", "--full", "--strict"])
            .status
            .success()
    );
    let ancestor = String::from_utf8(first.stdout).unwrap();
    let historical = git(&[
        "-C",
        clone_dir,
        "show",
        &format!("{}:nested/hello.txt", ancestor.trim()),
    ]);
    assert!(historical.status.success());
    assert_eq!(historical.stdout, b"first real contents\n");
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
        git(&["-C", clone_dir, "pull", "--ff-only", "origin", "main"])
            .status
            .success()
    );
    assert_eq!(
        std::fs::read(clone_path.join("nested/hello.txt")).unwrap(),
        b"second real contents\n"
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
    let trace = String::from_utf8_lossy(&pushed.stderr);
    assert!(
        trace.lines().any(|line| line.contains("> ")
            && line.contains("report-status-v2")
            && line.contains("side-band-64k")),
        "native client must negotiate report-status-v2: {trace}"
    );
    assert!(
        trace.contains("unpack ok"),
        "native client must receive unpack status: {trace}"
    );
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
    let atomic = git(&[
        "-C",
        clone_dir,
        "push",
        "--atomic",
        "origin",
        "HEAD:refs/heads/atomic-one",
        "HEAD:refs/tags/atomic-two",
    ]);
    let atomic_trace = String::from_utf8_lossy(&atomic.stderr);
    assert!(atomic.status.success(), "{atomic_trace}");
    assert!(
        atomic_trace
            .lines()
            .any(|line| line.contains("> ") && line.contains(" atomic")),
        "{atomic_trace}"
    );
    for reference in ["refs/heads/atomic-one", "refs/tags/atomic-two"] {
        assert!(!git(&["ls-remote", &url, reference]).stdout.is_empty());
    }
    let atomic_refusal = git(&[
        "-C",
        clone_dir,
        "push",
        "--atomic",
        "origin",
        "HEAD:refs/heads/atomic-must-not-exist",
        ":refs/heads/main",
    ]);
    let atomic_refusal_trace = String::from_utf8_lossy(&atomic_refusal.stderr);
    assert!(!atomic_refusal.status.success(), "{atomic_refusal_trace}");
    assert!(
        atomic_refusal_trace.contains("ng refs/heads/atomic-must-not-exist"),
        "{atomic_refusal_trace}"
    );
    assert!(
        atomic_refusal_trace.contains("ng refs/heads/main"),
        "{atomic_refusal_trace}"
    );
    assert!(
        git(&["ls-remote", &url, "refs/heads/atomic-must-not-exist"])
            .stdout
            .is_empty()
    );
    assert_eq!(
        main_refs_before.stdout,
        git(&["ls-remote", &url, "refs/heads/main"]).stdout
    );
    let atomic_deletion = git(&[
        "-C",
        clone_dir,
        "push",
        "--atomic",
        "origin",
        ":refs/heads/atomic-one",
        ":refs/tags/atomic-two",
    ]);
    assert!(
        atomic_deletion.status.success(),
        "{}",
        String::from_utf8_lossy(&atomic_deletion.stderr)
    );
    for reference in ["refs/heads/atomic-one", "refs/tags/atomic-two"] {
        assert!(git(&["ls-remote", &url, reference]).stdout.is_empty());
    }
    let mut large = vec![0u8; 256 * 1024];
    let deleted_branch = git(&["-C", clone_dir, "push", "origin", ":refs/heads/same-head"]);
    assert!(
        deleted_branch.status.success(),
        "{}",
        String::from_utf8_lossy(&deleted_branch.stderr)
    );
    assert!(
        git(&["ls-remote", &url, "refs/heads/same-head"])
            .stdout
            .is_empty()
    );
    let refused = git(&["-C", clone_dir, "push", "origin", ":refs/heads/main"]);
    assert!(!refused.status.success());
    let refusal_trace = String::from_utf8_lossy(&refused.stderr);
    assert!(
        refusal_trace
            .lines()
            .any(|line| line.contains("> ") && line.contains("report-status-v2")),
        "{refusal_trace}"
    );
    assert!(
        refusal_trace.contains("ng refs/heads/main"),
        "native client must receive rejected-ref status: {refusal_trace}"
    );
    assert_eq!(
        main_refs_before.stdout,
        git(&["ls-remote", &url, "refs/heads/main"]).stdout
    );
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
