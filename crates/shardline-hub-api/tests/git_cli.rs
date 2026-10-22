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
    commit_to(test, "git-cli", content).await;
}

async fn commit_to(test: &common::HubTestContext, repo: &str, content: &[u8]) {
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
                .uri(format!("/api/models/alice/{repo}/commit/main"))
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
        .env("GIT_AUTHOR_DATE", "2030-01-01T00:00:00+00:00")
        .env("GIT_COMMITTER_DATE", "2030-01-01T00:00:00+00:00")
        .output()
        .expect("installed Git client")
}

fn git_without_lazy_fetch(args: &[&str]) -> std::process::Output {
    std::process::Command::new("git")
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_HTTP_LOW_SPEED_LIMIT", "1")
        .env("GIT_HTTP_LOW_SPEED_TIME", "10")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TRACE_PACKET", "1")
        .env("GIT_NO_LAZY_FETCH", "1")
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
    let deepen_trace = String::from_utf8_lossy(&deepen.stderr);
    assert!(
        deepen_trace.contains("deepen-relative"),
        "native Git should negotiate relative deepening when extending a shallow clone: {deepen_trace}"
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
    let option_tag = git(&["-C", clone_dir, "tag", "push-options-probe"]);
    assert!(option_tag.status.success());
    let push_with_option = git(&[
        "-C",
        clone_dir,
        "push",
        "--push-option=shardline.test=value",
        "origin",
        "refs/tags/push-options-probe",
    ]);
    let option_trace = String::from_utf8_lossy(&push_with_option.stderr);
    assert!(
        push_with_option.status.success(),
        "push with option: {option_trace}"
    );
    assert!(
        option_trace.contains("push-options"),
        "native Git must negotiate push-options: {option_trace}"
    );
    let option_tag_ref = git(&["ls-remote", &url, "refs/tags/push-options-probe"]);
    assert!(option_tag_ref.status.success());
    assert!(
        String::from_utf8_lossy(&option_tag_ref.stdout).contains("refs/tags/push-options-probe")
    );
    let second_path = work.path().join("clone-git");
    let partial_path = work.path().join("partial-clone");
    let partial_dir = partial_path.to_str().unwrap();
    let partial = git(&[
        "clone",
        "--filter=blob:none",
        "--no-checkout",
        &url,
        partial_dir,
    ]);
    assert!(
        partial.status.success(),
        "partial clone: {}",
        String::from_utf8_lossy(&partial.stderr)
    );
    let local_objects = git(&[
        "-C",
        partial_dir,
        "cat-file",
        "--batch-all-objects",
        "--batch-check",
    ]);
    assert!(local_objects.status.success());
    assert!(
        !String::from_utf8_lossy(&local_objects.stdout).contains(" blob "),
        "initial clone must omit blobs"
    );
    let checkout = git(&["-C", partial_dir, "checkout", "main"]);
    assert!(
        checkout.status.success(),
        "lazy checkout: {}",
        String::from_utf8_lossy(&checkout.stderr)
    );
    assert_eq!(
        std::fs::read(partial_path.join("nested/hello.txt")).unwrap(),
        b"from Git push\n"
    );
    assert!(
        git(&["-C", partial_dir, "fsck", "--strict"])
            .status
            .success()
    );
    for depth in [0, 1] {
        let tree_path = work.path().join(format!("tree-filter-{depth}"));
        let tree_dir = tree_path.to_str().unwrap();
        let filter = format!("--filter=tree:{depth}");
        let tree_cloned = git(&["clone", &filter, "--no-checkout", &url, tree_dir]);
        assert!(
            tree_cloned.status.success(),
            "tree clone: {}",
            String::from_utf8_lossy(&tree_cloned.stderr)
        );
        let listed = git(&[
            "-C",
            tree_dir,
            "cat-file",
            "--batch-all-objects",
            "--batch-check",
        ]);
        assert!(listed.status.success());
        let listing = String::from_utf8_lossy(&listed.stdout);
        assert!(!listing.contains(" blob "));
        // Git can synthesize its canonical empty tree locally; it is not a
        // downloaded repository directory tree.
        let nonempty_tree = listing
            .lines()
            .any(|line| line.contains(" tree ") && !line.ends_with(" tree 0"));
        assert_eq!(nonempty_tree, depth > 0, "{listing}");
        let checked_out = git(&["-C", tree_dir, "checkout", "main"]);
        assert!(
            checked_out.status.success(),
            "tree lazy checkout: {}",
            String::from_utf8_lossy(&checked_out.stderr)
        );
        assert_eq!(
            std::fs::read(tree_path.join("nested/hello.txt")).unwrap(),
            b"from Git push\n"
        );
        assert_eq!(
            std::fs::read(tree_path.join("foo/child.txt")).unwrap(),
            b"directory child\n"
        );
        assert!(git(&["-C", tree_dir, "fsck", "--strict"]).status.success());
    }
    let limited_path = work.path().join("limited-clone");
    let limited_dir = limited_path.to_str().unwrap();
    let limited = git(&[
        "clone",
        "--filter=blob:limit=16",
        "--no-checkout",
        &url,
        limited_dir,
    ]);
    assert!(
        limited.status.success(),
        "limited clone: {}",
        String::from_utf8_lossy(&limited.stderr)
    );
    let listed = git(&[
        "-C",
        limited_dir,
        "cat-file",
        "--batch-all-objects",
        "--batch-check",
    ]);
    assert!(listed.status.success());
    let listing = String::from_utf8(listed.stdout).unwrap();
    let sizes: Vec<_> = listing
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            fields.next()?;
            (fields.next()? == "blob").then(|| fields.next().unwrap().parse::<usize>().unwrap())
        })
        .collect();
    assert!(!sizes.is_empty());
    assert!(sizes.iter().all(|size| *size < 16));
    let historical_partial = git(&[
        "-C",
        limited_dir,
        "show",
        &format!("{}:nested/hello.txt", ancestor.trim()),
    ]);
    assert!(
        historical_partial.status.success(),
        "lazy history: {}",
        String::from_utf8_lossy(&historical_partial.stderr)
    );
    assert_eq!(historical_partial.stdout, b"first real contents\n");
    assert!(
        git(&["-C", limited_dir, "checkout", "main"])
            .status
            .success()
    );
    assert_eq!(
        std::fs::read(limited_path.join("nested/hello.txt")).unwrap(),
        b"from Git push\n"
    );
    assert!(
        git(&["-C", limited_dir, "fsck", "--strict"])
            .status
            .success()
    );
    let baseline = git(&[
        "-C",
        clone_dir,
        "push",
        "origin",
        "HEAD^:refs/tags/shallow-base",
    ]);
    assert!(
        baseline.status.success(),
        "baseline tag: {}",
        String::from_utf8_lossy(&baseline.stderr)
    );
    let excluded_path = work.path().join("excluded-shallow");
    let excluded_dir = excluded_path.to_str().unwrap();
    let excluded = git(&[
        "clone",
        "--shallow-exclude=shallow-base",
        "--single-branch",
        "--no-tags",
        &url,
        excluded_dir,
    ]);
    assert!(
        excluded.status.success(),
        "excluded clone: {}",
        String::from_utf8_lossy(&excluded.stderr)
    );
    assert_eq!(
        git(&["-C", excluded_dir, "rev-list", "--count", "HEAD"]).stdout,
        b"1\n"
    );
    assert_eq!(
        git(&["-C", excluded_dir, "rev-parse", "HEAD"]).stdout,
        pushed_head.stdout
    );
    assert_eq!(
        std::fs::read(excluded_path.join("nested/hello.txt")).unwrap(),
        b"from Git push\n"
    );
    assert!(
        git(&["-C", excluded_dir, "fsck", "--strict"])
            .status
            .success()
    );
    let restored = git(&["-C", excluded_dir, "fetch", "--unshallow", "origin", "main"]);
    assert!(
        restored.status.success(),
        "excluded unshallow: {}",
        String::from_utf8_lossy(&restored.stderr)
    );
    assert_eq!(
        git(&["-C", excluded_dir, "rev-list", "HEAD"]).stdout,
        git(&["-C", clone_dir, "rev-list", "HEAD"]).stdout
    );
    assert!(
        git(&["-C", excluded_dir, "fsck", "--strict"])
            .status
            .success()
    );
    assert!(
        git(&[
            "-C",
            clone_dir,
            "push",
            "origin",
            "HEAD^:refs/heads/shallow-base"
        ])
        .status
        .success()
    );
    let ambiguous_path = work.path().join("ambiguous-exclusion");
    let ambiguous = git(&[
        "clone",
        "--shallow-exclude=shallow-base",
        "--single-branch",
        "--no-tags",
        &url,
        ambiguous_path.to_str().unwrap(),
    ]);
    assert!(
        !ambiguous.status.success(),
        "ambiguous branch/tag exclusion must fail"
    );
    let explicit_path = work.path().join("explicit-exclusion");
    let explicit = git(&[
        "clone",
        "--shallow-exclude=refs/tags/shallow-base",
        "--single-branch",
        "--no-tags",
        &url,
        explicit_path.to_str().unwrap(),
    ]);
    assert!(
        explicit.status.success(),
        "explicit exclusion: {}",
        String::from_utf8_lossy(&explicit.stderr)
    );
    assert_eq!(
        git(&[
            "-C",
            explicit_path.to_str().unwrap(),
            "rev-list",
            "--count",
            "HEAD"
        ])
        .stdout,
        b"1\n"
    );
    assert!(
        git(&[
            "-C",
            clone_dir,
            "push",
            "origin",
            ":refs/tags/shallow-base",
            ":refs/heads/shallow-base"
        ])
        .status
        .success()
    );
    let dated_path = work.path().join("dated-shallow");
    let dated_dir = dated_path.to_str().unwrap();
    let dated = git(&[
        "clone",
        "--shallow-since=2029-01-01T00:00:00Z",
        "--single-branch",
        "--no-tags",
        &url,
        dated_dir,
    ]);
    assert!(
        dated.status.success(),
        "date shallow clone: {}",
        String::from_utf8_lossy(&dated.stderr)
    );
    assert_eq!(
        git(&["-C", dated_dir, "rev-list", "--count", "HEAD"]).stdout,
        b"1\n"
    );
    assert_eq!(
        git(&["-C", dated_dir, "rev-list", "HEAD"]).stdout,
        git(&[
            "-C",
            clone_dir,
            "rev-list",
            "--since=2029-01-01T00:00:00Z",
            "HEAD"
        ])
        .stdout
    );
    assert_eq!(
        git(&["-C", dated_dir, "rev-parse", "HEAD"]).stdout,
        pushed_head.stdout
    );
    assert_eq!(
        std::fs::read(dated_path.join("nested/hello.txt")).unwrap(),
        b"from Git push\n"
    );
    assert!(git(&["-C", dated_dir, "fsck", "--strict"]).status.success());
    let older = git(&[
        "-C",
        dated_dir,
        "fetch",
        "--shallow-since=2000-01-01T00:00:00Z",
        "origin",
        "main",
    ]);
    assert!(
        older.status.success(),
        "date deepen: {}",
        String::from_utf8_lossy(&older.stderr)
    );
    assert_eq!(
        git(&["-C", dated_dir, "rev-list", "HEAD"]).stdout,
        git(&["-C", clone_dir, "rev-list", "HEAD"]).stdout
    );
    assert_eq!(
        git(&["-C", dated_dir, "rev-parse", "--is-shallow-repository"]).stdout,
        b"false\n"
    );
    assert!(git(&["-C", dated_dir, "fsck", "--strict"]).status.success());
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_cli_protocol_v2_clone_and_incremental_fetch() {
    let test = common::setup();
    test.state()
        .store
        .create_repo(
            shardline_index::hub::HubRepoType::Model,
            "alice/git-v2",
            false,
        )
        .unwrap();
    commit_to(&test, "git-v2", b"protocol v2 first revision\n").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = test.app();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let work = tempfile::tempdir().unwrap();
    let clone_path = work.path().join("clone-v2");
    let clone_dir = clone_path.to_str().unwrap();
    let url = format!("http://{addr}/models/alice/git-v2");
    let cloned = git(&["-c", "protocol.version=2", "clone", &url, clone_dir]);
    assert!(
        cloned.status.success(),
        "protocol v2 clone: {}",
        String::from_utf8_lossy(&cloned.stderr)
    );
    let clone_trace = String::from_utf8_lossy(&cloned.stderr);
    assert!(clone_trace.contains("version 2"), "{clone_trace}");
    assert!(clone_trace.contains("command=ls-refs"), "{clone_trace}");
    assert!(clone_trace.contains("command=fetch"), "{clone_trace}");
    assert_eq!(
        std::fs::read(clone_path.join("nested/hello.txt")).unwrap(),
        b"protocol v2 first revision\n"
    );

    let object_type_path = work.path().join("object-type-filter-v2");
    let object_type_dir = object_type_path.to_str().unwrap();
    let filtered_clone = git(&[
        "-c",
        "protocol.version=2",
        "clone",
        "--no-checkout",
        "--filter=object:type=blob",
        &url,
        object_type_dir,
    ]);
    assert!(
        filtered_clone.status.success(),
        "protocol v2 object:type clone: {}",
        String::from_utf8_lossy(&filtered_clone.stderr)
    );
    let missing_tree =
        git_without_lazy_fetch(&["-C", object_type_dir, "cat-file", "-e", "HEAD^{tree}"]);
    assert!(
        !missing_tree.status.success(),
        "tree should be omitted by object:type=blob before lazy checkout; clone: {}; lookup: {}",
        String::from_utf8_lossy(&filtered_clone.stderr),
        String::from_utf8_lossy(&missing_tree.stderr)
    );
    let head_tree = git(&["-C", object_type_dir, "rev-parse", "HEAD^{tree}"]);
    assert!(head_tree.status.success());
    let object_type_checkout = git(&["-C", object_type_dir, "checkout", "--detach", "HEAD"]);
    assert!(
        object_type_checkout.status.success(),
        "lazy tree fetch for object:type clone: {}",
        String::from_utf8_lossy(&object_type_checkout.stderr)
    );
    assert_eq!(
        std::fs::read(object_type_path.join("nested/hello.txt")).unwrap(),
        b"protocol v2 first revision\n"
    );
    assert!(
        git(&["-C", object_type_dir, "fsck", "--full", "--strict"])
            .status
            .success()
    );
    for protocol_version in ["0", "1"] {
        let filtered_path = work
            .path()
            .join(format!("object-type-filter-v{protocol_version}"));
        let filtered_dir = filtered_path.to_str().unwrap();
        let legacy_filtered_clone = git(&[
            "-c",
            &format!("protocol.version={protocol_version}"),
            "clone",
            "--no-checkout",
            "--filter=object:type=blob",
            &url,
            filtered_dir,
        ]);
        assert!(
            legacy_filtered_clone.status.success(),
            "protocol v{protocol_version} object:type clone: {}",
            String::from_utf8_lossy(&legacy_filtered_clone.stderr)
        );
        let omitted_tree =
            git_without_lazy_fetch(&["-C", filtered_dir, "cat-file", "-e", "HEAD^{tree}"]);
        assert!(
            !omitted_tree.status.success(),
            "protocol v{protocol_version} should omit tree objects"
        );
        let checkout = git(&["-C", filtered_dir, "checkout", "--detach", "HEAD"]);
        assert!(
            checkout.status.success(),
            "protocol v{protocol_version} lazy tree fetch: {}",
            String::from_utf8_lossy(&checkout.stderr)
        );
        assert_eq!(
            std::fs::read(filtered_path.join("nested/hello.txt")).unwrap(),
            b"protocol v2 first revision\n"
        );
        assert!(
            git(&["-C", filtered_dir, "fsck", "--full", "--strict"])
                .status
                .success()
        );
    }

    commit_to(&test, "git-v2", b"protocol v2 second revision\n").await;
    let fetched = git(&[
        "-C",
        clone_dir,
        "-c",
        "protocol.version=2",
        "fetch",
        "origin",
    ]);
    assert!(
        fetched.status.success(),
        "protocol v2 fetch: {}",
        String::from_utf8_lossy(&fetched.stderr)
    );
    let fetch_trace = String::from_utf8_lossy(&fetched.stderr);
    assert!(fetch_trace.contains("command=ls-refs"), "{fetch_trace}");
    assert!(fetch_trace.contains("command=fetch"), "{fetch_trace}");
    let latest = git(&["-C", clone_dir, "show", "origin/main:nested/hello.txt"]);
    assert!(latest.status.success());
    assert_eq!(latest.stdout, b"protocol v2 second revision\n");
    assert!(
        git(&["-C", clone_dir, "fsck", "--full", "--strict"])
            .status
            .success()
    );

    let shallow_path = work.path().join("shallow-v2");
    let shallow_dir = shallow_path.to_str().unwrap();
    let shallow = git(&[
        "-c",
        "protocol.version=2",
        "clone",
        "--depth=1",
        "--no-tags",
        "--single-branch",
        &url,
        shallow_dir,
    ]);
    assert!(
        shallow.status.success(),
        "protocol v2 shallow clone: {}",
        String::from_utf8_lossy(&shallow.stderr)
    );
    assert_eq!(
        git(&["-C", shallow_dir, "rev-list", "--count", "HEAD"]).stdout,
        b"1\n"
    );
    let deepen = git(&[
        "-C",
        shallow_dir,
        "-c",
        "protocol.version=2",
        "fetch",
        "--deepen=1",
        "origin",
        "main",
    ]);
    assert!(
        deepen.status.success(),
        "protocol v2 deepen: {}",
        String::from_utf8_lossy(&deepen.stderr)
    );
    assert_eq!(
        git(&["-C", shallow_dir, "rev-list", "--count", "HEAD"]).stdout,
        b"2\n"
    );
    assert!(
        git(&["-C", shallow_dir, "fsck", "--strict"])
            .status
            .success()
    );

    let full_history_count = git(&["-C", clone_dir, "rev-list", "--count", "origin/main"]);
    assert!(full_history_count.status.success());
    let shallow_base = git(&["-C", clone_dir, "rev-parse", "origin/main^"]);
    assert!(shallow_base.status.success());
    let tagged_base = git(&[
        "-C",
        clone_dir,
        "-c",
        "protocol.version=2",
        "push",
        "origin",
        &format!(
            "{}:refs/tags/v2-base",
            String::from_utf8_lossy(&shallow_base.stdout).trim()
        ),
    ]);
    assert!(
        tagged_base.status.success(),
        "push protocol v2 shallow boundary: {}",
        String::from_utf8_lossy(&tagged_base.stderr)
    );
    let excluded_path = work.path().join("shallow-excluded-v2");
    let excluded = git(&[
        "-c",
        "protocol.version=2",
        "clone",
        "--shallow-exclude=v2-base",
        "--single-branch",
        "--no-tags",
        &url,
        excluded_path.to_str().unwrap(),
    ]);
    assert!(
        excluded.status.success(),
        "protocol v2 shallow exclusion: {}",
        String::from_utf8_lossy(&excluded.stderr)
    );
    let exclude_trace = String::from_utf8_lossy(&excluded.stderr);
    assert!(exclude_trace.contains("command=fetch"), "{exclude_trace}");
    assert!(exclude_trace.contains("deepen-not"), "{exclude_trace}");
    assert_eq!(
        git(&[
            "-C",
            excluded_path.to_str().unwrap(),
            "rev-list",
            "--count",
            "HEAD"
        ])
        .stdout,
        b"1\n"
    );
    assert!(
        git(&["-C", excluded_path.to_str().unwrap(), "fsck", "--strict"])
            .status
            .success()
    );
    let restored = git(&[
        "-C",
        excluded_path.to_str().unwrap(),
        "-c",
        "protocol.version=2",
        "fetch",
        "--unshallow",
        "origin",
        "main",
    ]);
    assert!(
        restored.status.success(),
        "protocol v2 unshallow: {}",
        String::from_utf8_lossy(&restored.stderr)
    );
    assert_eq!(
        git(&[
            "-C",
            excluded_path.to_str().unwrap(),
            "rev-list",
            "--count",
            "HEAD"
        ])
        .stdout,
        full_history_count.stdout
    );
    assert!(
        git(&["-C", excluded_path.to_str().unwrap(), "fsck", "--strict"])
            .status
            .success()
    );

    // Add a real Git-created commit with the test helper's 2030 committer date
    // so deepen-since must retain that tip while cutting off the 2026 Hub
    // revisions below it.
    let old_commit_times = git(&["-C", clone_dir, "log", "--format=%ct", "origin/main"]);
    assert!(old_commit_times.status.success());
    let old_commit_times = String::from_utf8(old_commit_times.stdout).unwrap();
    assert!(
        old_commit_times
            .lines()
            .all(|timestamp| timestamp.parse::<u64>().unwrap() < 1_861_920_000),
        "fixture commits must predate the 2029 cutoff: {old_commit_times}"
    );
    assert!(git(&["-C", clone_dir, "checkout", "main"]).status.success());
    let reset = git(&["-C", clone_dir, "reset", "--hard", "origin/main"]);
    assert!(reset.status.success());
    std::fs::write(clone_path.join("dated-v2.txt"), b"v2 date boundary\n").unwrap();
    assert!(
        git(&["-C", clone_dir, "add", "dated-v2.txt"])
            .status
            .success()
    );
    let dated_commit = git(&["-C", clone_dir, "commit", "-m", "v2 date boundary"]);
    assert!(
        dated_commit.status.success(),
        "create date-boundary commit: {}",
        String::from_utf8_lossy(&dated_commit.stderr)
    );
    let dated_head = git(&["-C", clone_dir, "rev-parse", "HEAD"]);
    assert!(dated_head.status.success());
    let dated_push = git(&[
        "-C",
        clone_dir,
        "-c",
        "protocol.version=2",
        "push",
        "origin",
        "HEAD:main",
    ]);
    assert!(
        dated_push.status.success(),
        "push date-boundary commit: {}",
        String::from_utf8_lossy(&dated_push.stderr)
    );
    let dated_path = work.path().join("shallow-since-v2");
    let dated = git(&[
        "-c",
        "protocol.version=2",
        "clone",
        "--shallow-since=2029-01-01T00:00:00Z",
        "--single-branch",
        "--no-tags",
        &url,
        dated_path.to_str().unwrap(),
    ]);
    assert!(
        dated.status.success(),
        "protocol v2 date-limited clone: {}",
        String::from_utf8_lossy(&dated.stderr)
    );
    let dated_trace = String::from_utf8_lossy(&dated.stderr);
    assert!(dated_trace.contains("command=fetch"), "{dated_trace}");
    assert!(dated_trace.contains("deepen-since"), "{dated_trace}");
    assert_eq!(
        git(&[
            "-C",
            dated_path.to_str().unwrap(),
            "rev-list",
            "--count",
            "HEAD"
        ])
        .stdout,
        b"1\n"
    );
    assert_eq!(
        git(&["-C", dated_path.to_str().unwrap(), "rev-parse", "HEAD"]).stdout,
        dated_head.stdout
    );
    assert_eq!(
        std::fs::read(dated_path.join("dated-v2.txt")).unwrap(),
        b"v2 date boundary\n"
    );
    assert!(
        git(&["-C", dated_path.to_str().unwrap(), "fsck", "--strict"])
            .status
            .success()
    );
    let full_history_after_date = git(&["-C", clone_dir, "rev-list", "--count", "HEAD"]);
    assert!(full_history_after_date.status.success());
    let unshallow = git(&[
        "-C",
        dated_path.to_str().unwrap(),
        "-c",
        "protocol.version=2",
        "fetch",
        "--unshallow",
        "origin",
        "main",
    ]);
    assert!(
        unshallow.status.success(),
        "protocol v2 date unshallow: {}",
        String::from_utf8_lossy(&unshallow.stderr)
    );
    assert_eq!(
        git(&[
            "-C",
            dated_path.to_str().unwrap(),
            "rev-list",
            "--count",
            "HEAD"
        ])
        .stdout,
        full_history_after_date.stdout
    );
    assert!(
        git(&["-C", dated_path.to_str().unwrap(), "fsck", "--strict"])
            .status
            .success()
    );

    let partial_path = work.path().join("partial-v2");
    let partial_dir = partial_path.to_str().unwrap();
    let partial = git(&[
        "-c",
        "protocol.version=2",
        "clone",
        "--filter=blob:none",
        "--no-checkout",
        &url,
        partial_dir,
    ]);
    assert!(
        partial.status.success(),
        "protocol v2 partial clone: {}",
        String::from_utf8_lossy(&partial.stderr)
    );
    let local_objects = git(&[
        "-C",
        partial_dir,
        "cat-file",
        "--batch-all-objects",
        "--batch-check",
    ]);
    assert!(local_objects.status.success());
    assert!(!String::from_utf8_lossy(&local_objects.stdout).contains(" blob "));
    let checkout = git(&["-C", partial_dir, "checkout", "main"]);
    assert!(
        checkout.status.success(),
        "protocol v2 lazy checkout: {}",
        String::from_utf8_lossy(&checkout.stderr)
    );
    assert_eq!(
        std::fs::read(partial_path.join("nested/hello.txt")).unwrap(),
        b"protocol v2 second revision\n"
    );
    assert!(
        git(&["-C", partial_dir, "fsck", "--strict"])
            .status
            .success()
    );

    // Protocol v2 advertises the same filter capability as v0/v1. Exercise
    // each filter with the real Git client, then prove lazy retrieval restores
    // the exact repository bytes and object connectivity.
    for (name, filter) in [
        ("v2-limited", "blob:limit=16"),
        ("v2-tree", "tree:1"),
        ("v2-combined", "combine:blob%3Alimit%3D16+tree%3A2"),
    ] {
        let filtered_path = work.path().join(name);
        let filtered_dir = filtered_path.to_str().unwrap();
        let filter_argument = format!("--filter={filter}");
        let filtered = git(&[
            "-c",
            "protocol.version=2",
            "clone",
            &filter_argument,
            "--no-checkout",
            &url,
            filtered_dir,
        ]);
        assert!(
            filtered.status.success(),
            "protocol v2 {filter} clone: {}",
            String::from_utf8_lossy(&filtered.stderr)
        );
        let listing = git(&[
            "-C",
            filtered_dir,
            "cat-file",
            "--batch-all-objects",
            "--batch-check",
        ]);
        assert!(listing.status.success());
        let listing = String::from_utf8_lossy(&listing.stdout);
        if filter.starts_with("blob:") {
            let sizes: Vec<_> = listing
                .lines()
                .filter_map(|line| {
                    let mut fields = line.split_whitespace();
                    fields.next()?;
                    (fields.next()? == "blob")
                        .then(|| fields.next().unwrap().parse::<usize>().unwrap())
                })
                .collect();
            assert!(sizes.iter().all(|size| *size < 16), "{listing}");
            assert!(
                !sizes.is_empty(),
                "small blobs should remain in the limited pack"
            );
        } else if filter.starts_with("tree:") {
            assert!(!listing.contains(" blob "), "{listing}");
        } else {
            let blob_sizes: Vec<_> = listing
                .lines()
                .filter_map(|line| {
                    let mut fields = line.split_whitespace();
                    fields.next()?;
                    (fields.next()? == "blob")
                        .then(|| fields.next().unwrap().parse::<usize>().unwrap())
                })
                .collect();
            assert!(!blob_sizes.is_empty(), "{listing}");
            assert!(blob_sizes.iter().all(|size| *size < 16), "{listing}");
            assert!(listing.contains(" tree "), "{listing}");
        }
        let checked_out = git(&["-C", filtered_dir, "checkout", "main"]);
        assert!(
            checked_out.status.success(),
            "protocol v2 lazy checkout for {filter}: {}",
            String::from_utf8_lossy(&checked_out.stderr)
        );
        assert_eq!(
            std::fs::read(filtered_path.join("nested/hello.txt")).unwrap(),
            b"protocol v2 second revision\n"
        );
        assert!(
            git(&["-C", filtered_dir, "fsck", "--strict"])
                .status
                .success()
        );
    }
    server.abort();
}
