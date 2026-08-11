#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use,
    clippy::format_push_string,
    clippy::needless_pass_by_value
)]

//! Integration tests for Git Smart HTTP protocol endpoints.
//!
//! These tests exercise the full request/response cycle through Axum,
//! verifying discovery, upload-pack (clone), receive-pack (push), and
//! HEAD reference serving.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use shardline_hub_api::git::pack::{
    GitObject, create_blob_object, create_commit_object, create_tree_object, generate_pack,
};
use shardline_hub_api::git::pktline;
use shardline_index::hub::{HubFileEntry, HubRepoType};
use shardline_storage::ObjectStore;
use std::io::Read;
use tower::ServiceExt;

use common::{app, setup};
use serial_test::serial;

// ---- Helpers ----

fn create_repo_and_commit(
    repo_type: &str,
    ns: &str,
    repo: &str,
    files: Vec<HubFileEntry>,
    message: &str,
) -> String {
    let state = common::state();
    let repo_id = format!("{ns}/{repo}");
    let rt = HubRepoType::parse_str(repo_type).unwrap();
    let _ = state.store.create_repo(rt, &repo_id, false);
    let parent_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
    let files_hash = format!("{:016x}", files.len());
    let commit_sha =
        shardline_index::hub::HubRepo::compute_commit_sha(parent_sha, message, &files_hash)
            .unwrap();
    state
        .store
        .store_files(&commit_sha, &files)
        .expect("store_files");
    state
        .store
        .create_revision(&repo_id, Some(parent_sha), &commit_sha, "main", message)
        .expect("create_revision");
    commit_sha
}

fn build_upload_pack_request(want_sha: &str) -> Vec<u8> {
    let mut body = Vec::new();
    // want line
    let want_line = format!("want {want_sha} side-band-64k thin-pack\n");
    let len = want_line.len() + 4;
    body.extend_from_slice(format!("{len:04x}").as_bytes());
    body.extend_from_slice(want_line.as_bytes());
    // flush
    body.extend_from_slice(b"0000");
    body
}

fn build_receive_pack_request(old_sha: &str, new_sha: &str, refname: &str) -> Vec<u8> {
    let mut body = Vec::new();
    let update = format!("{old_sha} {new_sha} {refname}\n");
    let len = update.len() + 4;
    body.extend_from_slice(format!("{len:04x}").as_bytes());
    body.extend_from_slice(update.as_bytes());
    // flush (no pack data for simple test)
    body.extend_from_slice(b"0000");
    body
}

/// Builds a simple Git commit (blob + tree + commit) and returns the objects
/// and the commit SHA.
fn build_simple_commit(
    file_path: &str,
    file_content: &[u8],
    message: &str,
    parent_sha: Option<&[u8; 20]>,
) -> (Vec<GitObject>, [u8; 20]) {
    let blob = create_blob_object(file_content);
    let blob_sha = blob.sha1();
    let tree = create_tree_object(&[(0o100644u32, file_path, &blob_sha)]);
    let tree_sha = tree.sha1();
    let commit = create_commit_object(
        &tree_sha,
        parent_sha,
        "Test User <test@example.com>",
        message,
    );
    let commit_sha = commit.sha1();
    (vec![blob, tree, commit], commit_sha)
}

/// Builds a Git receive-pack request body with real pack objects.
///
/// Creates a pkt-line update line and appends a valid pack file containing the
/// given objects. Returns the full request body bytes.
fn build_receive_pack_with_objects(
    old_sha_hex: &str,
    refname: &str,
    objects: &[GitObject],
    commit_sha: &[u8; 20],
) -> Vec<u8> {
    let commit_sha_hex = hex::encode(commit_sha);
    let pack_data = generate_pack(objects).expect("pack generation should not fail");
    let ref_line = format!("{old_sha_hex} {commit_sha_hex} {refname}\n");
    let mut body = Vec::new();
    body.extend_from_slice(
        pktline::encode_line(&ref_line)
            .expect("pkt-line too large")
            .as_bytes(),
    );
    body.extend_from_slice(pktline::FLUSH.as_bytes());
    body.extend_from_slice(&pack_data);
    body
}

async fn collect_body_bytes(response: axum::response::Response) -> Vec<u8> {
    response
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes()
        .to_vec()
}

// ---- Tests ----

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn health_endpoint() {
    setup();
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ok");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn info_refs_upload_pack_empty_repo() {
    setup();
    let repo_id = format!("test-{}/empty", std::process::id());
    let state = common::state();
    let _ = state.store.create_repo(HubRepoType::Model, &repo_id, false);

    let response = app()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/models/test-{}/empty/info/refs?service=git-upload-pack",
                    std::process::id()
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let ct = response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(ct, "application/x-git-upload-pack-advertisement");

    let body = collect_body_bytes(response).await;
    let body_str = String::from_utf8(body).unwrap();
    assert!(body_str.contains("# service=git-upload-pack"));
    // Empty repo should advertise capabilities
    assert!(body_str.contains("capabilities^{}"));
    assert!(body_str.contains("side-band-64k"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn info_refs_upload_pack_with_refs() {
    setup();
    let uid = std::process::id();
    let commit_sha = create_repo_and_commit(
        "models",
        &format!("test-{uid}"),
        "with-refs",
        vec![],
        "Initial commit",
    );

    let response = app()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/models/test-{uid}/with-refs/info/refs?service=git-upload-pack"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = collect_body_bytes(response).await;
    let body_str = String::from_utf8(body).unwrap();
    assert!(body_str.contains(&commit_sha));
    assert!(body_str.contains("refs/heads/main"));
    assert!(body_str.contains("HEAD"));
    assert!(body_str.contains("capabilities^{}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn info_refs_invalid_service() {
    setup();
    let uid = std::process::id();
    let _ = create_repo_and_commit(
        "models",
        &format!("test-{uid}"),
        "invalid-svc",
        vec![],
        "msg",
    );

    let response = app()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/models/test-{uid}/invalid-svc/info/refs?service=git-bogus"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn info_refs_nonexistent_repo() {
    setup();
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/models/nonexistent/nonexistent/info/refs?service=git-upload-pack")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Repo not found returns empty refs (no error, just empty advertisement).
    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let body_str = String::from_utf8(body).unwrap();
    assert!(body_str.contains("capabilities^{}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn upload_pack_empty_repo() {
    setup();
    let uid = std::process::id();
    let _ = create_repo_and_commit(
        "models",
        &format!("test-{uid}"),
        "upload-empty",
        vec![],
        "msg",
    );

    let want_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
    let req_body = build_upload_pack_request(want_sha);

    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/models/test-{uid}/upload-empty/git-upload-pack"))
                .header("content-type", "application/x-git-upload-pack-request")
                .body(Body::from(req_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let ct = response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(ct, "application/x-git-upload-pack-result");
    assert_eq!(
        response
            .headers()
            .get("cache-control")
            .unwrap()
            .to_str()
            .unwrap(),
        "no-cache"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn upload_pack_with_files() {
    setup();
    let uid = std::process::id();
    let commit_sha = create_repo_and_commit(
        "models",
        &format!("test-{uid}"),
        "upload-files",
        vec![
            HubFileEntry {
                path: "README.md".to_owned(),
                size: 13,
                sha: "deadbeef".to_owned(),
                is_lfs: false,
            },
            HubFileEntry {
                path: "src/main.rs".to_owned(),
                size: 100,
                sha: "cafebabe".to_owned(),
                is_lfs: false,
            },
        ],
        "Add files",
    );

    let req_body = build_upload_pack_request(&commit_sha);

    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/models/test-{uid}/upload-files/git-upload-pack"))
                .header("content-type", "application/x-git-upload-pack-request")
                .body(Body::from(req_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = collect_body_bytes(response).await;
    // The response is sideband-multiplexed. Extract the pack data.
    let (pack_data, _messages) = shardline_hub_api::git::pktline::decode_sideband(&body);
    assert!(!pack_data.is_empty(), "pack data should not be empty");

    // Verify it's a valid pack file.
    assert_eq!(&pack_data[0..4], b"PACK");
    let version = u32::from_be_bytes([pack_data[4], pack_data[5], pack_data[6], pack_data[7]]);
    assert_eq!(version, 2);
    let num_objects =
        u32::from_be_bytes([pack_data[8], pack_data[9], pack_data[10], pack_data[11]]);
    // At minimum: 1 root tree + 1 sub-tree (src/) + 2 blobs + 1 commit = 5
    assert!(
        num_objects >= 5,
        "expected at least 5 objects, got {num_objects}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn upload_pack_with_lfs_files() {
    setup();
    let uid = std::process::id();
    let commit_sha = create_repo_and_commit(
        "models",
        &format!("test-{uid}"),
        "upload-lfs",
        vec![
            HubFileEntry {
                path: "model.bin".to_owned(),
                size: 1024,
                sha: "lfs-oid-123".to_owned(),
                is_lfs: true,
            },
            HubFileEntry {
                path: "README.md".to_owned(),
                size: 100,
                sha: "deadbeef".to_owned(),
                is_lfs: false,
            },
        ],
        "Add LFS file",
    );

    let req_body = build_upload_pack_request(&commit_sha);

    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/models/test-{uid}/upload-lfs/git-upload-pack"))
                .header("content-type", "application/x-git-upload-pack-request")
                .body(Body::from(req_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = collect_body_bytes(response).await;
    let (pack_data, _messages) = shardline_hub_api::git::pktline::decode_sideband(&body);
    assert!(!pack_data.is_empty());

    // Verify it's a valid pack file.
    assert_eq!(&pack_data[0..4], b"PACK");
    let version = u32::from_be_bytes([pack_data[4], pack_data[5], pack_data[6], pack_data[7]]);
    assert_eq!(version, 2);
    // Parse the pack to verify it has LFS pointer blobs.
    let objects = parse_pack_objects(&pack_data);
    let blobs: Vec<_> = objects
        .iter()
        .filter(|o| o.0 == 3) // blob type
        .collect();
    assert!(
        blobs.len() >= 2,
        "expected at least 2 blobs (LFS pointer + README), got {}",
        blobs.len()
    );
    let blobs: Vec<_> = objects
        .iter()
        .filter(|o| o.0 == 3) // blob type
        .collect();
    assert!(
        blobs.len() >= 2,
        "expected at least 2 blobs (LFS pointer + README), got {}",
        blobs.len()
    );

    // One of the blob contents should be an LFS pointer.
    let has_lfs_pointer = blobs.iter().any(|(_, data)| {
        let s = String::from_utf8_lossy(data);
        s.contains("version https://git-lfs.github.com/spec/v1")
    });
    assert!(has_lfs_pointer, "expected LFS pointer blob in pack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn receive_pack_push() {
    setup();
    let uid = std::process::id();
    let _ = create_repo_and_commit(
        "models",
        &format!("test-{uid}"),
        "receive-push",
        vec![],
        "Initial",
    );

    let old_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
    let new_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let req_body = build_receive_pack_request(old_sha, new_sha, "refs/heads/main");

    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/models/test-{uid}/receive-push/git-receive-pack"))
                .header("content-type", "application/x-git-receive-pack")
                .body(Body::from(req_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let ct = response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(ct, "application/x-git-receive-pack-result");

    let body = collect_body_bytes(response).await;
    let body_str = String::from_utf8(body).unwrap();
    assert!(body_str.contains("unpack ok"));
    // Pushes are rejected because Git object storage is not implemented.
    assert!(
        body_str.contains("ng refs/heads/main"),
        "expected push rejection, got: {body_str}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn receive_pack_empty_update() {
    setup();
    let uid = std::process::id();
    let _ = create_repo_and_commit(
        "models",
        &format!("test-{uid}"),
        "receive-empty",
        vec![],
        "Initial",
    );

    // Build request with no updates (just a flush).
    let req_body = b"0000";

    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/models/test-{uid}/receive-empty/git-receive-pack"))
                .header("content-type", "application/x-git-receive-pack")
                .body(Body::from(req_body.as_slice()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = collect_body_bytes(response).await;
    let body_str = String::from_utf8(body).unwrap();
    assert!(body_str.contains("unpack ok"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn git_head_endpoint() {
    setup();
    let uid = std::process::id();
    let commit_sha = create_repo_and_commit(
        "models",
        &format!("test-{uid}"),
        "head-test",
        vec![],
        "Initial",
    );

    let response = app()
        .oneshot(
            Request::builder()
                .uri(format!("/models/test-{uid}/head-test/HEAD"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = collect_body_bytes(response).await;
    let body_str = String::from_utf8(body).unwrap();
    assert!(body_str.contains("ref: refs/heads/main"));
    assert!(body_str.contains(&commit_sha));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn git_head_nonexistent_repo() {
    setup();
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/models/noone/nothing/HEAD")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Git HEAD for a nonexistent repo returns the zero SHA (not 404).
    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let body_str = String::from_utf8(body).unwrap();
    assert!(body_str.contains("0000000000000000000000000000000000000000"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn info_refs_receive_pack_requires_write() {
    setup();
    let uid = std::process::id();
    let _ = create_repo_and_commit("models", &format!("test-{uid}"), "rp-auth", vec![], "msg");

    // receive-pack discovery should also work (no auth configured, so always allowed).
    let response = app()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/models/test-{uid}/rp-auth/info/refs?service=git-receive-pack"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let ct = response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(ct, "application/x-git-receive-pack-advertisement");
}

// ---- Force push handling ----

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn receive_pack_rejects_non_fast_forward_push() {
    setup();
    let uid = std::process::id();
    let repo_id = format!("test-{}/ff-reject", uid);
    let _ = create_repo_and_commit(
        "models",
        &format!("test-{uid}"),
        "ff-reject",
        vec![],
        "Initial",
    );

    let null_sha = "0000000000000000000000000000000000000000";

    // Step 1: Push commit_A to refs/heads/main (old=0000).
    let (objects_a, commit_a_sha) =
        build_simple_commit("file_a.txt", b"content A", "Commit A", None);
    let body_a =
        build_receive_pack_with_objects(null_sha, "refs/heads/main", &objects_a, &commit_a_sha);

    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/models/test-{uid}/ff-reject/git-receive-pack"))
                .header("content-type", "application/x-git-receive-pack")
                .body(Body::from(body_a))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let body_str = String::from_utf8(body).unwrap();
    assert!(
        body_str.contains("ok refs/heads/main"),
        "first push should succeed: {body_str}"
    );

    // Verify refs/heads/main now points to commit_A via the store.
    let state = common::state();
    let current = state
        .store
        .resolve_revision(&repo_id, "refs/heads/main")
        .expect("resolve_revision should not error");
    assert_eq!(
        current.as_deref(),
        Some(hex::encode(commit_a_sha).as_str()),
        "refs/heads/main should point to commit_A after first push"
    );

    // Step 2: Push commit_B with stale old_sha (= commit_A).
    // Main is now at commit_A, so using old=commit_A and creating a
    // DIFFERENT commit_B means we're trying to replace commit_A with
    // commit_B. This IS a fast-forward (commit_A → commit_B).
    // To trigger non-fast-forward, we need old_sha that DOESN'T match
    // the current ref. Use old=<some wrong sha>.
    let (objects_b, commit_b_sha) =
        build_simple_commit("file_b.txt", b"content B", "Commit B", None);

    // Use a bogus old_sha that doesn't match the current ref.
    // refs/heads/main is at commit_A, but we claim it's at all-zeros.
    // Since all-zeros means "create new ref" and the ref already exists,
    // the non-fast-forward check skips (old=0000), BUT the create_revision
    // CAS check catches it because default_branch != parent.
    //
    // Instead, use old=<commit_B_sha> — a SHA that is NOT the current ref value.
    // refs/heads/main is at commit_A, we claim it's at commit_B.
    let body_b = build_receive_pack_with_objects(
        &hex::encode(commit_b_sha), // wrong — main is at commit_A, not commit_B
        "refs/heads/main",
        &objects_b,
        &commit_b_sha,
    );

    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/models/test-{uid}/ff-reject/git-receive-pack"))
                .header("content-type", "application/x-git-receive-pack")
                .body(Body::from(body_b))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let body_str = String::from_utf8(body).unwrap();
    assert!(
        body_str.contains("non-fast-forward"),
        "push with wrong old_sha should be rejected as non-fast-forward: {body_str}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn receive_pack_deletes_branch_without_removing_commit_history() {
    setup();
    let uid = std::process::id();
    let repo_id = format!("test-{}/delete-branch", uid);
    let _ = create_repo_and_commit(
        "models",
        &format!("test-{uid}"),
        "delete-branch",
        vec![],
        "Initial",
    );
    let null_sha = "0000000000000000000000000000000000000000";

    let (objects, commit_sha) =
        build_simple_commit("feature.txt", b"feature content", "Feature", None);
    let commit_sha_hex = hex::encode(commit_sha);
    let create_body =
        build_receive_pack_with_objects(null_sha, "refs/heads/feature", &objects, &commit_sha);
    let create_response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/models/test-{uid}/delete-branch/git-receive-pack"))
                .header("content-type", "application/x-git-receive-pack")
                .body(Body::from(create_body))
                .unwrap(),
        )
        .await
        .unwrap();
    let create_body = String::from_utf8(collect_body_bytes(create_response).await).unwrap();
    assert!(
        create_body.contains("ok refs/heads/feature"),
        "feature push should succeed: {create_body}"
    );

    let delete_body = build_receive_pack_request(&commit_sha_hex, null_sha, "refs/heads/feature");
    let delete_response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/models/test-{uid}/delete-branch/git-receive-pack"))
                .header("content-type", "application/x-git-receive-pack")
                .body(Body::from(delete_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete_response.status(), StatusCode::OK);
    let delete_body = String::from_utf8(collect_body_bytes(delete_response).await).unwrap();
    assert!(
        delete_body.contains("ok refs/heads/feature"),
        "branch deletion should succeed: {delete_body}"
    );

    let state = common::state();
    assert_eq!(
        state
            .store
            .resolve_revision(&repo_id, "refs/heads/feature")
            .unwrap(),
        None,
        "deleted branch must no longer resolve"
    );
    assert_eq!(
        state
            .store
            .resolve_revision(&repo_id, &commit_sha_hex)
            .unwrap()
            .as_deref(),
        Some(commit_sha_hex.as_str()),
        "deleting a ref must retain immutable commit history"
    );

    let refs_response = app()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/models/test-{uid}/delete-branch/info/refs?service=git-upload-pack"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let refs_body = String::from_utf8(collect_body_bytes(refs_response).await).unwrap();
    assert!(
        !refs_body.contains("refs/heads/feature"),
        "deleted branch must not be advertised: {refs_body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn receive_pack_rejects_stale_or_default_branch_deletion() {
    setup();
    let uid = std::process::id();
    let repo_id = format!("test-{}/delete-protection", uid);
    let _ = create_repo_and_commit(
        "models",
        &format!("test-{uid}"),
        "delete-protection",
        vec![],
        "Initial",
    );
    let null_sha = "0000000000000000000000000000000000000000";

    let (objects, commit_sha) =
        build_simple_commit("feature.txt", b"feature content", "Feature", None);
    let commit_sha_hex = hex::encode(commit_sha);
    let create_body =
        build_receive_pack_with_objects(null_sha, "refs/heads/feature", &objects, &commit_sha);
    let _ = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/models/test-{uid}/delete-protection/git-receive-pack"
                ))
                .body(Body::from(create_body))
                .unwrap(),
        )
        .await
        .unwrap();

    let stale_delete = build_receive_pack_request(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        null_sha,
        "refs/heads/feature",
    );
    let stale_response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/models/test-{uid}/delete-protection/git-receive-pack"
                ))
                .body(Body::from(stale_delete))
                .unwrap(),
        )
        .await
        .unwrap();
    let stale_body = String::from_utf8(collect_body_bytes(stale_response).await).unwrap();
    assert!(
        stale_body.contains("ng refs/heads/feature"),
        "stale deletion must be rejected: {stale_body}"
    );
    assert_eq!(
        common::state()
            .store
            .resolve_revision(&repo_id, "refs/heads/feature")
            .unwrap(),
        Some(commit_sha_hex.clone()),
        "stale deletion must leave the ref intact"
    );

    let current_main = common::state()
        .store
        .resolve_revision(&repo_id, "main")
        .unwrap()
        .expect("main ref exists");
    let default_delete = build_receive_pack_request(&current_main, null_sha, "refs/heads/main");
    let default_response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/models/test-{uid}/delete-protection/git-receive-pack"
                ))
                .body(Body::from(default_delete))
                .unwrap(),
        )
        .await
        .unwrap();
    let default_body = String::from_utf8(collect_body_bytes(default_response).await).unwrap();
    assert!(
        default_body.contains("ng refs/heads/main"),
        "default branch deletion must be rejected: {default_body}"
    );
}

// ---- Git tags ----

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn receive_pack_pushes_tag() {
    setup();
    let uid = std::process::id();
    let _ = create_repo_and_commit(
        "models",
        &format!("test-{uid}"),
        "tag-test",
        vec![],
        "Initial",
    );

    let null_sha = "0000000000000000000000000000000000000000";

    // Step 1: Push a commit to refs/heads/main.
    let (objects_a, commit_a_sha) =
        build_simple_commit("README.md", b"# Tag Test", "Add README", None);
    let body_a =
        build_receive_pack_with_objects(null_sha, "refs/heads/main", &objects_a, &commit_a_sha);

    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/models/test-{uid}/tag-test/git-receive-pack"))
                .header("content-type", "application/x-git-receive-pack")
                .body(Body::from(body_a))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let body_str = String::from_utf8(body).unwrap();
    assert!(
        body_str.contains("ok refs/heads/main"),
        "commit push should succeed: {body_str}"
    );

    // Step 2: Push a lightweight tag refs/tags/v1.0.
    // The current store uses (repo_id, sha) as primary key, so the tag
    // must use a unique SHA. We create a separate commit for the tag.
    let (tag_objects, tag_commit_sha) =
        build_simple_commit("tagged.txt", b"tagged content", "Tag v1.0", None);

    let tag_ref_line = format!(
        "{} {} refs/tags/v1.0\n",
        null_sha,
        hex::encode(tag_commit_sha),
    );
    let pack_data = generate_pack(&tag_objects).expect("pack generation");
    let mut tag_body = Vec::new();
    tag_body.extend_from_slice(
        pktline::encode_line(&tag_ref_line)
            .expect("pkt-line too large")
            .as_bytes(),
    );
    tag_body.extend_from_slice(pktline::FLUSH.as_bytes());
    tag_body.extend_from_slice(&pack_data);

    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/models/test-{uid}/tag-test/git-receive-pack"))
                .header("content-type", "application/x-git-receive-pack")
                .body(Body::from(tag_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let body_str = String::from_utf8(body).unwrap();
    assert!(
        body_str.contains("ok refs/tags/v1.0"),
        "tag push should succeed: {body_str}"
    );

    // Step 3: Verify info/refs advertises both refs/heads/main and refs/tags/v1.0.
    let response = app()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/models/test-{uid}/tag-test/info/refs?service=git-upload-pack"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let body_str = String::from_utf8(body).unwrap();
    assert!(
        body_str.contains("refs/heads/main"),
        "info/refs should advertise refs/heads/main: {body_str}"
    );
    assert!(
        body_str.contains("refs/tags/v1.0"),
        "info/refs should advertise the tag: {body_str}"
    );

    // Step 4: Verify the tag revision is stored in the store.
    let state = common::state();
    let repo_id = format!("test-{}/tag-test", uid);
    let tag_sha = state
        .store
        .resolve_revision(&repo_id, "refs/tags/v1.0")
        .expect("resolve_revision should not error");
    assert!(
        tag_sha.is_some(),
        "tag revision should be stored in the store"
    );
}

// ---- LFS push workflow ----

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn receive_pack_stores_lfs_objects() {
    setup();
    let uid = std::process::id();
    let _ = create_repo_and_commit(
        "models",
        &format!("test-{uid}"),
        "lfs-push",
        vec![],
        "Initial",
    );

    // Build a commit containing an LFS pointer blob.
    let lfs_oid = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    let lfs_pointer =
        format!("version https://git-lfs.github.com/spec/v1\noid sha256:{lfs_oid}\nsize 1234\n");

    let lfs_blob = create_blob_object(lfs_pointer.as_bytes());
    let lfs_blob_sha = lfs_blob.sha1();

    // Also include a normal blob so the tree has content.
    let readme_blob = create_blob_object(b"# LFS Test");
    let readme_blob_sha = readme_blob.sha1();

    // Build a tree with both files.
    let tree = create_tree_object(&[
        (0o100644u32, "model.bin", &lfs_blob_sha),
        (0o100644, "README.md", &readme_blob_sha),
    ]);
    let tree_sha = tree.sha1();

    let commit = create_commit_object(
        &tree_sha,
        None,
        "Test User <test@example.com>",
        "Add LFS file",
    );
    let commit_sha = commit.sha1();

    let objects = vec![lfs_blob, readme_blob, tree, commit];
    let null_sha = "0000000000000000000000000000000000000000";
    let body = build_receive_pack_with_objects(null_sha, "refs/heads/main", &objects, &commit_sha);

    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/models/test-{uid}/lfs-push/git-receive-pack"))
                .header("content-type", "application/x-git-receive-pack")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let body_str = String::from_utf8(body).unwrap();
    assert!(
        body_str.contains("ok refs/heads/main"),
        "LFS push should succeed: {body_str}"
    );

    // Verify the LFS object was stored via ObjectStore.
    let state = common::state();
    // receive_pack now stores LFS objects under the repository namespace
    // (global, since the common test state has no auth configured).
    let key =
        shardline_storage::ObjectKey::parse(&format!("protocols/lfs/global/objects/{lfs_oid}"))
            .unwrap();
    assert!(
        state.object_store.contains(&key).unwrap(),
        "LFS object should be stored after push"
    );

    // Verify the stored data matches the pointer blob content.
    let meta = state.object_store.metadata(&key).unwrap().unwrap();
    let range_end = meta.length().saturating_sub(1);
    let range = shardline_protocol::ByteRange::new(0, range_end).unwrap();
    let data = state.object_store.read_range(&key, range).unwrap();
    let data_str = String::from_utf8_lossy(&data);
    assert!(
        data_str.contains("version https://git-lfs.github.com/spec/v1"),
        "stored LFS object should be the pointer blob content: {data_str}"
    );
    assert!(
        data_str.contains(lfs_oid),
        "stored LFS object should contain the OID: {data_str}"
    );
}

// ---- Pack parsing helper ----

/// Parses a raw Git pack file and returns (type, decompressed_data) for each object.
fn parse_pack_objects(data: &[u8]) -> Vec<(u8, Vec<u8>)> {
    if data.len() < 12 || &data[0..4] != b"PACK" {
        return Vec::new();
    }
    let version = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    let num_objects = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
    if version != 2 {
        return Vec::new();
    }

    let mut objects = Vec::new();
    let mut pos = 12;

    for _ in 0..num_objects {
        if pos >= data.len() {
            break;
        }
        let byte = data[pos];
        pos += 1;

        let obj_type = (byte >> 4) & 0x07;
        let mut current = byte;
        while current & 0x80 != 0 && pos < data.len() {
            current = data[pos];
            pos += 1;
        }

        match obj_type {
            1..=4 => {
                let mut decoder = flate2::read::ZlibDecoder::new(&data[pos..]);
                let mut output = Vec::new();
                if decoder.read_to_end(&mut output).is_ok() {
                    pos += decoder.total_in() as usize;
                    objects.push((obj_type, output));
                } else {
                    break;
                }
            }
            6 => {
                // OFS_DELTA — skip
                let mut c = data[pos];
                pos += 1;
                while c & 0x80 != 0 && pos < data.len() {
                    c = data[pos];
                    pos += 1;
                }
                let mut decoder = flate2::read::ZlibDecoder::new(&data[pos..]);
                let mut buf = Vec::new();
                if decoder.read_to_end(&mut buf).is_ok() {
                    pos += decoder.total_in() as usize;
                }
            }
            7 => {
                // REF_DELTA — skip
                if pos + 20 <= data.len() {
                    pos += 20;
                }
                let mut decoder = flate2::read::ZlibDecoder::new(&data[pos..]);
                let mut buf = Vec::new();
                if decoder.read_to_end(&mut buf).is_ok() {
                    pos += decoder.total_in() as usize;
                }
            }
            _ => break,
        }
    }

    objects
}
