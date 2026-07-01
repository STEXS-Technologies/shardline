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
use shardline_hub_api::routes::HubState;
use shardline_index::hub::{BoxedHubStore, HubFileEntry, HubRepoType};
use shardline_index::LocalIndexStore;
use std::io::Read;
use std::sync::{Mutex, Once, OnceLock};
use tempfile::TempDir;
use tower::ServiceExt;

use common::{app, setup};

// ---- Helpers ----

fn create_repo_and_commit(
    repo_type: &str,
    ns: &str,
    repo: &str,
    files: Vec<HubFileEntry>,
    message: &str,
) -> String {
    let state = shardline_hub_api::state::get_for_test();
    let repo_id = format!("{repo_type}/{ns}/{repo}");
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
async fn info_refs_upload_pack_empty_repo() {
    setup();
    let repo_id = format!("models/test-{}/empty", std::process::id());
    let state = shardline_hub_api::state::get_for_test();
    let _ = state
        .store
        .create_repo(HubRepoType::Model, &repo_id, false);

    let response = app()
        .oneshot(
            Request::builder()
                .uri(format!("/models/test-{}/empty/info/refs?service=git-upload-pack", std::process::id()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let ct = response.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(ct, "application/x-git-upload-pack-advertisement");

    let body = collect_body_bytes(response).await;
    let body_str = String::from_utf8(body).unwrap();
    assert!(body_str.contains("# service=git-upload-pack"));
    // Empty repo should advertise capabilities
    assert!(body_str.contains("capabilities^{}"));
    assert!(body_str.contains("side-band-64k"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
                .uri(format!("/models/test-{uid}/with-refs/info/refs?service=git-upload-pack"))
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
    let ct = response.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(ct, "application/x-git-upload-pack-result");
    assert_eq!(
        response.headers().get("cache-control").unwrap().to_str().unwrap(),
        "no-cache"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
                inline_content: None,
            },
            HubFileEntry {
                path: "src/main.rs".to_owned(),
                size: 100,
                sha: "cafebabe".to_owned(),
                is_lfs: false,
                inline_content: None,
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
    let num_objects = u32::from_be_bytes([
        pack_data[8], pack_data[9], pack_data[10], pack_data[11],
    ]);
    // At minimum: 1 root tree + 1 sub-tree (src/) + 2 blobs + 1 commit = 5
    assert!(num_objects >= 5, "expected at least 5 objects, got {num_objects}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
                inline_content: None,
            },
            HubFileEntry {
                path: "README.md".to_owned(),
                size: 100,
                sha: "deadbeef".to_owned(),
                is_lfs: false,
                inline_content: None,
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
    assert!(blobs.len() >= 2, "expected at least 2 blobs (LFS pointer + README), got {}", blobs.len());
    let blobs: Vec<_> = objects
        .iter()
        .filter(|o| o.0 == 3) // blob type
        .collect();
    assert!(blobs.len() >= 2, "expected at least 2 blobs (LFS pointer + README), got {}", blobs.len());

    // One of the blob contents should be an LFS pointer.
    let has_lfs_pointer = blobs.iter().any(|(_, data)| {
        let s = String::from_utf8_lossy(data);
        s.contains("version https://git-lfs.github.com/spec/v1")
    });
    assert!(has_lfs_pointer, "expected LFS pointer blob in pack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
    let ct = response.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(ct, "application/x-git-receive-pack-result");

    let body = collect_body_bytes(response).await;
    let body_str = String::from_utf8(body).unwrap();
    assert!(body_str.contains("unpack ok"));
    assert!(body_str.contains("ok refs/heads/main"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
async fn info_refs_receive_pack_requires_write() {
    setup();
    let uid = std::process::id();
    let _ = create_repo_and_commit(
        "models",
        &format!("test-{uid}"),
        "rp-auth",
        vec![],
        "msg",
    );

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
    let ct = response.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(ct, "application/x-git-receive-pack-advertisement");
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
            1 | 2 | 3 | 4 => {
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
