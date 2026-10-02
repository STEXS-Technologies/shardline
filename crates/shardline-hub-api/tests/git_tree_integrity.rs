#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::let_underscore_must_use,
    clippy::shadow_unrelated,
    clippy::unwrap_in_result
)]

#[path = "support/common.rs"]
mod common;

use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use sha2::{Digest, Sha256};
use shardline_hub_api::git::{
    pack::{create_blob_object, create_commit_object, create_tree_object, generate_pack},
    pktline,
    smart_http::walk_git_tree,
};
use shardline_index::hub::HubRepoType;
use shardline_protocol::{ByteRange, ShardlineHash};
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
use std::collections::HashMap;
use tower::ServiceExt;

#[test]
fn invalid_tree_names_modes_and_duplicates_rejected() {
    let blob = create_blob_object(b"content");
    for name in [
        "", "a/b", "a\\b", ".", "..", "a\n", ".git", ".GIT", ".git.", ".git ",
    ] {
        let tree = create_tree_object(&[(0o100644, name, &blob.sha1())]);
        let objects = HashMap::from([(blob.sha1(), &blob), (tree.sha1(), &tree)]);
        assert!(
            walk_git_tree(&tree.sha1(), &objects, "").is_err(),
            "name {name:?}"
        );
    }
    for mode in [0o120000, 0o160000, 0o100600] {
        let tree = create_tree_object(&[(mode, "entry", &blob.sha1())]);
        let objects = HashMap::from([(blob.sha1(), &blob), (tree.sha1(), &tree)]);
        assert!(walk_git_tree(&tree.sha1(), &objects, "").is_err());
    }
    let tree = create_tree_object(&[
        (0o100644, "same", &blob.sha1()),
        (0o100644, "same", &blob.sha1()),
    ]);
    let objects = HashMap::from([(blob.sha1(), &blob), (tree.sha1(), &tree)]);
    assert!(walk_git_tree(&tree.sha1(), &objects, "").is_err());
}

#[test]
fn repeated_tree_dag_is_bounded_even_with_empty_leaves() {
    let mut objects = vec![create_tree_object(&[])];
    for _ in 0..30 {
        let child = objects.last().unwrap().sha1();
        objects.push(create_tree_object(&[
            (0o40000, "left", &child),
            (0o40000, "right", &child),
        ]));
    }
    let index = objects.iter().map(|obj| (obj.sha1(), obj)).collect();
    let result = walk_git_tree(&objects.last().unwrap().sha1(), &index, "");
    assert!(result.unwrap_err().to_string().contains("100000 entries"));
}

async fn push_lfs(
    test: &common::HubTestContext,
    oid: &str,
    size: u64,
    payload: Option<&[u8]>,
) -> (String, String) {
    test.state()
        .store
        .create_repo(HubRepoType::Model, "alice/model", false)
        .unwrap();
    let pointer = create_blob_object(
        format!("version https://git-lfs.github.com/spec/v1\noid sha256:{oid}\nsize {size}\n")
            .as_bytes(),
    );
    let tree = create_tree_object(&[(0o100644, "weights.bin", &pointer.sha1())]);
    let commit = create_commit_object(&tree.sha1(), None, "Alice <alice@example.com>", "LFS");
    let commit_sha = hex::encode(commit.sha1());
    let mut objects = vec![pointer, tree, commit];
    if let Some(bytes) = payload {
        objects.push(create_blob_object(bytes));
    }
    let mut request = pktline::encode_line(&format!(
        "{} {commit_sha} refs/heads/main\n",
        "0".repeat(40)
    ))
    .unwrap()
    .into_bytes();
    request.extend_from_slice(b"0000");
    request.extend_from_slice(&generate_pack(&objects).unwrap());
    let response = test
        .app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/models/alice/model/git-receive-pack")
                .body(Body::from(request))
                .unwrap(),
        )
        .await
        .unwrap();
    let text = String::from_utf8(
        to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    (text, commit_sha)
}

fn store_payload(test: &common::HubTestContext, oid: &str, payload: &[u8]) -> ObjectKey {
    let key = ObjectKey::parse(&format!("protocols/lfs/global/objects/{oid}")).unwrap();
    test.state()
        .object_store
        .put_if_absent(
            &key,
            ObjectBody::from_slice(payload),
            &ObjectIntegrity::new(
                ShardlineHash::from_bytes(*blake3::hash(payload).as_bytes()),
                payload.len() as u64,
            ),
        )
        .unwrap();
    key
}

#[tokio::test]
async fn pointer_only_push_rejects_without_persisting_tree_or_fake_content() {
    let test = common::setup();
    let oid = hex::encode(Sha256::digest(b"actual bytes"));
    let (response, commit) = push_lfs(&test, &oid, 12, None).await;
    assert!(response.contains("ng refs/heads/main"), "{response}");
    assert!(test.state().store.get_files(&commit).unwrap().is_empty());
    let key = ObjectKey::parse(&format!("protocols/lfs/global/objects/{oid}")).unwrap();
    assert!(!test.state().object_store.contains(&key).unwrap());
}

#[tokio::test]
async fn uploaded_payload_is_reused_and_corruption_or_size_mismatch_rejected() {
    let payload = b"uploaded actual bytes";
    let oid = hex::encode(Sha256::digest(payload));
    for (stored, declared, success) in [
        (payload.as_slice(), payload.len() as u64, true),
        (
            b"corrupt payload bytes".as_slice(),
            payload.len() as u64,
            false,
        ),
        (payload.as_slice(), 999, false),
    ] {
        let test = common::setup();
        let key = store_payload(&test, &oid, stored);
        let (response, commit) = push_lfs(&test, &oid, declared, None).await;
        assert_eq!(
            response.contains("ok refs/heads/main"),
            success,
            "{response}"
        );
        assert_eq!(
            test.state()
                .object_store
                .read_range(&key, ByteRange::new(0, stored.len() as u64 - 1).unwrap())
                .unwrap(),
            stored
        );
        if !success {
            assert!(test.state().store.get_files(&commit).unwrap().is_empty());
        } else {
            let files = test.state().store.get_files(&commit).unwrap();
            assert_eq!(files.len(), 1);
            assert_eq!(files[0].sha, oid);
            assert_eq!(files[0].size, payload.len() as u64);
            let mut want = pktline::encode_line(&format!("want {commit}\n"))
                .unwrap()
                .into_bytes();
            want.extend_from_slice(b"0000");
            let response = test
                .app()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/models/alice/model/git-upload-pack")
                        .body(Body::from(want))
                        .unwrap(),
                )
                .await
                .unwrap();
            let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let offset = bytes.windows(4).position(|bytes| bytes == b"PACK").unwrap();
            let objects =
                shardline_hub_api::git::smart_http::parse_pack_data(&bytes[offset..]).unwrap();
            let pointer = format!(
                "version https://git-lfs.github.com/spec/v1\noid sha256:{oid}\nsize {}\n",
                payload.len()
            );
            assert!(
                objects
                    .iter()
                    .any(|object| object.data == pointer.as_bytes())
            );
        }
    }
}

#[tokio::test]
async fn packed_actual_payload_size_must_match_pointer() {
    let test = common::setup();
    let payload = b"real";
    let oid = hex::encode(Sha256::digest(payload));
    let (response, commit) = push_lfs(&test, &oid, 100, Some(payload)).await;
    assert!(response.contains("ng refs/heads/main"), "{response}");
    assert!(test.state().store.get_files(&commit).unwrap().is_empty());
}

struct ScopedAuth;
impl shardline_server_core::AuthProvider for ScopedAuth {
    fn verify_token(
        &self,
        _: &str,
    ) -> Result<shardline_protocol::TokenClaims, shardline_server_core::AuthError> {
        let scope = shardline_protocol::RepositoryScope::new(
            shardline_protocol::RepositoryProvider::GitHub,
            "alice",
            "model",
            None,
        )
        .unwrap();
        Ok(shardline_protocol::TokenClaims::new(
            "test",
            "alice",
            shardline_protocol::TokenScope::Write,
            scope,
            u64::MAX,
        )
        .unwrap())
    }
    fn mint_token(
        &self,
        _: &shardline_protocol::TokenClaims,
    ) -> Result<String, shardline_server_core::AuthError> {
        Ok("scoped".to_owned())
    }
}

#[tokio::test]
async fn scoped_http_lfs_upload_is_reused_without_global_fallback() {
    let test = common::setup();
    let mut state = test.state().clone();
    state.auth = Some(shardline_hub_api::auth::HubAuth::new(Box::new(ScopedAuth)));
    state
        .store
        .create_repo(HubRepoType::Model, "alice/model", false)
        .unwrap();
    let payload = b"uploaded via scoped HTTP";
    let oid = hex::encode(Sha256::digest(payload));
    let pointer = create_blob_object(
        format!(
            "version https://git-lfs.github.com/spec/v1\noid sha256:{oid}\nsize {}\n",
            payload.len()
        )
        .as_bytes(),
    );
    let tree = create_tree_object(&[(0o100644, "weights.bin", &pointer.sha1())]);
    let commit = create_commit_object(&tree.sha1(), None, "Alice <alice@example.com>", "LFS");
    let commit_sha = hex::encode(commit.sha1());
    let objects = vec![pointer, tree, commit];
    let mut request = pktline::encode_line(&format!(
        "{} {commit_sha} refs/heads/main\n",
        "0".repeat(40)
    ))
    .unwrap()
    .into_bytes();
    request.extend_from_slice(b"0000");
    request.extend_from_slice(&generate_pack(&objects).unwrap());
    // A matching object in global storage must not satisfy the scoped push.
    store_payload(&test, &oid, payload);
    let response = shardline_hub_api::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/models/alice/model/git-receive-pack")
                .header("authorization", "Bearer scoped")
                .body(Body::from(request.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    let response = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    assert!(String::from_utf8_lossy(&response).contains("ng refs/heads/main"));
    let response = shardline_hub_api::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/lfs/objects/{oid}"))
                .header("authorization", "Bearer scoped")
                .body(Body::from(payload.as_slice()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());
    let response = shardline_hub_api::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/models/alice/model/git-receive-pack")
                .header("authorization", "Bearer scoped")
                .body(Body::from(request))
                .unwrap(),
        )
        .await
        .unwrap();
    let response = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    assert!(String::from_utf8_lossy(&response).contains("ok refs/heads/main"));
    let scope = shardline_protocol::RepositoryScope::new(
        shardline_protocol::RepositoryProvider::GitHub,
        "alice",
        "model",
        None,
    )
    .unwrap();
    let namespace = shardline_server_core::protocol_support::scope_namespace(Some(&scope));
    let key = ObjectKey::parse(&format!("protocols/lfs/{namespace}/objects/{oid}")).unwrap();
    assert_eq!(
        state
            .object_store
            .read_range(&key, ByteRange::new(0, payload.len() as u64 - 1).unwrap())
            .unwrap(),
        payload
    );
}

#[test]
fn malformed_or_ambiguous_lfs_pointers_rejected() {
    let valid_oid = hex::encode(Sha256::digest(b"content"));
    let version = "version https://git-lfs.github.com/spec/v1";
    for text in [
        format!("{version}\noid sha256:short\nsize 7\n"),
        format!(
            "{version}\noid sha256:{}\nsize 7\n",
            valid_oid.to_uppercase()
        ),
        format!("{version}\noid sha256:{valid_oid}\noid sha256:{valid_oid}\nsize 7\n"),
        format!("{version}\noid sha256:{valid_oid}\nsize 7\nsize 9\n"),
        format!("{version}-other\noid sha256:{valid_oid}\nsize 7\n"),
    ] {
        let blob = create_blob_object(text.as_bytes());
        let tree = create_tree_object(&[(0o100644, "weights.bin", &blob.sha1())]);
        let objects = HashMap::from([(blob.sha1(), &blob), (tree.sha1(), &tree)]);
        assert!(walk_git_tree(&tree.sha1(), &objects, "").is_err());
    }
}
