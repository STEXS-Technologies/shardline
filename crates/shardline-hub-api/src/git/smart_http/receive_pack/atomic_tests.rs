//! Complete HTTP atomic-push failure and retry qualification.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::git::pack::{
    create_blob_object, create_commit_object, create_tree_object, generate_pack,
};
use axum::http::{Request, StatusCode};
use shardline_index::hub::HubRepoType;
use shardline_index::{HubRecoveryMode, HubRecoveryOutboxStore, LocalIndexStore};
use shardline_server_core::ServerObjectStore;
use shardline_storage::ObjectStore;
use tower::ServiceExt;

async fn push(state: &HubState, body: Vec<u8>) -> String {
    let response = crate::hub_routes(state.clone(), true)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/models/atomic/http/git-receive-pack")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body = String::from_utf8(bytes.to_vec()).unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failures_reject_every_ref_and_retry_publishes_together() {
    let (directory, state) = super::super::tests::make_hub_state();
    state
        .store
        .create_repo(
            shardline_index::hub::HubRepoType::Model,
            "atomic/http",
            false,
        )
        .unwrap();
    let initial_count = state.store.list_refs("atomic/http").unwrap().len();
    let initial_main = state.store.resolve_revision("atomic/http", "main").unwrap();
    let blob = create_blob_object(b"durable atomic payload");
    let blob_sha = blob.sha1();
    let tree = create_tree_object(&[(0o100644, "file.txt", &blob_sha)]);
    let commit = create_commit_object(&tree.sha1(), None, "Test <test@example.com>", "Atomic");
    let head = hex::encode(commit.sha1());
    let pack = generate_pack(&[blob, tree, commit]).unwrap();
    let zero = "0".repeat(40);
    let first = pktline::encode_line(&format!(
        "{zero} {head} refs/heads/first\0report-status atomic\n"
    ))
    .unwrap();
    let make_request = |second_head: &str, payload: &[u8]| {
        let second =
            pktline::encode_line(&format!("{zero} {second_head} refs/heads/reject\n")).unwrap();
        let mut request = format!("{first}{second}0000").into_bytes();
        request.extend_from_slice(payload);
        request
    };
    for request in [
        make_request(&"f".repeat(40), &pack),
        make_request(&head, b"PACK"),
    ] {
        let report = push(&state, request).await;
        assert!(report.contains("ng refs/heads/first"), "{report}");
        assert!(report.contains("ng refs/heads/reject"), "{report}");
        assert_eq!(
            state.store.list_refs("atomic/http").unwrap().len(),
            initial_count
        );
        assert_eq!(
            state.store.resolve_revision("atomic/http", "main").unwrap(),
            initial_main
        );
        assert!(
            state
                .store
                .get_revision("atomic/http", &head)
                .unwrap()
                .is_none()
        );
    }
    let connection = rusqlite::Connection::open(directory.path().join("metadata.sqlite3")).unwrap();
    connection.execute_batch("CREATE TRIGGER atomic_http_failure BEFORE INSERT ON shardline_hub_refs WHEN NEW.ref_name = 'reject' BEGIN SELECT RAISE(ABORT, 'injected ref failure'); END;").unwrap();
    let report = push(&state, make_request(&head, &pack)).await;
    connection
        .execute_batch("DROP TRIGGER atomic_http_failure")
        .unwrap();
    assert!(report.contains("ng refs/heads/first"), "{report}");
    assert!(report.contains("ng refs/heads/reject"), "{report}");
    assert_eq!(
        state.store.list_refs("atomic/http").unwrap().len(),
        initial_count
    );
    assert_eq!(
        state.store.resolve_revision("atomic/http", "main").unwrap(),
        initial_main
    );
    assert!(
        state
            .store
            .get_revision("atomic/http", &head)
            .unwrap()
            .is_none()
    );
    let report = push(&state, make_request(&head, &pack)).await;
    assert!(report.contains("ok refs/heads/first"), "{report}");
    assert!(report.contains("ok refs/heads/reject"), "{report}");
    let refs = state.store.list_refs("atomic/http").unwrap();
    assert_eq!(refs.len(), initial_count + 2);
    assert!(
        refs.iter()
            .filter(|reference| reference.ref_name != "main")
            .all(|reference| reference.sha == head)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shadow_atomic_receive_pack_binds_one_exact_manifest_for_shared_commit_heads() {
    let (directory, mut state) = super::super::tests::make_hub_state();
    let store = LocalIndexStore::open(directory.path().to_owned());
    state
        .store
        .create_repo(HubRepoType::Model, "atomic/http", false)
        .unwrap();
    let blob = create_blob_object(b"durable atomic payload");
    let blob_sha = blob.sha1();
    let tree = create_tree_object(&[(0o100644, "file.txt", &blob_sha)]);
    let commit = create_commit_object(&tree.sha1(), None, "Test <test@example.com>", "Atomic");
    let head = hex::encode(commit.sha1());
    let pack = generate_pack(&[blob, tree, commit]).unwrap();
    let zero = "0".repeat(40);
    let first = pktline::encode_line(&format!(
        "{zero} {head} refs/heads/first\0report-status atomic\n"
    ))
    .unwrap();
    let mut request = format!("{first}0000").into_bytes();
    request.extend_from_slice(&pack);
    let report = push(&state, request).await;
    assert!(report.contains("ok refs/heads/first"), "{report}");
    let original_revision = state
        .store
        .get_revision("atomic/http", &head)
        .unwrap()
        .unwrap();
    let deleted_tag_head = head.clone();
    state
        .store
        .create_revision_if_absent(
            "atomic/http",
            None,
            &deleted_tag_head,
            "refs/tags/remove-me",
            "tag to delete",
        )
        .unwrap();

    store
        .set_hub_recovery_mode(HubRecoveryMode::Shadow, Some("deployment-test"))
        .await
        .unwrap();
    let archive = ServerObjectStore::local(directory.path().join("recovery-archive")).unwrap();
    state.recovery_archive = Some(crate::routes::HubArchiveContext {
        deployment_id: "deployment-test".to_owned(),
        resource_domain: "hub".to_owned(),
        object_store: archive.clone(),
    });

    let noop = pktline::encode_line(&format!(
        "{head} {head} refs/heads/first\0report-status atomic\n"
    ))
    .unwrap();
    let new_branch = pktline::encode_line(&format!("{zero} {head} refs/heads/second\n")).unwrap();
    let tag = pktline::encode_line(&format!("{zero} {head} refs/tags/release\n")).unwrap();
    let delete_tag =
        pktline::encode_line(&format!("{deleted_tag_head} {zero} refs/tags/remove-me\n")).unwrap();
    let request = format!("{noop}{new_branch}{tag}{delete_tag}0000").into_bytes();

    let report = push(&state, request).await;
    assert!(report.contains("ok refs/heads/second"), "{report}");
    assert!(report.contains("ok refs/tags/release"), "{report}");
    assert!(report.contains("ok refs/tags/remove-me"), "{report}");
    assert!(report.contains("ok refs/heads/first"), "{report}");

    let revision = state
        .store
        .get_revision("atomic/http", &head)
        .unwrap()
        .unwrap();
    assert_eq!(
        revision, original_revision,
        "preexisting commit metadata must be reused"
    );
    let files = state.store.get_files(&head).unwrap();
    let manifest = shardline_index::HubTreeVersionManifestV1::new(
        "deployment-test",
        "hub",
        revision.clone(),
        head.clone(),
        files,
    )
    .unwrap();
    let (digest, key, _) = manifest.storage_identity().unwrap();
    assert!(archive.contains(&key).unwrap());

    let pending = store
        .next_pending_hub_ref_event("atomic/http")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.event.after.manifest_refs.len(), 1);
    let reference = &pending.event.after.manifest_refs[0];
    assert_eq!(reference.revision_sha(), head);
    assert_eq!(reference.digest_blake3(), digest);
    assert_eq!(reference.object_key(), key.as_str());
    assert_eq!(
        revision.created_at_unix_seconds,
        manifest.revision.created_at_unix_seconds
    );
    let after_refs: Vec<_> = pending
        .event
        .after
        .refs
        .iter()
        .map(|entry| entry.ref_name.as_str())
        .collect();
    assert_eq!(
        after_refs,
        ["refs/tags/release", "refs/tags/remove-me", "second"]
    );
    assert_eq!(
        pending.event.after.refs[1].head_sha, None,
        "tag deletion remains part of the same atomic after snapshot"
    );
    assert_eq!(
        state
            .store
            .resolve_revision("atomic/http", "refs/heads/first")
            .unwrap()
            .as_deref(),
        Some(head.as_str()),
        "the no-op ref remains intact and does not add an outbox change"
    );
    assert!(
        state
            .store
            .resolve_revision("atomic/http", "refs/tags/remove-me")
            .unwrap()
            .is_none(),
        "tag deletion must be applied atomically with the ref updates"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shadow_atomic_receive_pack_tag_only_update_links_its_manifest() {
    let (directory, mut state) = super::super::tests::make_hub_state();
    let store = LocalIndexStore::open(directory.path().to_owned());
    state
        .store
        .create_repo(HubRepoType::Model, "atomic/http", false)
        .unwrap();
    let blob = create_blob_object(b"tag-only payload");
    let blob_sha = blob.sha1();
    let tree = create_tree_object(&[(0o100644, "file.txt", &blob_sha)]);
    let commit = create_commit_object(&tree.sha1(), None, "Test <test@example.com>", "Tag only");
    let head = hex::encode(commit.sha1());
    let pack = generate_pack(&[blob, tree, commit]).unwrap();
    let zero = "0".repeat(40);
    let branch = pktline::encode_line(&format!(
        "{zero} {head} refs/heads/seed\0report-status atomic\n"
    ))
    .unwrap();
    let mut initial_request = format!("{branch}0000").into_bytes();
    initial_request.extend_from_slice(&pack);
    let report = push(&state, initial_request).await;
    assert!(report.contains("ok refs/heads/seed"), "{report}");

    store
        .set_hub_recovery_mode(HubRecoveryMode::Shadow, Some("deployment-test"))
        .await
        .unwrap();
    let archive = ServerObjectStore::local(directory.path().join("recovery-archive")).unwrap();
    state.recovery_archive = Some(crate::routes::HubArchiveContext {
        deployment_id: "deployment-test".to_owned(),
        resource_domain: "hub".to_owned(),
        object_store: archive.clone(),
    });
    let tag = pktline::encode_line(&format!(
        "{zero} {head} refs/tags/release\0report-status atomic\n"
    ))
    .unwrap();
    let report = push(&state, format!("{tag}0000").into_bytes()).await;
    assert!(report.contains("ok refs/tags/release"), "{report}");

    let pending = store
        .next_pending_hub_ref_event("atomic/http")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.event.after.refs.len(), 1);
    assert_eq!(pending.event.after.refs[0].ref_name, "refs/tags/release");
    assert_eq!(pending.event.after.manifest_refs.len(), 1);
    let revision = state
        .store
        .get_revision("atomic/http", &head)
        .unwrap()
        .unwrap();
    let manifest = shardline_index::HubTreeVersionManifestV1::new(
        "deployment-test",
        "hub",
        revision,
        head.clone(),
        state.store.get_files(&head).unwrap(),
    )
    .unwrap();
    let (digest, key, _) = manifest.storage_identity().unwrap();
    let reference = &pending.event.after.manifest_refs[0];
    assert_eq!(reference.revision_sha(), head);
    assert_eq!(reference.digest_blake3(), digest);
    assert_eq!(reference.object_key(), key.as_str());
    assert!(archive.contains(&key).unwrap());
}
