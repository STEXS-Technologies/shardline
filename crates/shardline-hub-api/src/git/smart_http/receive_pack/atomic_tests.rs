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

async fn enable_shadow(
    state: &mut HubState,
    directory: &tempfile::TempDir,
) -> (LocalIndexStore, ServerObjectStore) {
    let store = LocalIndexStore::open(directory.path().to_owned());
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
    (store, archive)
}

async fn push_ref(
    state: &HubState,
    old_sha: &str,
    new_sha: &str,
    ref_name: &str,
    pack: &[u8],
) -> String {
    let command =
        pktline::encode_line(&format!("{old_sha} {new_sha} {ref_name}\0report-status\n")).unwrap();
    let mut request = format!("{command}0000").into_bytes();
    request.extend_from_slice(pack);
    push(state, request).await
}

fn assert_manifest_archive(
    archive: &ServerObjectStore,
    reference: &shardline_index::HubRefBatchManifestReferenceV1,
    manifest: &shardline_index::HubTreeVersionManifestV1,
) {
    let (digest, key, _) = manifest.storage_identity().unwrap();
    assert_eq!(reference.revision_sha(), manifest.revision.sha);
    assert_eq!(reference.digest_blake3(), digest);
    assert_eq!(reference.object_key(), key.as_str());
    let (bytes, _) = archive
        .read_object_if_present(&key, 8 * 1024 * 1024)
        .unwrap()
        .expect("manifest object is present");
    let stored = shardline_index::HubTreeVersionManifestV1::decode(&bytes).unwrap();
    assert_eq!(stored, *manifest);
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

    let (store, archive) = enable_shadow(&mut state, &directory).await;

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
    let pending = store
        .next_pending_hub_ref_event("atomic/http")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.event.after.manifest_refs.len(), 1);
    let reference = &pending.event.after.manifest_refs[0];
    assert_manifest_archive(&archive, reference, &manifest);
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

    let (store, archive) = enable_shadow(&mut state, &directory).await;
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
    let reference = &pending.event.after.manifest_refs[0];
    assert_manifest_archive(&archive, reference, &manifest);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shadow_non_atomic_receive_pack_reuses_existing_revision_for_new_ref() {
    let (directory, mut state) = super::super::tests::make_hub_state();
    state
        .store
        .create_repo(HubRepoType::Model, "atomic/http", false)
        .unwrap();
    let blob = create_blob_object(b"non-atomic SHADOW payload");
    let blob_sha = blob.sha1();
    let tree = create_tree_object(&[(0o100644, "file.txt", &blob_sha)]);
    let commit = create_commit_object(&tree.sha1(), None, "Test <test@example.com>", "SHADOW");
    let head = hex::encode(commit.sha1());
    let pack = generate_pack(&[blob, tree, commit]).unwrap();
    let zero = "0".repeat(40);
    let seed =
        pktline::encode_line(&format!("{zero} {head} refs/heads/seed\0report-status\n")).unwrap();
    let mut request = format!("{seed}0000").into_bytes();
    request.extend_from_slice(&pack);
    let report = push(&state, request).await;
    assert!(report.contains("ok refs/heads/seed"), "{report}");
    let original = state
        .store
        .get_revision("atomic/http", &head)
        .unwrap()
        .unwrap();

    let (store, archive) = enable_shadow(&mut state, &directory).await;
    let report = push_ref(&state, &zero, &head, "refs/heads/second", &pack).await;
    assert!(report.contains("ok refs/heads/second"), "{report}");
    assert_eq!(
        state
            .store
            .resolve_revision("atomic/http", "second")
            .unwrap()
            .as_deref(),
        Some(head.as_str())
    );
    assert_eq!(
        state.store.get_revision("atomic/http", &head).unwrap(),
        Some(original.clone())
    );
    let files = state.store.get_files(&head).unwrap();
    let manifest = shardline_index::HubTreeVersionManifestV1::new(
        "deployment-test",
        "hub",
        original,
        head.clone(),
        files,
    )
    .unwrap();
    let pending = store
        .next_pending_hub_ref_event("atomic/http")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.event.after.manifest_refs.len(), 1);
    let reference = &pending.event.after.manifest_refs[0];
    assert_manifest_archive(&archive, reference, &manifest);

    // A stale client old SHA conflicts without advancing the ref or emitting
    // another receipt, even though the requested new SHA is already present.
    let report = push_ref(&state, &"f".repeat(40), &head, "refs/heads/second", &pack).await;
    assert!(report.contains("ng refs/heads/second"), "{report}");
    assert_eq!(
        state
            .store
            .resolve_revision("atomic/http", "second")
            .unwrap()
            .as_deref(),
        Some(head.as_str())
    );
    let final_outbox_count = rusqlite::Connection::open(directory.path().join("metadata.sqlite3"))
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM shardline_hub_recovery_outbox",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    assert_eq!(final_outbox_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shadow_non_atomic_receive_pack_maps_public_git_parent_to_internal_revision() {
    let (directory, mut state) = super::super::tests::make_hub_state();
    let repo = "atomic/http";
    state
        .store
        .create_repo(HubRepoType::Model, repo, false)
        .unwrap();
    let auth = shardline_server_core::AuthorizedRepository::anonymous_full_access();
    let parent_internal = state.store.resolve_revision(repo, "main").unwrap().unwrap();
    let created = crate::routes::apply_commit(
        &state,
        repo,
        &parent_internal,
        &crate::commit::ParsedCommit {
            message: "native Hub revision".to_owned(),
            parent_commit: None,
            instructions: vec![crate::commit::CommitInstruction::InlineFile {
                path: "base.txt".to_owned(),
                content: b"base".to_vec(),
            }],
        },
        &auth,
    )
    .await
    .unwrap()
    .0;
    let hub_internal = created.commit_id;
    assert_ne!(hub_internal.len(), 40);
    let old_git =
        super::super::identity::public_commit_id_sync(&state, repo, &hub_internal, &auth).unwrap();
    assert_eq!(old_git.len(), 40);

    let (store, archive) = enable_shadow(&mut state, &directory).await;
    let blob = create_blob_object(b"next commit");
    let blob_sha = blob.sha1();
    let tree = create_tree_object(&[(0o100644, "next.txt", &blob_sha)]);
    let old_git_bytes: [u8; 20] = hex::decode(&old_git).unwrap().try_into().unwrap();
    let commit = create_commit_object(
        &tree.sha1(),
        Some(&old_git_bytes),
        "Test <test@example.com>",
        "next commit",
    );
    let new_git = hex::encode(commit.sha1());
    let pack = generate_pack(&[blob, tree, commit]).unwrap();
    let report = push_ref(&state, &old_git, &new_git, "refs/heads/main", &pack).await;
    assert!(report.contains("ok refs/heads/main"), "{report}");

    let next = state.store.get_revision(repo, &new_git).unwrap().unwrap();
    assert_eq!(next.parent_sha.as_deref(), Some(hub_internal.as_str()));
    assert_eq!(next.sha, new_git);
    assert_eq!(next.ref_name, "main");
    let pending = store
        .next_pending_hub_ref_event(repo)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.event.after.manifest_refs.len(), 1);
    let reference = &pending.event.after.manifest_refs[0];
    assert_eq!(reference.revision_sha(), new_git);
    assert_ne!(reference.revision_sha(), hub_internal);
    let manifest = shardline_index::HubTreeVersionManifestV1::new(
        "deployment-test",
        "hub",
        next.clone(),
        new_git,
        state.store.get_files(&next.sha).unwrap(),
    )
    .unwrap();
    assert_manifest_archive(&archive, reference, &manifest);
}
