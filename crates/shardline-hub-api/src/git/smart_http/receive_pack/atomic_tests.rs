//! Complete HTTP atomic-push failure and retry qualification.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::git::pack::{
    create_blob_object, create_commit_object, create_tree_object, generate_pack,
};
use axum::http::{Request, StatusCode};
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
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
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
