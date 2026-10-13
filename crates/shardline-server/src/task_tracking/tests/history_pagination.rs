//! Administrative pagination must preserve damaged evidence and recording warnings.
use super::*;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use tower::ServiceExt;

async fn get(app: &Router, path: &str) -> (StatusCode, serde_json::Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(path)
                .header("Authorization", "Bearer pagination-fixture")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 65536).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn configuration(root: &std::path::Path) -> ServerConfig {
    config(root)
        .with_server_frontends([crate::ServerFrontend::Lfs])
        .unwrap()
        .with_admin_read_token(b"pagination-fixture".to_vec())
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_history_pages_preserve_corrupt_entries_and_reject_unsupported_filters() {
    let root = tempfile::tempdir().unwrap();
    let configured = configuration(root.path());
    let app = crate::app::router(configured.clone()).await.unwrap();
    let store = TaskEvidenceStore::from_config(&configured).unwrap();
    let deployment = deployment_fingerprint(&configured).unwrap();
    let mut expected: std::collections::BTreeSet<String> = store
        .summaries(deployment, None, 10)
        .unwrap()
        .into_iter()
        .map(|run| run.id)
        .collect();
    assert!(!expected.is_empty());
    for id in ["first-corrupt", "last-corrupt"] {
        store
            .publish(&format!("manifests/{id}"), b"unreadable identity")
            .unwrap();
        expected.insert(id.to_owned());
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut path = "/api/v1/tasks/maintenance?limit=1".to_owned();
    for _ in 0..=expected.len() {
        let (status, body) = get(&app, &path).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["history_notice"].is_null());
        let entries = body["tasks"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(body["page"]["returned"], 1);
        for entry in entries {
            let id = entry["id"].as_str().unwrap();
            assert!(
                seen.insert(id.to_owned()),
                "cursor repeated a history entry"
            );
            if id.ends_with("-corrupt") {
                assert_eq!(entry["state"], "corrupt");
                assert_eq!(entry["identity_verified"], false);
                for field in ["owner", "operation", "execution", "created_at"] {
                    assert!(entry.get(field).is_none());
                }
                assert!(entry["next_step"].as_str().unwrap().contains("Preserve"));
            } else {
                assert_eq!(entry["state"], "completed");
            }
        }
        let Some(cursor) = body["page"]["next_cursor"].as_str() else {
            break;
        };
        path = format!("/api/v1/tasks/maintenance?limit=1&cursor={cursor}");
    }
    assert_eq!(seen, expected);
    assert_eq!(
        get(&app, "/api/v1/tasks/maintenance?state=ready").await.0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_empty_history_warns_when_retirement_slots_are_full_without_failing_readiness() {
    let root = tempfile::tempdir().unwrap();
    let configured = configuration(root.path());
    let app = crate::app::router(configured.clone()).await.unwrap();
    let store = TaskEvidenceStore::from_config(&configured).unwrap();
    let deployment = deployment_fingerprint(&configured).unwrap();
    // Simulate cleanup interrupted after hiding the existing finished manifests.
    for run in store.summaries(deployment, None, 10).unwrap() {
        let suffix = format!("manifests/{}", run.id);
        let identity: TaskRunIdentity =
            serde_json::from_slice(&store.read(&suffix, 65536).unwrap().unwrap()).unwrap();
        assert!(store.begin_retirement(&identity).unwrap());
        store
            .delete_artifact(&TaskEvidenceStore::key(&suffix).unwrap())
            .unwrap();
    }
    // Explicit old/corrupt imports can exceed automatic admission. Occupy its
    // slots without inventing successful completion or deleting existing proof.
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    connection
        .execute(
            "WITH RECURSIVE ids(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM ids WHERE n<4095)
         INSERT INTO shardline_task_artifacts_v1(artifact_key,payload)
         SELECT '_shardline_tasks/v1/retiring/full-' || printf('%04d',n), ?1 FROM ids",
            [b"unreadable imported retirement".as_slice()],
        )
        .unwrap();
    let (status, history) = get(&app, "/api/v1/tasks/maintenance?limit=1").await;
    assert_eq!(status, StatusCode::OK);
    assert!(history["tasks"].as_array().unwrap().is_empty());
    assert_eq!(history["page"]["returned"], 0);
    assert!(history["page"]["next_cursor"].is_null());
    assert!(
        history["history_notice"]
            .as_str()
            .unwrap()
            .contains("history is full")
    );
    let (status, readiness) = get(&app, "/api/v1/status").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(readiness["state"], "ready");
}
