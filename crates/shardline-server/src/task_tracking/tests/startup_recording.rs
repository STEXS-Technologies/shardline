//! Startup work remains available when optional final evidence cannot be saved.
use super::*;
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use tower::ServiceExt;

#[tokio::test]
async fn startup_sweep_preserves_work_and_readiness_when_final_recording_fails() {
    for denied_suffix in ["artifacts/*", "records/0000000002"] {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path())
            .with_server_frontends([crate::ServerFrontend::Lfs])
            .unwrap()
            .with_admin_read_token(b"recording-admin".to_vec())
            .unwrap();
        let store = TaskEvidenceStore::from_config(&config).unwrap();
        store
            .publish("runs/fixture/blobs/seed", b"original")
            .unwrap();
        let database = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
        database
            .execute_batch(&format!(
                "CREATE TRIGGER fixture_reject_final BEFORE INSERT ON shardline_task_artifacts_v1
             WHEN NEW.artifact_key GLOB '_shardline_tasks/v1/runs/*/{denied_suffix}'
             BEGIN SELECT RAISE(ABORT, 'fixture denies final recording'); END;"
            ))
            .unwrap();
        let sessions = root.path().join("tmp/lfs-patch");
        std::fs::create_dir_all(&sessions).unwrap();
        let seed_expired = |number: u32| {
            let id = format!("{number:064x}");
            let data = sessions.join(&id);
            let touched = sessions.join(format!("{id}.meta"));
            std::fs::write(&data, b"expired staging").unwrap();
            std::fs::write(&touched, b"0").unwrap();
            (data, touched)
        };
        let (data, touched) = seed_expired(1);
        let app = crate::app::router(config.clone()).await.unwrap();
        assert!(!data.exists());
        assert!(!touched.exists());
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/status")
                    .header("Authorization", "Bearer recording-admin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let status: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
        assert_eq!(status["state"], "ready");
        let deployment = deployment_fingerprint(&config).unwrap();
        let history = store.summaries(deployment, None, 10).unwrap();
        assert_eq!(history.len(), 1);
        let first = history.first().unwrap();
        let first_id = first.id.clone();
        assert_eq!(first.operation, "lfs-startup-session-sweep");
        assert!(!matches!(
            first.state,
            TaskRunState::Completed | TaskRunState::Failed
        ));
        let record = store
            .read(&format!("runs/{first_id}/records/0000000002"), 65536)
            .unwrap();
        if denied_suffix.starts_with("records/") {
            assert!(record.is_none());
        } else if let Some(bytes) = record {
            let record: penelope_core::task::TaskRecord = serde_json::from_slice(&bytes).unwrap();
            assert!(matches!(
                record.event,
                penelope_core::task::TaskExecutionEvent::FaultObserved { .. }
            ));
        }
        assert_eq!(store.prune_completed(deployment, u64::MAX).unwrap(), 0);
        drop(app);
        database
            .execute_batch("DROP TRIGGER fixture_reject_final")
            .unwrap();
        let (fresh_data, fresh_touched) = seed_expired(2);
        let fresh_app = crate::app::router(config).await.unwrap();
        assert!(!fresh_data.exists());
        assert!(!fresh_touched.exists());
        let history = store.summaries(deployment, None, 10).unwrap();
        assert_eq!(history.len(), 2);
        assert!(
            history
                .iter()
                .any(|task| task.id != first_id && task.state == TaskRunState::Completed)
        );
        assert!(history.iter().any(|task| task.id == first_id
            && !matches!(task.state, TaskRunState::Completed | TaskRunState::Failed)));
        assert_eq!(
            store
                .read("runs/fixture/blobs/seed", 16)
                .unwrap()
                .as_deref(),
            Some(b"original".as_slice())
        );
        drop(fresh_app);
    }
}
