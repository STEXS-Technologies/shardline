//! Public library startup needs no tracker configuration or logging ownership.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::{body::Body, http::Request};
use shardline_server::{
    DeploymentMode, ServerConfig, ServerFrontend, ServerRole,
    task_tracking::{TaskEvidenceStore, TaskExecutionMode, TaskRunState, deployment_fingerprint},
};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tower::ServiceExt;

struct ApplicationLogs(Arc<AtomicUsize>);

impl tracing::Subscriber for ApplicationLogs {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if event.metadata().target() == "embedding_fixture" {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test]
async fn public_router_preserves_application_logging_and_records_fresh_startup_runs() {
    let logs = Arc::new(AtomicUsize::new(0));
    tracing::subscriber::set_global_default(ApplicationLogs(logs.clone())).unwrap();
    for role in [ServerRole::All, ServerRole::Api, ServerRole::Transfer] {
        let root = tempfile::tempdir().unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:8080".into(),
            root.path().into(),
            NonZeroUsize::new(65536).unwrap(),
        )
        .with_deployment_mode(DeploymentMode::Insecure)
        .with_server_frontends([ServerFrontend::Lfs])
        .unwrap()
        .with_server_role(role)
        .with_admin_read_token(b"owned-embedding-admin-token".to_vec())
        .unwrap();
        let deployment = deployment_fingerprint(&config).unwrap();
        let mut previous = None;
        for count in 1..=2 {
            // Introduce a new expired legacy session before each startup. A
            // second receipt alone would not prove that the sweep ran afresh.
            let sessions = root.path().join("tmp/lfs-patch");
            std::fs::create_dir_all(&sessions).unwrap();
            let oid = format!("{count:064x}");
            let data = sessions.join(&oid);
            let touched = sessions.join(format!("{oid}.meta"));
            std::fs::write(&data, b"expired legacy staging").unwrap();
            std::fs::write(&touched, b"0").unwrap();
            let router = shardline_server::app::router(config.clone()).await.unwrap();
            assert!(!data.exists(), "startup must perform its own sweep");
            assert!(!touched.exists());
            let response = router
                .oneshot(
                    Request::builder()
                        .uri("/api/v1/status")
                        .header("authorization", "Bearer owned-embedding-admin-token")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(response.status().is_success());
            let store = TaskEvidenceStore::from_config(&config).unwrap();
            let runs = store.summaries(deployment, None, 10).unwrap();
            assert_eq!(runs.len(), count);
            for run in &runs {
                assert_eq!(run.operation, "lfs-startup-session-sweep");
                assert_eq!(run.state, TaskRunState::Completed);
                assert_eq!(run.execution, TaskExecutionMode::Embedded);
                assert_eq!(run.owner.pid, std::process::id());
            }
            if let Some(id) = &previous {
                assert!(runs.iter().any(|run| &run.id == id));
                assert!(runs.iter().any(|run| &run.id != id));
            }
            previous = Some(runs.first().unwrap().id.clone());
            assert!(!root.path().join("chunks/_shardline_tasks").exists());
            assert!(!root.path().join(".shardline-maintenance").exists());
            tracing::info!(target: "embedding_fixture", "application still owns logging");
        }
    }
    assert_eq!(logs.load(Ordering::SeqCst), 6);
}
