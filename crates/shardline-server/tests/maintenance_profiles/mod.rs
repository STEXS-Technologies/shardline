use super::*;
mod storage_copy;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_s3_startup_records_history_in_metadata_for_every_role() {
    use shardline_server::task_tracking::{
        TaskEvidenceStore, TaskRunState, deployment_fingerprint,
    };
    let stack = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
        .expect("owned MinIO fixture requires Docker");
    for role in [ServerRole::All, ServerRole::Api, ServerRole::Transfer] {
        let root = TempDir::new().unwrap();
        let prefix = stack.unique_s3_key_prefix("sqlite-maintenance-profile");
        let raw = stack.s3_raw_config(Some(&prefix)).unwrap();
        let objects = S3ObjectStoreConfig::new(raw.bucket, raw.region)
            .with_endpoint(raw.endpoint)
            .with_credentials(
                raw.access_key.map(SecretString::new),
                raw.secret_key.map(SecretString::new),
                raw.session_token.map(SecretString::new),
            )
            .with_key_prefix(raw.key_prefix.as_deref())
            .with_allow_http(raw.allow_http);
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://localhost:8080".to_owned(),
            root.path().to_path_buf(),
            NonZeroUsize::new(65536).unwrap(),
        )
        .with_server_role(role)
        .with_server_frontends([ServerFrontend::Lfs])
        .unwrap()
        .with_deployment_mode(shardline_server::DeploymentMode::Insecure)
        .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
        .unwrap()
        .with_object_storage(ObjectStorageAdapter::S3, Some(objects));
        let router = app::router(config.clone()).await.unwrap();
        let deployment = deployment_fingerprint(&config).unwrap();
        let (evidence, _) = TaskEvidenceStore::for_inspection(&config, None)
            .await
            .unwrap();
        let tasks =
            tokio::task::spawn_blocking(move || evidence.summaries(deployment, None, 100).unwrap())
                .await
                .unwrap();
        assert_eq!(tasks.len(), 1, "missing SQLite/S3 history for {role:?}");
        assert!(
            tasks
                .iter()
                .all(|task| task.operation == "lfs-startup-session-sweep"
                    && task.state == TaskRunState::Completed
                    && task.owner.pid == std::process::id())
        );
        let metadata = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
        let count: i64 = metadata
            .query_row(
                "SELECT count(*) FROM shardline_task_artifacts_v1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(count > 0);
        let payloads = shardline_server::object_store_from_config(&config).unwrap();
        assert!(
            shardline_storage::ObjectStore::list_prefix(
                &payloads,
                &shardline_storage::ObjectPrefix::parse("_shardline_tasks/").unwrap(),
            )
            .unwrap()
            .is_empty()
        );
        assert!(!root.path().join(".shardline-maintenance").exists());
        drop(router);
    }
}
