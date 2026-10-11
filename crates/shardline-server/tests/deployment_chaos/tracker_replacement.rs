//! Actual container PID replacement and normal SIGTERM shutdown.
use super::*;
use shardline_server::{
    ObjectStorageAdapter, ServerConfig,
    task_tracking::{TaskEvidenceStore, TaskRunState, deployment_fingerprint},
};
use std::{fs, num::NonZeroUsize, os::unix::fs::PermissionsExt};

struct ServerContainer(String);

impl Drop for ServerContainer {
    fn drop(&mut self) {
        let _removed = Command::new("docker")
            .args(["rm", "--force", &self.0])
            .output();
    }
}

impl ServerContainer {
    async fn start(binary: &Path, stack: &ChaosStack, root: &Path, bind: &str) -> Self {
        let name = format!(
            "{}-{}",
            stack.project,
            root.file_name().unwrap().to_string_lossy()
        );
        let container = Self(name);
        let certificate = [
            "/etc/ssl/certs/ca-certificates.crt",
            "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
        ]
        .iter()
        .find_map(|path| fs::canonicalize(path).ok())
        .expect("existing trust bundle");
        // Only the existing deployment root is writable; no tracker mount.
        fs::set_permissions(root, fs::Permissions::from_mode(0o777)).unwrap();
        let mut args = vec![
            "run".to_owned(),
            "-d".to_owned(),
            "--name".to_owned(),
            container.0.clone(),
            "--read-only".to_owned(),
            "--user".to_owned(),
            "1000:1000".to_owned(),
            "--cap-drop".to_owned(),
            "ALL".to_owned(),
            "--network".to_owned(),
            "host".to_owned(),
            "--security-opt".to_owned(),
            "no-new-privileges".to_owned(),
            "--mount".to_owned(),
            format!("type=bind,src={},dst=/shardline,readonly", binary.display()),
            "--mount".to_owned(),
            format!(
                "type=bind,src={},dst=/certificates.pem,readonly",
                certificate.display()
            ),
            "--mount".to_owned(),
            format!("type=bind,src={},dst=/var/lib/shardline", root.display()),
        ];
        let public_url = format!("http://{bind}");
        for (key, value) in [
            ("SSL_CERT_FILE", "/certificates.pem"),
            ("SHARDLINE_ROOT_DIR", "/var/lib/shardline"),
            ("SHARDLINE_BIND_ADDR", bind),
            ("SHARDLINE_PUBLIC_BASE_URL", public_url.as_str()),
            ("SHARDLINE_AUTH_PROVIDER", "local"),
            (
                "SHARDLINE_TOKEN_SIGNING_KEY",
                "0123456789abcdef0123456789abcdef",
            ),
            ("SHARDLINE_ADMIN_READ_TOKEN", ADMIN_READ_TOKEN),
            ("SHARDLINE_SERVER_FRONTENDS", "lfs"),
            ("SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER", "memory"),
            ("SHARDLINE_OBJECT_STORAGE_ADAPTER", "s3"),
            ("SHARDLINE_S3_BUCKET", OBJECT_BUCKET),
            ("SHARDLINE_S3_REGION", "us-east-1"),
            ("SHARDLINE_S3_ENDPOINT", stack.s3_endpoint.as_str()),
            ("SHARDLINE_S3_ACCESS_KEY_ID", "shardline"),
            ("SHARDLINE_S3_SECRET_ACCESS_KEY", "shardline-dev-password"),
            ("SHARDLINE_S3_ALLOW_HTTP", "true"),
            ("SHARDLINE_INDEX_POSTGRES_URL", stack.pg_url.as_str()),
        ] {
            args.extend(["-e".to_owned(), format!("{key}={value}")]);
        }
        args.extend([
            "debian:13-slim".to_owned(),
            "/shardline".to_owned(),
            "serve".to_owned(),
        ]);
        let output = docker_run_owned(args).await;
        assert!(
            output.status.success(),
            "start fixture: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        container
    }

    async fn state(&self) -> serde_json::Value {
        let output = docker_run_owned(vec![
            "inspect".to_owned(),
            "--format".to_owned(),
            "{{json .State}}".to_owned(),
            self.0.clone(),
        ])
        .await;
        assert!(output.status.success());
        serde_json::from_slice(&output.stdout).unwrap()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_tracker_container_replacement_and_graceful_shutdown() {
    let drill = "drill_tracker_container_replacement_and_graceful_shutdown";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} — current executable unavailable");
        return;
    };
    migrate_chaos_postgres(&stack.pg_url).await;
    let s3 = S3ObjectStoreConfig::new(OBJECT_BUCKET.to_owned(), "us-east-1".to_owned())
        .with_endpoint(Some(stack.s3_endpoint.clone()))
        .with_credentials(
            Some(SecretString::from_secret("shardline")),
            Some(SecretString::from_secret("shardline-dev-password")),
            None,
        )
        .with_allow_http(true);
    let config = ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/var/lib/shardline"),
        NonZeroUsize::new(CHUNK_SIZE).unwrap(),
    )
    .with_index_postgres_url(stack.pg_url.clone())
    .unwrap()
    .with_object_storage(ObjectStorageAdapter::S3, Some(s3));
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let deployment = deployment_fingerprint(&config).unwrap();
    let first_root = TempDir::new().unwrap();
    let sessions = first_root.path().join("tmp/lfs-patch");
    fs::create_dir_all(&sessions).unwrap();
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(sessions.join(".sessions.lock"))
        .unwrap();
    lock.set_permissions(fs::Permissions::from_mode(0o666))
        .unwrap();
    lock.lock().unwrap();
    let bind = format!("127.0.0.1:{}", free_tcp_port().unwrap());
    let first = ServerContainer::start(&binary, &stack, first_root.path(), &bind).await;
    wait_for(
        "embedded startup admission",
        || async {
            store
                .summaries(deployment, None, 10)
                .unwrap()
                .iter()
                .any(|task| {
                    task.operation == "lfs-startup-session-sweep"
                        && matches!(
                            task.state,
                            TaskRunState::Running | TaskRunState::RunningOrInterrupted
                        )
                })
        },
        Duration::from_secs(20),
    )
    .await;
    let interrupted = store.summaries(deployment, None, 10).unwrap().remove(0);
    // Keep the old namespace identity allocated after exit. Linux may recycle
    // its inode immediately; this drill deliberately exercises foreign namespaces.
    let host_pid = first.state().await["Pid"].as_u64().unwrap();
    let _old_namespace = fs::File::open(format!("/proc/{host_pid}/ns/pid")).unwrap();
    let before = [0_u32, 1].map(|sequence| {
        store
            .read(
                &format!("runs/{}/records/{sequence:010}", interrupted.id),
                16384,
            )
            .unwrap()
            .unwrap()
    });
    let killed = docker_run_owned(vec![
        "kill".to_owned(),
        "--signal".to_owned(),
        "KILL".to_owned(),
        first.0.clone(),
    ])
    .await;
    assert!(killed.status.success());
    wait_for(
        "first container exits",
        || async { first.state().await["Running"] == false },
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(first.state().await["ExitCode"], 137);
    lock.unlock().unwrap();

    let replacement_root = TempDir::new().unwrap();
    let replacement = ServerContainer::start(&binary, &stack, replacement_root.path(), &bind).await;
    let base = format!("http://{bind}");
    wait_for("replacement readiness", || async {
        matches!(reqwest::Client::new().get(format!("{base}/healthz")).timeout(Duration::from_secs(2)).send().await, Ok(response) if response.status().as_u16() == 200)
    }, Duration::from_secs(20)).await;
    let response = admin_get(&base, "/api/v1/tasks/maintenance", ADMIN_READ_TOKEN).await;
    assert_eq!(response.status().as_u16(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["tasks"].as_array().unwrap().len(), 2);
    let tasks = store.summaries(deployment, None, 10).unwrap();
    let completed = tasks.iter().find(|task| task.id != interrupted.id).unwrap();
    assert_eq!(completed.state, TaskRunState::Completed);
    assert_eq!(completed.owner.pid, 1);
    assert_ne!(completed.owner.namespace, interrupted.owner.namespace);
    assert!(tasks.iter().any(|task| task.id == interrupted.id
        && matches!(
            task.state,
            TaskRunState::NeedsReconciliation | TaskRunState::RunningOrInterrupted
        )));
    let completed_id = completed.id.clone();
    let completed_records = [0_u32, 1, 2].map(|sequence| {
        store
            .read(
                &format!("runs/{completed_id}/records/{sequence:010}"),
                16384,
            )
            .unwrap()
            .unwrap()
    });
    let top = docker_run_owned(vec![
        "top".to_owned(),
        replacement.0.clone(),
        "-eo".to_owned(),
        "pid,comm".to_owned(),
    ])
    .await;
    assert!(top.status.success());
    assert_eq!(
        String::from_utf8_lossy(&top.stdout).lines().count(),
        2,
        "one server process, no tracker PID"
    );
    let status = admin_get(&base, "/api/v1/status", ADMIN_READ_TOKEN).await;
    assert_eq!(status.status().as_u16(), 200);
    assert_eq!(
        status.json::<serde_json::Value>().await.unwrap()["state"],
        "ready"
    );
    assert!(
        !replacement_root
            .path()
            .join(".shardline-maintenance")
            .exists()
    );
    let stopped = docker_run_owned(vec![
        "stop".to_owned(),
        "--time".to_owned(),
        "10".to_owned(),
        replacement.0.clone(),
    ])
    .await;
    assert!(stopped.status.success());
    assert_eq!(replacement.state().await["Running"], false);
    assert_eq!(
        replacement.state().await["ExitCode"],
        0,
        "normal SIGTERM shutdown, no forced kill"
    );

    let third_root = TempDir::new().unwrap();
    let _third = ServerContainer::start(&binary, &stack, third_root.path(), &bind).await;
    wait_for(
        "fresh startup after graceful shutdown",
        || async {
            store.summaries(deployment, None, 10).unwrap().len() == 3
                && store
                    .summaries(deployment, None, 10)
                    .unwrap()
                    .iter()
                    .filter(|task| task.state == TaskRunState::Completed)
                    .count()
                    == 2
        },
        Duration::from_secs(20),
    )
    .await;
    for (sequence, bytes) in before.iter().enumerate() {
        assert_eq!(
            store
                .read(
                    &format!("runs/{}/records/{sequence:010}", interrupted.id),
                    16384
                )
                .unwrap()
                .as_ref(),
            Some(bytes)
        );
    }
    assert!(
        store
            .read(
                &format!("runs/{}/records/0000000002", interrupted.id),
                16384
            )
            .unwrap()
            .is_none(),
        "replacement never completes the old run"
    );
    for (sequence, bytes) in completed_records.iter().enumerate() {
        assert_eq!(
            store
                .read(
                    &format!("runs/{completed_id}/records/{sequence:010}"),
                    16384
                )
                .unwrap()
                .as_ref(),
            Some(bytes)
        );
    }
    assert!(!third_root.path().join(".shardline-maintenance").exists());
}
