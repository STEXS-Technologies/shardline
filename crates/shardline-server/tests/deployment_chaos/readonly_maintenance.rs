//! Real released/current CLI comparison without a writable root or /tmp.
use super::*;
use shardline_server::{
    ObjectStorageAdapter, ServerConfig,
    task_tracking::{TaskEvidenceStore, TaskExecutionMode, TaskRunState, deployment_fingerprint},
};
use std::num::NonZeroUsize;

async fn readonly_cli(
    binary: &Path,
    stack: &ChaosStack,
    command: &[&str],
    readonly_objects: bool,
) -> std::process::Output {
    let certificate = [
        "/etc/ssl/certs/ca-certificates.crt",
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    ]
    .iter()
    .find_map(|path| std::fs::canonicalize(path).ok())
    .expect("test host needs its existing certificate trust bundle");
    let (access_key, secret_key) = if readonly_objects {
        ("readonly-cli", "readonly-cli-password")
    } else {
        ("shardline", "shardline-dev-password")
    };
    let mut args = vec![
        "run".to_owned(),
        "--rm".to_owned(),
        "--read-only".to_owned(),
        "--user".to_owned(),
        "1000:1000".to_owned(),
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
    ];
    for (key, value) in [
        ("SSL_CERT_FILE", "/certificates.pem"),
        ("SHARDLINE_ROOT_DIR", "/var/lib/shardline"),
        ("SHARDLINE_AUTH_PROVIDER", "local"),
        (
            "SHARDLINE_TOKEN_SIGNING_KEY",
            "0123456789abcdef0123456789abcdef",
        ),
        ("SHARDLINE_SERVER_FRONTENDS", "s3"),
        ("SHARDLINE_OBJECT_STORAGE_ADAPTER", "s3"),
        ("SHARDLINE_S3_BUCKET", OBJECT_BUCKET),
        ("SHARDLINE_S3_REGION", "us-east-1"),
        ("SHARDLINE_S3_ENDPOINT", stack.s3_endpoint.as_str()),
        ("SHARDLINE_S3_ACCESS_KEY_ID", access_key),
        ("SHARDLINE_S3_SECRET_ACCESS_KEY", secret_key),
        ("SHARDLINE_S3_ALLOW_HTTP", "true"),
        ("SHARDLINE_INDEX_POSTGRES_URL", stack.pg_url.as_str()),
    ] {
        args.extend(["-e".to_owned(), format!("{key}={value}")]);
    }
    args.extend(["debian:13-slim".to_owned(), "/shardline".to_owned()]);
    args.extend(command.iter().map(|argument| (*argument).to_owned()));
    docker_run_owned(args).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_readonly_postgres_cli_preserves_released_commands() {
    let drill = "drill_readonly_postgres_cli_preserves_released_commands";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(current) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} — current Shardline executable is unavailable");
        return;
    };
    let Some(previous) = resolve_n_minus_one_binary(drill) else {
        return;
    };
    migrate_chaos_postgres_with_binary(&previous, &stack.pg_url).await;
    for command in [&["fsck"][..], &["index", "rebuild"][..], &["gc"][..]] {
        let baseline = readonly_cli(&previous, &stack, command, false).await;
        assert!(
            baseline.status.success(),
            "released {command:?}: {}",
            String::from_utf8_lossy(&baseline.stderr)
        );
        let upgraded = readonly_cli(&current, &stack, command, false).await;
        assert!(
            upgraded.status.success(),
            "upgraded {command:?}: {}",
            String::from_utf8_lossy(&upgraded.stderr)
        );
        assert_eq!(
            upgraded.stdout, baseline.stdout,
            "preserve {command:?} output"
        );
    }
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
    let tasks = store
        .summaries(deployment_fingerprint(&config).unwrap(), None, 10)
        .unwrap();
    assert_eq!(
        tasks.len(),
        2,
        "fsck and rebuild publish; read-only GC creates no task objects"
    );
    assert!(
        tasks
            .iter()
            .all(|task| task.state == TaskRunState::Completed
                && task.execution == TaskExecutionMode::Inline)
    );
    // Existing read-only S3 credentials must not require permission changes just
    // to inspect Postgres metadata, nor produce an invented verified outcome.
    let policy = docker_run_owned(vec![
        "compose".to_owned(), "-f".to_owned(), stack.compose_file.display().to_string(),
        "-p".to_owned(), stack.project.clone(), "run".to_owned(), "--rm".to_owned(),
        "--no-deps".to_owned(), "--entrypoint".to_owned(), "/bin/sh".to_owned(),
        "chaos-minio-init".to_owned(), "-c".to_owned(),
        "mc alias set local http://chaos-minio:9000 shardline shardline-dev-password && mc admin user add local readonly-cli readonly-cli-password && mc admin policy attach local readonly --user readonly-cli".to_owned(),
    ]).await;
    assert!(
        policy.status.success(),
        "create fixture policy: {}",
        String::from_utf8_lossy(&policy.stderr)
    );
    let baseline = readonly_cli(&previous, &stack, &["fsck"], true).await;
    let upgraded = readonly_cli(&current, &stack, &["fsck"], true).await;
    assert!(
        baseline.status.success(),
        "released read-only S3 fsck: {}",
        String::from_utf8_lossy(&baseline.stderr)
    );
    assert!(
        upgraded.status.success(),
        "upgraded read-only S3 fsck: {}",
        String::from_utf8_lossy(&upgraded.stderr)
    );
    let stdout = String::from_utf8_lossy(&upgraded.stdout);
    let stderr = String::from_utf8_lossy(&upgraded.stderr);
    assert!(
        stdout.contains("maintenance tracking unavailable")
            || stderr.contains("maintenance tracking unavailable")
    );
    assert!(stdout.contains(String::from_utf8_lossy(&baseline.stdout).as_ref()));
    assert_eq!(
        store
            .summaries(deployment_fingerprint(&config).unwrap(), None, 10)
            .unwrap()
            .len(),
        2,
        "unconfirmed execution cannot add verified completion"
    );
    eprintln!(
        "chaos({drill}): PASS — released/current fsck, rebuild and read-only GC preserve output without a writable root or /tmp; native outcomes verify"
    );
}
