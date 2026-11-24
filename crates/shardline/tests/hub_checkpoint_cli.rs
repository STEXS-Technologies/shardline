#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::{
    fs,
    process::{Command, Output},
    time::Duration,
};

use ed25519_dalek::{SigningKey, VerifyingKey};
use shardline_index::hub::{HubRepoType, HubStore};
use shardline_index::{
    HubRecoveryMode, HubRecoveryOutboxStore, HubRefStateCheckpointReadPin, LocalIndexStore,
    read_hub_ref_state_checkpoint,
};
use shardline_protocol::SecretString;
use shardline_storage::{ObjectKey, S3ObjectStore, S3ObjectStoreConfig};
use shardline_test_support::{DockerLocalStack, S3RawConfig, native_client_output};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const DEPLOYMENT_ID: &str = "cli-checkpoint-e2e";
const KEY_ID: &str = "cli-checkpoint-e2e-key";
const REPOSITORY_ID: &str = "cli-checkpoint/model";
const SIGNING_SEED: [u8; 32] = [0x39; 32];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(unix)]
#[ignore = "requires disposable Docker MinIO"]
async fn operator_cli_fails_offline_then_publishes_a_pinned_local_checkpoint() {
    let stack = DockerLocalStack::builder()
        .with_minio()
        .start()
        .expect("start disposable MinIO")
        .expect("Docker must be available for required qualification");
    let prefix = stack.unique_s3_key_prefix("hub-checkpoint-cli");
    let raw = stack
        .s3_raw_config(Some(&prefix))
        .expect("MinIO raw configuration");
    let archive_config = s3_config(&raw);

    let root = tempfile::tempdir().unwrap();
    let key_file = root.path().join("hub-recovery.key");
    fs::write(&key_file, SIGNING_SEED).unwrap();
    fs::set_permissions(&key_file, fs::Permissions::from_mode(0o600)).unwrap();
    let store = LocalIndexStore::new(root.path().join("hub")).unwrap();
    store
        .create_repo(HubRepoType::Model, REPOSITORY_ID, false)
        .unwrap();
    store
        .set_hub_recovery_mode(HubRecoveryMode::Shadow, Some(DEPLOYMENT_ID))
        .await
        .unwrap();
    let database_path = root.path().join("hub").join("metadata.sqlite3");
    let before = blake3::hash(&fs::read(&database_path).unwrap());

    let failed = run_cli(root.path(), &key_file, &raw, Some("http://127.0.0.1:1"));
    assert!(
        !failed.status.success(),
        "offline archive unexpectedly succeeded"
    );
    let failed_output = String::from_utf8_lossy(&failed.stderr);
    assert!(
        failed_output.contains("object store") || failed_output.contains("Hub recovery"),
        "offline failure lacked an operational diagnostic: {failed_output}"
    );
    assert_eq!(blake3::hash(&fs::read(&database_path).unwrap()), before);
    println!(
        "offline archive failure exited {}; diagnostic: {}",
        failed.status,
        failed_output.trim()
    );
    assert_eq!(
        store.hub_recovery_mode().await.unwrap(),
        (HubRecoveryMode::Shadow, Some(DEPLOYMENT_ID.to_owned()))
    );

    let succeeded = run_cli(root.path(), &key_file, &raw, None);
    assert!(
        succeeded.status.success(),
        "checkpoint CLI failed: stdout={} stderr={}",
        String::from_utf8_lossy(&succeeded.stdout),
        String::from_utf8_lossy(&succeeded.stderr)
    );
    let stdout = String::from_utf8(succeeded.stdout).unwrap();
    assert!(stdout.contains("Hub state checkpoint verified after archive read-back"));
    assert!(stdout.contains(&format!("repository_id={REPOSITORY_ID}")));
    assert!(stdout.contains(&format!("deployment_id={DEPLOYMENT_ID}")));
    assert!(stdout.contains("committed_sequence=0"));
    assert!(stdout.contains("archived_sequence=0"));
    assert_eq!(blake3::hash(&fs::read(&database_path).unwrap()), before);
    println!("successful operator output:\n{stdout}");
    println!("Hub SQLite metadata BLAKE3 unchanged across both CLI calls");

    let fields = output_fields(&stdout);
    let key = ObjectKey::parse(required(&fields, "object_key")).unwrap();
    let checksum = required(&fields, "blake3");
    let byte_length = required(&fields, "byte_length").parse::<u64>().unwrap();
    let pin = HubRefStateCheckpointReadPin::new(
        key,
        checksum,
        byte_length,
        DEPLOYMENT_ID,
        REPOSITORY_ID,
        0,
    )
    .unwrap();
    let signing_key = SigningKey::from_bytes(&SIGNING_SEED);
    let record = read_hub_ref_state_checkpoint(
        &S3ObjectStore::new(archive_config).unwrap(),
        &pin,
        &VerifyingKey::from(&signing_key),
    )
    .await
    .unwrap();
    assert_eq!(record.state.repository_id, REPOSITORY_ID);
    assert_eq!(record.state.deployment_id, DEPLOYMENT_ID);
    assert_eq!(record.state.committed_sequence, 0);
    assert_eq!(record.state.archived_sequence, 0);
}

fn run_cli(
    root: &std::path::Path,
    key_file: &std::path::Path,
    raw: &S3RawConfig,
    endpoint_override: Option<&str>,
) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_shardline"));
    command
        .arg("backup")
        .arg("hub-checkpoint")
        .arg("--root")
        .arg(root)
        .arg("--repository-id")
        .arg(REPOSITORY_ID)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("SHARDLINE_HUB_RECOVERY_MODE", "shadow")
        .env("SHARDLINE_HUB_RECOVERY_DEPLOYMENT_ID", DEPLOYMENT_ID)
        .env("SHARDLINE_HUB_RECOVERY_KEY_ID", KEY_ID)
        .env(
            "SHARDLINE_HUB_RECOVERY_SIGNING_KEY_FILE",
            key_file.as_os_str(),
        )
        .env("SHARDLINE_HUB_RECOVERY_OBJECT_STORAGE_ADAPTER", "s3")
        .env("SHARDLINE_HUB_RECOVERY_S3_BUCKET", &raw.bucket)
        .env("SHARDLINE_HUB_RECOVERY_S3_REGION", &raw.region)
        .env(
            "SHARDLINE_HUB_RECOVERY_S3_KEY_PREFIX",
            raw.key_prefix.as_deref().unwrap(),
        )
        .env(
            "SHARDLINE_HUB_RECOVERY_S3_ALLOW_HTTP",
            raw.allow_http.to_string(),
        );
    if let Some(endpoint) = endpoint_override.or(raw.endpoint.as_deref()) {
        command.env("SHARDLINE_HUB_RECOVERY_S3_ENDPOINT", endpoint);
    }
    if let Some(access_key) = &raw.access_key {
        command.env("SHARDLINE_HUB_RECOVERY_S3_ACCESS_KEY_ID", access_key);
    }
    if let Some(secret_key) = &raw.secret_key {
        command.env("SHARDLINE_HUB_RECOVERY_S3_SECRET_ACCESS_KEY", secret_key);
    }
    native_client_output(&mut command, Duration::from_secs(30)).unwrap()
}

fn s3_config(raw: &S3RawConfig) -> S3ObjectStoreConfig {
    S3ObjectStoreConfig::new(raw.bucket.clone(), raw.region.clone())
        .with_endpoint(raw.endpoint.clone())
        .with_credentials(
            raw.access_key.clone().map(SecretString::new),
            raw.secret_key.clone().map(SecretString::new),
            raw.session_token.clone().map(SecretString::new),
        )
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_allow_http(raw.allow_http)
}

fn output_fields(output: &str) -> std::collections::HashMap<&str, &str> {
    output
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect()
}

fn required<'fields>(
    fields: &'fields std::collections::HashMap<&str, &str>,
    name: &str,
) -> &'fields str {
    fields.get(name).unwrap()
}
