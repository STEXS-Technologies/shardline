#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{fs, path::Path, process::Command, time::Duration};

use sha2::{Digest, Sha256};
use shardline_index::{
    LocalIndexStore, PostgresIndexStore,
    hub::{BoxedHubStore, EMPTY_HUB_REVISION, HubFileEntry, HubRepoType},
};
use shardline_protocol::{RepositoryProvider, RepositoryScope, SecretString, ShardlineHash};
use shardline_server::ServerObjectStore;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, S3ObjectStoreConfig};

#[test]
fn actual_cli_restores_verified_tree_and_rejects_stale_manifest() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.join("hub")).unwrap());
    let objects = ServerObjectStore::local(root.join("chunks")).unwrap();
    verify_recovery(root, &store, &objects, None, &[]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Docker PostgreSQL and MinIO"]
async fn actual_cli_restores_verified_tree_with_postgres_and_minio() {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()
        .expect("recovery services")
        .expect("remote recovery qualification requires Docker");
    let database = services.postgres_url().unwrap();
    let pool = sqlx::PgPool::connect(&database).await.unwrap();
    shardline_server::apply_database_migrations(&pool)
        .await
        .unwrap();
    let store = BoxedHubStore::from_store(PostgresIndexStore::new(pool.clone()));
    let prefix = services.unique_s3_key_prefix("hub-tree-recovery");
    let raw = services.s3_raw_config(Some(&prefix)).unwrap();
    let environment = vec![
        ("SHARDLINE_INDEX_POSTGRES_URL", database),
        ("SHARDLINE_OBJECT_STORAGE_ADAPTER", "s3".to_owned()),
        ("SHARDLINE_S3_BUCKET", raw.bucket.clone()),
        ("SHARDLINE_S3_REGION", raw.region.clone()),
        ("SHARDLINE_S3_ENDPOINT", raw.endpoint.clone().unwrap()),
        (
            "SHARDLINE_S3_ACCESS_KEY_ID",
            raw.access_key.clone().unwrap(),
        ),
        (
            "SHARDLINE_S3_SECRET_ACCESS_KEY",
            raw.secret_key.clone().unwrap(),
        ),
        ("SHARDLINE_S3_KEY_PREFIX", prefix),
        ("SHARDLINE_S3_ALLOW_HTTP", "true".to_owned()),
    ];
    let objects = ServerObjectStore::s3(
        S3ObjectStoreConfig::new(raw.bucket, raw.region)
            .with_endpoint(raw.endpoint)
            .with_credentials(
                raw.access_key.map(SecretString::new),
                raw.secret_key.map(SecretString::new),
                None,
            )
            .with_key_prefix(raw.key_prefix.as_deref())
            .with_allow_http(true),
    )
    .unwrap();
    tokio::task::spawn_blocking(move || {
        let temp = tempfile::tempdir().unwrap();
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "alice", "model", None).unwrap();
        verify_recovery(temp.path(), &store, &objects, Some(scope), &environment);
    })
    .await
    .unwrap();
    pool.close().await;
}

fn verify_recovery(
    root: &Path,
    store: &BoxedHubStore,
    objects: &ServerObjectStore,
    scope: Option<RepositoryScope>,
    environment: &[(&str, String)],
) {
    let legacy = "1234567890abcdef";
    store
        .create_repo(HubRepoType::Model, "alice/model", true)
        .unwrap();
    store
        .create_revision(
            "alice/model",
            Some(EMPTY_HUB_REVISION),
            legacy,
            "main",
            "legacy",
        )
        .unwrap();
    let content = b"verified original bytes";
    let hash = blake3::hash(content);
    let digest = hash.to_hex().to_string();
    let namespace = shardline_protocol_adapters::scope_namespace(scope.as_ref());
    let key = ObjectKey::parse(&format!("protocols/lfs/{namespace}/objects/{digest}")).unwrap();
    objects
        .put_if_absent(
            &key,
            ObjectBody::from_slice(content),
            &ObjectIntegrity::new(
                ShardlineHash::from_bytes(*hash.as_bytes()),
                content.len() as u64,
            ),
        )
        .unwrap();
    let file = HubFileEntry {
        path: "original.txt".to_owned(),
        size: content.len() as u64,
        sha: digest,
        is_lfs: false,
    };
    // Exercise SHA-256 verification across the command's 1 MiB scan boundary.
    let lfs_content = vec![37_u8; 2 * 1024 * 1024 + 17];
    let lfs_sha = hex::encode(Sha256::digest(&lfs_content));
    let lfs_key =
        ObjectKey::parse(&format!("protocols/lfs/{namespace}/objects/{lfs_sha}")).unwrap();
    objects
        .put_if_absent(
            &lfs_key,
            ObjectBody::from_slice(&lfs_content),
            &ObjectIntegrity::new(
                ShardlineHash::from_bytes(*blake3::hash(&lfs_content).as_bytes()),
                lfs_content.len() as u64,
            ),
        )
        .unwrap();
    let lfs_file = HubFileEntry {
        path: "weights.bin".to_owned(),
        size: lfs_content.len() as u64,
        sha: lfs_sha,
        is_lfs: true,
    };
    let manifest = shardline::HubTreeRecoveryInput {
        repo_id: "alice/model".to_owned(),
        ref_name: "main".to_owned(),
        expected_head: legacy.to_owned(),
        repository_scope: scope,
        files: vec![file.clone(), lfs_file.clone()],
    };
    let state_file = root.join("tree.json");
    fs::write(&state_file, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let config_file = root.join("empty.toml");
    fs::write(&config_file, "").unwrap();
    let command = || {
        let mut command = Command::new(
            std::env::var_os("CARGO_BIN_EXE_shardline")
                .unwrap_or_else(|| env!("CARGO_BIN_EXE_shardline").into()),
        );
        command
            .env_clear()
            .env("TMPDIR", std::env::temp_dir())
            .envs(environment.iter().map(|(name, value)| (*name, value)))
            .current_dir(root)
            .arg("--config")
            .arg(&config_file)
            .args(["repair", "hub-tree", "--root"])
            .arg(root)
            .arg("--state-file")
            .arg(&state_file);
        shardline_test_support::native_client_output(&mut command, Duration::from_secs(120))
            .unwrap()
    };
    // A valid manifest with unavailable source bytes must not advance the ref.
    let mut missing = manifest.clone();
    missing.files.first_mut().unwrap().sha = "f".repeat(64);
    fs::write(&state_file, serde_json::to_vec(&missing).unwrap()).unwrap();
    let rejected = command();
    assert!(
        !rejected.status.success(),
        "missing recovery source must fail"
    );
    assert_eq!(
        store
            .resolve_revision("alice/model", "main")
            .unwrap()
            .as_deref(),
        Some(legacy)
    );
    assert!(store.get_files(legacy).is_err());
    let mut wrong_digest = manifest.clone();
    wrong_digest.files.first_mut().unwrap().is_lfs = true;
    fs::write(&state_file, serde_json::to_vec(&wrong_digest).unwrap()).unwrap();
    assert!(
        !command().status.success(),
        "mismatched source digest must fail"
    );
    assert_eq!(
        store
            .resolve_revision("alice/model", "main")
            .unwrap()
            .as_deref(),
        Some(legacy)
    );
    fs::write(&state_file, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let output = command();
    assert!(
        output.status.success(),
        "recovery command failed: {} (captured diagnostics withheld)",
        output.status
    );
    let revision = store
        .resolve_revision("alice/model", "main")
        .unwrap()
        .unwrap();
    assert_eq!(revision.len(), 64);
    assert_eq!(store.get_files(&revision).unwrap(), vec![file, lfs_file]);
    assert!(store.get_files(legacy).is_err());
    let stale = command();
    assert!(!stale.status.success());
    assert!(String::from_utf8_lossy(&stale.stderr).contains("expected_head"));
    assert_eq!(
        store.resolve_revision("alice/model", "main").unwrap(),
        Some(revision)
    );
}
