//! Real candidate maintenance against metadata shared with released processes.
use super::*;

pub(super) async fn gc_before_rollback(
    binary: &Path,
    root: &Path,
    postgres: Option<&str>,
    storage_env: &[(&str, String)],
    frontends: &str,
) {
    let config = NamedTempFile::new().unwrap();
    let mut command = Command::new(binary);
    command
        .env_clear()
        .arg("--config")
        .arg(config.path())
        .args(["gc", "--mark", "--sweep", "--retention-seconds", "0"])
        .env("SHARDLINE_ROOT_DIR", root)
        .env("TMPDIR", std::env::temp_dir())
        .env("SHARDLINE_OBJECT_STORAGE_ADAPTER", "local")
        .env("SHARDLINE_SERVER_FRONTENDS", frontends);
    if let Some(url) = postgres {
        command.env("SHARDLINE_INDEX_POSTGRES_URL", url);
    }
    for (name, value) in storage_env {
        command.env(name, value);
    }
    let output = tokio::task::spawn_blocking(move || {
        shardline_test_support::native_client_output(&mut command, Duration::from_secs(90))
    })
    .await
    .expect("pre-rollback GC worker")
    .expect("pre-rollback GC process");
    assert!(
        output.status.success(),
        "candidate GC must succeed before actual predecessor reads: {}",
        output.status
    );
}
