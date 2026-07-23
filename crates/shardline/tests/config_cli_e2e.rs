#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{env::var, fs, num::NonZeroUsize, process::Command};

use shardline_server::LocalBackend;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_check_uses_explicit_config_path_with_spaces() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().join("runtime root");
    LocalBackend::new(
        root.clone(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::MIN,
    )
    .await
    .unwrap();

    let config_dir = workspace.path().join("config directory");
    fs::create_dir(&config_dir).unwrap();
    let config = config_dir.join("active config.toml");
    let signing_key = workspace.path().join("token-signing-key");
    fs::write(&signing_key, b"0123456789abcdef0123456789abcdef").unwrap();
    fs::write(
        &config,
        format!(
            "[server]\nroot_dir = {:?}\nbind_addr = \"127.0.0.1:9911\"\n\n[auth]\ntoken_signing_key_path = {:?}\n",
            root, signing_key
        ),
    )
    .unwrap();

    let output = Command::new(shardline_binary())
        .args(["--config", config.to_str().unwrap(), "config", "check"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "config check failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("status: ok"));
}

#[test]
fn invalid_explicit_config_fails_through_cli_runtime() {
    let workspace = tempfile::tempdir().unwrap();
    let config = workspace.path().join("invalid config.toml");
    fs::write(&config, "[[[not valid toml]]]").unwrap();

    let output = Command::new(shardline_binary())
        .args(["--config", config.to_str().unwrap(), "config", "check"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("TOML parse error"));
}

#[test]
fn missing_or_invalid_env_file_fails_during_cli_parsing() {
    let workspace = tempfile::tempdir().unwrap();
    let missing = workspace.path().join("missing.env");
    let missing_output = Command::new(shardline_binary())
        .args(["--env-file", missing.to_str().unwrap(), "config", "check"])
        .output()
        .unwrap();
    assert!(!missing_output.status.success());
    assert!(String::from_utf8_lossy(&missing_output.stderr).contains("failed to load env file"));

    let invalid = workspace.path().join("invalid.env");
    fs::write(&invalid, "not a valid dotenv line =\n").unwrap();
    let invalid_output = Command::new(shardline_binary())
        .args(["--env-file", invalid.to_str().unwrap(), "config", "check"])
        .output()
        .unwrap();
    assert!(!invalid_output.status.success());
    assert!(String::from_utf8_lossy(&invalid_output.stderr).contains("failed to load env file"));
}

fn shardline_binary() -> String {
    #[allow(clippy::expect_used)]
    var("CARGO_BIN_EXE_shardline").expect("Cargo should provide the shardline binary path")
}
