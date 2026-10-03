#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{env::var, fs, num::NonZeroUsize, path::Path, process::Command};

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
    // The secret-file gate rejects group/world-readable keys (mode > 0600).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&signing_key, fs::Permissions::from_mode(0o600)).unwrap();
    }
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

#[cfg(unix)]
#[test]
fn config_check_preserves_literal_and_native_config_filenames() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};

    let workspace = tempfile::tempdir().unwrap();
    // A substituted path exists too, so success must come from the literal
    // selected file rather than accidentally resolving another config.
    fs::write(workspace.path().join("expanded.toml"), "[[[invalid TOML]]]").unwrap();
    let mut filenames = [
        "$LABEL.toml",
        "${LABEL}.toml",
        "quote\".toml",
        "single'.toml",
        "backslash\\.toml",
        "newline\n.toml",
        "tab\t.toml",
    ]
    .map(OsString::from)
    .to_vec();
    filenames.push(OsString::from_vec(b"native-\xff.toml".to_vec()));

    for filename in filenames {
        let config = workspace.path().join(filename);
        fs::write(&config, "[server]\n").unwrap();
        let output = Command::new(shardline_binary())
            .arg("--config")
            .arg(&config)
            .args(["config", "check"])
            .env_clear()
            .env("LABEL", "expanded")
            .env("RUST_LOG", "off")
            .env("SHARDLINE_BIND_ADDR", "127.0.0.1:8080")
            .env("SHARDLINE_ROOT_DIR", workspace.path().join("data"))
            .current_dir(workspace.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "literal config {config:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("status: ok"));
    }
}

#[test]
fn public_parse_replaces_and_clears_config_selection() {
    const CHILD: &str = "SHARDLINE_TEST_NATIVE_CONFIG_SELECTION";
    if std::env::var_os(CHILD).is_none() {
        let workspace = tempfile::tempdir().unwrap();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "public_parse_replaces_and_clears_config_selection",
                "--nocapture",
            ])
            .env_clear()
            .env(CHILD, "1")
            .current_dir(workspace.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "isolated parse test failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    use shardline::{CliCommand, load_server_config};
    let workspace = tempfile::tempdir().unwrap();
    let valid = workspace.path().join("valid.toml");
    let invalid = workspace.path().join("invalid.toml");
    fs::write(&valid, "[server]\n").unwrap();
    fs::write(&invalid, "[[[invalid TOML]]]").unwrap();
    fs::write("shardline.toml", "[[[invalid auto-detected TOML]]]").unwrap();
    let parse = |path: &Path| {
        CliCommand::parse([
            std::ffi::OsString::from("shardline"),
            std::ffi::OsString::from("--config"),
            path.as_os_str().to_owned(),
            std::ffi::OsString::from("config"),
            std::ffi::OsString::from("check"),
        ])
        .unwrap();
    };
    parse(&valid);
    assert!(load_server_config(None, None).is_ok());
    // Explicit library arguments still outrank the selected CLI path.
    assert!(load_server_config(None, Some(&invalid)).is_err());
    assert!(CliCommand::parse(["shardline", "--unknown-option"]).is_err());
    assert!(load_server_config(None, None).is_ok());
    parse(&invalid);
    assert!(load_server_config(None, None).is_err());
    parse(&valid);
    assert!(load_server_config(None, None).is_ok());
    CliCommand::parse(["shardline", "config", "check"]).unwrap();
    assert!(load_server_config(None, None).is_err());
}

fn shardline_binary() -> String {
    if let Ok(path) = var("CARGO_BIN_EXE_shardline") {
        return path;
    }

    // Cargo sets CARGO_BIN_EXE_* for `cargo test`, but nextest intentionally
    // does not. Derive the sibling binary from the integration-test path so
    // the CLI tests remain valid in both runners.
    let test_executable = std::env::current_exe().expect("test executable path");
    let binary = test_executable
        .parent()
        .and_then(Path::parent)
        .map(|target| target.join("shardline"))
        .expect("test executable should be under target/debug/deps");
    assert!(
        binary.is_file(),
        "shardline binary does not exist: {binary:?}"
    );
    binary.to_string_lossy().into_owned()
}
