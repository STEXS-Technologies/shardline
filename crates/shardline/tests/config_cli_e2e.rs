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

#[test]
fn db_migration_reads_selected_and_auto_detected_toml_without_server_bootstrap() {
    let workspace = tempfile::tempdir().unwrap();
    let selected = workspace.path().join("selected.toml");
    // Migration configuration must not require a valid server auth provider.
    fs::write(
        &selected,
        "[index]\npostgres_url = \"\"\n[auth]\nprovider = \"not-a-server-provider\"\n",
    )
    .unwrap();
    let explicit = Command::new(shardline_binary())
        .arg("--config")
        .arg(&selected)
        .args(["db", "migrate", "status"])
        .env_clear()
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert_eq!(explicit.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&explicit.stderr).contains("database URL must not be empty"));
    fs::rename(selected, workspace.path().join("shardline.toml")).unwrap();
    let detected = Command::new(shardline_binary())
        .args(["db", "migrate", "status"])
        .env_clear()
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert_eq!(detected.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&detected.stderr).contains("database URL must not be empty"));
    assert!(!workspace.path().join(".shardline").exists());
}

#[test]
fn db_migration_validates_toml_and_preserves_url_precedence_and_interpolation() {
    let workspace = tempfile::tempdir().unwrap();
    let config = workspace.path().join("selected.toml");
    let run = |override_url: Option<&str>, environment_url: Option<&str>| {
        let mut command = Command::new(shardline_binary());
        command
            .arg("--config")
            .arg(&config)
            .args(["db", "migrate", "status"])
            .env_clear()
            .env("MIGRATION_URL", "")
            .current_dir(workspace.path());
        if let Some(url) = override_url {
            command.args(["--database-url", url]);
        }
        if let Some(url) = environment_url {
            command.env("SHARDLINE_INDEX_POSTGRES_URL", url);
        }
        command.output().unwrap()
    };
    fs::write(&config, "[index]\npostgres_url = \"${MIGRATION_URL}\"\n").unwrap();
    let interpolated = run(None, None);
    assert_eq!(interpolated.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&interpolated.stderr).contains("database URL must not be empty")
    );

    // Invalid lower-priority URLs would fail parsing; selected empty values
    // stop before any network I/O and establish each precedence boundary.
    fs::write(&config, "[index]\npostgres_url = \"not-a-postgres-url\"\n").unwrap();
    let environment = run(None, Some(""));
    assert_eq!(environment.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&environment.stderr).contains("database URL must not be empty")
    );
    let override_selected = run(Some(""), Some("not-a-postgres-url"));
    assert_eq!(override_selected.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&override_selected.stderr)
            .contains("database URL must not be empty")
    );

    fs::write(&config, "[[[invalid TOML]]]").unwrap();
    let malformed = run(Some(""), Some(""));
    assert_eq!(malformed.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&malformed.stderr).contains("TOML parse error"));
}

#[test]
fn dotenv_preflight_preserves_state_and_native_semantics() {
    const CHILD: &str = "SHARDLINE_TEST_DOTENV_PREFLIGHT";
    if std::env::var_os(CHILD).is_none() {
        let workspace = tempfile::tempdir().unwrap();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "dotenv_preflight_preserves_state_and_native_semantics",
                "--nocapture",
            ])
            .env_clear()
            .env(CHILD, "1")
            .env("CLI_ENV_EXISTING", "from-process")
            .current_dir(workspace.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "isolated dotenv test failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    use shardline::{CliCommand, effective_root};
    let workspace = tempfile::tempdir().unwrap();
    let env_file = workspace.path().join("selected.env");
    let parse = || {
        CliCommand::parse([
            std::ffi::OsString::from("shardline"),
            std::ffi::OsString::from("--env-file"),
            env_file.as_os_str().to_owned(),
            std::ffi::OsString::from("config"),
            std::ffi::OsString::from("check"),
        ])
    };
    let before_root = effective_root(None).unwrap();
    let before_env = std::env::var_os("SHARDLINE_ROOT_DIR");
    fs::write(
        &env_file,
        format!(
            "SHARDLINE_ROOT_DIR={:?}\nCLI_ENV_PARTIAL=from-malformed\nnot a valid dotenv line =\n",
            workspace.path().join("partial-root")
        ),
    )
    .unwrap();
    assert!(
        parse()
            .unwrap_err()
            .to_string()
            .contains("failed to load env file")
    );
    assert_eq!(std::env::var_os("SHARDLINE_ROOT_DIR"), before_env);
    assert!(std::env::var_os("CLI_ENV_PARTIAL").is_none());
    assert_eq!(effective_root(None).unwrap(), before_root);

    fs::write(
        &env_file,
        b"CLI_ENV_PARTIAL=from-malformed\nCLI_ENV_BAD=secret\0value\n",
    )
    .unwrap();
    let nul_error = parse().unwrap_err().to_string();
    assert!(nul_error.contains("NUL bytes"));
    assert!(!nul_error.contains("secret"));
    assert!(std::env::var_os("CLI_ENV_PARTIAL").is_none());
    assert!(std::env::var_os("CLI_ENV_BAD").is_none());
    assert_eq!(effective_root(None).unwrap(), before_root);

    let healthy_root = workspace.path().join("healthy-root");
    fs::write(
        &env_file,
        format!(
            "CLI_ENV_EXISTING=from-file\nCLI_ENV_DUPLICATE=first\nCLI_ENV_DUPLICATE=second\nCLI_ENV_REFERENCE=${{CLI_ENV_DUPLICATE}}\nCLI_ENV_EARLIER=source\nCLI_ENV_INTERPOLATED=${{CLI_ENV_EARLIER}}/suffix\nCLI_ENV_EXISTING_REF=${{CLI_ENV_EXISTING}}\nCLI_ENV_LITERAL='literal-$NAME'\nSHARDLINE_ROOT_DIR={healthy_root:?}\n"
        ),
    )
    .unwrap();
    assert!(parse().is_ok());
    for (name, expected) in [
        ("CLI_ENV_EXISTING", "from-process"),
        ("CLI_ENV_DUPLICATE", "first"),
        ("CLI_ENV_REFERENCE", "first"),
        ("CLI_ENV_INTERPOLATED", "source/suffix"),
        ("CLI_ENV_EXISTING_REF", "from-process"),
        ("CLI_ENV_LITERAL", "literal-$NAME"),
    ] {
        assert_eq!(std::env::var(name).unwrap(), expected);
    }
    assert_eq!(effective_root(None).unwrap(), healthy_root);
}

#[test]
fn dotenv_lookup_preserves_native_finder_behavior() {
    const CASE: &str = "SHARDLINE_TEST_DOTENV_LOOKUP_CASE";
    const ROOT: &str = "SHARDLINE_TEST_DOTENV_LOOKUP_ROOT";
    let Some(case) = std::env::var_os(CASE) else {
        #[cfg(unix)]
        let cases = [
            "parent",
            "nearest",
            "directory",
            "absolute",
            "native",
            "metadata-error",
        ];
        #[cfg(not(unix))]
        let cases = ["parent", "nearest", "directory", "absolute"];
        for case in cases {
            let workspace = tempfile::tempdir().unwrap();
            let nested = workspace.path().join("child").join("grandchild");
            fs::create_dir_all(&nested).unwrap();
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "dotenv_lookup_preserves_native_finder_behavior",
                    "--nocapture",
                ])
                .env_clear()
                .env(CASE, case)
                .env(ROOT, workspace.path())
                .current_dir(nested)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "isolated dotenv lookup {case} failed: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    };
    use shardline::CliCommand;
    use std::ffi::OsString;
    let root = std::path::PathBuf::from(std::env::var_os(ROOT).unwrap());
    let nested = std::env::current_dir().unwrap();
    fs::write(root.join("selected.env"), "CLI_ENV_LOOKUP=parent\n").unwrap();
    let mut selected = std::path::PathBuf::from("selected.env");
    let expected = match case.to_str().unwrap() {
        "parent" => "parent",
        "nearest" | "directory" | "absolute" => {
            fs::write(root.join("child/selected.env"), "CLI_ENV_LOOKUP=nearest\n").unwrap();
            if case == "directory" {
                fs::create_dir(nested.join("selected.env")).unwrap();
            }
            if case == "absolute" {
                selected = root.join("selected.env");
                "parent"
            } else {
                "nearest"
            }
        }
        #[cfg(unix)]
        "native" => {
            use std::os::unix::ffi::OsStringExt;
            selected = OsString::from_vec(b"native-\xff.env".to_vec()).into();
            fs::write(root.join(&selected), "CLI_ENV_LOOKUP=native\n").unwrap();
            "native"
        }
        #[cfg(unix)]
        "metadata-error" => {
            fs::create_dir(root.join("blocked")).unwrap();
            fs::write(
                root.join("blocked/selected.env"),
                "CLI_ENV_LOOKUP=ancestor\n",
            )
            .unwrap();
            fs::write(nested.join("blocked"), "not a directory").unwrap();
            selected = "blocked/selected.env".into();
            "error"
        }
        _ => "",
    };
    assert!(!expected.is_empty(), "unknown lookup test case");
    let result = CliCommand::parse([
        OsString::from("shardline"),
        OsString::from("--env-file"),
        selected.into_os_string(),
        OsString::from("config"),
        OsString::from("check"),
    ]);
    if expected == "error" {
        assert!(result.is_err());
        assert!(std::env::var_os("CLI_ENV_LOOKUP").is_none());
    } else {
        assert!(result.is_ok());
        assert_eq!(std::env::var("CLI_ENV_LOOKUP").unwrap(), expected);
    }
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
