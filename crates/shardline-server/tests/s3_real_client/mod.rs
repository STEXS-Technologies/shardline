//! Check the actual S3 test entry point with child-only environment changes.
use std::{process::Command, time::Duration};

fn missing_mc(required: bool) -> std::process::Output {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "mc_put_get_list_stat_rm_roundtrip",
            "--nocapture",
        ])
        .env("PATH", "/nonexistent-shardline-native-clients");
    if required {
        command.env("SHARDLINE_REQUIRE_NATIVE_CLIENTS", "1");
    } else {
        command.env_remove("SHARDLINE_REQUIRE_NATIVE_CLIENTS");
    }
    shardline_test_support::native_client_output(&mut command, Duration::from_secs(30)).unwrap()
}

#[test]
fn required_missing_s3_client_is_a_failure() {
    let output = missing_mc(true);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("required native client mc is missing or cannot run")
    );
}

#[test]
fn optional_missing_s3_client_preserves_local_developer_behavior() {
    assert!(missing_mc(false).status.success());
}

#[cfg(unix)]
#[test]
fn failed_native_client_output_is_not_exposed_by_test_diagnostics() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let program = directory.path().join("mc");
    let secret = "synthetic-client-credential-must-stay-private";
    std::fs::write(
        &program,
        format!(
            "#!/bin/sh\nif [ \"$1\" = --version ]; then exit 0; fi\nprintf '%s' '{secret}'\nprintf '%s' '{secret}' >&2\nexit 17\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = shardline_test_support::native_client_output(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "mc_put_get_list_stat_rm_roundtrip",
                "--nocapture",
            ])
            .env("PATH", directory.path())
            .env("SHARDLINE_REQUIRE_NATIVE_CLIENTS", "1"),
        Duration::from_secs(30),
    )
    .unwrap();
    assert!(!output.status.success());
    let diagnostic = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(diagnostic.contains("mc alias set failed with status"));
    assert!(!diagnostic.contains(secret));
}

#[cfg(unix)]
#[test]
fn failed_boto3_interpreter_output_stays_private() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let interpreter = directory.path().join("python");
    let secret = "synthetic-boto3-credential-must-stay-private";
    std::fs::write(
        &interpreter,
        format!(
            "#!/bin/sh\nif [ \"$1\" = -c ]; then exit 0; fi\nprintf '%s' '{secret}'\nprintf '%s\\n' '{secret}' 'boto3 failure type: ParamValidationError' >&2\nexit 17\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&interpreter, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = shardline_test_support::native_client_output(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "boto3_sdk_roundtrip", "--nocapture"])
            .env("BOTOCORE_PYTHON", interpreter)
            .env("SHARDLINE_REQUIRE_NATIVE_CLIENTS", "1"),
        Duration::from_secs(30),
    )
    .unwrap();
    assert!(!output.status.success());
    let diagnostic = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(diagnostic.contains("SDK parameter validation"));
    assert!(!diagnostic.contains(secret));
}

#[cfg(unix)]
#[test]
fn failed_empty_listing_cannot_prove_successful_deletion() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let program = directory.path().join("mc");
    std::fs::write(
        &program,
        r#"#!/bin/sh
if [ "$1" = --version ]; then exit 0; fi
case "$3" in
  alias|mb|cp|rm) exit 0 ;;
  cat) printf 'hello real-client\n' ;;
  stat) printf 'hello.txt' ;;
  ls)
    if [ -f "$SHARDLINE_LIST_MARKER" ]; then exit 17; fi
    : > "$SHARDLINE_LIST_MARKER"
    printf 'hello.txt'
    ;;
  *) exit 18 ;;
esac
"#,
    )
    .unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = shardline_test_support::native_client_output(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "mc_put_get_list_stat_rm_roundtrip",
                "--nocapture",
            ])
            .env("PATH", directory.path())
            .env("SHARDLINE_LIST_MARKER", directory.path().join("listed"))
            .env("SHARDLINE_REQUIRE_NATIVE_CLIENTS", "1"),
        Duration::from_secs(30),
    )
    .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("mc ls after removal failed"));
}
