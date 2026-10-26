//! Configured remote qualification must never silently become a local test.
use super::*;
use std::{ffi::OsStr, os::unix::ffi::OsStrExt};

fn rejects_configuration(fields: &[(&str, &OsStr)]) -> Result {
    let root = tempfile::tempdir()?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args(["--exact", "native_upload_child", "--ignored", "--nocapture"])
        .env(CHILD_ROOT, root.path())
        .env("SHARDLINE_TEST_NATIVE_PROCESS_ADDRESS", address.to_string())
        .env_remove("SHARDLINE_TEST_NATIVE_PROCESS_CUT")
        .env_remove("SHARDLINE_TEST_NATIVE_PROCESS_INTENT");
    for field in REMOTE_FIELDS {
        command.env_remove(field);
    }
    for (name, value) in fields {
        command.env(name, value);
    }
    let output =
        shardline_test_support::native_client_output(&mut command, Duration::from_secs(2))?;
    assert!(!output.status.success(), "invalid fixture cannot qualify");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stdout.contains("native process remote backend configuration")
            || stderr.contains("native process remote backend configuration"),
        "fixture must report its missing or invalid configuration without values"
    );
    Ok(())
}

#[test]
fn partial_s3_configuration_cannot_fall_back_to_local_storage() -> Result {
    rejects_configuration(&[("NATIVE_PROCESS_S3_BUCKET", OsStr::new("partial-fixture"))])
}

#[test]
fn partial_postgres_configuration_is_explicitly_rejected() -> Result {
    rejects_configuration(&[(
        "NATIVE_PROCESS_POSTGRES_URL",
        OsStr::new("postgres://127.0.0.1/partial_fixture"),
    )])
}

#[test]
fn invalid_unicode_remote_configuration_cannot_fall_back_to_local_storage() -> Result {
    rejects_configuration(&[("NATIVE_PROCESS_POSTGRES_URL", OsStr::from_bytes(&[0xff]))])
}

#[test]
fn fully_present_remote_configuration_rejects_invalid_unicode() -> Result {
    let mut fields = REMOTE_FIELDS.map(|name| (name, OsStr::new("fixture")));
    fields[0].1 = OsStr::from_bytes(&[0xff]);
    rejects_configuration(&fields)
}

#[test]
fn empty_remote_credentials_cannot_use_an_ambient_backend() -> Result {
    let mut fields = REMOTE_FIELDS.map(|name| (name, OsStr::new("fixture")));
    fields[6].1 = OsStr::new("");
    rejects_configuration(&fields)
}
