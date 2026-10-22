//! Check backend admission at the real native Xet test entry point.
use std::process::{Command, Output};

fn without_docker(required: bool) -> Output {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "shardline_accepts_native_xet_upload_and_download_flows_with_s3_when_configured",
        ])
        .env("PATH", "/nonexistent-shardline-native-clients")
        .env_remove("SHARDLINE_REQUIRE_NATIVE_CLIENTS");
    if required {
        command.env("SHARDLINE_REQUIRE_NATIVE_CLIENTS", "1");
    }
    command.output().unwrap()
}

#[test]
fn required_missing_backend_fails_instead_of_passing() {
    let output = without_docker(true);
    assert!(!output.status.success());
    let diagnostic = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        diagnostic.contains("required native client Docker/MinIO native Xet backend"),
        "unexpected child failure: {diagnostic}"
    );
}

#[test]
fn optional_local_missing_backend_keeps_existing_behavior() {
    assert!(without_docker(false).status.success());
}
