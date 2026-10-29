//! Exercise the real test entry point in a child process without native tools.
//! Environment changes stay in the child, so concurrent tests remain isolated.
use std::process::Command;

#[test]
fn required_missing_client_fails_instead_of_passing() {
    let executable = std::env::current_exe().unwrap();
    let output = shardline_test_support::native_client_output(
        Command::new(&executable)
            .args([
                "--exact",
                "native_helm_oci_chart_push_and_pull_work_against_shardline_oci_frontend",
                "--nocapture",
            ])
            .env("PATH", "/nonexistent-shardline-native-clients")
            .env("SHARDLINE_REQUIRE_NATIVE_CLIENTS", "1"),
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("required native client helm is missing or cannot run"),
        "unexpected child failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn optional_local_missing_client_keeps_existing_behavior() {
    let output = shardline_test_support::native_client_output(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_helm_oci_chart_push_and_pull_work_against_shardline_oci_frontend",
            ])
            .env("PATH", "/nonexistent-shardline-native-clients")
            .env_remove("SHARDLINE_REQUIRE_NATIVE_CLIENTS"),
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    assert!(output.status.success());
}
