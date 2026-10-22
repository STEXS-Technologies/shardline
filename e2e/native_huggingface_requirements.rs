//! Check the real CLI test entry point without mutating the parent environment.
use std::process::Command;

#[test]
fn required_missing_hf_fails_instead_of_passing() {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "native_huggingface_cli_model_and_dataset_flows_work_against_shardline",
        ])
        .env("PATH", "/nonexistent-shardline-native-clients")
        .env("SHARDLINE_REQUIRE_NATIVE_CLIENTS", "1")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let diagnostic = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        diagnostic.contains("required native client hf is missing or cannot run"),
        "unexpected child failure: {diagnostic}"
    );
}

#[test]
fn optional_local_missing_hf_keeps_existing_behavior() {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "native_huggingface_cli_model_and_dataset_flows_work_against_shardline",
        ])
        .env("PATH", "/nonexistent-shardline-native-clients")
        .env_remove("SHARDLINE_REQUIRE_NATIVE_CLIENTS")
        .output()
        .unwrap();
    assert!(output.status.success());
}
