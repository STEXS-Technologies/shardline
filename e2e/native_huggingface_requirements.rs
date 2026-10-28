//! Check the real CLI test entry point without mutating the parent environment.
use std::process::Command;

#[test]
fn required_missing_hf_fails_instead_of_passing() {
    for test in [
        "native_huggingface_cli_model_and_dataset_flows_work_against_shardline",
        "native_hf_cli_and_s3_clients_coexist_on_one_server",
        "native_huggingface_whoami::native_huggingface_permissive_whoami_preserves_anonymous_identity",
        "native_huggingface_expiry::native_huggingface_cli_recovers_after_expired_xorb_url",
        "native_huggingface_expiry::native_huggingface_cli_recovers_after_expired_xorb_url_split_roles",
        "native_huggingface_http_analytics::duckdb_queries_authenticated_regular_and_xet_hub_ranges",
        "native_huggingface_http_analytics::duckdb_queries_authenticated_hub_ranges_split_roles",
    ] {
        assert_missing_hf_fails(test);
    }
}

fn assert_missing_hf_fails(test: &str) {
    let output = shardline_test_support::native_client_output(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test])
            .env("PATH", "/nonexistent-shardline-native-clients")
            .env("SHARDLINE_REQUIRE_NATIVE_CLIENTS", "1"),
        std::time::Duration::from_secs(30),
    )
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
    let output = shardline_test_support::native_client_output(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_huggingface_cli_model_and_dataset_flows_work_against_shardline",
            ])
            .env("PATH", "/nonexistent-shardline-native-clients")
            .env_remove("SHARDLINE_REQUIRE_NATIVE_CLIENTS"),
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    assert!(output.status.success());
}

#[cfg(unix)]
#[test]
fn required_analytical_missing_curl_fails_instead_of_passing() {
    if !super::command_available("hf") {
        return;
    }
    let hf = shardline_test_support::native_client_output(
        Command::new("which").arg("hf"),
        std::time::Duration::from_secs(10),
    )
    .unwrap();
    assert!(hf.status.success(), "prerequisite contract requires installed hf");
    let hf = String::from_utf8(hf.stdout).unwrap();
    let tools = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(hf.trim(), tools.path().join("hf")).unwrap();
    let output = shardline_test_support::native_client_output(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_huggingface_http_analytics::duckdb_queries_authenticated_regular_and_xet_hub_ranges",
            ])
            .env("PATH", tools.path())
            .env("SHARDLINE_REQUIRE_NATIVE_CLIENTS", "1"),
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    assert!(!output.status.success());
    let diagnostic = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        diagnostic.contains("required native client curl is missing or cannot run"),
        "unexpected child failure: {diagnostic}"
    );
}
