//! Exercise required backend admission through the actual test entry point.
use std::process::Command;

#[test]
fn required_missing_docker_fails() {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "postgres_backend_lfs_round_trip"])
        .env("PATH", "/nonexistent-shardline-native-clients")
        .env("SHARDLINE_REQUIRE_NATIVE_CLIENTS", "1")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let diagnostic = String::from_utf8_lossy(&output.stdout);
    assert!(
        diagnostic.contains("required native client Docker data-integrity backend"),
        "{diagnostic}"
    );
}
