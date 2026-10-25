//! Exercise required database admission through the actual test entry point.
use std::process::Command;

#[test]
fn required_missing_database_fails() {
    let output = shardline_test_support::native_client_output(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "health_route_boots_with_postgres_metadata_config",
            ])
            .env_remove("DATABASE_URL")
            .env("SHARDLINE_REQUIRE_NATIVE_CLIENTS", "1"),
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    assert!(!output.status.success());
    let diagnostic = String::from_utf8_lossy(&output.stdout);
    assert!(
        diagnostic.contains("required native client PostgreSQL DATABASE_URL transfer backend"),
        "{diagnostic}"
    );
}
