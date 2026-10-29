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
            .env_remove("SHARDLINE_REQUIRE_POSTGRES_TESTS")
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

#[test]
fn required_postgres_fixture_missing_fails_without_native_client_requirement() {
    let output = shardline_test_support::native_client_output(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "health_route_boots_with_postgres_metadata_config",
            ])
            .env_remove("DATABASE_URL")
            .env_remove("SHARDLINE_REQUIRE_NATIVE_CLIENTS")
            .env("SHARDLINE_REQUIRE_POSTGRES_TESTS", "1"),
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("PostgreSQL fixture DATABASE_URL is missing or invalid")
    );
}
