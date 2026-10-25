//! An explicitly selected live-forge campaign must never pass without fixtures.
use std::process::Command;

#[test]
fn explicit_live_campaign_without_fixtures_fails() {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args([
        "--exact",
        "live_provider_bridge_supports_private_github_and_gitea_repositories",
        "--ignored",
    ]);
    for name in [
        "SHARDLINE_LIVE_GITHUB_OWNER",
        "SHARDLINE_LIVE_GITHUB_REPO",
        "SHARDLINE_LIVE_GITHUB_TOKEN",
        "SHARDLINE_LIVE_GITEA_BASE_URL",
        "SHARDLINE_LIVE_GITEA_OWNER",
        "SHARDLINE_LIVE_GITEA_REPO",
        "SHARDLINE_LIVE_GITEA_TOKEN",
    ] {
        command.env_remove(name);
    }
    let output = shardline_test_support::native_client_output(
        &mut command,
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    assert!(!output.status.success());
    let diagnostic = String::from_utf8_lossy(&output.stdout);
    assert!(
        diagnostic.contains("missing live-provider fixture variable")
            || diagnostic.contains("explicit live-provider qualification requires"),
        "{diagnostic}"
    );
}

#[test]
fn command_failure_diagnostics_do_not_disclose_provider_credentials() {
    let mut output = shardline_test_support::native_client_output(
        Command::new(std::env::current_exe().unwrap()).arg("--list"),
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    output.stdout = b"https://private-user:private-token@example.invalid/repository".to_vec();
    output.stderr = b"Authorization: Bearer private-token".to_vec();
    let diagnostic = super::command_error("git", &output).to_string();
    assert!(diagnostic.contains("git failed"));
    assert!(!diagnostic.contains("private-user"));
    assert!(!diagnostic.contains("private-token"));
    assert!(!diagnostic.contains("example.invalid"));
}
