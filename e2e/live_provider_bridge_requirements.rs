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
    let output = command.output().unwrap();
    assert!(!output.status.success());
    let diagnostic = String::from_utf8_lossy(&output.stdout);
    assert!(
        diagnostic.contains("missing live-provider fixture variable")
            || diagnostic.contains("explicit live-provider qualification requires"),
        "{diagnostic}"
    );
}
