#![cfg(all(feature = "docker", unix))]
#![allow(clippy::unwrap_used)]

use shardline_test_support::{DockerLocalStack, native_client_output};
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Output};
use std::time::Duration;

const PRIVATE_FAILURE: &str = "private-fixture-error-must-not-escape";

fn invoke(script: Option<&str>, required: bool, mode: &str) -> Output {
    let directory = tempfile::tempdir().unwrap();
    if let Some(script) = script {
        let executable = directory.path().join("docker");
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    native_client_output(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "fixture_child", "--ignored", "--nocapture"])
            .env_clear()
            .env("PATH", directory.path())
            .env(
                "SHARDLINE_REQUIRE_NATIVE_CLIENTS",
                if required { "1" } else { "0" },
            )
            .env("SHARDLINE_FIXTURE_MODE", mode),
        Duration::from_secs(20),
    )
    .unwrap()
}

#[test]
fn missing_docker_is_optional_locally_but_fails_required_qualification() {
    assert!(invoke(None, false, "absent").status.success());
    let required = invoke(None, true, "absent");
    assert!(!required.status.success());
    assert!(
        String::from_utf8_lossy(&required.stdout).contains("required native client Docker daemon")
            || String::from_utf8_lossy(&required.stderr)
                .contains("required native client Docker daemon")
    );
}

#[test]
fn failed_service_startup_cannot_be_silently_discarded_in_required_mode() {
    let script = format!(
        "#!/bin/sh\nif [ \"$1\" = version ]; then exit 0; fi\necho {PRIVATE_FAILURE} >&2\nexit 1\n"
    );
    assert!(invoke(Some(&script), false, "startup").status.success());
    let required = invoke(Some(&script), true, "startup");
    assert!(!required.status.success());
    let stdout = String::from_utf8_lossy(&required.stdout);
    let stderr = String::from_utf8_lossy(&required.stderr);
    assert!(
        stdout.contains("required native client disposable Docker service fixture")
            || stderr.contains("required native client disposable Docker service fixture")
    );
    assert!(!stdout.contains(PRIVATE_FAILURE));
    assert!(!stderr.contains(PRIVATE_FAILURE));
}

#[test]
fn docker_version_probe_has_a_deadline() {
    assert!(
        invoke(Some("#!/bin/sh\nexec /bin/sleep 30\n"), false, "probe")
            .status
            .success()
    );
}

#[test]
#[ignore = "subprocess fixture invoked by prerequisite policy tests"]
fn fixture_child() {
    match std::env::var("SHARDLINE_FIXTURE_MODE").unwrap().as_str() {
        "absent" => assert!(matches!(
            DockerLocalStack::builder().with_postgres().start(),
            Ok(None)
        )),
        "startup" => {
            // Even callers that discard startup errors cannot turn required
            // qualification into a successful early return.
            let stack = DockerLocalStack::builder()
                .with_postgres()
                .start()
                .ok()
                .flatten();
            assert!(stack.is_none());
        }
        "probe" => assert!(!DockerLocalStack::docker_available()),
        _ => std::process::exit(2),
    }
}
