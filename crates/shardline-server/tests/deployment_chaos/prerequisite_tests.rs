//! Exercise real drill entry points without touching the host Docker daemon.
use super::*;
use std::os::unix::fs::PermissionsExt;

#[test]
fn recovery_cleanup_targets_only_isolated_resources() {
    const CHILD: &str = "SHARDLINE_TEST_RECOVERY_CLEANUP";
    if std::env::var_os(CHILD).is_some() {
        let mut guard = ServiceRecoveryGuard::new();
        guard.stopped.push(CONTAINER_POSTGRES);
        guard.disconnected.push(CONTAINER_REDIS);
        guard.netem.push(CONTAINER_MINIO);
        drop(guard);
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("docker");
    let calls = directory.path().join("calls");
    std::fs::write(
        &executable,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$SHARDLINE_TEST_CLEANUP_CALLS\"\n",
    )
    .unwrap();
    std::fs::set_permissions(executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = shardline_test_support::native_client_output(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "prerequisite_tests::recovery_cleanup_targets_only_isolated_resources",
            ])
            .env_clear()
            .env(CHILD, "1")
            .env("PATH", directory.path())
            .env("SHARDLINE_TEST_CLEANUP_CALLS", &calls)
            .env("SHARDLINE_CHAOS_POSTGRES_CONTAINER", "owned-postgres")
            .env("SHARDLINE_CHAOS_REDIS_CONTAINER", "owned-redis")
            .env("SHARDLINE_CHAOS_MINIO_CONTAINER", "owned-minio")
            .env("SHARDLINE_CHAOS_NETWORK", "owned-network"),
        Duration::from_secs(20),
    )
    .unwrap();
    assert!(output.status.success(), "cleanup child: {}", output.status);
    let calls = std::fs::read_to_string(calls).unwrap();
    let calls: Vec<_> = calls.lines().collect();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0], "start owned-postgres");
    assert_eq!(calls[1], "network connect owned-network owned-redis");
    assert!(calls[2].starts_with("run --rm --network=container:owned-minio "));
    assert!(!calls.iter().any(|call| call.contains("chaos-")));
}

fn run(required: bool, script: Option<&str>) -> std::process::Output {
    run_drill(
        required,
        script,
        "drill_deploy_a_postgres_kill_mid_upload_no_lost_commits",
    )
}

fn run_drill(required: bool, script: Option<&str>, drill: &str) -> std::process::Output {
    let directory = tempfile::tempdir().unwrap();
    if let Some(script) = script {
        let executable = directory.path().join("docker");
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    shardline_test_support::native_client_output(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", drill, "--nocapture"])
            .env_clear()
            .env("PATH", directory.path())
            .env(
                "SHARDLINE_REQUIRE_NATIVE_CLIENTS",
                if required { "1" } else { "0" },
            ),
        Duration::from_secs(20),
    )
    .unwrap()
}

#[test]
fn missing_workload_opt_in_cannot_pass_required_qualification() {
    let drill = "chaos_workload_against_deployment";
    assert!(run_drill(false, None, drill).status.success());
    let required = run_drill(true, None, drill);
    assert!(!required.status.success());
    assert!(
        String::from_utf8_lossy(&required.stderr)
            .contains("required deployment workload needs SHARDLINE_CHAOS_DEPLOYMENT=1")
    );
}

#[cfg(not(feature = "docker"))]
#[test]
fn missing_postgres_cannot_pass_required_rolling_upgrade() {
    for required in [
        None,
        Some("SHARDLINE_REQUIRE_NATIVE_CLIENTS"),
        Some("SHARDLINE_REQUIRE_POSTGRES_TESTS"),
    ] {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "local_protocol_upgrade::postgres::released_postgres_rolling_upgrade_preserves_live_writes_and_old_restart",
            "--nocapture",
        ]).env_clear();
        if let Some(variable) = required {
            command.env(variable, "1");
        }
        let output =
            shardline_test_support::native_client_output(&mut command, Duration::from_secs(20))
                .unwrap();
        assert_eq!(output.status.success(), required.is_none());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(match required {
            None => "SKIPPED: PostgreSQL rolling upgrade requires",
            Some("SHARDLINE_REQUIRE_NATIVE_CLIENTS") => "required PostgreSQL rolling upgrade needs",
            Some(_) => "PostgreSQL fixture DATABASE_URL is missing or invalid",
        }));
    }
}

#[test]
fn missing_docker_cannot_pass_required_deployment_qualification() {
    assert!(run(false, None).status.success());
    let required = run(true, None);
    assert!(!required.status.success());
    assert!(
        String::from_utf8_lossy(&required.stderr)
            .contains("required deployment-chaos infrastructure is unavailable")
    );
}

#[test]
fn failed_compose_cannot_pass_or_expose_private_driver_errors() {
    let private_error = "private-compose-error-must-not-escape";
    let script = format!(
        "#!/bin/sh\nif [ \"$1\" = version ]; then exit 0; fi\necho {private_error} >&2\nexit 1\n"
    );
    assert!(run(false, Some(&script)).status.success());
    let required = run(true, Some(&script));
    assert!(!required.status.success());
    let stdout = String::from_utf8_lossy(&required.stdout);
    let stderr = String::from_utf8_lossy(&required.stderr);
    assert!(stderr.contains("required deployment-chaos infrastructure is unavailable"));
    assert!(!stdout.contains(private_error));
    assert!(!stderr.contains(private_error));
}
