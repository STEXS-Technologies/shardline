//! A failed Docker CLI cleanup must not lose ownership of the probe container.
#![allow(clippy::expect_used, clippy::panic_in_result_fn)]

use super::*;

#[test]
fn failed_probe_cleanup_is_retried_at_process_exit() -> Result<(), IoError> {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir()?;
    let docker = root.path().join("docker");
    fs::write(
        &docker,
        b"#!/bin/sh\n\
case \"$1\" in\n\
run) : > \"$PROBE_CLEANUP_ROOT/container\"; printf private-provider-diagnostic >&2; exit 7;;\n\
rm) printf 'attempt\\n' >> \"$PROBE_CLEANUP_ROOT/attempts\"\n\
    if [ ! -f \"$PROBE_CLEANUP_ROOT/first-failed\" ]; then\n\
      : > \"$PROBE_CLEANUP_ROOT/first-failed\"; exit 8\n\
    fi\n\
    /bin/rm \"$PROBE_CLEANUP_ROOT/container\";;\n\
*) exit 9;;\n\
esac\n",
    )?;
    fs::set_permissions(&docker, fs::Permissions::from_mode(0o700))?;
    let output = crate::native_client_output(
        Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "docker::minio_probe::cleanup_retry::failed_probe_child",
                "--ignored",
            ])
            .env("PATH", root.path())
            .env("PROBE_CLEANUP_ROOT", root.path()),
        Duration::from_secs(10),
    )?;
    assert!(output.status.success(), "owned probe child failed");
    assert!(
        !root.path().join("container").exists(),
        "failed probe lost its cleanup registration"
    );
    assert_eq!(
        fs::read_to_string(root.path().join("attempts"))?,
        "attempt\nattempt\n"
    );
    Ok(())
}

#[test]
#[ignore = "owned subprocess launched by failed_probe_cleanup_is_retried_at_process_exit"]
fn failed_probe_child() -> Result<(), IoError> {
    let root = std::env::var_os("PROBE_CLEANUP_ROOT")
        .ok_or_else(|| IoError::other("probe child requires its parent"))?;
    let root = std::path::PathBuf::from(root);
    let error = run_mc(
        "cleanup-control",
        "private-fixture-host",
        None,
        None,
        &["mb"],
    )
    .expect_err("failed probe must still fail qualification");
    assert!(!error.to_string().contains("private-provider-diagnostic"));
    assert!(root.join("container").exists());
    Ok(())
}
