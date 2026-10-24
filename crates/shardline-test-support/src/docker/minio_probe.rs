//! HTTP readiness and bucket creation do not prove that a provider accepts data.
use super::*;

pub(super) fn create_bucket(run_id: &str, host: &str) -> Result<(), IoError> {
    run_mc(
        run_id,
        host,
        None,
        &[
            "mb",
            "--ignore-existing",
            &format!("local/{DEFAULT_S3_BUCKET}"),
        ],
    )?;
    Ok(())
}

pub(super) fn verify_writable(run_id: &str, host: &str) -> Result<(), IoError> {
    let payload = vec![42_u8; 65536];
    let source = tempfile::NamedTempFile::new()?;
    fs::write(source.path(), &payload)?;
    #[cfg(unix)]
    fs::set_permissions(source.path(), fs::Permissions::from_mode(0o644))?;
    let target = format!("local/{DEFAULT_S3_BUCKET}/.fixture-preflight/{run_id}");
    run_mc(
        run_id,
        host,
        Some(source.path()),
        &["cp", "/probe", &target],
    )?;
    let fetched = run_mc(run_id, host, None, &["cat", &target])?;
    if fetched.stdout != payload {
        return Err(IoError::other(
            "MinIO storage preflight returned different bytes",
        ));
    }
    run_mc(run_id, host, None, &["rm", &target])?;
    Ok(())
}

fn run_mc(
    run_id: &str,
    host: &str,
    source: Option<&Path>,
    args: &[&str],
) -> Result<Output, IoError> {
    let name = format!("shardline-test-minio-preflight-{run_id}");
    let mut command = Command::new("docker");
    command
        .args(["run", "--rm", "--network", "host", "--name", &name, "-e"])
        .arg(format!("MC_HOST_local={host}"));
    if let Some(source) = source {
        command
            .arg("-v")
            .arg(format!("{}:/probe:ro", source.display()));
    }
    command.arg(MINIO_MC_IMAGE).arg("--json").args(args);
    let result = crate::native_client_output(&mut command, Duration::from_secs(30));
    // Killing the Docker CLI process group does not prove its container stopped.
    // Remove only this fixture's named probe, including after a deadline.
    let _cleanup = remove_container(&name);
    let output = result?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let full = [stderr.as_ref(), stdout.as_ref()].iter().any(|message| {
            message.contains("XMinioStorageFull")
                || message.contains("minimum free drive threshold")
        });
        return Err(IoError::other(if full {
            "MinIO storage preflight failed: XMinioStorageFull; restore free disk space before running storage qualification"
        } else {
            "MinIO storage preflight failed; HTTP readiness and bucket creation do not prove writable object storage"
        }));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires Docker and writable disk-backed MinIO storage"]
    fn disk_backed_minio_requires_writable_verified_object_storage() -> Result<(), IoError> {
        let _stack = DockerLocalStack::builder()
            .with_minio()
            .start()?
            .ok_or_else(|| IoError::other("Docker is required for disk-backed qualification"))?;
        Ok(())
    }

    #[test]
    #[ignore = "requires Docker; verifies fixture preflight, not disk-backed storage qualification"]
    fn memory_backed_minio_exercises_probe_readback_and_cleanup() -> Result<(), IoError> {
        let _stack = DockerLocalStack::builder()
            .with_minio_tmpfs()
            .start()?
            .ok_or_else(|| IoError::other("Docker is required for fixture verification"))?;
        Ok(())
    }
}
