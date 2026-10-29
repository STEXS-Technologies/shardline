use super::*;

#[test]
#[ignore = "requires Docker and disk-backed PostgreSQL/MinIO"]
fn teardown_removes_owned_anonymous_data_volumes() -> Result<(), IoError> {
    let stack = DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or_else(|| IoError::other("Docker is required for teardown qualification"))?;
    let containers = [
        &stack
            .postgres
            .as_ref()
            .ok_or_else(|| IoError::other("missing PostgreSQL fixture"))?
            .container_name,
        &stack
            .minio
            .as_ref()
            .ok_or_else(|| IoError::other("missing MinIO fixture"))?
            .container_name,
    ];
    let mut volumes = Vec::new();
    for container in containers {
        let output = run_command_checked(
            Command::new("docker").args([
                "inspect",
                "--format",
                "{{range .Mounts}}{{if eq .Type \"volume\"}}{{println .Name}}{{end}}{{end}}",
                container,
            ]),
            "inspect owned fixture volumes",
        )?;
        let names = String::from_utf8(output.stdout)
            .map_err(|error| IoError::new(ErrorKind::InvalidData, error))?;
        let names: Vec<_> = names.lines().filter(|name| !name.is_empty()).collect();
        if names.len() != 1 {
            return Err(IoError::other("fixture must exercise its data volume"));
        }
        volumes.extend(names.into_iter().map(str::to_owned));
    }
    drop(stack);
    let mut leaked = 0;
    for volume in volumes {
        let output = run_command(Command::new("docker").args(["volume", "inspect", &volume]))?;
        if output.status.success() {
            leaked += 1;
            // Clean only volumes captured from this test's own containers,
            // including when exercising the pre-fix failure.
            run_command_checked(
                Command::new("docker").args(["volume", "rm", &volume]),
                "remove leaked test-owned volume",
            )?;
        }
    }
    if leaked != 0 {
        return Err(IoError::other(
            "fixture teardown leaked anonymous data volumes",
        ));
    }
    Ok(())
}
