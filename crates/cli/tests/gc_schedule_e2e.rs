mod support;

use std::{
    env::var,
    error::Error,
    fs::{create_dir_all, read_to_string, write as write_file},
    path::PathBuf,
    process::Command,
};

use support::CliE2eInvariantError;

#[test]
fn gc_schedule_install_and_uninstall_manage_systemd_units() {
    let result = exercise_gc_schedule_install_and_uninstall();
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "gc schedule e2e failed: {error:?}");
}

#[cfg(unix)]
#[test]
fn gc_schedule_install_rejects_symlinked_output_directory() {
    let result = exercise_gc_schedule_install_rejects_symlinked_output_directory();
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc schedule symlink output-dir e2e failed: {error:?}"
    );
}

#[cfg(unix)]
#[test]
fn gc_schedule_install_rejects_symlinked_working_directory() {
    let result = exercise_gc_schedule_install_rejects_symlinked_working_directory();
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc schedule symlink working-dir e2e failed: {error:?}"
    );
}

fn exercise_gc_schedule_install_and_uninstall() -> Result<(), Box<dyn Error>> {
    let sandbox = tempfile::tempdir()?;
    let output_dir = sandbox.path().join("systemd");
    let shardline_bin = shardline_binary()?;
    let env_file = sandbox.path().join("shardline.env");
    let root_dir = sandbox.path().join("srv").join("assets");
    let signing_key = sandbox.path().join("token.key");
    create_dir_all(&root_dir)?;
    write_file(&signing_key, b"token")?;
    write_file(
        &env_file,
        format!(
            "SHARDLINE_ROOT_DIR={}\nSHARDLINE_TOKEN_SIGNING_KEY_FILE={}\n",
            root_dir.display(),
            signing_key.display()
        ),
    )?;

    let install = Command::new(&shardline_bin)
        .args([
            "gc",
            "schedule",
            "install",
            "--output-dir",
            output_dir
                .to_str()
                .ok_or("output dir was not valid utf-8")?,
            "--unit-prefix",
            "assets-gc",
            "--calendar",
            "hourly",
            "--retention-seconds",
            "600",
            "--binary-path",
            shardline_bin
                .to_str()
                .ok_or("binary path was not valid utf-8")?,
            "--env-file",
            env_file
                .to_str()
                .ok_or("env file path was not valid utf-8")?,
            "--working-directory",
            root_dir
                .to_str()
                .ok_or("root dir path was not valid utf-8")?,
            "--user",
            "root",
            "--group",
            "root",
        ])
        .output()?;
    if !install.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "gc schedule install failed: {}",
            String::from_utf8_lossy(&install.stderr)
        ))
        .into());
    }

    let service_path = output_dir.join("assets-gc.service");
    let timer_path = output_dir.join("assets-gc.timer");
    if !service_path.is_file() {
        return Err(CliE2eInvariantError::new("gc schedule did not create service unit").into());
    }
    if !timer_path.is_file() {
        return Err(CliE2eInvariantError::new("gc schedule did not create timer unit").into());
    }

    let service = read_to_string(&service_path)?;
    let expected_exec = format!(
        "ExecStart={} gc --mark --sweep --retention-seconds 600",
        shardline_bin.display()
    );
    if !service.contains(&expected_exec) {
        return Err(CliE2eInvariantError::new("gc service unit used unexpected command").into());
    }
    if !service.contains(&format!("EnvironmentFile={}", env_file.display())) {
        return Err(
            CliE2eInvariantError::new("gc service unit used unexpected environment file").into(),
        );
    }
    if !service.contains(&format!("WorkingDirectory={}", root_dir.display())) {
        return Err(
            CliE2eInvariantError::new("gc service unit used unexpected working directory").into(),
        );
    }

    let timer = read_to_string(&timer_path)?;
    if !timer.contains("OnCalendar=hourly") {
        return Err(CliE2eInvariantError::new("gc timer unit used unexpected calendar").into());
    }

    let uninstall = Command::new(shardline_bin)
        .args([
            "gc",
            "schedule",
            "uninstall",
            "--output-dir",
            output_dir
                .to_str()
                .ok_or("output dir was not valid utf-8")?,
            "--unit-prefix",
            "assets-gc",
        ])
        .output()?;
    if !uninstall.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "gc schedule uninstall failed: {}",
            String::from_utf8_lossy(&uninstall.stderr)
        ))
        .into());
    }

    if service_path.exists() {
        return Err(CliE2eInvariantError::new("gc schedule uninstall kept service unit").into());
    }
    if timer_path.exists() {
        return Err(CliE2eInvariantError::new("gc schedule uninstall kept timer unit").into());
    }

    Ok(())
}

#[cfg(unix)]
fn exercise_gc_schedule_install_rejects_symlinked_output_directory() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::symlink;

    let sandbox = tempfile::tempdir()?;
    let real_output_dir = sandbox.path().join("real-systemd");
    create_dir_all(&real_output_dir)?;
    let output_dir = sandbox.path().join("systemd");
    symlink(&real_output_dir, &output_dir)?;
    let shardline_bin = shardline_binary()?;
    let env_file = sandbox.path().join("shardline.env");
    let root_dir = sandbox.path().join("srv").join("assets");
    let signing_key = sandbox.path().join("token.key");
    create_dir_all(&root_dir)?;
    write_file(&signing_key, b"token")?;
    write_file(
        &env_file,
        format!(
            "SHARDLINE_ROOT_DIR={}\nSHARDLINE_TOKEN_SIGNING_KEY_FILE={}\n",
            root_dir.display(),
            signing_key.display()
        ),
    )?;

    let install = Command::new(shardline_bin)
        .args([
            "gc",
            "schedule",
            "install",
            "--output-dir",
            output_dir
                .to_str()
                .ok_or("output dir was not valid utf-8")?,
            "--unit-prefix",
            "assets-gc",
            "--calendar",
            "hourly",
            "--retention-seconds",
            "600",
            "--binary-path",
            "/bin/true",
            "--env-file",
            env_file
                .to_str()
                .ok_or("env file path was not valid utf-8")?,
            "--working-directory",
            root_dir
                .to_str()
                .ok_or("root dir path was not valid utf-8")?,
            "--user",
            "root",
            "--group",
            "root",
        ])
        .output()?;
    if install.status.success() {
        return Err(CliE2eInvariantError::new(
            "gc schedule install unexpectedly accepted a symlinked output directory",
        )
        .into());
    }

    if real_output_dir.join("assets-gc.service").exists() {
        return Err(
            CliE2eInvariantError::new("gc schedule install wrote through the symlink").into(),
        );
    }

    Ok(())
}

#[cfg(unix)]
fn exercise_gc_schedule_install_rejects_symlinked_working_directory() -> Result<(), Box<dyn Error>>
{
    use std::os::unix::fs::symlink;

    let sandbox = tempfile::tempdir()?;
    let output_dir = sandbox.path().join("systemd");
    let shardline_bin = shardline_binary()?;
    let env_file = sandbox.path().join("shardline.env");
    let real_root_dir = sandbox.path().join("real-assets");
    let working_directory = sandbox.path().join("working-link");
    let signing_key = sandbox.path().join("token.key");
    create_dir_all(&real_root_dir)?;
    symlink(&real_root_dir, &working_directory)?;
    write_file(&signing_key, b"token")?;
    write_file(
        &env_file,
        format!(
            "SHARDLINE_ROOT_DIR={}\nSHARDLINE_TOKEN_SIGNING_KEY_FILE={}\n",
            working_directory.display(),
            signing_key.display()
        ),
    )?;

    let install = Command::new(shardline_bin)
        .args([
            "gc",
            "schedule",
            "install",
            "--output-dir",
            output_dir
                .to_str()
                .ok_or("output dir was not valid utf-8")?,
            "--unit-prefix",
            "assets-gc",
            "--calendar",
            "hourly",
            "--retention-seconds",
            "600",
            "--binary-path",
            "/bin/true",
            "--env-file",
            env_file
                .to_str()
                .ok_or("env file path was not valid utf-8")?,
            "--working-directory",
            working_directory
                .to_str()
                .ok_or("working dir path was not valid utf-8")?,
            "--user",
            "root",
            "--group",
            "root",
        ])
        .output()?;
    if install.status.success() {
        return Err(CliE2eInvariantError::new(
            "gc schedule install unexpectedly accepted a symlinked working directory",
        )
        .into());
    }

    Ok(())
}

fn shardline_binary() -> Result<PathBuf, Box<dyn Error>> {
    if let Ok(path) = var("CARGO_BIN_EXE_shardline") {
        return Ok(PathBuf::from(path));
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let Some(workspace_root) = manifest_dir.ancestors().nth(2) else {
        return Err(CliE2eInvariantError::new("workspace root could not be resolved").into());
    };
    let binary = workspace_root
        .join("target")
        .join("debug")
        .join("shardline");
    if !binary.is_file() {
        return Err(CliE2eInvariantError::new(format!(
            "shardline binary was not built at {}",
            binary.display()
        ))
        .into());
    }

    Ok(binary)
}
