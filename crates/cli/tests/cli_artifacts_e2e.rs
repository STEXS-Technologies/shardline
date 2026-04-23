mod support;

use std::{
    env::var,
    error::Error,
    fs::{read, read_to_string, write as write_file},
    path::PathBuf,
    process::Command,
};

use support::CliE2eInvariantError;

#[test]
fn completion_and_manpage_commands_emit_artifacts() {
    let result = exercise_completion_and_manpage();
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "cli artifact e2e failed: {error:?}");
}

#[cfg(unix)]
#[test]
fn completion_command_rejects_symlinked_output_path() {
    let result = exercise_completion_rejects_symlinked_output_path();
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "cli artifact symlink-output e2e failed: {error:?}"
    );
}

fn exercise_completion_and_manpage() -> Result<(), Box<dyn Error>> {
    let shardline_bin = shardline_binary()?;
    let sandbox = tempfile::tempdir()?;
    let completion_path = sandbox.path().join("shardline.bash");
    let manpage_path = sandbox.path().join("shardline.1");

    let completion = Command::new(&shardline_bin)
        .args([
            "completion",
            "bash",
            "--output",
            completion_path
                .to_str()
                .ok_or("completion path was not valid utf-8")?,
        ])
        .output()?;
    if !completion.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "completion generation failed: {}",
            String::from_utf8_lossy(&completion.stderr)
        ))
        .into());
    }

    let completion_script = read_to_string(&completion_path)?;
    if !completion_script.contains("shardline") || !completion_script.contains("complete") {
        return Err(CliE2eInvariantError::new(
            "completion output was not a bash completion script",
        )
        .into());
    }

    let manpage = Command::new(&shardline_bin)
        .args([
            "manpage",
            "--output",
            manpage_path
                .to_str()
                .ok_or("manpage path was not valid utf-8")?,
        ])
        .output()?;
    if !manpage.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "manpage generation failed: {}",
            String::from_utf8_lossy(&manpage.stderr)
        ))
        .into());
    }

    let manpage_text = read_to_string(&manpage_path)?;
    if !manpage_text.contains(".TH shardline") || !manpage_text.contains("gc") {
        return Err(
            CliE2eInvariantError::new("manpage output was missing expected content").into(),
        );
    }

    Ok(())
}

#[cfg(unix)]
fn exercise_completion_rejects_symlinked_output_path() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::symlink;

    let shardline_bin = shardline_binary()?;
    let sandbox = tempfile::tempdir()?;
    let target = sandbox.path().join("victim.bash");
    write_file(&target, b"keep")?;
    let completion_path = sandbox.path().join("shardline.bash");
    symlink(&target, &completion_path)?;

    let completion = Command::new(&shardline_bin)
        .args([
            "completion",
            "bash",
            "--output",
            completion_path
                .to_str()
                .ok_or("completion path was not valid utf-8")?,
        ])
        .output()?;
    if completion.status.success() {
        return Err(CliE2eInvariantError::new(
            "completion generation unexpectedly accepted a symlinked output path",
        )
        .into());
    }

    if read(&target)? != b"keep" {
        return Err(
            CliE2eInvariantError::new("completion generation changed the symlink target").into(),
        );
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
