use super::{Result, artifact, artifact::Stream};
use penelope::core::task::{Digest, TaskGeneration};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{BufRead, Write},
    path::{Path, PathBuf},
    process::ExitCode,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema: u32,
    exit_code: u8,
    stdout: Stream,
    stderr: Stream,
}

pub(crate) struct CapturedOutput {
    pub stdout: File,
    pub stderr: File,
    pub exit_code: u8,
}

fn path(directory: &Path, generation: TaskGeneration, stream: &str) -> PathBuf {
    directory.join(format!("{stream}-{}", generation.get()))
}

pub(super) fn publish(
    directory: &Path,
    generation: TaskGeneration,
    status: ExitCode,
    writer: &mut impl Write,
) -> Result<()> {
    let exit_code = super::command_exit_code(status);
    let mut stdout = artifact::open_private(&path(directory, generation, "stdout"), u64::MAX)?;
    let mut stderr = artifact::open_private(&path(directory, generation, "stderr"), u64::MAX)?;
    let manifest = Manifest {
        schema: 1,
        exit_code,
        stdout: artifact::inspect_stream(&mut stdout)?,
        stderr: artifact::inspect_stream(&mut stderr)?,
    };
    writeln!(writer, "maintenance.success: {}", exit_code == 0)?;
    serde_json::to_writer(writer, &manifest)?;
    Ok(())
}

pub(super) fn verify(
    directory: &Path,
    digest: Digest,
    generation: TaskGeneration,
) -> Result<CapturedOutput> {
    let bytes = artifact::read_private(
        &directory.join("artifacts").join(hex::encode(digest.0)),
        65536,
    )?;
    let mut body = bytes.as_slice();
    let mut header = String::new();
    body.read_line(&mut header)?;
    let manifest: Manifest = serde_json::from_slice(body)?;
    if manifest.schema != 1
        || manifest.exit_code > 2
        || header != format!("maintenance.success: {}\n", manifest.exit_code == 0)
    {
        return Err("invalid captured command outcome".into());
    }
    let mut stdout = artifact::open_private(
        &path(directory, generation, "stdout"),
        manifest.stdout.bytes,
    )?;
    let mut stderr = artifact::open_private(
        &path(directory, generation, "stderr"),
        manifest.stderr.bytes,
    )?;
    if artifact::inspect_stream(&mut stdout)? != manifest.stdout
        || artifact::inspect_stream(&mut stderr)? != manifest.stderr
    {
        return Err("captured command output does not match its signed result".into());
    }
    Ok(CapturedOutput {
        stdout,
        stderr,
        exit_code: manifest.exit_code,
    })
}
