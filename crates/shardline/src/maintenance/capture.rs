use super::{Result, artifact};
use penelope::core::task::{Digest, TaskGeneration};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Write},
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

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Stream {
    digest: Digest,
    bytes: u64,
}

pub(crate) struct CapturedOutput {
    pub stdout: File,
    pub stderr: File,
    pub exit_code: u8,
}

fn path(directory: &Path, generation: TaskGeneration, stream: &str) -> PathBuf {
    directory.join(format!("{stream}-{}", generation.get()))
}

fn inspect(file: &mut File) -> Result<Stream> {
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(0))?;
    let mut hash = Sha256::new();
    let bytes = std::io::copy(file, &mut HashWriter(&mut hash))?;
    file.seek(SeekFrom::Start(0))?;
    Ok(Stream {
        digest: Digest(hash.finalize().into()),
        bytes,
    })
}

struct HashWriter<'hash>(&'hash mut Sha256);
impl Write for HashWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn publish(
    directory: &Path,
    generation: TaskGeneration,
    status: ExitCode,
    writer: &mut impl Write,
) -> Result<()> {
    let exit_code = if status == ExitCode::SUCCESS {
        0
    } else if status == ExitCode::FAILURE {
        1
    } else {
        2
    };
    let mut stdout = artifact::open_private(&path(directory, generation, "stdout"), u64::MAX)?;
    let mut stderr = artifact::open_private(&path(directory, generation, "stderr"), u64::MAX)?;
    let manifest = Manifest {
        schema: 1,
        exit_code,
        stdout: inspect(&mut stdout)?,
        stderr: inspect(&mut stderr)?,
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
    let mut reader = BufReader::new(bytes.as_slice());
    let mut header = String::new();
    reader.read_line(&mut header)?;
    let mut body = Vec::new();
    reader.read_to_end(&mut body)?;
    let manifest: Manifest = serde_json::from_slice(&body)?;
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
    if inspect(&mut stdout)? != manifest.stdout || inspect(&mut stderr)? != manifest.stderr {
        return Err("captured command output does not match its signed result".into());
    }
    Ok(CapturedOutput {
        stdout,
        stderr,
        exit_code: manifest.exit_code,
    })
}
