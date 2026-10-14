use penelope::core::task::{Digest, TaskGeneration, TaskScope, runtime::TaskPortError};
use rustix::{
    fd::OwnedFd,
    process::{Pid, PidfdFlags, Signal, pidfd_open, pidfd_send_signal},
};
use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

pub(super) fn io_error(_error: std::io::Error) -> TaskPortError {
    TaskPortError::Unavailable
}

pub(super) fn boot_id() -> Result<String, TaskPortError> {
    let value = fs::read_to_string("/proc/sys/kernel/random/boot_id").map_err(io_error)?;
    let value = value.trim();
    if value.len() != 36 {
        return Err(TaskPortError::Runtime);
    }
    Ok(value.to_owned())
}

pub(super) fn birth(pid: u32) -> Result<Option<String>, TaskPortError> {
    let text = match fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(error)),
    };
    // comm is parenthesized and may contain spaces or ')'. Field 22 follows
    // the final ')' and nineteen fields beginning with process state (field 3).
    let tail = text.rsplit_once(')').ok_or(TaskPortError::Corrupt)?.1;
    if matches!(tail.split_whitespace().next(), Some("Z" | "X")) {
        return Ok(None);
    }
    let ticks = tail
        .split_whitespace()
        .nth(19)
        .ok_or(TaskPortError::Corrupt)?;
    ticks
        .parse::<u64>()
        .map_err(|_error| TaskPortError::Corrupt)?;
    Ok(Some(format!("{}:{ticks}", boot_id()?)))
}

pub(super) fn process_handle(pid: u32, expected: &str) -> Result<Option<OwnedFd>, TaskPortError> {
    let pid_value = Pid::from_raw(i32::try_from(pid).map_err(|_error| TaskPortError::Corrupt)?)
        .ok_or(TaskPortError::Corrupt)?;
    let handle = match pidfd_open(pid_value, PidfdFlags::empty()) {
        Ok(handle) => handle,
        Err(rustix::io::Errno::SRCH) => return Ok(None),
        Err(_error) => return Err(TaskPortError::Runtime),
    };
    if birth(pid)?.as_deref() != Some(expected) {
        return Ok(None);
    }
    Ok(Some(handle))
}

pub(super) fn stop(pid: u32, expected: &str) -> Result<(), TaskPortError> {
    if let Some(handle) = process_handle(pid, expected)? {
        match pidfd_send_signal(&handle, Signal::KILL) {
            Ok(()) | Err(rustix::io::Errno::SRCH) => {}
            Err(_error) => return Err(TaskPortError::Runtime),
        }
    }
    Ok(())
}

pub(super) fn scope_key(scope: TaskScope) -> Result<Vec<u8>, TaskPortError> {
    serde_json::to_vec(&scope).map_err(|_error| TaskPortError::Corrupt)
}

pub(super) fn gate_name(
    scope: TaskScope,
    generation: TaskGeneration,
) -> Result<String, TaskPortError> {
    Ok(format!(
        "worker-{}-{}.lock",
        hex::encode(Digest::of(&scope_key(scope)?).0),
        generation.get()
    ))
}

pub(super) fn secure_file(path: &Path) -> Result<File, TaskPortError> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(io_error)?;
    let metadata = file.metadata().map_err(io_error)?;
    if !metadata.is_file()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o077 != 0
    {
        return Err(TaskPortError::Conflict);
    }
    Ok(file)
}

pub(super) fn executable_digest(path: &Path) -> Result<Digest, TaskPortError> {
    use sha2::{Digest as _, Sha256};
    let mut file = File::open(path).map_err(io_error)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 16 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(io_error)?;
        if count == 0 {
            break;
        }
        hash.update(buffer.get(..count).ok_or(TaskPortError::Corrupt)?);
    }
    Ok(Digest(hash.finalize().into()))
}
