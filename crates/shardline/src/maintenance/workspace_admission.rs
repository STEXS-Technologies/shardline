//! Bounded admission of new private tracking state; never execution authority.

use super::{Result, artifact};
use std::{
    fs::{File, OpenOptions, TryLockError},
    io::{Error, ErrorKind},
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

const MAX_PRIVATE_WORKSPACES: usize = 4096;

pub(super) fn ensure(state: &Path, directory: &Path) -> Result<()> {
    ensure_with_limit(state, directory, MAX_PRIVATE_WORKSPACES)
}

fn ensure_with_limit(state: &Path, directory: &Path, limit: usize) -> Result<()> {
    if directory.parent() != Some(state)
        || !directory
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
    {
        return Err("invalid private tracking workspace".into());
    }
    artifact::private_directory(state)?;
    // Reuse the existing directory inode and export lock. No lock file,
    // background service or blocking wait is needed for optional admission.
    let lease: File = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(state)?;
    match lease.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            return Err(
                Error::new(ErrorKind::WouldBlock, "private tracking admission is busy").into(),
            );
        }
        Err(TryLockError::Error(error)) => return Err(error.into()),
    }
    // A previously admitted workspace consumes no additional slot. Its normal
    // ownership and path checks still apply; no file is deleted or overwritten.
    match std::fs::symlink_metadata(directory) {
        Ok(_) => return artifact::private_directory(directory),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut retained = 0_usize;
    for (scanned, entry) in std::fs::read_dir(state)?.enumerate() {
        if scanned >= limit.saturating_mul(2) {
            return Err("private tracking archive inspection budget reached; retained evidence is preserved".into());
        }
        let entry = entry?;
        if entry.file_name().to_str().is_some_and(|name| {
            name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
        }) {
            retained = retained.saturating_add(1);
            if retained >= limit {
                return Err(
                    "private tracking archive capacity reached; retained evidence is preserved"
                        .into(),
                );
            }
        }
    }
    // Creation is serialized with other admissions; the lease ends before the
    // original handler starts. Existing domain barriers authorize application work.
    artifact::private_directory(directory)
}

#[cfg(test)]
mod tests;
