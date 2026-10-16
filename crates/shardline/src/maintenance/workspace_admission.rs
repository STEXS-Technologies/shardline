//! Bounded admission of new private tracking state; never execution authority.

use super::{Result, archive_lease::ArchiveLease, artifact};
use std::{
    io::{Error, ErrorKind},
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
    // Reuse the existing directory inode and export lock. No lock file,
    // background service or blocking wait is needed for optional admission.
    let lease = ArchiveLease::try_open(state)?
        .ok_or_else(|| Error::new(ErrorKind::WouldBlock, "private tracking admission is busy"))?;
    // A previously admitted workspace consumes no additional slot. Its normal
    // ownership and path checks still apply; no file is deleted or overwritten.
    match std::fs::symlink_metadata(directory) {
        Ok(_) => return artifact::existing_private_directory(directory),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut retained = 0_usize;
    let mut scanned = 0_usize;
    count_retained(state, limit, &mut retained, &mut scanned)?;
    let retired = state.join(super::cleanup::RETIRED);
    match std::fs::symlink_metadata(&retired) {
        Ok(_) => {
            artifact::existing_private_directory(&retired)?;
            count_retained(&retired, limit, &mut retained, &mut scanned)?;
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    // Creation and retirement are serialized on this root. Moving a workspace
    // into the cleanup queue therefore cannot hide it from the combined budget.
    lease.require_root(state)?;
    artifact::private_directory(directory)
}

fn count_retained(
    root: &Path,
    limit: usize,
    retained: &mut usize,
    scanned: &mut usize,
) -> Result<()> {
    for entry in std::fs::read_dir(root)? {
        if *scanned >= limit.saturating_mul(2) {
            return Err("private tracking archive inspection budget reached; retained evidence is preserved".into());
        }
        *scanned = scanned.saturating_add(1);
        let entry = entry?;
        if entry.file_name().to_str().is_some_and(|name| {
            name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
        }) {
            *retained = retained.saturating_add(1);
            if *retained >= limit {
                return Err(
                    "private tracking archive capacity reached; retained evidence is preserved"
                        .into(),
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
