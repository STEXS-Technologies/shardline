//! Retire only scratch whose full history has already been confirmed elsewhere.
use super::{Result, archive_lease::ArchiveLease, artifact, outbox};
use std::{
    fs::{self, File},
    io::ErrorKind,
    path::Path,
};

pub(super) const RETIRED: &str = "retired";

/// Call only after verified completion and successful history publication.
/// Rename before recursive deletion, so a partial deletion cannot turn a
/// confirmed job into an unrecognizable unfinished journal.
pub(super) fn completed(directory: &Path) -> Result<()> {
    let state = directory.parent().ok_or("missing maintenance state root")?;
    let lease = ArchiveLease::try_open(state)?.ok_or("private archive retirement is busy")?;
    completed_with_lease(directory, &lease)
}

/// The caller must have verified completion and successful history publication.
pub(super) fn completed_with_lease(directory: &Path, lease: &ArchiveLease) -> Result<()> {
    let state = directory.parent().ok_or("missing maintenance state root")?;
    lease.require_root(state)?;
    if let Err(error) = artifact::existing_private_directory(directory) {
        if error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == ErrorKind::NotFound)
        {
            return Ok(());
        }
        return Err(error);
    }
    let name = directory
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or("invalid completed task directory")?;
    let retired = state.join(RETIRED);
    artifact::private_directory(&retired)?;
    let destination = retired.join(name);
    match fs::symlink_metadata(&destination) {
        Ok(_) => return Err("retired task directory already exists".into()),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    fs::rename(directory, &destination)?;
    File::open(state)?.sync_all()?;
    File::open(&retired)?.sync_all()?;
    remove(&destination)
}

/// Remove at most 64 already-retired directories, with a fair persistent cursor.
/// This never reads unfinished journals, publishes history or executes work.
pub(super) fn retry(state: &Path) -> Result<()> {
    let retired = state.join(RETIRED);
    if !retired.exists() {
        return Ok(());
    }
    let Some(batch) = outbox::Batch::open(&retired)? else {
        return Ok(());
    };
    for directory in &batch.jobs {
        batch.advance(directory)?;
        if let Err(error) =
            artifact::existing_private_directory(directory).and_then(|()| remove(directory))
        {
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|io| io.kind() == ErrorKind::NotFound)
            {
                continue;
            }
            tracing::warn!(workspace = %directory.display(), "confirmed scratch cleanup remains unavailable; no application retry is authorized: {error}");
        }
    }
    batch.finish()
}

fn remove(directory: &Path) -> Result<()> {
    match fs::remove_dir_all(directory) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests;
