//! Nonblocking coordination of private archive bookkeeping, not application work.

use super::{Result, artifact};
use std::{
    fs::{File, OpenOptions, TryLockError},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

pub(super) struct ArchiveLease {
    file: File,
    root: PathBuf,
}

impl ArchiveLease {
    pub(super) fn try_open(root: &Path) -> Result<Option<Self>> {
        artifact::private_directory(root)?;
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(root)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
        let lease = Self {
            file,
            root: root.to_owned(),
        };
        lease.require_root(root)?;
        Ok(Some(lease))
    }

    pub(super) fn require_root(&self, root: &Path) -> Result<()> {
        let locked = self.file.metadata()?;
        let current = std::fs::symlink_metadata(root)?;
        if self.root != root
            || !current.is_dir()
            || locked.dev() != current.dev()
            || locked.ino() != current.ino()
            || locked.uid() != rustix::process::geteuid().as_raw()
            || locked.permissions().mode() & 0o077 != 0
        {
            return Err("private archive lease does not match its root".into());
        }
        Ok(())
    }
}
