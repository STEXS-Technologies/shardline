//! Existing Linux directory inode shared by every current local barrier owner.
//! This adds no root entry and keeps restricted legacy SDK roots writable when
//! their SQLite files and payload directories still permit writes.
use super::{ServerError, lock_local_file};
use std::{fs::File, os::unix::fs::OpenOptionsExt, path::Path};

pub(super) async fn acquire(root: &Path, exclusive: bool) -> Result<File, ServerError> {
    let root = root.to_path_buf();
    let directory = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&root)?;
        File::options()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(root)
    })
    .await
    .map_err(|error| ServerError::Io(std::io::Error::other(error)))??;
    lock_local_file(directory, exclusive).await
}

#[cfg(test)]
mod tests;
