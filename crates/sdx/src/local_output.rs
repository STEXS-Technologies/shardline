//! Atomic publication of completed file downloads.

use std::{fs, io, path::Path};

fn invalid_path() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "invalid download destination")
}

fn check_target(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => Ok(()),
        Ok(_metadata) => Err(invalid_path()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
pub(crate) fn write_atomic(dest: &Path, bytes: &[u8]) -> io::Result<()> {
    use shardline_storage::{
        AnchoredTarget, ensure_parent_path_matches_anchor, open_directory_chain, remove_if_present,
        rename_at, resolve_platform_symlinks, sync_parent_directory, write_anchored_temporary_file,
    };

    let dest = resolve_platform_symlinks(dest);
    let name = dest.file_name().ok_or_else(invalid_path)?;
    let parent = dest
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    // Walk every component without following symlinks, then perform all writes
    // through the held directory descriptor even if its logical path is moved.
    let directory = open_directory_chain(parent, false, None, invalid_path)?;
    let anchored = AnchoredTarget::new(directory, parent.to_path_buf(), name.to_os_string());
    check_target(&anchored.final_path())?;
    let temporary = write_anchored_temporary_file(&anchored, bytes, Some(0o600))?;
    let result = (|| {
        ensure_parent_path_matches_anchor(&anchored, "download parent directory changed")?;
        check_target(&anchored.final_path())?;
        rename_at(
            anchored.parent_dir(),
            temporary.file_name().ok_or_else(invalid_path)?,
            anchored.file_name(),
        )?;
        sync_parent_directory(&anchored)?;
        ensure_parent_path_matches_anchor(&anchored, "download parent directory changed")
    })();
    // On any failure before rename, the complete old destination survives and
    // temporary bytes are removed. Once renamed, publication is already atomic.
    let cleanup = remove_if_present(&temporary);
    result?;
    cleanup.map(|_removed| ())
}

#[cfg(not(unix))]
pub(crate) fn write_atomic(dest: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;

    let parent = dest
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    for ancestor in parent
        .ancestors()
        .filter(|path| !path.as_os_str().is_empty())
    {
        let metadata = fs::symlink_metadata(ancestor)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(invalid_path());
        }
    }
    check_target(dest)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    check_target(dest)?;
    temporary.persist(dest).map_err(|error| error.error)?;
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::write_atomic;
    use shardline_storage::{LocalPublishBoundary, LocalPublishFault, arm_fault};

    #[test]
    fn failed_write_or_sync_preserves_destination_and_cleans_temporary() {
        let dir = tempfile::tempdir().unwrap();
        let dest = shardline_storage::resolve_platform_symlinks(&dir.path().join("download"));
        std::fs::write(&dest, b"original").unwrap();
        for (boundary, fault) in [
            (
                LocalPublishBoundary::DuringTemporaryWrite,
                LocalPublishFault::PartialWrite,
            ),
            (
                LocalPublishBoundary::BeforeTemporarySync,
                LocalPublishFault::SyncFailure,
            ),
        ] {
            let _guard = arm_fault(dest.clone(), boundary, fault);
            assert!(write_atomic(&dest, b"replacement").is_err());
            assert_eq!(std::fs::read(&dest).unwrap(), b"original");
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        }
    }

    #[test]
    fn symlinked_ancestor_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir_all(real.join("nested")).unwrap();
        std::os::unix::fs::symlink(&real, dir.path().join("link")).unwrap();
        assert!(write_atomic(&dir.path().join("link/nested/download"), b"replacement").is_err());
        assert!(!real.join("nested/download").exists());
    }
}
