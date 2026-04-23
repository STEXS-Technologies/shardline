#[cfg(not(unix))]
use std::fs::create_dir_all;
use std::{
    fs::rename,
    io::{self, ErrorKind},
    path::{Path, PathBuf},
};

#[cfg(not(unix))]
use std::{
    fs::OpenOptions,
    io::Write,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use shardline_storage::anchored_fs::{
    AnchoredPathOptions, AnchoredTarget,
    ensure_parent_path_matches_anchor as ensure_parent_path_matches_anchor_shared,
    open_anchored_target as open_anchored_target_shared, remove_if_present,
    write_anchored_temporary_file as write_anchored_temporary_file_shared,
};

#[cfg(not(unix))]
static TEMPORARY_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);
#[cfg(unix)]
const LOCAL_DIRECTORY_MODE: u32 = 0o700;
#[cfg(unix)]
const LOCAL_FILE_MODE: u32 = 0o600;

pub(crate) fn write_file_atomically(root: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        write_file_atomically_unix(root, path, bytes)
    }

    #[cfg(not(unix))]
    {
        let parent = path.parent().ok_or_else(invalid_local_path_error)?;
        create_dir_all(parent)?;
        let temporary = write_temporary_file(path, bytes)?;
        rename(temporary, path)?;
        Ok(())
    }
}

const fn run_before_local_write_hook(_path: &Path) {}

#[cfg(unix)]
fn write_file_atomically_unix(root: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    let anchored = open_anchored_target(root, path)?;
    run_before_local_write_hook(&anchored.logical_path());
    let temporary = write_anchored_temporary_file(&anchored, bytes)?;
    let final_path = anchored.final_path();
    match rename(&temporary, &final_path) {
        Ok(()) => {}
        Err(error) => {
            remove_if_present(&temporary)?;
            return Err(error);
        }
    }
    if let Err(error) = ensure_parent_path_matches_anchor(&anchored) {
        remove_if_present(&final_path)?;
        return Err(error);
    }

    Ok(())
}

#[cfg(unix)]
fn open_anchored_target(root: &Path, path: &Path) -> io::Result<AnchoredTarget> {
    open_anchored_target_shared(
        root,
        path,
        anchored_path_options(),
        invalid_local_path_error,
    )
}

#[cfg(unix)]
fn write_anchored_temporary_file(anchored: &AnchoredTarget, bytes: &[u8]) -> io::Result<PathBuf> {
    write_anchored_temporary_file_shared(anchored, bytes, anchored_path_options().file_mode)
}

#[cfg(unix)]
fn ensure_parent_path_matches_anchor(anchored: &AnchoredTarget) -> io::Result<()> {
    ensure_parent_path_matches_anchor_shared(
        anchored,
        "local filesystem path changed during anchored write",
    )
}

#[cfg(not(unix))]
fn write_temporary_file(path: &Path, bytes: &[u8]) -> io::Result<PathBuf> {
    loop {
        let temporary = {
            let counter = TEMPORARY_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
            let unix_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0_u128, |duration| duration.as_nanos());
            path.with_extension(format!("tmp-{unix_nanos}-{counter}"))
        };
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(mut file) => {
                file.write_all(bytes)?;
                file.flush()?;
                return Ok(temporary);
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
}

#[cfg(unix)]
const fn anchored_path_options() -> AnchoredPathOptions {
    AnchoredPathOptions::new(Some(LOCAL_DIRECTORY_MODE), Some(LOCAL_FILE_MODE))
}

fn invalid_local_path_error() -> io::Error {
    io::Error::new(
        ErrorKind::InvalidData,
        "local filesystem path must remain under the configured root",
    )
}

#[cfg(test)]
mod tests {
    use std::fs::metadata;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[cfg(unix)]
    use super::write_file_atomically;

    #[cfg(unix)]
    #[test]
    fn write_file_atomically_creates_private_file_and_directory_modes() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let root = sandbox.path().join("root");
        let path = root.join("nested").join("record.json");

        let wrote = write_file_atomically(&root, &path, br#"{"ok":true}"#);
        assert!(wrote.is_ok());

        let file_metadata = metadata(&path);
        assert!(file_metadata.is_ok());
        let Ok(file_metadata) = file_metadata else {
            return;
        };
        let directory_metadata = metadata(root.join("nested"));
        assert!(directory_metadata.is_ok());
        let Ok(directory_metadata) = directory_metadata else {
            return;
        };

        assert_eq!(file_metadata.permissions().mode() & 0o777, 0o600);
        assert_eq!(directory_metadata.permissions().mode() & 0o777, 0o700);
    }
}
