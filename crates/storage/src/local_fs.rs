#[cfg(test)]
use std::path::PathBuf;
#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind, Read},
    path::Path,
};

#[cfg(not(unix))]
use std::io::Write;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

#[cfg(unix)]
use crate::anchored_fs::{
    AnchoredPathOptions, AnchoredTarget,
    ensure_parent_path_matches_anchor as ensure_parent_path_matches_anchor_shared,
    open_anchored_target as open_anchored_target_shared, remove_if_present,
    write_anchored_temporary_file as write_anchored_temporary_file_shared,
};

#[cfg(unix)]
const LOCAL_DIRECTORY_MODE: u32 = 0o700;
#[cfg(unix)]
const LOCAL_FILE_MODE: u32 = 0o600;

#[cfg(test)]
type LocalWriteHook = Box<dyn FnOnce() + Send>;

#[cfg(test)]
struct LocalWriteHookRegistration {
    path: PathBuf,
    hook: LocalWriteHook,
}

#[cfg(test)]
type LocalWriteHookSlot = Option<LocalWriteHookRegistration>;

#[cfg(test)]
static BEFORE_LOCAL_WRITE_HOOK: LazyLock<Mutex<LocalWriteHookSlot>> =
    LazyLock::new(|| Mutex::new(None));

#[cfg(test)]
pub(crate) fn set_before_local_write_hook(path: PathBuf, hook: impl FnOnce() + Send + 'static) {
    let mut slot = match BEFORE_LOCAL_WRITE_HOOK.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *slot = Some(LocalWriteHookRegistration {
        path,
        hook: Box::new(hook),
    });
}

#[cfg(test)]
fn run_before_local_write_hook(path: &Path) {
    let hook = match BEFORE_LOCAL_WRITE_HOOK.lock() {
        Ok(mut guard) => take_matching_local_write_hook(&mut guard, path),
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            take_matching_local_write_hook(&mut guard, path)
        }
    };
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn take_matching_local_write_hook(
    slot: &mut LocalWriteHookSlot,
    path: &Path,
) -> Option<LocalWriteHook> {
    if slot
        .as_ref()
        .is_none_or(|registration| registration.path != path)
    {
        return None;
    }

    slot.take().map(|registration| registration.hook)
}

#[cfg(not(test))]
const fn run_before_local_write_hook(_path: &Path) {}

pub(crate) fn hard_link_file_if_absent(
    root: &Path,
    path: &Path,
    temporary: &Path,
) -> io::Result<()> {
    #[cfg(unix)]
    {
        hard_link_file_if_absent_unix(root, path, temporary)
    }

    #[cfg(not(unix))]
    {
        let _ = root;
        let parent = path.parent().ok_or_else(invalid_local_path_error)?;
        fs::create_dir_all(parent)?;
        fs::hard_link(temporary, path)
    }
}

pub(crate) enum PutBytesIfAbsentOutcome {
    Inserted,
    AlreadyExists,
}

pub(crate) fn put_bytes_if_absent(
    root: &Path,
    path: &Path,
    bytes: &[u8],
) -> io::Result<PutBytesIfAbsentOutcome> {
    #[cfg(unix)]
    {
        put_bytes_if_absent_unix(root, path, bytes)
    }

    #[cfg(not(unix))]
    {
        let _ = root;
        let parent = path.parent().ok_or_else(invalid_local_path_error)?;
        fs::create_dir_all(parent)?;
        match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(mut file) => {
                file.write_all(bytes)?;
                file.flush()?;
                Ok(PutBytesIfAbsentOutcome::Inserted)
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                let file = File::open(path)?;
                ensure_file_matches_bytes(file, bytes)?;
                Ok(PutBytesIfAbsentOutcome::AlreadyExists)
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(unix)]
fn hard_link_file_if_absent_unix(root: &Path, path: &Path, temporary: &Path) -> io::Result<()> {
    let anchored = open_anchored_target(root, path)?;
    run_before_local_write_hook(&anchored.logical_path());
    let final_path = anchored.final_path();
    fs::hard_link(temporary, &final_path)?;
    if let Err(error) = ensure_parent_path_matches_anchor(&anchored) {
        remove_if_present(&final_path)?;
        return Err(error);
    }

    Ok(())
}

#[cfg(unix)]
fn put_bytes_if_absent_unix(
    root: &Path,
    path: &Path,
    bytes: &[u8],
) -> io::Result<PutBytesIfAbsentOutcome> {
    let anchored = open_anchored_target(root, path)?;
    run_before_local_write_hook(&anchored.logical_path());
    let final_path = anchored.final_path();

    match open_existing_regular_file(&final_path) {
        Ok(file) => {
            ensure_file_matches_bytes(file, bytes)?;
            ensure_parent_path_matches_anchor(&anchored)?;
            return Ok(PutBytesIfAbsentOutcome::AlreadyExists);
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let temporary =
        write_anchored_temporary_file_shared(&anchored, bytes, anchored_path_options().file_mode)?;
    match fs::hard_link(&temporary, &final_path) {
        Ok(()) => {
            remove_if_present(&temporary)?;
            if let Err(mismatch_error) = ensure_parent_path_matches_anchor(&anchored) {
                remove_if_present(&final_path)?;
                return Err(mismatch_error);
            }
            Ok(PutBytesIfAbsentOutcome::Inserted)
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            remove_if_present(&temporary)?;
            let existing = open_existing_regular_file(&final_path)?;
            ensure_file_matches_bytes(existing, bytes)?;
            if let Err(mismatch_error) = ensure_parent_path_matches_anchor(&anchored) {
                remove_if_present(&final_path)?;
                return Err(mismatch_error);
            }
            Ok(PutBytesIfAbsentOutcome::AlreadyExists)
        }
        Err(error) => {
            remove_if_present(&temporary)?;
            Err(error)
        }
    }
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
fn ensure_parent_path_matches_anchor(anchored: &AnchoredTarget) -> io::Result<()> {
    ensure_parent_path_matches_anchor_shared(
        anchored,
        "local filesystem path changed during anchored write",
    )
}

#[cfg(unix)]
fn open_existing_regular_file(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "local object path must be a regular file and must not be a symlink",
        ));
    }
    Ok(file)
}

#[cfg(unix)]
const fn anchored_path_options() -> AnchoredPathOptions {
    AnchoredPathOptions::new(Some(LOCAL_DIRECTORY_MODE), Some(LOCAL_FILE_MODE))
}

fn ensure_file_matches_bytes(mut file: File, expected: &[u8]) -> io::Result<()> {
    let mut actual = Vec::new();
    file.read_to_end(&mut actual)?;
    if actual == expected {
        return Ok(());
    }

    Err(io::Error::new(
        ErrorKind::AlreadyExists,
        "existing object did not match expected bytes",
    ))
}

fn invalid_local_path_error() -> io::Error {
    io::Error::new(
        ErrorKind::InvalidData,
        "local filesystem path must remain under the configured root",
    )
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::fs::metadata;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[cfg(unix)]
    use super::{PutBytesIfAbsentOutcome, put_bytes_if_absent};

    #[cfg(unix)]
    #[test]
    fn put_bytes_if_absent_creates_private_file_and_directory_modes() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let root = sandbox.path().join("root");
        let path = root.join("nested").join("chunk.bin");

        let wrote = put_bytes_if_absent(&root, &path, b"payload");
        assert!(matches!(wrote, Ok(PutBytesIfAbsentOutcome::Inserted)));

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
