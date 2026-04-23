#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::{
    fs::{self, OpenOptions},
    io::{self, ErrorKind, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use shardline_storage::anchored_fs::{
    AnchoredPathOptions, AnchoredTarget,
    ensure_parent_path_matches_anchor as ensure_parent_path_matches_anchor_shared,
    open_anchored_target as open_anchored_target_shared, open_new_file as open_new_file_shared,
    remove_if_present, write_anchored_temporary_file as write_anchored_temporary_file_shared,
};

static TEMPORARY_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

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

pub(crate) fn write_file_atomically(root: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        write_file_atomically_unix(root, path, bytes)
    }

    #[cfg(not(unix))]
    {
        let parent = path.parent().ok_or_else(invalid_local_path_error)?;
        fs::create_dir_all(parent)?;
        let temporary = write_temporary_file(path, bytes)?;
        fs::rename(temporary, path)?;
        Ok(())
    }
}

pub(crate) fn write_new_file(root: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        write_new_file_unix(root, path, bytes)
    }

    #[cfg(not(unix))]
    {
        let parent = path.parent().ok_or_else(invalid_local_path_error)?;
        fs::create_dir_all(parent)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        file.write_all(bytes)?;
        file.flush()?;
        Ok(())
    }
}

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

#[cfg(unix)]
fn write_file_atomically_unix(root: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    let anchored = open_anchored_target(root, path)?;
    run_before_local_write_hook(&anchored.logical_path());
    let temporary = write_anchored_temporary_file(&anchored, bytes)?;
    let final_path = anchored.final_path();
    match fs::rename(&temporary, &final_path) {
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
fn write_new_file_unix(root: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    let anchored = open_anchored_target(root, path)?;
    run_before_local_write_hook(&anchored.logical_path());
    let final_path = anchored.final_path();
    let mut file = open_new_file(&final_path)?;
    file.write_all(bytes)?;
    file.flush()?;
    if let Err(error) = ensure_parent_path_matches_anchor(&anchored) {
        remove_if_present(&final_path)?;
        return Err(error);
    }

    Ok(())
}

#[cfg(unix)]
fn open_anchored_target(root: &Path, path: &Path) -> io::Result<AnchoredTarget> {
    open_anchored_target_shared(root, path, anchored_path_options(), invalid_local_path_error)
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
fn open_new_file(path: &Path) -> io::Result<std::fs::File> {
    open_new_file_shared(path, anchored_path_options().file_mode)
}

#[cfg(unix)]
const fn anchored_path_options() -> AnchoredPathOptions {
    AnchoredPathOptions::new(None, None)
}

fn invalid_local_path_error() -> io::Error {
    io::Error::new(
        ErrorKind::InvalidData,
        "local filesystem path must remain under the configured root",
    )
}
