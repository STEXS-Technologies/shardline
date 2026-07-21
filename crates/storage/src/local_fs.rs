#[cfg(test)]
use std::path::PathBuf;
#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind},
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
        path.strip_prefix(root)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path escapes root"))?;
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
        path.strip_prefix(root)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path escapes root"))?;
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

pub(crate) fn write_bytes_atomically(root: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        write_bytes_atomically_unix(root, path, bytes)
    }

    #[cfg(not(unix))]
    {
        // Defense-in-depth: ensure the path stays within the root directory.
        path.strip_prefix(root)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path escapes root"))?;
        let parent = path.parent().ok_or_else(invalid_local_path_error)?;
        fs::create_dir_all(parent)?;
        let temporary = write_temporary_file(path, bytes)?;
        fs::rename(temporary, path)?;
        Ok(())
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
fn write_bytes_atomically_unix(root: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    let anchored = open_anchored_target(root, path)?;
    run_before_local_write_hook(&anchored.logical_path());
    let final_path = anchored.final_path();
    let temporary =
        write_anchored_temporary_file_shared(&anchored, bytes, anchored_path_options().file_mode)?;
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

#[cfg(not(unix))]
fn write_temporary_file(path: &Path, bytes: &[u8]) -> io::Result<std::path::PathBuf> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id();
    let seq = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = path.with_extension(format!("tmp-{pid}-{seq}-{now_nanos}"));
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
    {
        Ok(mut file) => {
            file.write_all(bytes)?;
            file.flush()?;
            Ok(temporary)
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            fs::remove_file(&temporary)?;
            write_temporary_file(path, bytes)
        }
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
const fn anchored_path_options() -> AnchoredPathOptions {
    AnchoredPathOptions::new(Some(LOCAL_DIRECTORY_MODE), Some(LOCAL_FILE_MODE))
}

fn ensure_file_matches_bytes(mut file: File, expected: &[u8]) -> io::Result<()> {
    use std::io::Read;
    const LOCAL_FILE_COMPARE_CHUNK_BYTES: usize = 256 * 1024;
    let mut offset = 0_usize;
    let mut buffer = vec![0_u8; LOCAL_FILE_COMPARE_CHUNK_BYTES];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            if offset != expected.len() {
                break;
            }
            return Ok(());
        }
        let chunk = buffer
            .get(..read)
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "read exceeded buffer length"))?;
        let expected_chunk = expected.get(offset..).and_then(|s| s.get(..read));
        match expected_chunk {
            Some(ec) if chunk == ec => {}
            _ => break,
        }
        offset = offset.checked_add(read).ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                "offset overflow while comparing file contents",
            )
        })?;
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

    // ── put_bytes_if_absent ───────────────────────────────────────────

    #[cfg(unix)]
    #[test]
    fn put_bytes_if_absent_inserts_and_idempotent() {
        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("root");
        let path = root.join("aa").join("chunk.bin");

        let first = put_bytes_if_absent(&root, &path, b"hello");
        assert!(matches!(first, Ok(PutBytesIfAbsentOutcome::Inserted)));

        let second = put_bytes_if_absent(&root, &path, b"hello");
        assert!(matches!(second, Ok(PutBytesIfAbsentOutcome::AlreadyExists)));
    }

    #[cfg(unix)]
    #[test]
    fn put_bytes_if_absent_rejects_different_bytes() {
        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("root");
        let path = root.join("aa").join("chunk.bin");

        put_bytes_if_absent(&root, &path, b"hello").unwrap();

        let result = put_bytes_if_absent(&root, &path, b"world");
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn put_bytes_if_absent_creates_parent_directories() {
        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("root");
        let path = root
            .join("deep")
            .join("nested")
            .join("dir")
            .join("chunk.bin");

        let result = put_bytes_if_absent(&root, &path, b"data");
        assert!(matches!(result, Ok(PutBytesIfAbsentOutcome::Inserted)));
        assert!(path.exists());
    }

    // ── write_bytes_atomically ────────────────────────────────────────

    #[cfg(unix)]
    #[test]
    fn write_bytes_atomically_creates_file() {
        use super::write_bytes_atomically;

        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("root");
        let path = root.join("aa").join("output.bin");

        write_bytes_atomically(&root, &path, b"atomic payload").unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"atomic payload");
    }

    #[cfg(unix)]
    #[test]
    fn write_bytes_atomically_overwrites_existing() {
        use super::write_bytes_atomically;

        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("root");
        let path = root.join("aa").join("output.bin");

        write_bytes_atomically(&root, &path, b"first").unwrap();
        write_bytes_atomically(&root, &path, b"second").unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"second");
    }

    #[cfg(unix)]
    #[test]
    fn write_bytes_atomically_empty_bytes() {
        use super::write_bytes_atomically;

        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("root");
        let path = root.join("aa").join("empty.bin");

        write_bytes_atomically(&root, &path, b"").unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert!(contents.is_empty());
    }

    // ── hard_link_file_if_absent ──────────────────────────────────────

    #[cfg(unix)]
    #[test]
    fn hard_link_file_if_absent_links_and_errors_on_duplicate() {
        use super::hard_link_file_if_absent;

        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("root");
        let temporary = sandbox.path().join("source.tmp");
        std::fs::write(&temporary, b"linked").unwrap();
        let dest = root.join("aa").join("linked.bin");

        // First hard link succeeds
        hard_link_file_if_absent(&root, &dest, &temporary).unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"linked");

        // Second hard link returns AlreadyExists (callers handle this)
        let err = hard_link_file_if_absent(&root, &dest, &temporary).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
    }

    // ── ensure_file_matches_bytes ─────────────────────────────────────

    #[cfg(unix)]
    #[test]
    fn ensure_file_matches_bytes_ok_for_identical() {
        use super::ensure_file_matches_bytes;
        use std::fs::File;

        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("match.bin");
        std::fs::write(&path, b"test data").unwrap();
        let file = File::open(&path).unwrap();

        assert!(ensure_file_matches_bytes(file, b"test data").is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn ensure_file_matches_bytes_err_for_mismatch() {
        use super::ensure_file_matches_bytes;
        use std::fs::File;

        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("mismatch.bin");
        std::fs::write(&path, b"actual").unwrap();
        let file = File::open(&path).unwrap();

        assert!(ensure_file_matches_bytes(file, b"expected").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn ensure_file_matches_bytes_err_for_length_mismatch() {
        use super::ensure_file_matches_bytes;
        use std::fs::File;

        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("short.bin");
        std::fs::write(&path, b"ab").unwrap();
        let file = File::open(&path).unwrap();

        assert!(ensure_file_matches_bytes(file, b"abcdef").is_err());
    }

    // ── open_existing_regular_file ────────────────────────────────────

    #[cfg(unix)]
    #[test]
    fn open_existing_regular_file_rejects_directory() {
        use super::open_existing_regular_file;
        use std::io::ErrorKind;

        let sandbox = tempfile::tempdir().unwrap();
        let dir_path = sandbox.path().join("a_directory");
        std::fs::create_dir(&dir_path).unwrap();

        let result = open_existing_regular_file(&dir_path);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[cfg(unix)]
    #[test]
    fn open_existing_regular_file_rejects_nonexistent() {
        use super::open_existing_regular_file;
        use std::io::ErrorKind;

        let sandbox = tempfile::tempdir().unwrap();
        let missing = sandbox.path().join("does_not_exist.bin");

        let result = open_existing_regular_file(&missing);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::NotFound);
    }

    // ── ensure_file_matches_bytes read error ──────────────────────────

    #[cfg(unix)]
    #[test]
    fn ensure_file_matches_bytes_errors_on_io_failure() {
        use super::ensure_file_matches_bytes;
        use std::fs::File;

        // Open a directory as a file — reading will fail
        let sandbox = tempfile::tempdir().unwrap();
        let dir_path = sandbox.path().join("dir");
        std::fs::create_dir(&dir_path).unwrap();
        let file = File::open(&dir_path).unwrap();

        let result = ensure_file_matches_bytes(file, b"anything");
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn ensure_file_matches_bytes_empty_file_with_empty_expected_ok() {
        use super::ensure_file_matches_bytes;
        use std::fs::File;

        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("empty.bin");
        std::fs::write(&path, b"").unwrap();
        let file = File::open(&path).unwrap();

        assert!(ensure_file_matches_bytes(file, b"").is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn ensure_file_matches_bytes_empty_file_with_nonempty_expected_fails() {
        use super::ensure_file_matches_bytes;
        use std::fs::File;

        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("empty2.bin");
        std::fs::write(&path, b"").unwrap();
        let file = File::open(&path).unwrap();

        let result = ensure_file_matches_bytes(file, b"non-empty");
        assert!(result.is_err());
    }

    // ── invalid_local_path_error returns the correct error ────────────

    #[test]
    fn invalid_local_path_error_returns_invalid_data() {
        let err = super::invalid_local_path_error();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("root"),
            "error message should mention root: {}",
            err
        );
    }

    // ── put_bytes_if_absent AlreadyExists race via concurrent writes ───

    #[cfg(unix)]
    #[test]
    fn put_bytes_if_absent_concurrent_race_handles_already_exists() {
        use super::{PutBytesIfAbsentOutcome, put_bytes_if_absent};
        use std::sync::{Arc, Barrier};

        let sandbox = Arc::new(tempfile::tempdir().unwrap());
        let root = Arc::new(sandbox.path().join("root"));

        let path = Arc::new(root.join("race.bin"));
        let barrier = Arc::new(Barrier::new(2));

        let t1 = {
            let r = root.clone();
            let p = path.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                put_bytes_if_absent(&r, &p, b"data")
            })
        };

        let t2 = {
            let r = root;
            let p = path;
            let b = barrier;
            std::thread::spawn(move || {
                b.wait();
                put_bytes_if_absent(&r, &p, b"data")
            })
        };

        let result1 = t1.join().unwrap();
        let result2 = t2.join().unwrap();

        // Both should succeed — one inserts, the other detects AlreadyExists
        assert!(
            result1
                .as_ref()
                .is_ok_and(|o| matches!(o, PutBytesIfAbsentOutcome::Inserted))
                || result2
                    .as_ref()
                    .is_ok_and(|o| matches!(o, PutBytesIfAbsentOutcome::Inserted)),
            "at least one should have inserted"
        );
        assert!(
            result1.is_ok() && result2.is_ok(),
            "both results should be Ok"
        );
    }

    #[cfg(unix)]
    #[test]
    fn put_bytes_if_absent_concurrent_different_bytes_mismatch() {
        use super::put_bytes_if_absent;
        use std::sync::{Arc, Barrier};

        let sandbox = Arc::new(tempfile::tempdir().unwrap());
        let root = Arc::new(sandbox.path().join("root2"));
        let path = Arc::new(root.join("race-diff.bin"));
        let barrier = Arc::new(Barrier::new(2));

        let t1 = {
            let r = root.clone();
            let p = path.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                put_bytes_if_absent(&r, &p, b"data1")
            })
        };

        let t2 = {
            let r = root;
            let p = path;
            let b = barrier;
            std::thread::spawn(move || {
                b.wait();
                put_bytes_if_absent(&r, &p, b"data2")
            })
        };

        let result1 = t1.join().unwrap();
        let result2 = t2.join().unwrap();

        // At least one should succeed, the other may fail with content mismatch
        assert!(result1.is_ok() || result2.is_ok());
        // If one failed, the error should be about content mismatch
        if let Err(e) = &result1 {
            assert_eq!(e.kind(), std::io::ErrorKind::AlreadyExists);
        }
        if let Err(e) = &result2 {
            assert_eq!(e.kind(), std::io::ErrorKind::AlreadyExists);
        }
    }

    // ── write_bytes_atomically_unix rename error path ──────────────────

    #[cfg(unix)]
    #[test]
    fn write_bytes_atomically_rename_error_cleans_up() {
        use super::{set_before_local_write_hook, write_bytes_atomically};
        let _guard = HOOK_TEST_MUTEX
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap();

        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("root");
        let dest = root.join("sub").join("target.bin");

        // Hook creates a directory at the final path, causing rename to fail
        let dest_clone = dest.clone();
        set_before_local_write_hook(dest.clone(), move || {
            // Create a directory where the file should go
            let _ = std::fs::create_dir_all(&dest_clone);
        });

        let result = write_bytes_atomically(&root, &dest, b"payload");
        // Renaming a file to a path that is already a directory fails
        assert!(
            result.is_err(),
            "expected rename error when final path is a directory"
        );
    }

    // ── hard_link_file_if_absent with race via hook ───────────────────

    /// Serializes hook-based tests that use the global BEFORE_LOCAL_WRITE_HOOK.
    #[cfg(unix)]
    static HOOK_TEST_MUTEX: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();

    #[cfg(unix)]
    #[test]
    fn hard_link_file_if_absent_detects_parent_swap_via_hook() {
        use super::{hard_link_file_if_absent, set_before_local_write_hook};
        use std::os::unix::fs::symlink;
        let _guard = HOOK_TEST_MUTEX
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap();

        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("root");
        let temporary = sandbox.path().join("src.tmp");
        std::fs::write(&temporary, b"race test").unwrap();
        let dest = root.join("sub").join("target.bin");

        // Set up a hook that swaps the parent directory before final link verification
        let moved_parent = sandbox.path().join("moved-parent");
        std::fs::create_dir_all(root.join("sub")).unwrap();

        let escape_dir = sandbox.path().join("escape");
        std::fs::create_dir(&escape_dir).unwrap();

        let hook_root = root.clone();
        set_before_local_write_hook(dest.clone(), move || {
            let _ = std::fs::rename(hook_root.join("sub"), &moved_parent);
            let _ = symlink(&escape_dir, hook_root.join("sub"));
        });

        let result = hard_link_file_if_absent(&root, &dest, &temporary);
        // Should fail because the parent directory was swapped
        assert!(result.is_err(), "expected error from parent swap, got Ok");
    }

    // ── test hook mechanism ───────────────────────────────────────────

    #[cfg(unix)]
    #[test]
    fn set_before_local_write_hook_is_called_on_write() {
        use super::{set_before_local_write_hook, write_bytes_atomically};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        let _guard = HOOK_TEST_MUTEX
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap();

        let called = Arc::new(AtomicBool::new(false));
        let flag = called.clone();

        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("root");
        let path = root.join("aa").join("hooked.bin");

        // Register a hook that fires for this specific path
        set_before_local_write_hook(path.clone(), move || {
            flag.store(true, Ordering::SeqCst);
        });

        write_bytes_atomically(&root, &path, b"hooked").unwrap();

        assert!(
            called.load(Ordering::SeqCst),
            "before-local-write hook was not called"
        );
    }

    // ── put_bytes_if_absent with directory target (non-NotFound error) ──

    #[cfg(unix)]
    #[test]
    fn put_bytes_if_absent_directory_target_fails() {
        use super::put_bytes_if_absent;

        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("root");
        let dir_target = root.join("a_dir");
        std::fs::create_dir_all(&dir_target).unwrap();

        // The target path is an existing directory, not a file.
        // open_existing_regular_file will open it (dirs can be opened),
        // then is_file() returns false → returns Err(InvalidData).
        // put_bytes_if_absent_unix checks for ErrorKind::NotFound which
        // does NOT match InvalidData, so the error propagates.
        let result = put_bytes_if_absent(&root, &dir_target, b"data");
        assert!(result.is_err(), "expected error when target is a directory");
    }

    // ── write_bytes_atomically detects parent swap via hook ────────────

    #[cfg(unix)]
    #[test]
    fn write_bytes_atomically_detects_parent_swap() {
        use super::{set_before_local_write_hook, write_bytes_atomically};
        use std::os::unix::fs::symlink;
        let _guard = HOOK_TEST_MUTEX
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap();

        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("root");
        let dest = root.join("sub").join("target.bin");

        // Set up a hook that swaps the parent directory to a symlink escape
        let moved_parent = sandbox.path().join("moved-write-parent");
        std::fs::create_dir_all(root.join("sub")).unwrap();

        let escape_dir = sandbox.path().join("escape-write");
        std::fs::create_dir(&escape_dir).unwrap();
        let escape_dir_for_assert = escape_dir.clone();

        let hook_root = root.clone();
        set_before_local_write_hook(dest.clone(), move || {
            let _ = std::fs::rename(hook_root.join("sub"), &moved_parent);
            let _ = symlink(&escape_dir, hook_root.join("sub"));
        });

        let result = write_bytes_atomically(&root, &dest, b"payload");
        // Should fail because the parent directory was swapped
        assert!(result.is_err(), "expected error from parent swap, got Ok");
        // The escape dir should not contain the file
        assert!(
            !escape_dir_for_assert.join("target.bin").exists(),
            "file should not have been written to the attacker-controlled escape directory"
        );
    }

    // ── ensure_file_matches_bytes with empty expected and non-empty file ──

    #[cfg(unix)]
    #[test]
    fn ensure_file_matches_bytes_empty_expected_with_content_fails() {
        use super::ensure_file_matches_bytes;
        use std::fs::File;

        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("content-present.bin");
        std::fs::write(&path, b"some data").unwrap();
        let file = File::open(&path).unwrap();

        // expected is empty but file has content — read returns bytes,
        // then there are no more expected bytes → mismatch break → error
        let result = ensure_file_matches_bytes(file, b"");
        assert!(result.is_err());
    }

    // ── hard_link_file_if_absent with race via hook (anchor check fail after hard link) ──

    #[cfg(unix)]
    #[test]
    fn hard_link_file_if_absent_anchor_check_fails_after_link() {
        // This tests the ensure_parent_path_matches_anchor failure AFTER
        // a successful hard link (line 172-175 in hard_link_file_if_absent_unix).
        // The hook swaps the parent AFTER the hard link succeeds but
        // BEFORE the anchor verification.
        //
        // Hard links are tested with hook swaps in
        // hard_link_file_if_absent_detects_parent_swap_via_hook above.
        // That test already covers this path (hook runs before the
        // anchor check inside hard_link_file_if_absent_unix).
        //
        // This test verifies a slightly different timing: the hook makes
        // the parent a symlink duplicate so anchor check fails.
        use super::{hard_link_file_if_absent, set_before_local_write_hook};
        use std::os::unix::fs::symlink;
        let _guard = HOOK_TEST_MUTEX
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap();

        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("root");
        let temporary = sandbox.path().join("src.tmp");
        std::fs::write(&temporary, b"anchor test").unwrap();
        let dest = root.join("sub").join("target.bin");

        let moved_parent = sandbox.path().join("moved-link-anchor");
        std::fs::create_dir_all(root.join("sub")).unwrap();

        let escape_dir = sandbox.path().join("escape-link-anchor");
        std::fs::create_dir(&escape_dir).unwrap();

        let hook_root = root.clone();
        set_before_local_write_hook(dest.clone(), move || {
            let _ = std::fs::rename(hook_root.join("sub"), &moved_parent);
            let _ = symlink(&escape_dir, hook_root.join("sub"));
        });

        let result = hard_link_file_if_absent(&root, &dest, &temporary);
        // The hard link itself succeeds to /proc/self/fd/N/target.bin
        // in the ORIGINAL directory, but the anchor check detects the
        // parent was swapped (logical path is now a symlink) and fails.
        assert!(result.is_err(), "expected error from anchor check, got Ok");
    }
}
