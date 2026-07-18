#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::{
    error::Error,
    fs::{self, File, remove_file, symlink_metadata},
    io::{self, ErrorKind, Write},
    path::{Path, PathBuf},
};

#[cfg(unix)]
const LOCAL_DIRECTORY_MODE: u32 = 0o700;
#[cfg(unix)]
const LOCAL_FILE_MODE: u32 = 0o600;

#[cfg(unix)]
use shardline_storage::anchored_fs::{
    AnchoredPathOptions, AnchoredTarget,
    ensure_parent_path_matches_anchor as ensure_parent_path_matches_anchor_shared, fd_child_path,
    open_directory_chain as open_directory_chain_shared, open_new_file as open_new_file_shared,
    remove_at, remove_if_present, rename_at, temporary_file_name,
};

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

/// Writes one local CLI output file through an anchored temporary path.
///
/// # Errors
///
/// Returns [`io::Error`] when the parent directory cannot be created or opened safely, the
/// output path is symlinked or otherwise non-regular, or the parent directory changes before
/// the final commit completes.
pub fn write_output_bytes(path: &Path, bytes: &[u8], create_parent: bool) -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut writer = AtomicOutputFile::create(path, create_parent)?;
        writer.write_all(bytes)?;
        writer.commit()
    }

    #[cfg(not(unix))]
    {
        if create_parent {
            ensure_parent_directory(path)?;
        }
        fs::write(path, bytes)
    }
}

pub(crate) fn remove_output_file_if_present(path: &Path) -> io::Result<bool> {
    #[cfg(unix)]
    {
        remove_output_file_if_present_unix(path)
    }

    #[cfg(not(unix))]
    {
        match remove_file(path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }
}

pub(crate) fn ensure_output_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        let _directory = open_directory_chain(path, true)?;
        Ok(())
    }

    #[cfg(not(unix))]
    {
        fs::create_dir_all(path)
    }
}

#[cfg(not(unix))]
fn ensure_parent_directory(path: &Path) -> io::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    fs::create_dir_all(parent)
}

#[cfg(unix)]
pub(crate) struct AtomicOutputFile {
    file: File,
    temporary_path: PathBuf,
    final_path: PathBuf,
    anchored: AnchoredTarget,
    committed: bool,
}

#[cfg(unix)]
impl AtomicOutputFile {
    pub(crate) fn create(path: &Path, create_parent: bool) -> io::Result<Self> {
        let anchored = open_anchored_target(path, create_parent)?;
        let final_path = anchored.final_path();
        let temporary_path = fd_child_path(
            anchored.parent_dir(),
            &temporary_file_name(anchored.file_name()),
        );
        let file = open_new_file(&temporary_path)?;
        Ok(Self {
            file,
            temporary_path,
            final_path,
            anchored,
            committed: false,
        })
    }

    pub(crate) fn commit(mut self) -> io::Result<()> {
        self.file.flush()?;
        run_before_local_write_hook(&self.anchored.logical_path());
        ensure_existing_target_is_regular_or_missing(&self.final_path)?;
        let temp_name = self
            .temporary_path
            .file_name()
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "temp path has no file name"))?;
        rename_at(
            self.anchored.parent_dir(),
            temp_name,
            self.anchored.file_name(),
        )?;
        if let Err(error) = ensure_parent_path_matches_anchor(&self.anchored) {
            drop(remove_at(
                self.anchored.parent_dir(),
                self.anchored.file_name(),
            ));
            return Err(error);
        }
        self.committed = true;
        Ok(())
    }
}

#[cfg(unix)]
impl Write for AtomicOutputFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.file.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[cfg(unix)]
impl Drop for AtomicOutputFile {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let _result = remove_if_present(&self.temporary_path);
    }
}

#[cfg(unix)]
fn remove_output_file_if_present_unix(path: &Path) -> io::Result<bool> {
    let anchored = open_anchored_target(path, false)?;
    let final_path = anchored.final_path();
    match remove_file(&final_path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn open_anchored_target(path: &Path, create_parent: bool) -> io::Result<AnchoredTarget> {
    let file_name = path.file_name().ok_or_else(invalid_output_path_error)?;
    let parent_path = effective_parent_path(path).to_path_buf();

    // Reject symlinked parent directories to prevent write-through-symlink attacks.
    if let Ok(metadata) = fs::symlink_metadata(&parent_path)
        && metadata.file_type().is_symlink()
    {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "local output parent directory must not be a symlink",
        ));
    }

    let parent_dir = open_directory_chain(&parent_path, create_parent)?;
    Ok(AnchoredTarget::new(
        parent_dir,
        parent_path,
        file_name.to_os_string(),
    ))
}

#[cfg(unix)]
fn effective_parent_path(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

#[cfg(unix)]
fn open_directory_chain(path: &Path, create_missing: bool) -> io::Result<File> {
    open_directory_chain_shared(
        path,
        create_missing,
        anchored_path_options().directory_mode,
        invalid_output_path_error,
    )
}

#[cfg(unix)]
fn open_new_file(path: &Path) -> io::Result<File> {
    open_new_file_shared(path, anchored_path_options().file_mode)
}

#[cfg(unix)]
fn ensure_existing_target_is_regular_or_missing(path: &Path) -> io::Result<()> {
    match symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(()),
        Ok(_metadata) => Err(io::Error::new(
            ErrorKind::InvalidInput,
            "output path must be a regular file and must not be a symlink",
        )),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn ensure_parent_path_matches_anchor(anchored: &AnchoredTarget) -> io::Result<()> {
    ensure_parent_path_matches_anchor_shared(
        anchored,
        "local output path changed during anchored write",
    )
}

#[cfg(unix)]
const fn anchored_path_options() -> AnchoredPathOptions {
    AnchoredPathOptions::new(Some(LOCAL_DIRECTORY_MODE), Some(LOCAL_FILE_MODE))
}

fn invalid_output_path_error() -> io::Error {
    io::Error::new(
        ErrorKind::InvalidInput,
        "output path must resolve to a regular file under a real directory",
    )
}

/// Prints an error and its full source chain to stderr.
pub fn print_error_chain(error: &dyn Error) {
    eprintln!("{error}");
    let mut source = error.source();
    while let Some(next) = source {
        eprintln!("caused by: {next}");
        source = next.source();
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::{fs, io::ErrorKind, path::PathBuf};

    use super::{
        effective_parent_path, remove_output_file_if_present, set_before_local_write_hook,
        write_output_bytes,
    };

    #[cfg(unix)]
    #[test]
    fn write_output_bytes_rejects_symlinked_output_path() {
        let sandbox = tempfile::tempdir().unwrap();
        let target = sandbox.path().join("target.json");
        let write = fs::write(&target, b"original");
        assert!(write.is_ok());
        let link = sandbox.path().join("output.json");
        let linked = symlink(&target, &link);
        assert!(linked.is_ok());

        let result = write_output_bytes(&link, b"replacement", false);

        assert!(matches!(
            result,
            Err(error) if error.kind() == ErrorKind::InvalidInput
        ));
        let target_bytes = fs::read(&target).unwrap();
        assert_eq!(target_bytes, b"original");
    }

    #[cfg(unix)]
    #[test]
    fn write_output_bytes_rejects_parent_swap_race() {
        let sandbox = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();

        let output = sandbox.path().join("reports").join("output.json");
        let parent = output.parent().map(PathBuf::from).unwrap();
        let created = fs::create_dir_all(&parent);
        assert!(created.is_ok());
        let moved_parent = sandbox.path().join("detached-reports");
        let moved_parent_for_hook = moved_parent.clone();
        let escape_dir = outside.path().to_path_buf();

        set_before_local_write_hook(output.clone(), move || {
            let renamed = fs::rename(&parent, &moved_parent_for_hook);
            assert!(renamed.is_ok());
            let linked = symlink(&escape_dir, &parent);
            assert!(linked.is_ok());
        });

        let result = write_output_bytes(&output, br#"{"ok":true}"#, true);

        assert!(matches!(
            result,
            Err(error) if error.kind() == ErrorKind::InvalidData
        ));
        assert!(
            !outside.path().join("output.json").exists(),
            "output write escaped into a symlink target"
        );
        assert!(
            !moved_parent.join("output.json").exists(),
            "output write left a committed file behind in the detached original directory"
        );
    }

    #[cfg(unix)]
    #[test]
    fn remove_output_file_if_present_rejects_symlinked_parent_directory() {
        let sandbox = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();

        let output_dir = sandbox.path().join("units");
        let linked = symlink(outside.path(), &output_dir);
        assert!(linked.is_ok());
        let result = remove_output_file_if_present(&output_dir.join("shardline-gc.service"));

        assert!(result.is_err());
    }

    #[test]
    fn ensure_output_directory_creates_missing_parent_dirs() {
        let sandbox = tempfile::tempdir().unwrap();
        let nested = sandbox.path().join("a").join("b").join("c");
        assert!(!nested.exists());
        let result = super::ensure_output_directory(&nested);
        assert!(result.is_ok());
        assert!(nested.exists());
    }

    #[test]
    fn ensure_output_directory_succeeds_on_existing_directory() {
        let sandbox = tempfile::tempdir().unwrap();
        let result = super::ensure_output_directory(sandbox.path());
        assert!(result.is_ok());
    }

    #[test]
    fn remove_output_file_if_present_returns_false_for_missing_file() {
        let sandbox = tempfile::tempdir().unwrap();
        let missing = sandbox.path().join("nonexistent.txt");
        let result = super::remove_output_file_if_present(&missing);
        assert!(matches!(result, Ok(false)));
    }

    #[test]
    fn remove_output_file_if_present_returns_true_for_existing_file() {
        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("exists.txt");
        std::fs::write(&path, b"content").unwrap();
        let result = super::remove_output_file_if_present(&path);
        assert!(matches!(result, Ok(true)));
        assert!(!path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn write_output_bytes_creates_private_file_and_directory_modes() {
        let sandbox = tempfile::tempdir().unwrap();
        let output = sandbox.path().join("reports").join("output.json");

        let wrote = write_output_bytes(&output, br#"{"ok":true}"#, true);
        assert!(wrote.is_ok());

        let file_metadata = fs::metadata(&output).unwrap();
        let directory_metadata = fs::metadata(sandbox.path().join("reports")).unwrap();

        assert_eq!(file_metadata.permissions().mode() & 0o777, 0o600);
        assert_eq!(directory_metadata.permissions().mode() & 0o777, 0o700);
    }

    #[cfg(unix)]
    #[test]
    fn effective_parent_path_uses_dot_for_rootless_path() {
        // Simulate a path with no parent (e.g. just "file.txt")
        let path = std::path::Path::new("file.txt");
        let parent = effective_parent_path(path);
        assert_eq!(parent, std::path::Path::new("."));
    }

    #[cfg(unix)]
    #[test]
    fn effective_parent_path_uses_given_parent() {
        let path = std::path::Path::new("/some/dir/file.txt");
        let parent = effective_parent_path(path);
        assert_eq!(parent, std::path::Path::new("/some/dir"));
    }

    #[cfg(unix)]
    #[test]
    fn invalid_output_path_error_has_expected_message() {
        let err = super::invalid_output_path_error();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("regular file"));
    }

    #[cfg(unix)]
    #[test]
    fn write_output_bytes_creates_file_with_content() {
        let sandbox = tempfile::tempdir().unwrap();
        let output = sandbox.path().join("output.json");
        let wrote = write_output_bytes(&output, br#"{"ok":true}"#, true);
        assert!(wrote.is_ok());
        let bytes = std::fs::read(&output).unwrap();
        assert_eq!(bytes, br#"{"ok":true}"#);
    }

    #[cfg(unix)]
    #[test]
    fn write_output_bytes_overwrites_existing_file() {
        let sandbox = tempfile::tempdir().unwrap();
        let output = sandbox.path().join("existing.json");
        std::fs::write(&output, b"old").unwrap();
        let wrote = write_output_bytes(&output, br#"{"updated":true}"#, false);
        assert!(wrote.is_ok());
        let bytes = std::fs::read(&output).unwrap();
        assert_eq!(bytes, br#"{"updated":true}"#);
    }

    #[cfg(unix)]
    #[test]
    fn atomic_output_file_cleanup_on_drop() {
        use super::AtomicOutputFile;
        let sandbox = tempfile::tempdir().unwrap();
        let output = sandbox.path().join("cleanup-test.json");
        // Create and then drop without committing
        let file = AtomicOutputFile::create(&output, true);
        assert!(file.is_ok());
        drop(file); // Drop should clean up the temp file
        // The final path should not exist
        assert!(!output.exists());
    }

    #[cfg(unix)]
    #[test]
    fn ensure_existing_target_is_regular_or_missing_rejects_directory() {
        let sandbox = tempfile::tempdir().unwrap();
        let result = super::ensure_existing_target_is_regular_or_missing(sandbox.path());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(unix)]
    #[test]
    fn ensure_existing_target_is_regular_or_missing_accepts_missing() {
        let sandbox = tempfile::tempdir().unwrap();
        let missing = sandbox.path().join("nonexistent.json");
        let result = super::ensure_existing_target_is_regular_or_missing(&missing);
        assert!(result.is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn ensure_existing_target_is_regular_or_missing_accepts_regular_file() {
        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("regular.json");
        std::fs::write(&path, b"content").unwrap();
        let result = super::ensure_existing_target_is_regular_or_missing(&path);
        assert!(result.is_ok());
    }

    // ── print_error_chain ─────────────────────────────────────────────

    #[derive(Debug)]
    struct TestError(&'static str);

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for TestError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            None
        }
    }

    #[derive(Debug)]
    struct ChainedError {
        message: &'static str,
        source: Option<Box<dyn std::error::Error>>,
    }

    impl std::fmt::Display for ChainedError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.message)
        }
    }

    impl std::error::Error for ChainedError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.source.as_ref().map(|e| e.as_ref())
        }
    }

    #[test]
    fn print_error_chain_single_error() {
        let err = TestError("simple error");
        super::print_error_chain(&err);
    }

    #[test]
    fn print_error_chain_chained_error() {
        let inner = TestError("inner cause");
        let outer = ChainedError {
            message: "outer error",
            source: Some(Box::new(inner)),
        };
        super::print_error_chain(&outer);
    }

    #[test]
    fn print_error_chain_deeply_chained() {
        let level3 = TestError("root cause");
        let level2 = ChainedError {
            message: "middle layer",
            source: Some(Box::new(level3)),
        };
        let level1 = ChainedError {
            message: "outer layer",
            source: Some(Box::new(level2)),
        };
        super::print_error_chain(&level1);
    }

    #[test]
    fn print_error_chain_no_source() {
        #[derive(Debug)]
        struct NoSourceError;

        impl std::fmt::Display for NoSourceError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("no source error")
            }
        }

        impl std::error::Error for NoSourceError {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                None
            }
        }

        super::print_error_chain(&NoSourceError);
    }
}
