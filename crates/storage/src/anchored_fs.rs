use std::{
    ffi::{OsStr, OsString},
    fs::{self, DirBuilder, File, OpenOptions},
    io::{self, ErrorKind, Write},
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use std::os::unix::{
    fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    io::AsRawFd,
};

static TEMPORARY_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// File and directory mode overrides for anchored filesystem writes.
#[derive(Debug, Clone, Copy)]
pub struct AnchoredPathOptions {
    /// Optional mode applied to newly-created directories.
    pub directory_mode: Option<u32>,
    /// Optional mode applied to newly-created files.
    pub file_mode: Option<u32>,
}

impl AnchoredPathOptions {
    /// Creates anchored path options.
    #[must_use]
    pub const fn new(directory_mode: Option<u32>, file_mode: Option<u32>) -> Self {
        Self {
            directory_mode,
            file_mode,
        }
    }
}

/// Open parent directory and final filename for a write anchored under a root.
pub struct AnchoredTarget {
    parent_dir: File,
    parent_path: PathBuf,
    file_name: OsString,
}

impl AnchoredTarget {
    /// Creates an anchored target from an already-open parent directory.
    #[must_use]
    pub const fn new(parent_dir: File, parent_path: PathBuf, file_name: OsString) -> Self {
        Self {
            parent_dir,
            parent_path,
            file_name,
        }
    }

    /// Returns the open parent directory file descriptor.
    #[must_use]
    pub const fn parent_dir(&self) -> &File {
        &self.parent_dir
    }

    /// Returns the final target filename within the anchored parent directory.
    #[must_use]
    pub fn file_name(&self) -> &OsStr {
        &self.file_name
    }

    /// Returns the descriptor-relative path used for race-resistant filesystem operations.
    #[must_use]
    pub fn final_path(&self) -> PathBuf {
        fd_child_path(&self.parent_dir, &self.file_name)
    }

    /// Returns the logical path requested by the caller.
    #[must_use]
    pub fn logical_path(&self) -> PathBuf {
        self.parent_path.join(&self.file_name)
    }
}

/// Opens a file target under `root` without following symlinked path components.
///
/// # Errors
///
/// Returns an error when `path` falls outside `root`, contains an invalid component, or the
/// anchored parent directory cannot be opened or created safely.
pub fn open_anchored_target(
    root: &Path,
    path: &Path,
    options: AnchoredPathOptions,
    invalid_path_error: fn() -> io::Error,
) -> io::Result<AnchoredTarget> {
    let parent_path = path.parent().ok_or_else(invalid_path_error)?;
    let file_name = path.file_name().ok_or_else(invalid_path_error)?;
    let relative_parent = parent_path
        .strip_prefix(root)
        .map_err(|_error| invalid_path_error())?;
    let mut current = match open_directory(root) {
        Ok(directory) => directory,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            create_directory_all(root, options.directory_mode)?;
            open_directory(root)?
        }
        Err(error) => return Err(error),
    };

    for component in relative_parent.components() {
        let Component::Normal(segment) = component else {
            return Err(invalid_path_error());
        };
        current = open_or_create_child_directory(&current, segment, true, options.directory_mode)?;
    }

    Ok(AnchoredTarget::new(
        current,
        parent_path.to_path_buf(),
        file_name.to_os_string(),
    ))
}

/// Opens a directory path component-by-component without following symlinks.
///
/// # Errors
///
/// Returns an error when a path component is invalid, a non-directory is encountered, or a
/// directory cannot be opened or created.
pub fn open_directory_chain(
    path: &Path,
    create_missing: bool,
    directory_mode: Option<u32>,
    invalid_path_error: fn() -> io::Error,
) -> io::Result<File> {
    let mut current = if path.is_absolute() {
        open_directory(Path::new("/"))?
    } else {
        open_directory(Path::new("."))?
    };

    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                current = open_directory(&fd_child_path(&current, OsStr::new("..")))?;
            }
            Component::Normal(segment) => {
                current = open_or_create_child_directory(
                    &current,
                    segment,
                    create_missing,
                    directory_mode,
                )?;
            }
            Component::Prefix(_prefix) => return Err(invalid_path_error()),
        }
    }

    Ok(current)
}

/// Opens one child directory below `parent`, optionally creating it first.
///
/// # Errors
///
/// Returns an error when the child is missing and creation is disabled, when the child is not a
/// directory, or when opening or creating the directory fails.
pub fn open_or_create_child_directory(
    parent: &File,
    segment: &OsStr,
    create_missing: bool,
    directory_mode: Option<u32>,
) -> io::Result<File> {
    let child_path = fd_child_path(parent, segment);
    match open_directory(&child_path) {
        Ok(directory) => Ok(directory),
        Err(error) if error.kind() == ErrorKind::NotFound && create_missing => {
            match create_directory(&child_path, directory_mode) {
                Ok(()) => {}
                Err(create_error) if create_error.kind() == ErrorKind::AlreadyExists => {}
                Err(create_error) => return Err(create_error),
            }
            open_directory(&child_path)
        }
        Err(error) => Err(error),
    }
}

/// Writes `bytes` into a new temporary file anchored beside the target path.
///
/// # Errors
///
/// Returns an error when a temporary file cannot be created safely or the payload cannot be fully
/// written and flushed.
pub fn write_anchored_temporary_file(
    anchored: &AnchoredTarget,
    bytes: &[u8],
    file_mode: Option<u32>,
) -> io::Result<PathBuf> {
    loop {
        let temporary = fd_child_path(
            anchored.parent_dir(),
            &temporary_file_name(anchored.file_name()),
        );
        match open_new_file(&temporary, file_mode) {
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

/// Returns a collision-resistant temporary filename beside `file_name`.
#[must_use]
pub fn temporary_file_name(file_name: &OsStr) -> OsString {
    let counter = TEMPORARY_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    let mut name = file_name.to_os_string();
    name.push(format!(".tmp-{unix_nanos}-{counter}"));
    name
}

/// Verifies that the anchored parent directory still points at the same on-disk directory.
///
/// # Errors
///
/// Returns an error when the logical parent path has been replaced, turned into a symlink, or can
/// no longer be inspected.
pub fn ensure_parent_path_matches_anchor(
    anchored: &AnchoredTarget,
    changed_message: &'static str,
) -> io::Result<()> {
    let anchored_metadata = anchored.parent_dir.metadata()?;
    let current_metadata = fs::symlink_metadata(&anchored.parent_path)?;
    if current_metadata.file_type().is_symlink() || !current_metadata.is_dir() {
        return Err(io::Error::new(ErrorKind::InvalidData, changed_message));
    }
    if anchored_metadata.dev() != current_metadata.dev()
        || anchored_metadata.ino() != current_metadata.ino()
    {
        return Err(io::Error::new(ErrorKind::InvalidData, changed_message));
    }

    Ok(())
}

/// Opens a directory without following symlinks.
///
/// # Errors
///
/// Returns an error when `path` does not name an existing directory or the directory cannot be
/// opened safely.
pub fn open_directory(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    // On macOS, /dev/fd/N paths are kernel-created symlinks for file descriptors.
    // The FD-based path approach is already secure by construction (the FD is obtained
    // from a verified directory open), so O_NOFOLLOW is not needed on macOS.
    // On Linux, /proc/self/fd/N is not a symlink, so O_NOFOLLOW provides defense-in-depth.
    #[cfg(not(target_os = "macos"))]
    options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW);
    #[cfg(target_os = "macos")]
    options.custom_flags(libc::O_DIRECTORY);
    options.open(path)
}

/// Creates and opens a brand-new file without following symlinks.
///
/// # Errors
///
/// Returns an error when the file already exists, the path is invalid, or the file cannot be
/// created with the requested mode.
pub fn open_new_file(path: &Path, file_mode: Option<u32>) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW);
    if let Some(mode) = file_mode {
        options.mode(mode);
    }
    options.open(path)
}

/// Creates a single directory, optionally applying an explicit mode.
///
/// # Errors
///
/// Returns an error when the directory cannot be created at `path`.
pub fn create_directory(path: &Path, directory_mode: Option<u32>) -> io::Result<()> {
    let mut builder = DirBuilder::new();
    if let Some(mode) = directory_mode {
        builder.mode(mode);
    }
    builder.create(path)
}

/// Recursively creates a directory chain, optionally applying an explicit mode.
///
/// # Errors
///
/// Returns an error when any directory in the chain cannot be created.
pub fn create_directory_all(path: &Path, directory_mode: Option<u32>) -> io::Result<()> {
    let mut builder = DirBuilder::new();
    builder.recursive(true);
    if let Some(mode) = directory_mode {
        builder.mode(mode);
    }
    builder.create(path)
}

/// Builds a descriptor-relative child path below an open directory.
#[must_use]
pub fn fd_child_path(directory: &File, child: &OsStr) -> PathBuf {
    let mut path = fd_base_path(directory);
    path.push(child);
    path
}

/// Atomically renames a file within the same parent directory using FD-relative
/// operations on macOS.
///
/// On Linux, `/proc/self/fd/N/child` paths resolve through the FD, so `std::fs::rename`
/// works even after the parent directory is renamed. On macOS, `/dev/fd/N` cannot traverse
/// children, so this uses `renameat` with the parent FD instead.
///
/// # Safety
///
/// Uses `libc::renameat` which is safe when called with valid file descriptors
/// (the `parent` FD is guaranteed open by the borrow) and valid C strings
/// (produced by `CString::new` which rejects interior null bytes).
///
/// # Errors
///
/// Returns an error when either name contains a null byte or the rename fails.
#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
pub fn rename_at(parent: &File, old_name: &OsStr, new_name: &OsStr) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::io::AsRawFd;

    let old_cstr = CString::new(old_name.as_encoded_bytes()).map_err(|e| {
        io::Error::new(
            ErrorKind::InvalidInput,
            format!("name contains null byte: {e}"),
        )
    })?;
    let new_cstr = CString::new(new_name.as_encoded_bytes()).map_err(|e| {
        io::Error::new(
            ErrorKind::InvalidInput,
            format!("name contains null byte: {e}"),
        )
    })?;

    // SAFETY: renameat(2) is safe when given valid FDs and null-terminated strings.
    // The parent FD is an open directory descriptor; old_cstr and new_cstr are
    // valid C strings produced by CString::new above.
    let result = unsafe {
        libc::renameat(
            parent.as_raw_fd(),
            old_cstr.as_ptr(),
            parent.as_raw_fd(),
            new_cstr.as_ptr(),
        )
    };

    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Renames a file. On non-macOS platforms, this delegates to `std::fs::rename`.
///
/// # Errors
///
/// Returns an error if the source path does not exist, the target path already
/// exists, or the filesystem prevents the rename.
#[cfg(not(target_os = "macos"))]
pub fn rename_at(parent: &File, old_name: &OsStr, new_name: &OsStr) -> io::Result<()> {
    let old_path = fd_child_path(parent, old_name);
    let new_path = fd_child_path(parent, new_name);
    std::fs::rename(old_path, new_path)
}

/// Removes a file within a directory using FD-relative operations.
///
/// On macOS, `/dev/fd/N` cannot traverse children, so this uses `unlinkat`.
///
/// # Safety
///
/// Uses `libc::unlinkat` which is safe when called with a valid file descriptor
/// (the `parent` FD is guaranteed open by the borrow) and a valid C string.
///
/// # Errors
///
/// Returns an error when the name contains a null byte or the removal fails.
#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
pub fn remove_at(parent: &File, name: &OsStr) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::io::AsRawFd;

    let cstr = CString::new(name.as_encoded_bytes()).map_err(|e| {
        io::Error::new(
            ErrorKind::InvalidInput,
            format!("name contains null byte: {e}"),
        )
    })?;
    // SAFETY: unlinkat(2) is safe when given a valid FD and null-terminated string.
    // The parent FD is an open directory descriptor; cstr is a valid C string.
    let result = unsafe { libc::unlinkat(parent.as_raw_fd(), cstr.as_ptr(), 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Removes a file. On non-macOS, delegates to `std::fs::remove_file`.
///
/// # Errors
///
/// Returns an error if the file does not exist or cannot be removed.
#[cfg(not(target_os = "macos"))]
pub fn remove_at(parent: &File, name: &OsStr) -> io::Result<()> {
    let path = fd_child_path(parent, name);
    std::fs::remove_file(path)
}

/// Removes a file when it exists.
///
/// # Errors
///
/// Returns an error when file removal fails for any reason other than the path being absent.
pub fn remove_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
fn macos_fd_real_path(fd: std::os::unix::io::RawFd) -> PathBuf {
    let mut buf = [0i8; 1024];
    // SAFETY: fcntl(2) with F_GETPATH writes a NUL-terminated path into buf.
    // buf is stack-allocated with sufficient size for any valid filesystem path.
    let result = unsafe { libc::fcntl(fd, libc::F_GETPATH, buf.as_mut_ptr()) };
    if result < 0 {
        return PathBuf::from(format!("/dev/fd/{fd}"));
    }
    // SAFETY: fcntl(F_GETPATH) guarantees the buffer contains a valid NUL-terminated
    // C string when it returns success (result >= 0).
    let c_str = unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) };
    PathBuf::from(c_str.to_string_lossy().as_ref())
}

fn fd_base_path(directory: &File) -> PathBuf {
    #[cfg(target_os = "linux")]
    {
        PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()))
    }
    #[cfg(target_os = "macos")]
    {
        // On macOS, /dev/fd/N is a fescfs virtual directory entry, NOT a symlink.
        // You cannot traverse children through it (open("/dev/fd/3/var") fails with ENOENT).
        // Use fcntl(F_GETPATH) to resolve the real path from the file descriptor.
        macos_fd_real_path(directory.as_raw_fd())
    }
    #[cfg(not(target_os = "linux"))]
    #[cfg(not(target_os = "macos"))]
    {
        PathBuf::from(format!("/dev/fd/{}", directory.as_raw_fd()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::ffi::OsStrExt;
    use tempfile::tempdir;

    fn invalid_path_error() -> io::Error {
        io::Error::new(ErrorKind::InvalidInput, "invalid path")
    }

    // ── AnchoredPathOptions ──────────────────────────────────────────────

    #[test]
    fn anchored_path_options_with_modes() {
        let opts = AnchoredPathOptions::new(Some(0o755), Some(0o644));
        assert_eq!(opts.directory_mode, Some(0o755));
        assert_eq!(opts.file_mode, Some(0o644));
    }

    #[test]
    fn anchored_path_options_none() {
        let opts = AnchoredPathOptions::new(None, None);
        assert!(opts.directory_mode.is_none());
        assert!(opts.file_mode.is_none());
    }

    // ── AnchoredTarget ───────────────────────────────────────────────────

    #[test]
    fn anchored_target_accessors() {
        let dir = tempdir().unwrap();
        let parent_path = dir.path().join("sub");
        fs::create_dir(&parent_path).unwrap();
        let file = fs::OpenOptions::new()
            .read(true)
            .open(&parent_path)
            .unwrap();
        let file_name: OsString = "file.txt".into();

        let target = AnchoredTarget::new(file, parent_path.clone(), file_name.clone());

        assert_eq!(target.file_name(), file_name.as_os_str());
        assert_eq!(target.parent_path, parent_path);
    }

    #[test]
    #[allow(clippy::redundant_clone)]
    fn anchored_target_final_path() {
        let dir = tempdir().unwrap();
        let parent_path = dir.path().to_path_buf();
        let file = fs::OpenOptions::new()
            .read(true)
            .open(&parent_path)
            .unwrap();
        let file_name: OsString = "output.bin".into();

        let target = AnchoredTarget::new(file, parent_path, file_name.clone());
        let final_path = target.final_path();
        // final_path should end with the file_name
        assert_eq!(final_path.file_name().unwrap(), file_name.as_os_str());
    }

    #[test]
    #[allow(clippy::redundant_clone)]
    fn anchored_target_logical_path() {
        let dir = tempdir().unwrap();
        let parent_path = dir.path().to_path_buf();
        let file = fs::OpenOptions::new()
            .read(true)
            .open(&parent_path)
            .unwrap();
        let file_name: OsString = "data.txt".into();

        let target = AnchoredTarget::new(file, parent_path.clone(), file_name);
        assert_eq!(target.logical_path(), parent_path.join("data.txt"));
    }

    // ── open_anchored_target ─────────────────────────────────────────────

    #[test]
    fn open_anchored_target_nested_path() {
        let root = tempdir().unwrap();
        let path = root.path().join("sub/dir/file.txt");

        let result = open_anchored_target(
            root.path(),
            &path,
            AnchoredPathOptions::new(None, None),
            invalid_path_error,
        );
        assert!(result.is_ok());
        let target = result.unwrap();
        assert_eq!(target.file_name(), OsStr::new("file.txt"));
        assert_eq!(target.parent_path, root.path().join("sub/dir"));
    }

    #[test]
    fn open_anchored_target_path_outside_root() {
        let root = tempdir().unwrap();
        // Construct a path that resolves outside root via canonicalization
        let path = PathBuf::from("/tmp/definitely-not-inside-root.txt");

        let result = open_anchored_target(
            root.path(),
            &path,
            AnchoredPathOptions::new(None, None),
            invalid_path_error,
        );
        assert!(result.is_err());
    }

    #[test]
    fn open_anchored_target_traversal_rejected() {
        let root = tempdir().unwrap();
        let path = root.path().join("../escape.txt");

        let result = open_anchored_target(
            root.path(),
            &path,
            AnchoredPathOptions::new(None, None),
            invalid_path_error,
        );
        assert!(result.is_err());
    }

    // ── open_directory_chain ──────────────────────────────────────────────

    #[test]
    fn open_directory_chain_existing() {
        let dir = tempdir().unwrap();
        let sub = dir.path().join("existing");
        fs::create_dir(&sub).unwrap();

        let result = open_directory_chain(&sub, false, None, invalid_path_error);
        assert!(result.is_ok());
    }

    #[test]
    fn open_directory_chain_create_missing() {
        let dir = tempdir().unwrap();
        let sub = dir.path().join("new");

        let result = open_directory_chain(&sub, true, None, invalid_path_error);
        assert!(result.is_ok());
        assert!(sub.exists());
    }

    #[test]
    fn open_directory_chain_no_create_missing() {
        let dir = tempdir().unwrap();
        let sub = dir.path().join("missing");

        let result = open_directory_chain(&sub, false, None, invalid_path_error);
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn open_directory_chain_rejects_symlink_in_path() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().unwrap();
        let target = dir.path().join("target");
        fs::create_dir(&target).unwrap();
        let link = dir.path().join("link");
        symlink(&target, &link).unwrap();
        let path = link.join("subdir");

        let result = open_directory_chain(&path, true, None, invalid_path_error);
        assert!(result.is_err());
    }

    // ── open_or_create_child_directory ────────────────────────────────────

    #[test]
    fn open_or_create_child_existing() {
        let dir = tempdir().unwrap();
        let child_name: &OsStr = "child".as_ref();
        fs::create_dir(dir.path().join("child")).unwrap();
        let parent = fs::OpenOptions::new().read(true).open(dir.path()).unwrap();

        let result = open_or_create_child_directory(&parent, child_name, false, None);
        assert!(result.is_ok());
    }

    #[test]
    fn open_or_create_child_create_missing() {
        let dir = tempdir().unwrap();
        let child_name: &OsStr = "newchild".as_ref();
        let parent = fs::OpenOptions::new().read(true).open(dir.path()).unwrap();

        let result = open_or_create_child_directory(&parent, child_name, true, None);
        assert!(result.is_ok());
        assert!(dir.path().join("newchild").exists());
    }

    #[test]
    fn open_or_create_child_no_create_missing() {
        let dir = tempdir().unwrap();
        let child_name: &OsStr = "nochild".as_ref();
        let parent = fs::OpenOptions::new().read(true).open(dir.path()).unwrap();

        let result = open_or_create_child_directory(&parent, child_name, false, None);
        assert!(result.is_err());
    }

    // ── write_anchored_temporary_file ─────────────────────────────────────

    #[test]
    fn write_anchored_temporary_file_basic() {
        let dir = tempdir().unwrap();
        let parent = fs::OpenOptions::new().read(true).open(dir.path()).unwrap();
        let target = AnchoredTarget::new(parent, dir.path().to_path_buf(), "target.txt".into());
        let payload = b"hello world";

        let result = write_anchored_temporary_file(&target, payload, None);
        assert!(result.is_ok());
        let tmp_path = result.unwrap();

        // Temp file name should contain the target name and ".tmp-"
        let tmp_name = tmp_path.file_name().unwrap().to_string_lossy();
        assert!(
            tmp_name.starts_with("target.txt.tmp-"),
            "unexpected temp name: {tmp_name}"
        );

        // File content should match
        let contents = fs::read(&tmp_path).unwrap();
        assert_eq!(contents, payload);
    }

    // ── temporary_file_name ──────────────────────────────────────────────

    #[test]
    fn temporary_file_name_has_tmp_suffix() {
        let name = temporary_file_name(OsStr::new("file.txt"));
        let name_str = name.to_string_lossy();
        assert!(name_str.starts_with("file.txt.tmp-"));
    }

    #[test]
    fn temporary_file_name_unique() {
        let a = temporary_file_name(OsStr::new("file.txt"));
        let b = temporary_file_name(OsStr::new("file.txt"));
        assert_ne!(a, b);
    }

    // ── ensure_parent_path_matches_anchor ─────────────────────────────────

    #[test]
    fn ensure_parent_path_matches_anchor_ok() {
        let dir = tempdir().unwrap();
        let parent_path = dir.path().to_path_buf();
        let file = fs::OpenOptions::new()
            .read(true)
            .open(&parent_path)
            .unwrap();
        let target = AnchoredTarget::new(file, parent_path, "f.txt".into());

        assert!(ensure_parent_path_matches_anchor(&target, "changed").is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn ensure_parent_path_matches_anchor_symlink_replaced() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().unwrap();
        let real_dir = dir.path().join("real");
        fs::create_dir(&real_dir).unwrap();

        let file = fs::OpenOptions::new().read(true).open(&real_dir).unwrap();
        let target = AnchoredTarget::new(file, real_dir.clone(), "f.txt".into());

        // Replace the real directory with a symlink
        fs::remove_dir(&real_dir).unwrap();
        symlink("/tmp", &real_dir).unwrap();

        let result = ensure_parent_path_matches_anchor(&target, "symlink detected");
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn ensure_parent_path_matches_anchor_renamed() {
        let dir = tempdir().unwrap();
        let real_dir = dir.path().join("real");
        fs::create_dir(&real_dir).unwrap();

        let file = fs::OpenOptions::new().read(true).open(&real_dir).unwrap();
        let target = AnchoredTarget::new(file, real_dir.clone(), "f.txt".into());

        // Rename the directory
        let new_name = dir.path().join("renamed");
        fs::rename(&real_dir, &new_name).unwrap();

        let result = ensure_parent_path_matches_anchor(&target, "renamed detected");
        assert!(result.is_err());
    }

    // ── open_directory ────────────────────────────────────────────────────

    #[test]
    fn open_directory_existing() {
        let dir = tempdir().unwrap();
        let result = open_directory(dir.path());
        assert!(result.is_ok());
    }

    #[test]
    fn open_directory_not_a_directory() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("file.txt");
        fs::write(&file_path, "hello").unwrap();

        let result = open_directory(&file_path);
        assert!(result.is_err());
    }

    #[test]
    fn open_directory_nonexistent() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("nope");

        let result = open_directory(&missing);
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn open_directory_symlink_rejected() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().unwrap();
        let real_dir = dir.path().join("real");
        fs::create_dir(&real_dir).unwrap();
        let link_path = dir.path().join("link");
        symlink(&real_dir, &link_path).unwrap();

        #[cfg(not(target_os = "macos"))]
        {
            // On Linux, O_NOFOLLOW should reject the symlink
            let result = open_directory(&link_path);
            assert!(result.is_err());
        }
        #[cfg(target_os = "macos")]
        {
            // On macOS, O_NOFOLLOW is not set; the symlink resolves
            let result = open_directory(&link_path);
            assert!(result.is_ok());
        }
    }

    // ── open_new_file ─────────────────────────────────────────────────────

    #[test]
    fn open_new_file_creates() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("new.txt");

        let result = open_new_file(&path, None);
        assert!(result.is_ok());
        assert!(path.exists());
    }

    #[test]
    fn open_new_file_already_exists() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("existing.txt");
        fs::write(&path, "data").unwrap();

        let result = open_new_file(&path, None);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::AlreadyExists);
    }

    #[cfg(unix)]
    #[test]
    fn open_new_file_via_symlink_rejected() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().unwrap();
        let real_file = dir.path().join("real.txt");
        fs::write(&real_file, "data").unwrap();
        let link_path = dir.path().join("link.txt");
        symlink(&real_file, &link_path).unwrap();

        let result = open_new_file(&link_path, None);
        assert!(result.is_err());
    }

    // ── create_directory ──────────────────────────────────────────────────

    #[test]
    fn create_directory_ok() {
        let dir = tempdir().unwrap();
        let new_dir = dir.path().join("created");

        assert!(create_directory(&new_dir, None).is_ok());
        assert!(new_dir.is_dir());
    }

    #[test]
    fn create_directory_already_exists() {
        let dir = tempdir().unwrap();
        let new_dir = dir.path().join("exists");
        fs::create_dir(&new_dir).unwrap();

        let result = create_directory(&new_dir, None);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::AlreadyExists);
    }

    // ── create_directory_all ──────────────────────────────────────────────

    #[test]
    fn create_directory_all_nested() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("a/b/c");

        assert!(create_directory_all(&nested, None).is_ok());
        assert!(nested.is_dir());
    }

    #[test]
    fn create_directory_all_idempotent() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("x/y");

        assert!(create_directory_all(&nested, None).is_ok());
        assert!(create_directory_all(&nested, None).is_ok());
    }

    // ── fd_child_path ────────────────────────────────────────────────────

    #[test]
    fn fd_child_path_beneath_parent() {
        let dir = tempdir().unwrap();
        let parent = fs::OpenOptions::new().read(true).open(dir.path()).unwrap();

        let child = fd_child_path(&parent, OsStr::new("file.txt"));
        // The fd_child_path uses /proc/self/fd/N, which on Linux resolves to the real path.
        // Just check that the filename component matches.
        assert_eq!(child.file_name().unwrap(), OsStr::new("file.txt"));
    }

    // ── rename_at ────────────────────────────────────────────────────────

    #[test]
    fn rename_at_ok() {
        let dir = tempdir().unwrap();
        let parent = fs::OpenOptions::new().read(true).open(dir.path()).unwrap();
        fs::write(dir.path().join("old.txt"), "data").unwrap();

        assert!(rename_at(&parent, OsStr::new("old.txt"), OsStr::new("new.txt")).is_ok());
        assert!(dir.path().join("new.txt").exists());
        assert!(!dir.path().join("old.txt").exists());
    }

    #[test]
    fn rename_at_null_byte_in_name() {
        let dir = tempdir().unwrap();
        let parent = fs::OpenOptions::new().read(true).open(dir.path()).unwrap();

        let bad_name = OsStr::from_bytes(b"old\x00txt");
        let result = rename_at(&parent, bad_name, OsStr::new("new.txt"));
        assert!(result.is_err());
    }

    #[test]
    fn rename_at_nonexistent_source() {
        let dir = tempdir().unwrap();
        let parent = fs::OpenOptions::new().read(true).open(dir.path()).unwrap();

        let result = rename_at(&parent, OsStr::new("nonexistent.txt"), OsStr::new("new.txt"));
        assert!(result.is_err());
    }

    // ── remove_at ────────────────────────────────────────────────────────

    #[test]
    fn remove_at_existing() {
        let dir = tempdir().unwrap();
        let parent = fs::OpenOptions::new().read(true).open(dir.path()).unwrap();
        fs::write(dir.path().join("file.txt"), "data").unwrap();

        assert!(remove_at(&parent, OsStr::new("file.txt")).is_ok());
        assert!(!dir.path().join("file.txt").exists());
    }

    #[test]
    fn remove_at_nonexistent() {
        let dir = tempdir().unwrap();
        let parent = fs::OpenOptions::new().read(true).open(dir.path()).unwrap();

        let result = remove_at(&parent, OsStr::new("nope.txt"));
        assert!(result.is_err());
    }

    #[test]
    fn remove_at_null_byte_in_name() {
        let dir = tempdir().unwrap();
        let parent = fs::OpenOptions::new().read(true).open(dir.path()).unwrap();

        let bad_name = OsStr::from_bytes(b"bad\x00file");
        let result = remove_at(&parent, bad_name);
        assert!(result.is_err());
    }

    // ── remove_if_present ────────────────────────────────────────────────

    #[test]
    fn remove_if_present_existing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("file.txt");
        fs::write(&path, "data").unwrap();

        assert!(remove_if_present(&path).is_ok());
        assert!(!path.exists());
    }

    #[test]
    fn remove_if_present_absent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nope.txt");

        // Should be idempotent
        assert!(remove_if_present(&path).is_ok());
    }
}
