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
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
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

fn fd_base_path(directory: &File) -> PathBuf {
    #[cfg(target_os = "linux")]
    let prefix = "/proc/self/fd";
    #[cfg(not(target_os = "linux"))]
    let prefix = "/dev/fd";

    PathBuf::from(format!("{prefix}/{}", directory.as_raw_fd()))
}
