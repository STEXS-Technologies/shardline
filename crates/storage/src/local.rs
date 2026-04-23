#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    cell::RefCell,
    fs::{self, File, Metadata, OpenOptions},
    io::{Error as IoError, ErrorKind, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use shardline_protocol::{ByteRange, ShardlineHash};
use thiserror::Error;

use crate::{
    DeleteOutcome, DirectoryPathError, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata,
    ObjectPrefix, ObjectStore, PutOutcome,
    ensure_directory_path_components_are_not_symlinked as ensure_directory_path_components_are_not_symlinked_shared,
    local_fs::{PutBytesIfAbsentOutcome, hard_link_file_if_absent, put_bytes_if_absent},
};
const VERIFY_BUFFER_BYTES: usize = 256 * 1024;

thread_local! {
    static VERIFY_BUFFER_A: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    static VERIFY_BUFFER_B: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Local filesystem implementation of [`ObjectStore`].
#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    /// Opens a local object store rooted at `root` without mutating the filesystem.
    #[must_use]
    pub const fn open(root: PathBuf) -> Self {
        Self { root }
    }

    /// Creates a local object store rooted at `root`.
    ///
    /// # Errors
    ///
    /// Returns [`LocalObjectStoreError`] when the root directory cannot be created.
    pub fn new(root: PathBuf) -> Result<Self, LocalObjectStoreError> {
        let store = Self::open(root);
        ensure_parent_directories_are_not_symlinked(&store.root, &store.root)?;
        fs::create_dir_all(&store.root)?;
        Ok(store)
    }

    /// Returns the root directory used by this adapter.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns the local filesystem path for a validated object key.
    #[must_use]
    pub fn path_for_key(&self, key: &ObjectKey) -> PathBuf {
        self.key_path(key)
    }

    /// Opens the object for direct local streaming without following symlinks.
    ///
    /// # Errors
    ///
    /// Returns [`LocalObjectStoreError`] when the object is missing or the resolved
    /// path is not a regular file.
    pub fn open_object_file(&self, key: &ObjectKey) -> Result<File, LocalObjectStoreError> {
        let path = self.key_path(key);
        ensure_parent_directories_are_not_symlinked(&self.root, &path)?;
        open_existing_object_file(&path)
    }

    /// Stores a temporary file if no identical object exists yet.
    ///
    /// The temporary file is consumed and removed on every successful outcome. Callers
    /// should create it on the same filesystem as the store root so installation can
    /// use an atomic hard-link operation.
    ///
    /// # Errors
    ///
    /// Returns [`LocalObjectStoreError`] when integrity validation, comparison, or
    /// storage installation fails.
    pub fn put_temporary_file_if_absent(
        &self,
        key: &ObjectKey,
        temporary: &Path,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, LocalObjectStoreError> {
        verify_file_integrity(temporary, integrity)?;
        let path = self.key_path(key);
        ensure_parent_directories_are_not_symlinked(&self.root, &path)?;
        link_temporary_file_if_absent(&self.root, &path, temporary, integrity, None)
    }

    fn key_path(&self, key: &ObjectKey) -> PathBuf {
        self.root.join(key.as_str())
    }
}

impl ObjectStore for LocalObjectStore {
    type Error = LocalObjectStoreError;

    fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error> {
        verify_integrity(body.as_slice(), integrity)?;
        let path = self.key_path(key);
        ensure_parent_directories_are_not_symlinked(&self.root, &path)?;
        match open_existing_object_file(&path) {
            Ok(file) => {
                ensure_file_matches_bytes(file, body.as_slice())?;
                return Ok(PutOutcome::AlreadyExists);
            }
            Err(LocalObjectStoreError::Io(error)) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        match put_bytes_if_absent(&self.root, &path, body.as_slice())
            .map_err(LocalObjectStoreError::Io)?
        {
            PutBytesIfAbsentOutcome::Inserted => Ok(PutOutcome::Inserted),
            PutBytesIfAbsentOutcome::AlreadyExists => Ok(PutOutcome::AlreadyExists),
        }
    }

    fn read_range(&self, key: &ObjectKey, range: ByteRange) -> Result<Vec<u8>, Self::Error> {
        let path = self.key_path(key);
        ensure_parent_directories_are_not_symlinked(&self.root, &path)?;
        let mut file = open_existing_object_file(&path)?;
        let Some(length_u64) = range.len() else {
            return Err(LocalObjectStoreError::RangeOutOfBounds);
        };

        file.seek(SeekFrom::Start(range.start()))?;
        let capacity = usize::try_from(length_u64)
            .map_err(|_error| LocalObjectStoreError::RangeOutOfBounds)?;
        let mut output = vec![0_u8; capacity];
        if let Err(error) = file.read_exact(&mut output) {
            if error.kind() == ErrorKind::UnexpectedEof {
                return Err(LocalObjectStoreError::RangeOutOfBounds);
            }

            return Err(LocalObjectStoreError::Io(error));
        }

        Ok(output)
    }

    fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
        self.metadata(key).map(|metadata| metadata.is_some())
    }

    fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
        let path = self.key_path(key);
        ensure_parent_directories_are_not_symlinked(&self.root, &path)?;
        let Some(metadata) = object_file_metadata(&path)? else {
            return Ok(None);
        };
        Ok(Some(ObjectMetadata::new(key.clone(), metadata.len(), None)))
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error> {
        let mut listed = Vec::new();
        let mut visitor = |metadata| {
            listed.push(metadata);
            Ok::<(), LocalObjectStoreError>(())
        };
        collect_metadata_recursive(&self.root, &self.root, prefix.as_str(), &mut visitor)?;
        listed.sort_by(|left, right| left.key().as_str().cmp(right.key().as_str()));
        Ok(listed)
    }

    fn visit_prefix<Visitor, VisitorError>(
        &self,
        prefix: &ObjectPrefix,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(ObjectMetadata) -> Result<(), VisitorError>,
    {
        collect_metadata_recursive(&self.root, &self.root, prefix.as_str(), &mut visitor)
    }

    fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
        let path = self.key_path(key);
        ensure_parent_directories_are_not_symlinked(&self.root, &path)?;
        match fs::remove_file(&path) {
            Ok(()) => {
                remove_empty_ancestors(&path, &self.root)?;
                Ok(DeleteOutcome::Deleted)
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(DeleteOutcome::NotFound),
            Err(error) => Err(LocalObjectStoreError::Io(error)),
        }
    }
}

/// Local object-store failure.
#[derive(Debug, Error)]
pub enum LocalObjectStoreError {
    /// Local filesystem access failed.
    #[error("local object store operation failed")]
    Io(#[from] IoError),
    /// The supplied body length did not match the expected integrity metadata.
    #[error("object body length did not match expected integrity")]
    IntegrityLengthMismatch,
    /// The supplied body hash did not match the expected integrity metadata.
    #[error("object body hash did not match expected integrity")]
    IntegrityHashMismatch,
    /// An existing object for the same key had different bytes.
    #[error("object key already exists with conflicting bytes")]
    ExistingObjectConflict,
    /// The requested byte range exceeded the stored object length.
    #[error("requested byte range exceeded stored object length")]
    RangeOutOfBounds,
    /// A stored object path could not be represented as a valid object key.
    #[error("stored object path could not be represented as a valid object key")]
    InvalidStoredKey,
    /// The target object path was not representable on disk.
    #[error("validated object key could not be mapped to a local path")]
    InvalidObjectPath,
}

fn verify_integrity(
    bytes: &[u8],
    integrity: &ObjectIntegrity,
) -> Result<(), LocalObjectStoreError> {
    let body_length = u64::try_from(bytes.len())
        .map_err(|_error| LocalObjectStoreError::IntegrityLengthMismatch)?;
    if body_length != integrity.length() {
        return Err(LocalObjectStoreError::IntegrityLengthMismatch);
    }

    let actual = chunk_hash(bytes);
    if actual != integrity.hash() {
        return Err(LocalObjectStoreError::IntegrityHashMismatch);
    }

    Ok(())
}

fn chunk_hash(bytes: &[u8]) -> ShardlineHash {
    let digest = blake3::hash(bytes);
    ShardlineHash::from_bytes(*digest.as_bytes())
}

fn verify_file_integrity(
    path: &Path,
    integrity: &ObjectIntegrity,
) -> Result<(), LocalObjectStoreError> {
    let file = open_existing_object_file(path)?;
    verify_open_file_integrity(file, integrity)
}

fn verify_open_file_integrity(
    mut file: File,
    integrity: &ObjectIntegrity,
) -> Result<(), LocalObjectStoreError> {
    file.seek(SeekFrom::Start(0))?;
    with_verify_buffer(|buffer| {
        let mut hasher = blake3::Hasher::new();
        let mut length = 0_u64;
        loop {
            let read = file.read(buffer)?;
            if read == 0 {
                break;
            }
            let read = u64::try_from(read)
                .map_err(|_error| LocalObjectStoreError::IntegrityLengthMismatch)?;
            length = length
                .checked_add(read)
                .ok_or(LocalObjectStoreError::IntegrityLengthMismatch)?;
            let read = usize::try_from(read)
                .map_err(|_error| LocalObjectStoreError::IntegrityLengthMismatch)?;
            hasher.update(
                buffer
                    .get(..read)
                    .ok_or(LocalObjectStoreError::IntegrityLengthMismatch)?,
            );
        }

        if length != integrity.length() {
            return Err(LocalObjectStoreError::IntegrityLengthMismatch);
        }

        let actual = ShardlineHash::from_bytes(*hasher.finalize().as_bytes());
        if actual != integrity.hash() {
            return Err(LocalObjectStoreError::IntegrityHashMismatch);
        }

        Ok(())
    })
}

fn link_temporary_file_if_absent(
    root: &Path,
    path: &Path,
    temporary: &Path,
    _integrity: &ObjectIntegrity,
    temporary_bytes: Option<&[u8]>,
) -> Result<PutOutcome, LocalObjectStoreError> {
    ensure_parent_directories_are_not_symlinked(root, path)?;
    match hard_link_file_if_absent(root, path, temporary) {
        Ok(()) => {
            remove_temporary_file(temporary)?;
            Ok(PutOutcome::Inserted)
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            let outcome = existing_object_outcome(path, temporary, temporary_bytes);
            remove_temporary_file(temporary)?;
            outcome
        }
        Err(error) => {
            remove_temporary_file(temporary)?;
            Err(LocalObjectStoreError::Io(error))
        }
    }
}

fn existing_object_outcome(
    path: &Path,
    temporary: &Path,
    temporary_bytes: Option<&[u8]>,
) -> Result<PutOutcome, LocalObjectStoreError> {
    let existing = open_existing_object_file(path)?;
    if let Some(temporary_bytes) = temporary_bytes {
        ensure_file_matches_bytes(existing, temporary_bytes)?;
        return Ok(PutOutcome::AlreadyExists);
    }

    let temporary = open_existing_object_file(temporary)?;
    ensure_files_match(existing, temporary)?;
    Ok(PutOutcome::AlreadyExists)
}

fn object_file_metadata(path: &Path) -> Result<Option<Metadata>, LocalObjectStoreError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure_regular_file_metadata(&metadata)?;
            Ok(Some(metadata))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(LocalObjectStoreError::Io(error)),
    }
}

fn ensure_parent_directories_are_not_symlinked(
    root: &Path,
    path: &Path,
) -> Result<(), LocalObjectStoreError> {
    ensure_directory_path_components_are_not_symlinked(root)?;
    let parent = if path == root {
        root
    } else {
        path.parent().unwrap_or(root)
    };
    ensure_directory_path_components_are_not_symlinked(parent)?;

    Ok(())
}

fn ensure_directory_path_components_are_not_symlinked(
    path: &Path,
) -> Result<(), LocalObjectStoreError> {
    ensure_directory_path_components_are_not_symlinked_shared(path)
        .map_err(map_directory_path_error)
}

fn map_directory_path_error(error: DirectoryPathError) -> LocalObjectStoreError {
    match error {
        DirectoryPathError::UnsupportedPrefix
        | DirectoryPathError::SymlinkedComponent(_)
        | DirectoryPathError::NonDirectoryComponent(_) => LocalObjectStoreError::InvalidObjectPath,
        DirectoryPathError::Io(error) => LocalObjectStoreError::Io(error),
    }
}

fn ensure_regular_file_metadata(metadata: &Metadata) -> Result<(), LocalObjectStoreError> {
    if metadata.file_type().is_file() {
        return Ok(());
    }

    Err(LocalObjectStoreError::InvalidObjectPath)
}

fn open_existing_object_file(path: &Path) -> Result<File, LocalObjectStoreError> {
    let file = open_regular_file(path).map_err(map_object_open_error)?;
    ensure_regular_file_metadata(&file.metadata()?)?;
    Ok(file)
}

#[cfg(unix)]
fn open_regular_file(path: &Path) -> Result<File, IoError> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

#[cfg(not(unix))]
fn open_regular_file(path: &Path) -> Result<File, IoError> {
    OpenOptions::new().read(true).open(path)
}

fn map_object_open_error(error: IoError) -> LocalObjectStoreError {
    if is_symlink_open_error(&error) {
        return LocalObjectStoreError::InvalidObjectPath;
    }

    LocalObjectStoreError::Io(error)
}

#[cfg(unix)]
fn is_symlink_open_error(error: &IoError) -> bool {
    error.raw_os_error() == Some(libc::ELOOP)
}

#[cfg(not(unix))]
fn is_symlink_open_error(_error: &IoError) -> bool {
    false
}

fn ensure_file_matches_bytes(mut file: File, expected: &[u8]) -> Result<(), LocalObjectStoreError> {
    file.seek(SeekFrom::Start(0))?;
    let expected_length = u64::try_from(expected.len())
        .map_err(|_error| LocalObjectStoreError::IntegrityLengthMismatch)?;
    if file.metadata()?.len() != expected_length {
        return Err(LocalObjectStoreError::IntegrityLengthMismatch);
    }

    with_verify_buffer(|buffer| {
        let mut compared = 0_usize;
        loop {
            let read = file.read(buffer)?;
            if read == 0 {
                break;
            }
            let end = compared
                .checked_add(read)
                .ok_or(LocalObjectStoreError::IntegrityLengthMismatch)?;
            let Some(expected_slice) = expected.get(compared..end) else {
                return Err(LocalObjectStoreError::IntegrityLengthMismatch);
            };
            let Some(actual_slice) = buffer.get(..read) else {
                return Err(LocalObjectStoreError::IntegrityLengthMismatch);
            };
            if actual_slice != expected_slice {
                return Err(LocalObjectStoreError::IntegrityHashMismatch);
            }
            compared = end;
        }

        if compared != expected.len() {
            return Err(LocalObjectStoreError::IntegrityLengthMismatch);
        }

        Ok(())
    })
}

fn ensure_files_match(mut existing: File, mut expected: File) -> Result<(), LocalObjectStoreError> {
    existing.seek(SeekFrom::Start(0))?;
    expected.seek(SeekFrom::Start(0))?;
    if existing.metadata()?.len() != expected.metadata()?.len() {
        return Err(LocalObjectStoreError::IntegrityLengthMismatch);
    }

    with_verify_buffers(|existing_buffer, expected_buffer| {
        loop {
            let existing_read = existing.read(existing_buffer)?;
            let expected_read = expected.read(expected_buffer)?;
            if existing_read != expected_read {
                return Err(LocalObjectStoreError::IntegrityLengthMismatch);
            }
            if existing_read == 0 {
                break;
            }
            let Some(existing_slice) = existing_buffer.get(..existing_read) else {
                return Err(LocalObjectStoreError::IntegrityLengthMismatch);
            };
            let Some(expected_slice) = expected_buffer.get(..expected_read) else {
                return Err(LocalObjectStoreError::IntegrityLengthMismatch);
            };
            if existing_slice != expected_slice {
                return Err(LocalObjectStoreError::IntegrityHashMismatch);
            }
        }

        Ok(())
    })
}

fn with_verify_buffer<T>(
    callback: impl FnOnce(&mut [u8]) -> Result<T, LocalObjectStoreError>,
) -> Result<T, LocalObjectStoreError> {
    VERIFY_BUFFER_A.with(|buffer| {
        let mut buffer = buffer.borrow_mut();
        ensure_verify_buffer_length(&mut buffer);
        callback(buffer.as_mut_slice())
    })
}

fn with_verify_buffers<T>(
    callback: impl FnOnce(&mut [u8], &mut [u8]) -> Result<T, LocalObjectStoreError>,
) -> Result<T, LocalObjectStoreError> {
    VERIFY_BUFFER_A.with(|first| {
        VERIFY_BUFFER_B.with(|second| {
            let mut first = first.borrow_mut();
            let mut second = second.borrow_mut();
            ensure_verify_buffer_length(&mut first);
            ensure_verify_buffer_length(&mut second);
            callback(first.as_mut_slice(), second.as_mut_slice())
        })
    })
}

fn ensure_verify_buffer_length(buffer: &mut Vec<u8>) {
    if buffer.len() != VERIFY_BUFFER_BYTES {
        buffer.resize(VERIFY_BUFFER_BYTES, 0);
    }
}

fn remove_temporary_file(path: &Path) -> Result<(), LocalObjectStoreError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LocalObjectStoreError::Io(error)),
    }
}

fn collect_metadata_recursive<Visitor, VisitorError>(
    root: &Path,
    directory: &Path,
    prefix: &str,
    visitor: &mut Visitor,
) -> Result<(), VisitorError>
where
    LocalObjectStoreError: Into<VisitorError>,
    Visitor: FnMut(ObjectMetadata) -> Result<(), VisitorError>,
{
    let Some(entries) = read_dir_if_exists(directory).map_err(Into::into)? else {
        return Ok(());
    };

    for entry in entries {
        let entry = entry
            .map_err(LocalObjectStoreError::Io)
            .map_err(Into::into)?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(LocalObjectStoreError::Io)
            .map_err(Into::into)?;
        if file_type.is_dir() {
            collect_metadata_recursive(root, &path, prefix, visitor)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        let relative = path
            .strip_prefix(root)
            .map_err(|_error| LocalObjectStoreError::InvalidStoredKey)
            .map_err(Into::into)?;
        let relative = relative
            .to_str()
            .ok_or(LocalObjectStoreError::InvalidStoredKey)
            .map_err(Into::into)?;
        let relative = relative.replace('\\', "/");
        if !relative.starts_with(prefix) {
            continue;
        }

        let key = ObjectKey::parse(&relative)
            .map_err(|_error| LocalObjectStoreError::InvalidStoredKey)
            .map_err(Into::into)?;
        let metadata = fs::symlink_metadata(&path)
            .map_err(LocalObjectStoreError::Io)
            .map_err(Into::into)?;
        ensure_regular_file_metadata(&metadata).map_err(Into::into)?;
        visitor(ObjectMetadata::new(key, metadata.len(), None))?;
    }

    Ok(())
}

fn read_dir_if_exists(directory: &Path) -> Result<Option<fs::ReadDir>, LocalObjectStoreError> {
    ensure_directory_path_components_are_not_symlinked(directory)?;
    match fs::read_dir(directory) {
        Ok(entries) => Ok(Some(entries)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(LocalObjectStoreError::Io(error)),
    }
}

fn remove_empty_ancestors(path: &Path, root: &Path) -> Result<(), LocalObjectStoreError> {
    let mut current = path.parent();
    while let Some(directory) = current {
        if directory == root {
            break;
        }

        match fs::remove_dir(directory) {
            Ok(()) => {
                current = directory.parent();
            }
            Err(error) if error.kind() == ErrorKind::DirectoryNotEmpty => break,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                current = directory.parent();
            }
            Err(error) => return Err(LocalObjectStoreError::Io(error)),
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use std::{fs, io::ErrorKind as IoErrorKind, path::PathBuf};

    use shardline_protocol::ByteRange;

    use super::{LocalObjectStore, LocalObjectStoreError};
    use crate::{
        DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix, ObjectStore,
        PutOutcome, local_fs::set_before_local_write_hook,
    };

    #[test]
    fn local_object_store_roundtrips_metadata_ranges_inventory_and_delete() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let store = LocalObjectStore::new(storage.path().to_path_buf());
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };

        let key = ObjectKey::parse("xorbs/default/aa/hash.xorb");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let body = b"abcdefgh";
        let integrity = ObjectIntegrity::new(super::chunk_hash(body), 8);

        let inserted = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
        let duplicate = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);

        assert!(matches!(inserted, Ok(PutOutcome::Inserted)));
        assert!(matches!(duplicate, Ok(PutOutcome::AlreadyExists)));
        assert!(matches!(store.contains(&key), Ok(true)));
        let metadata = store.metadata(&key);
        assert!(matches!(metadata, Ok(Some(_))));
        if let Ok(Some(metadata)) = metadata {
            assert_eq!(metadata.length(), 8);
        }

        let range = ByteRange::new(2, 5);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };
        let read = store.read_range(&key, range);
        assert!(read.is_ok());
        if let Ok(read) = read {
            assert_eq!(read, b"cdef".to_vec());
        }

        let prefix = ObjectPrefix::parse("xorbs/default/");
        assert!(prefix.is_ok());
        let Ok(prefix) = prefix else {
            return;
        };
        let listed = store.list_prefix(&prefix);
        assert!(listed.is_ok());
        let Ok(listed) = listed else {
            return;
        };
        assert_eq!(listed.len(), 1);
        let first = listed.first();
        assert!(first.is_some());
        if let Some(first) = first {
            assert_eq!(first.key(), &key);
        }
        let mut visited = Vec::new();
        let visit = store.visit_prefix(&prefix, |visited_metadata| {
            visited.push(visited_metadata.key().clone());
            Ok::<(), LocalObjectStoreError>(())
        });
        assert!(visit.is_ok());
        assert_eq!(visited, vec![key.clone()]);

        assert!(matches!(
            store.delete_if_present(&key),
            Ok(DeleteOutcome::Deleted)
        ));
        assert!(matches!(
            store.delete_if_present(&key),
            Ok(DeleteOutcome::NotFound)
        ));
        assert!(matches!(store.contains(&key), Ok(false)));
    }

    #[test]
    fn local_object_store_rejects_integrity_mismatch_and_out_of_bounds_range() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let store = LocalObjectStore::new(storage.path().to_path_buf());
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };

        let key = ObjectKey::parse("xorbs/default/bb/hash.xorb");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let wrong_integrity = ObjectIntegrity::new(super::chunk_hash(b"abc"), 4);

        let result = store.put_if_absent(&key, ObjectBody::from_slice(b"abc"), &wrong_integrity);
        assert!(matches!(
            result,
            Err(LocalObjectStoreError::IntegrityLengthMismatch)
        ));

        let integrity = ObjectIntegrity::new(super::chunk_hash(b"abc"), 3);
        let inserted = store.put_if_absent(&key, ObjectBody::from_slice(b"abc"), &integrity);
        assert!(matches!(inserted, Ok(PutOutcome::Inserted)));

        let range = ByteRange::new(1, 5);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };
        let read = store.read_range(&key, range);
        assert!(matches!(read, Err(LocalObjectStoreError::RangeOutOfBounds)));
    }

    #[test]
    fn local_object_store_installs_temporary_file_without_buffering() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let store = LocalObjectStore::new(storage.path().join("objects"));
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };
        let key = ObjectKey::parse("xorbs/default/dd/hash.xorb");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let temporary = storage.path().join("body.tmp");
        let write = fs::write(&temporary, b"streamed-body");
        assert!(write.is_ok());
        let integrity = ObjectIntegrity::new(super::chunk_hash(b"streamed-body"), 13);

        let inserted = store.put_temporary_file_if_absent(&key, &temporary, &integrity);

        assert!(matches!(inserted, Ok(PutOutcome::Inserted)));
        assert!(!temporary.exists());
        let range = ByteRange::new(0, 12);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };
        let read = store.read_range(&key, range);
        assert!(matches!(read, Ok(bytes) if bytes == b"streamed-body"));
    }

    #[test]
    fn local_object_store_temporary_file_duplicate_is_idempotent() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let store = LocalObjectStore::new(storage.path().join("objects"));
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };
        let key = ObjectKey::parse("xorbs/default/ee/hash.xorb");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let body = b"same-body";
        let integrity = ObjectIntegrity::new(super::chunk_hash(body), 9);
        let inserted = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
        assert!(matches!(inserted, Ok(PutOutcome::Inserted)));
        let duplicate = storage.path().join("duplicate.tmp");
        let write = fs::write(&duplicate, body);
        assert!(write.is_ok());

        let outcome = store.put_temporary_file_if_absent(&key, &duplicate, &integrity);

        assert!(matches!(outcome, Ok(PutOutcome::AlreadyExists)));
        assert!(!duplicate.exists());
    }

    #[test]
    fn local_object_store_temporary_file_rejects_conflicting_existing_object() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let store = LocalObjectStore::new(storage.path().join("objects"));
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };
        let key = ObjectKey::parse("xorbs/default/ef/hash.xorb");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let inserted = store.put_if_absent(
            &key,
            ObjectBody::from_slice(b"same-size"),
            &ObjectIntegrity::new(super::chunk_hash(b"same-size"), 9),
        );
        assert!(matches!(inserted, Ok(PutOutcome::Inserted)));
        let duplicate = storage.path().join("conflict.tmp");
        let write = fs::write(&duplicate, b"other-val");
        assert!(write.is_ok());

        let outcome = store.put_temporary_file_if_absent(
            &key,
            &duplicate,
            &ObjectIntegrity::new(super::chunk_hash(b"other-val"), 9),
        );

        assert!(matches!(
            outcome,
            Err(LocalObjectStoreError::IntegrityHashMismatch)
        ));
        assert!(!duplicate.exists());
    }

    #[test]
    fn local_object_store_open_is_non_mutating_until_write() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let root = storage.path().join("objects");
        let store = LocalObjectStore::open(root.clone());

        assert!(!root.exists());

        let key = ObjectKey::parse("xorbs/default/cc/hash.xorb");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let contains = store.contains(&key);

        assert!(matches!(contains, Ok(false)));
        assert!(!root.exists());
    }

    #[cfg(unix)]
    #[test]
    fn local_object_store_new_rejects_symlinked_root_ancestor() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let target = storage.path().join("target");
        let created = fs::create_dir_all(&target);
        assert!(created.is_ok());
        let link = storage.path().join("link");
        let linked = symlink(&target, &link);
        assert!(linked.is_ok());

        let store = LocalObjectStore::new(link.join("objects"));

        assert!(matches!(
            store,
            Err(LocalObjectStoreError::InvalidObjectPath)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn local_object_store_list_prefix_rejects_symlinked_root_ancestor() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let target = storage.path().join("target");
        let object_root = target.join("objects");
        let parent = object_root.join("aa");
        let created = fs::create_dir_all(&parent);
        assert!(created.is_ok());
        let path = parent.join("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let written = fs::write(&path, b"payload");
        assert!(written.is_ok());
        let link = storage.path().join("link");
        let linked = symlink(&target, &link);
        assert!(linked.is_ok());
        let store = LocalObjectStore::open(link.join("objects"));
        let prefix = ObjectPrefix::parse("aa/");
        assert!(prefix.is_ok());
        let Ok(prefix) = prefix else {
            return;
        };

        let listed = store.list_prefix(&prefix);

        assert!(matches!(
            listed,
            Err(LocalObjectStoreError::InvalidObjectPath)
        ));
    }

    #[test]
    fn local_object_store_maps_validated_key_under_root() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let root = storage.path().join("objects");
        let store = LocalObjectStore::open(root.clone());
        let key = ObjectKey::parse("aa/hash");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };

        let path = store.path_for_key(&key);

        assert_eq!(path, root.join("aa/hash"));
    }

    #[test]
    fn local_object_store_rejects_corrupted_existing_object_under_same_key() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let store = LocalObjectStore::new(storage.path().join("objects"));
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };

        let key = ObjectKey::parse("xorbs/default/dd/hash.xorb");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let path = store.path_for_key(&key);
        let parent = path.parent();
        assert!(parent.is_some());
        let Some(parent) = parent else {
            return;
        };
        let created = fs::create_dir_all(parent);
        assert!(created.is_ok());

        let corrupted = fs::write(&path, b"baad");
        assert!(corrupted.is_ok());

        let body = b"good";
        let integrity = ObjectIntegrity::new(super::chunk_hash(body), 4);
        let outcome = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);

        assert!(matches!(
            outcome,
            Err(LocalObjectStoreError::IntegrityHashMismatch)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn local_object_store_rejects_symlinked_object_path() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let store = LocalObjectStore::new(storage.path().join("objects"));
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };
        let outside = storage.path().join("outside-secret");
        let outside_write = fs::write(&outside, b"secret");
        assert!(outside_write.is_ok());

        let key = ObjectKey::parse("xorbs/default/ff/hash.xorb");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let path = store.path_for_key(&key);
        let parent = path.parent();
        assert!(parent.is_some());
        let Some(parent) = parent else {
            return;
        };
        let created = fs::create_dir_all(parent);
        assert!(created.is_ok());
        let linked = symlink(&outside, &path);
        assert!(linked.is_ok());

        let metadata = store.metadata(&key);
        let range = ByteRange::new(0, 5);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };
        let read = store.read_range(&key, range);

        assert!(matches!(
            metadata,
            Err(LocalObjectStoreError::InvalidObjectPath)
        ));
        assert!(matches!(
            read,
            Err(LocalObjectStoreError::InvalidObjectPath)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn local_object_store_rejects_symlinked_parent_directory_reads() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let store = LocalObjectStore::new(storage.path().join("objects"));
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };

        let key = ObjectKey::parse("xorbs/default/aa/hash.xorb");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let outside = tempfile::tempdir();
        assert!(outside.is_ok());
        let Ok(outside) = outside else {
            return;
        };
        let outside_path = outside.path().join("hash.xorb");
        let outside_write = fs::write(&outside_path, b"secret");
        assert!(outside_write.is_ok());

        let link = storage
            .path()
            .join("objects")
            .join("xorbs")
            .join("default")
            .join("aa");
        let parent = link.parent();
        assert!(parent.is_some());
        let Some(parent) = parent else {
            return;
        };
        let created = fs::create_dir_all(parent);
        assert!(created.is_ok());
        let linked = symlink(outside.path(), &link);
        assert!(linked.is_ok());

        let metadata = store.metadata(&key);
        let range = ByteRange::new(0, 5);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };
        let read = store.read_range(&key, range);

        assert!(matches!(
            metadata,
            Err(LocalObjectStoreError::InvalidObjectPath)
        ));
        assert!(matches!(
            read,
            Err(LocalObjectStoreError::InvalidObjectPath)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn local_object_store_rejects_symlinked_parent_directory_writes() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let store = LocalObjectStore::new(storage.path().join("objects"));
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };

        let key = ObjectKey::parse("xorbs/default/bb/hash.xorb");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let outside = tempfile::tempdir();
        assert!(outside.is_ok());
        let Ok(outside) = outside else {
            return;
        };
        let link = storage
            .path()
            .join("objects")
            .join("xorbs")
            .join("default")
            .join("bb");
        let parent = link.parent();
        assert!(parent.is_some());
        let Some(parent) = parent else {
            return;
        };
        let created = fs::create_dir_all(parent);
        assert!(created.is_ok());
        let linked = symlink(outside.path(), &link);
        assert!(linked.is_ok());

        let body = b"payload";
        let integrity = ObjectIntegrity::new(super::chunk_hash(body), 7);
        let result = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);

        assert!(matches!(
            result,
            Err(LocalObjectStoreError::InvalidObjectPath)
        ));
        assert!(
            !outside.path().join("hash.xorb").exists(),
            "object write escaped into a symlink target outside the object root"
        );
    }

    #[cfg(unix)]
    #[test]
    fn local_object_store_rejects_parent_swap_race() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let outside = tempfile::tempdir();
        assert!(outside.is_ok());
        let Ok(outside) = outside else {
            return;
        };
        let store = LocalObjectStore::new(storage.path().join("objects"));
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };

        let key = ObjectKey::parse("xorbs/default/dd/hash.xorb");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let path = store.key_path(&key);
        let parent = path.parent().map(PathBuf::from);
        assert!(parent.is_some());
        let Some(parent) = parent else {
            return;
        };
        let moved_parent = storage.path().join("swapped-object-parent");
        let moved_parent_for_hook = moved_parent.clone();
        let escape_dir = outside.path().to_path_buf();

        set_before_local_write_hook(path, move || {
            let renamed = fs::rename(&parent, &moved_parent_for_hook);
            assert!(renamed.is_ok());
            let linked = symlink(&escape_dir, &parent);
            assert!(linked.is_ok());
        });

        let body = b"payload";
        let integrity = ObjectIntegrity::new(super::chunk_hash(body), 7);
        let result = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);

        assert!(matches!(
            result,
            Err(LocalObjectStoreError::Io(error)) if error.kind() == IoErrorKind::InvalidData
        ));
        assert!(
            !outside.path().join("hash.xorb").exists(),
            "object write escaped into an attacker-controlled symlink target"
        );
        assert!(
            !moved_parent.join("hash.xorb").exists(),
            "object write left a committed file behind in the detached original directory"
        );
    }

    #[cfg(unix)]
    #[test]
    fn local_object_store_rejects_symlinked_parent_directory_deletes() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let store = LocalObjectStore::new(storage.path().join("objects"));
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };

        let key = ObjectKey::parse("xorbs/default/cc/hash.xorb");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let outside = tempfile::tempdir();
        assert!(outside.is_ok());
        let Ok(outside) = outside else {
            return;
        };
        let outside_path = outside.path().join("hash.xorb");
        let outside_write = fs::write(&outside_path, b"secret");
        assert!(outside_write.is_ok());

        let link = storage
            .path()
            .join("objects")
            .join("xorbs")
            .join("default")
            .join("cc");
        let parent = link.parent();
        assert!(parent.is_some());
        let Some(parent) = parent else {
            return;
        };
        let created = fs::create_dir_all(parent);
        assert!(created.is_ok());
        let linked = symlink(outside.path(), &link);
        assert!(linked.is_ok());

        let result = store.delete_if_present(&key);

        assert!(matches!(
            result,
            Err(LocalObjectStoreError::InvalidObjectPath)
        ));
        assert!(
            outside_path.exists(),
            "object delete escaped into a symlink target outside the object root"
        );
    }
}
