use std::{
    fs::{self, File},
    io::{Error as IoError, ErrorKind, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use shardline_protocol::ByteRange;
use thiserror::Error;

use crate::{
    AsyncObjectStore, DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata,
    ObjectPrefix, ObjectStore, PutOutcome,
    local_fs::{
        PutBytesIfAbsentOutcome, hard_link_file_if_absent, put_bytes_if_absent,
        write_bytes_atomically,
    },
};

use super::{
    io::{
        self, ensure_file_matches_bytes, ensure_files_match, open_existing_object_file,
        verify_file_integrity,
    },
    metadata::{self, ensure_regular_file_metadata, object_file_metadata},
    util::verify_integrity,
    walk::{self, read_dir_if_exists},
};

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
        metadata::ensure_parent_directories_are_not_symlinked(&store.root, &store.root)?;
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
        metadata::ensure_parent_directories_are_not_symlinked(&self.root, &path)?;
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
        metadata::ensure_parent_directories_are_not_symlinked(&self.root, &path)?;
        io::link_temporary_file_if_absent(&self.root, &path, temporary, integrity, None)
    }

    /// Copies an existing object to a new key if the destination is absent.
    ///
    /// # Errors
    ///
    /// Returns [`LocalObjectStoreError`] when the source is missing, the destination
    /// conflicts with different bytes, or installation fails.
    pub fn copy_object_if_absent(
        &self,
        source: &ObjectKey,
        destination: &ObjectKey,
    ) -> Result<PutOutcome, LocalObjectStoreError> {
        let source_path = self.key_path(source);
        let destination_path = self.key_path(destination);
        let _source = open_existing_object_file(&source_path)?;
        metadata::ensure_parent_directories_are_not_symlinked(&self.root, &destination_path)?;
        match hard_link_file_if_absent(&self.root, &destination_path, &source_path) {
            Ok(()) => Ok(PutOutcome::Inserted),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                let existing = open_existing_object_file(&destination_path)?;
                let source_file = open_existing_object_file(&source_path)?;
                ensure_files_match(existing, source_file)?;
                Ok(PutOutcome::AlreadyExists)
            }
            Err(error) => Err(LocalObjectStoreError::Io(error)),
        }
    }

    /// Stores bytes at a key, replacing any existing object atomically.
    ///
    /// # Errors
    ///
    /// Returns [`LocalObjectStoreError`] when integrity validation or filesystem
    /// replacement fails.
    pub fn put_overwrite(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<(), LocalObjectStoreError> {
        let bytes = body.into_bytes();
        verify_integrity(&bytes, integrity)?;
        let path = self.key_path(key);
        metadata::ensure_parent_directories_are_not_symlinked(&self.root, &path)?;
        write_bytes_atomically(&self.root, &path, &bytes).map_err(LocalObjectStoreError::Io)
    }

    /// Lists a bounded page of direct child objects under a flat namespace prefix.
    ///
    /// # Errors
    ///
    /// Returns [`LocalObjectStoreError`] when the namespace path cannot be read or a
    /// listed child cannot be represented as a validated object key.
    pub fn list_flat_namespace_page(
        &self,
        prefix: &ObjectPrefix,
        start_after: Option<&ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, LocalObjectStoreError> {
        let prefix_str = prefix.as_str();
        if let Some(start_after) = start_after
            && !start_after.as_str().starts_with(prefix_str)
        {
            return Err(LocalObjectStoreError::InvalidStartAfter);
        }
        let directory = self.root.join(prefix_str);
        let Some(entries) = read_dir_if_exists(&directory)? else {
            return Ok(Vec::new());
        };
        let mut children = Vec::new();
        for entry in entries {
            let entry = entry.map_err(LocalObjectStoreError::Io)?;
            let file_type = entry.file_type().map_err(LocalObjectStoreError::Io)?;
            if !file_type.is_file() {
                continue;
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_error| LocalObjectStoreError::InvalidStoredKey)?;
            let key = ObjectKey::parse(&format!("{}{}", prefix.as_str(), name))
                .map_err(|_error| LocalObjectStoreError::InvalidStoredKey)?;
            if start_after.is_some_and(|offset| key.as_str() <= offset.as_str()) {
                continue;
            }
            let metadata = fs::symlink_metadata(entry.path()).map_err(LocalObjectStoreError::Io)?;
            ensure_regular_file_metadata(&metadata)?;
            children.push(ObjectMetadata::new(key, metadata.len(), None));
        }
        children.sort_by(|left, right| left.key().as_str().cmp(right.key().as_str()));
        children.truncate(limit);
        Ok(children)
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
        metadata::ensure_parent_directories_are_not_symlinked(&self.root, &path)?;
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
        metadata::ensure_parent_directories_are_not_symlinked(&self.root, &path)?;
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
        ObjectStore::metadata(self, key).map(|metadata| metadata.is_some())
    }

    fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
        let path = self.key_path(key);
        metadata::ensure_parent_directories_are_not_symlinked(&self.root, &path)?;
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
        walk::collect_metadata_recursive(&self.root, &self.root, prefix.as_str(), &mut visitor)?;
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
        walk::collect_metadata_recursive(&self.root, &self.root, prefix.as_str(), &mut visitor)
    }

    fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
        let path = self.key_path(key);
        metadata::ensure_parent_directories_are_not_symlinked(&self.root, &path)?;
        match fs::remove_file(&path) {
            Ok(()) => {
                walk::remove_empty_ancestors(&path, &self.root)?;
                Ok(DeleteOutcome::Deleted)
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(DeleteOutcome::NotFound),
            Err(error) => Err(LocalObjectStoreError::Io(error)),
        }
    }
}

#[async_trait]
impl AsyncObjectStore for LocalObjectStore {
    type Error = LocalObjectStoreError;

    async fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error> {
        let this = self.clone();
        let key = key.clone();
        let integrity = *integrity;
        let body_bytes = body.as_slice().to_vec();
        tokio::task::spawn_blocking(move || {
            ObjectStore::put_if_absent(&this, &key, ObjectBody::from_vec(body_bytes), &integrity)
        })
        .await
        .map_err(|join| {
            LocalObjectStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("{join}"),
            ))
        })?
    }

    async fn read_range(&self, key: &ObjectKey, range: ByteRange) -> Result<Vec<u8>, Self::Error> {
        let this = self.clone();
        let key = key.clone();
        tokio::task::spawn_blocking(move || ObjectStore::read_range(&this, &key, range))
            .await
            .map_err(|join| {
                LocalObjectStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("{join}"),
                ))
            })?
    }

    async fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
        let this = self.clone();
        let key = key.clone();
        tokio::task::spawn_blocking(move || ObjectStore::contains(&this, &key))
            .await
            .map_err(|join| {
                LocalObjectStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("{join}"),
                ))
            })?
    }

    async fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
        let this = self.clone();
        let key = key.clone();
        tokio::task::spawn_blocking(move || ObjectStore::metadata(&this, &key))
            .await
            .map_err(|join| {
                LocalObjectStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("{join}"),
                ))
            })?
    }

    async fn list_prefix(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error> {
        let this = self.clone();
        let prefix = prefix.clone();
        tokio::task::spawn_blocking(move || ObjectStore::list_prefix(&this, &prefix))
            .await
            .map_err(|join| {
                LocalObjectStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("{join}"),
                ))
            })?
    }

    async fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
        let this = self.clone();
        let key = key.clone();
        tokio::task::spawn_blocking(move || ObjectStore::delete_if_present(&this, &key))
            .await
            .map_err(|join| {
                LocalObjectStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("{join}"),
                ))
            })?
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
    /// The `start_after` key does not start with the requested prefix.
    #[error("start_after key is outside the requested prefix")]
    InvalidStartAfter,
}

#[cfg(test)]
mod async_tests {
    use crate::{
        AsyncObjectStore, DeleteOutcome, LocalObjectStore, ObjectBody, ObjectIntegrity, ObjectKey,
        ObjectPrefix, PutOutcome,
    };
    use shardline_protocol::{ByteRange, ShardlineHash};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn setup() -> (LocalObjectStore, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(dir.path().join("obj")).unwrap();
        (store, dir)
    }

    fn rt() -> Runtime {
        Runtime::new().unwrap()
    }

    #[test]
    fn async_local_put_if_absent_stores_object() {
        let (store, _dir) = setup();
        let key = ObjectKey::parse("test/key").unwrap();
        let hash = ShardlineHash::from_bytes(*blake3::hash(b"test data").as_bytes());
        let integrity = ObjectIntegrity::new(hash, 9);
        let result = rt().block_on(AsyncObjectStore::put_if_absent(
            &store,
            &key,
            ObjectBody::from_slice(b"test data"),
            &integrity,
        ));
        assert!(matches!(result, Ok(PutOutcome::Inserted)));
    }

    #[test]
    fn async_local_put_if_absent_idempotent() {
        let (store, _dir) = setup();
        let key = ObjectKey::parse("test/key").unwrap();
        let hash = ShardlineHash::from_bytes(*blake3::hash(b"data").as_bytes());
        let integrity = ObjectIntegrity::new(hash, 4);
        rt().block_on(AsyncObjectStore::put_if_absent(
            &store,
            &key,
            ObjectBody::from_slice(b"data"),
            &integrity,
        ))
        .unwrap();
        let second = rt().block_on(AsyncObjectStore::put_if_absent(
            &store,
            &key,
            ObjectBody::from_slice(b"data"),
            &integrity,
        ));
        assert!(matches!(second, Ok(PutOutcome::AlreadyExists)));
    }

    #[test]
    fn async_local_contains_returns_true() {
        let (store, _dir) = setup();
        let key = ObjectKey::parse("test/key").unwrap();
        let hash = ShardlineHash::from_bytes(*blake3::hash(b"data").as_bytes());
        let integrity = ObjectIntegrity::new(hash, 4);
        rt().block_on(AsyncObjectStore::put_if_absent(
            &store,
            &key,
            ObjectBody::from_slice(b"data"),
            &integrity,
        ))
        .unwrap();
        assert!(matches!(
            rt().block_on(AsyncObjectStore::contains(&store, &key)),
            Ok(true)
        ));
    }

    #[test]
    fn async_local_delete_if_present_removes() {
        let (store, _dir) = setup();
        let key = ObjectKey::parse("test/key").unwrap();
        let hash = ShardlineHash::from_bytes(*blake3::hash(b"data").as_bytes());
        let integrity = ObjectIntegrity::new(hash, 4);
        rt().block_on(AsyncObjectStore::put_if_absent(
            &store,
            &key,
            ObjectBody::from_slice(b"data"),
            &integrity,
        ))
        .unwrap();
        let deleted = rt().block_on(AsyncObjectStore::delete_if_present(&store, &key));
        assert!(matches!(deleted, Ok(DeleteOutcome::Deleted)));
    }

    #[test]
    fn async_local_metadata_returns_length() {
        let (store, _dir) = setup();
        let key = ObjectKey::parse("test/key").unwrap();
        let hash = ShardlineHash::from_bytes(*blake3::hash(b"12345").as_bytes());
        let integrity = ObjectIntegrity::new(hash, 5);
        rt().block_on(AsyncObjectStore::put_if_absent(
            &store,
            &key,
            ObjectBody::from_slice(b"12345"),
            &integrity,
        ))
        .unwrap();
        let meta = rt().block_on(AsyncObjectStore::metadata(&store, &key));
        assert!(matches!(meta, Ok(Some(ref m)) if m.length() == 5));
    }

    #[test]
    fn async_local_read_range_returns_data() {
        let (store, _dir) = setup();
        let key = ObjectKey::parse("test/rr").unwrap();
        let body = b"hello world";
        let hash = ShardlineHash::from_bytes(*blake3::hash(body).as_bytes());
        rt().block_on(AsyncObjectStore::put_if_absent(
            &store,
            &key,
            ObjectBody::from_slice(body),
            &ObjectIntegrity::new(hash, body.len() as u64),
        ))
        .unwrap();
        let range = ByteRange::new(0, 4).unwrap();
        let data = rt()
            .block_on(AsyncObjectStore::read_range(&store, &key, range))
            .unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn async_local_list_prefix_returns_filtered() {
        let (store, _dir) = setup();
        let hash = ShardlineHash::from_bytes(*blake3::hash(b"x").as_bytes());
        let integrity = ObjectIntegrity::new(hash, 1);
        rt().block_on(AsyncObjectStore::put_if_absent(
            &store,
            &ObjectKey::parse("ns/a").unwrap(),
            ObjectBody::from_slice(b"x"),
            &integrity,
        ))
        .unwrap();
        rt().block_on(AsyncObjectStore::put_if_absent(
            &store,
            &ObjectKey::parse("ns/b").unwrap(),
            ObjectBody::from_slice(b"x"),
            &integrity,
        ))
        .unwrap();
        rt().block_on(AsyncObjectStore::put_if_absent(
            &store,
            &ObjectKey::parse("other/c").unwrap(),
            ObjectBody::from_slice(b"x"),
            &integrity,
        ))
        .unwrap();
        let prefix = ObjectPrefix::parse("ns/").unwrap();
        let items = rt()
            .block_on(AsyncObjectStore::list_prefix(&store, &prefix))
            .unwrap();
        assert_eq!(items.len(), 2);
    }
}
