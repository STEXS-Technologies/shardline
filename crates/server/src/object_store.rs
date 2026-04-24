#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::{
    fs::File,
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
};

use shardline_index::{
    FileChunkRecord, FileRecord, FileRecordInvariantError, FileRecordStorageLayout,
};
use shardline_protocol::ByteRange;
use shardline_storage::{
    DeleteOutcome, LocalObjectStore, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata,
    ObjectPrefix, ObjectStore, PutOutcome, S3ObjectStore, S3ObjectStoreConfig,
};

use crate::{
    ObjectStorageAdapter, ServerConfig, ServerError, ServerFrontend, chunk_store::chunk_object_key,
    server_frontend::append_referenced_term_bytes,
};

#[cfg(test)]
type LocalObjectReadHook = Box<dyn FnOnce() + Send>;

#[cfg(test)]
struct LocalObjectReadHookRegistration {
    path: PathBuf,
    hook: LocalObjectReadHook,
}

#[cfg(test)]
type LocalObjectReadHookSlot = Option<LocalObjectReadHookRegistration>;

#[cfg(test)]
static BEFORE_LOCAL_OBJECT_READ_HOOK: LazyLock<Mutex<LocalObjectReadHookSlot>> =
    LazyLock::new(|| Mutex::new(None));

#[derive(Debug, Clone)]
pub(crate) enum ServerObjectStore {
    Local(LocalObjectStore),
    S3(S3ObjectStore),
    Blackhole,
}

impl ObjectStore for ServerObjectStore {
    type Error = ServerError;

    fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error> {
        match self {
            Self::Local(store) => Ok(store.put_if_absent(key, body, integrity)?),
            Self::S3(store) => Ok(store.put_if_absent(key, body, integrity)?),
            Self::Blackhole => Ok(PutOutcome::Inserted),
        }
    }

    fn read_range(&self, key: &ObjectKey, range: ByteRange) -> Result<Vec<u8>, Self::Error> {
        match self {
            Self::Local(store) => Ok(store.read_range(key, range)?),
            Self::S3(store) => Ok(store.read_range(key, range)?),
            Self::Blackhole => Err(ServerError::NotFound),
        }
    }

    fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
        match self {
            Self::Local(store) => Ok(store.contains(key)?),
            Self::S3(store) => Ok(store.contains(key)?),
            Self::Blackhole => Ok(false),
        }
    }

    fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
        match self {
            Self::Local(store) => Ok(store.metadata(key)?),
            Self::S3(store) => Ok(store.metadata(key)?),
            Self::Blackhole => Ok(None),
        }
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error> {
        match self {
            Self::Local(store) => Ok(store.list_prefix(prefix)?),
            Self::S3(store) => Ok(store.list_prefix(prefix)?),
            Self::Blackhole => Ok(Vec::new()),
        }
    }

    fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
        match self {
            Self::Local(store) => Ok(store.delete_if_present(key)?),
            Self::S3(store) => Ok(store.delete_if_present(key)?),
            Self::Blackhole => Ok(DeleteOutcome::NotFound),
        }
    }
}

impl ServerObjectStore {
    pub(crate) fn local(root: impl Into<PathBuf>) -> Result<Self, ServerError> {
        Ok(Self::Local(LocalObjectStore::new(root.into())?))
    }

    pub(crate) fn s3(config: S3ObjectStoreConfig) -> Result<Self, ServerError> {
        Ok(Self::S3(S3ObjectStore::new(config)?))
    }

    pub(crate) const fn blackhole() -> Self {
        Self::Blackhole
    }

    pub(crate) fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, ServerError> {
        match self {
            Self::Local(store) => Ok(store.put_if_absent(key, body, integrity)?),
            Self::S3(store) => Ok(store.put_if_absent(key, body, integrity)?),
            Self::Blackhole => Ok(PutOutcome::Inserted),
        }
    }

    pub(crate) fn put_overwrite(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<(), ServerError> {
        match self {
            Self::Local(store) => Ok(store.put_overwrite(key, body, integrity)?),
            Self::S3(store) => Ok(store.put_overwrite(key, body, integrity)?),
            Self::Blackhole => Ok(()),
        }
    }

    pub(crate) fn read_range(
        &self,
        key: &ObjectKey,
        range: ByteRange,
    ) -> Result<Vec<u8>, ServerError> {
        match self {
            Self::Local(store) => Ok(store.read_range(key, range)?),
            Self::S3(store) => Ok(store.read_range(key, range)?),
            Self::Blackhole => Err(ServerError::NotFound),
        }
    }

    pub(crate) fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, ServerError> {
        match self {
            Self::Local(store) => Ok(store.metadata(key)?),
            Self::S3(store) => Ok(store.metadata(key)?),
            Self::Blackhole => Ok(None),
        }
    }

    pub(crate) fn visit_prefix<Visitor>(
        &self,
        prefix: &ObjectPrefix,
        mut visitor: Visitor,
    ) -> Result<(), ServerError>
    where
        Visitor: FnMut(ObjectMetadata) -> Result<(), ServerError>,
    {
        match self {
            Self::Local(store) => store.visit_prefix(prefix, &mut visitor),
            Self::S3(store) => store.visit_prefix(prefix, &mut visitor),
            Self::Blackhole => Ok(()),
        }
    }

    pub(crate) fn list_flat_namespace_page(
        &self,
        prefix: &ObjectPrefix,
        start_after: Option<&ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, ServerError> {
        match self {
            Self::Local(store) => Ok(store.list_flat_namespace_page(prefix, start_after, limit)?),
            Self::S3(store) => Ok(store.list_flat_namespace_page(prefix, start_after, limit)?),
            Self::Blackhole => Ok(Vec::new()),
        }
    }

    pub(crate) fn local_path_for_key(&self, key: &ObjectKey) -> Option<PathBuf> {
        match self {
            Self::Local(store) => Some(store.path_for_key(key)),
            Self::S3(_store) => None,
            Self::Blackhole => None,
        }
    }

    pub(crate) fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, ServerError> {
        match self {
            Self::Local(store) => Ok(store.delete_if_present(key)?),
            Self::S3(store) => Ok(store.delete_if_present(key)?),
            Self::Blackhole => Ok(DeleteOutcome::NotFound),
        }
    }

    pub(crate) fn copy_if_absent(
        &self,
        source: &ObjectKey,
        destination: &ObjectKey,
    ) -> Result<PutOutcome, ServerError> {
        match self {
            Self::Local(store) => Ok(store.copy_object_if_absent(source, destination)?),
            Self::S3(store) => Ok(store.copy_object_if_absent(source, destination)?),
            Self::Blackhole => Err(ServerError::NotFound),
        }
    }

    pub(crate) fn put_content_addressed_file(
        &self,
        key: &ObjectKey,
        path: &Path,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, ServerError> {
        match self {
            Self::Local(store) => Ok(store.put_temporary_file_if_absent(key, path, integrity)?),
            Self::S3(store) => Ok(store.put_content_addressed_file(key, path, integrity)?),
            Self::Blackhole => Ok(PutOutcome::Inserted),
        }
    }

    pub(crate) fn local_root(&self) -> Option<&Path> {
        match self {
            Self::Local(store) => Some(store.root()),
            Self::S3(_store) => None,
            Self::Blackhole => None,
        }
    }

    pub(crate) const fn backend_name(&self) -> &'static str {
        match self {
            Self::Local(_store) => "local",
            Self::S3(_store) => "s3",
            Self::Blackhole => "blackhole",
        }
    }
}

pub(crate) fn read_full_object(
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    length: u64,
) -> Result<Vec<u8>, ServerError> {
    if length == 0 {
        return Ok(Vec::new());
    }

    if let ServerObjectStore::Local(store) = object_store {
        let path = store.path_for_key(object_key);
        let file = store.open_object_file(object_key)?;
        let actual_length = validate_local_object_length(&file, length)?;
        return read_open_local_object(&path, file, actual_length);
    }

    let end = length.checked_sub(1).ok_or(ServerError::Overflow)?;
    let range = ByteRange::new(0, end).map_err(|_error| ServerError::Overflow)?;
    object_store.read_range(object_key, range)
}

pub(crate) fn reconstruct_local_file_bytes(
    object_store: &ServerObjectStore,
    chunks: &[FileChunkRecord],
    capacity: usize,
) -> Result<Vec<u8>, ServerError> {
    let mut output = Vec::with_capacity(capacity);
    for chunk in chunks {
        let expected_offset = u64::try_from(output.len())?;
        if chunk.offset != expected_offset {
            return Err(ServerError::FileRecordInvariant(
                FileRecordInvariantError::NonContiguousChunkOffsets,
            ));
        }
        let object_key = chunk_object_key(&chunk.hash)?;
        let ServerObjectStore::Local(store) = object_store else {
            return Err(ServerError::NotFound);
        };
        let path = store.path_for_key(&object_key);
        let file = store.open_object_file(&object_key)?;
        read_open_local_object_append(&path, file, chunk.length, &mut output)?;
    }
    if output.len() != capacity {
        return Err(ServerError::StoredObjectLengthMismatch);
    }
    Ok(output)
}

pub(crate) fn reconstruct_file_record_bytes(
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    record: &FileRecord,
) -> Result<Vec<u8>, ServerError> {
    let capacity = usize::try_from(record.total_bytes)?;
    match record.storage_layout() {
        FileRecordStorageLayout::ReferencedObjectTerms => {
            reconstruct_referenced_object_file_bytes(object_store, frontends, record, capacity)
        }
        FileRecordStorageLayout::StoredChunks => {
            reconstruct_chunk_file_bytes(object_store, &record.chunks, capacity)
        }
    }
}

fn reconstruct_chunk_file_bytes(
    object_store: &ServerObjectStore,
    chunks: &[FileChunkRecord],
    capacity: usize,
) -> Result<Vec<u8>, ServerError> {
    if object_store.local_root().is_some() {
        return reconstruct_local_file_bytes(object_store, chunks, capacity);
    }

    let mut output = Vec::with_capacity(capacity);
    for chunk in chunks {
        let object_key = chunk_object_key(&chunk.hash)?;
        let bytes = read_full_object(object_store, &object_key, chunk.length)?;
        output.extend_from_slice(&bytes);
    }
    if output.len() != capacity {
        return Err(ServerError::StoredObjectLengthMismatch);
    }

    Ok(output)
}

fn reconstruct_referenced_object_file_bytes(
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    record: &FileRecord,
    capacity: usize,
) -> Result<Vec<u8>, ServerError> {
    let mut output = Vec::with_capacity(capacity);
    for term in &record.chunks {
        let term_start = u64::try_from(output.len())?;
        if term.offset != term_start {
            return Err(ServerError::FileRecordInvariant(
                FileRecordInvariantError::NonContiguousChunkOffsets,
            ));
        }
        append_referenced_term_bytes(frontends, object_store, term, &mut output)?;
        let term_end = term
            .offset
            .checked_add(term.length)
            .ok_or(ServerError::Overflow)?;
        if u64::try_from(output.len())? != term_end {
            return Err(ServerError::StoredObjectLengthMismatch);
        }
    }
    if output.len() != capacity {
        return Err(ServerError::StoredObjectLengthMismatch);
    }

    Ok(output)
}

fn read_open_local_object(path: &Path, file: File, length: u64) -> Result<Vec<u8>, ServerError> {
    let capacity = usize::try_from(length)?;
    let mut output = Vec::with_capacity(capacity);
    read_open_local_object_append(path, file, length, &mut output)?;
    if output.len() != capacity {
        return Err(ServerError::StoredObjectLengthMismatch);
    }
    Ok(output)
}

fn read_open_local_object_append(
    path: &Path,
    mut file: File,
    expected_length: u64,
    output: &mut Vec<u8>,
) -> Result<(), ServerError> {
    validate_local_object_length(&file, expected_length)?;
    run_before_local_object_read_hook(path);
    let start_len = output.len();
    let mut limited = file.by_ref().take(expected_length);
    if let Err(error) = limited.read_to_end(output) {
        if error.kind() == ErrorKind::UnexpectedEof {
            return Err(ServerError::StoredObjectLengthMismatch);
        }

        return Err(ServerError::Io(error));
    }
    let read_len = output
        .len()
        .checked_sub(start_len)
        .ok_or(ServerError::Overflow)?;
    if u64::try_from(read_len)? != expected_length {
        return Err(ServerError::StoredObjectLengthMismatch);
    }
    let mut trailing_byte = [0_u8; 1];
    match file.read(&mut trailing_byte) {
        Ok(0) => {}
        Ok(_observed) => return Err(ServerError::StoredObjectLengthMismatch),
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
            return Err(ServerError::StoredObjectLengthMismatch);
        }
        Err(error) => return Err(ServerError::Io(error)),
    }
    validate_local_object_length(&file, expected_length)?;

    Ok(())
}

fn validate_local_object_length(file: &File, expected_length: u64) -> Result<u64, ServerError> {
    let actual_length = file.metadata()?.len();
    if actual_length != expected_length {
        return Err(ServerError::StoredObjectLengthMismatch);
    }

    Ok(actual_length)
}

#[cfg(test)]
fn run_before_local_object_read_hook(path: &Path) {
    let hook = match BEFORE_LOCAL_OBJECT_READ_HOOK.lock() {
        Ok(mut guard) => take_local_object_read_hook_for_path(&mut guard, path),
        Err(poisoned) => take_local_object_read_hook_for_path(&mut poisoned.into_inner(), path),
    };
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn take_local_object_read_hook_for_path(
    slot: &mut LocalObjectReadHookSlot,
    path: &Path,
) -> Option<LocalObjectReadHook> {
    if !matches!(slot, Some(registration) if registration.path == path) {
        return None;
    }
    slot.take().map(|registration| registration.hook)
}

#[cfg(not(test))]
const fn run_before_local_object_read_hook(_path: &Path) {}

#[cfg(test)]
fn set_before_local_object_read_hook(path: PathBuf, hook: impl FnOnce() + Send + 'static) {
    let mut slot = match BEFORE_LOCAL_OBJECT_READ_HOOK.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *slot = Some(LocalObjectReadHookRegistration {
        path,
        hook: Box::new(hook),
    });
}

pub(crate) fn object_store_from_config(
    config: &ServerConfig,
) -> Result<ServerObjectStore, ServerError> {
    match config.object_storage_adapter() {
        ObjectStorageAdapter::Local => ServerObjectStore::local(config.root_dir().join("chunks")),
        ObjectStorageAdapter::S3 => {
            let s3_config = config
                .s3_object_store_config()
                .ok_or(ServerError::MissingS3ObjectStoreConfig)?
                .clone();
            ServerObjectStore::s3(s3_config)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{File, OpenOptions},
        io::Write,
    };

    use super::{read_open_local_object_append, set_before_local_object_read_hook};
    use crate::ServerError;

    #[test]
    fn local_object_read_rejects_growth_after_length_validation_without_retaining_growth_bytes() {
        let object = tempfile::NamedTempFile::new();
        assert!(object.is_ok());
        let Ok(mut object) = object else {
            return;
        };
        let initial_write = object.write_all(b"abcd");
        assert!(initial_write.is_ok());
        let object_sync = object.as_file().sync_all();
        assert!(object_sync.is_ok());

        let reader = File::open(object.path());
        assert!(reader.is_ok());
        let Ok(reader) = reader else {
            return;
        };
        let writer = OpenOptions::new().append(true).open(object.path());
        assert!(writer.is_ok());
        let Ok(mut writer) = writer else {
            return;
        };
        set_before_local_object_read_hook(object.path().to_path_buf(), move || {
            let appended = writer.write_all(b"efgh");
            assert!(appended.is_ok());
            let writer_sync = writer.sync_all();
            assert!(writer_sync.is_ok());
        });

        let mut output = Vec::new();
        let result = read_open_local_object_append(object.path(), reader, 4, &mut output);

        assert!(matches!(
            result,
            Err(ServerError::StoredObjectLengthMismatch)
        ));
        assert_eq!(output, b"abcd");
    }
}
