#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::{
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::{Error as IoError, ErrorKind, Read},
    path::{Path, PathBuf},
};

use serde::Deserialize;
use serde_json::from_slice;
use shardline_storage::{
    DirectoryPathError, ObjectKey, ObjectKeyError,
    ensure_directory_path_components_are_not_symlinked as ensure_directory_path_components_are_not_symlinked_shared,
};

use super::error::LocalIndexStoreError;
use super::records::{
    DedupeShardMapping, DedupeShardRecord, LegacyQuarantineCandidateRecord,
    ProviderRepositoryState, ProviderRepositoryStateRecord, QuarantineCandidate,
    QuarantineCandidateRecord, RetentionHold, RetentionHoldRecord, WebhookDelivery,
    WebhookDeliveryRecord,
};
use crate::local_fs::{write_file_atomically, write_new_file};
use crate::{parse_xet_hash_hex, xet_hash_hex_string};

pub(super) const MAX_CONTROL_PLANE_METADATA_BYTES: u64 = 1_048_576;
pub(super) const MAX_RECONSTRUCTION_METADATA_BYTES: u64 = 1_073_741_824;

#[cfg(test)]
type LocalMetadataReadHook = Box<dyn FnOnce() + Send>;

#[cfg(test)]
struct LocalMetadataReadHookRegistration {
    path: PathBuf,
    hook: LocalMetadataReadHook,
}

#[cfg(test)]
type LocalMetadataReadHookSlot = Option<LocalMetadataReadHookRegistration>;

#[cfg(test)]
static BEFORE_LOCAL_METADATA_READ_HOOK: LazyLock<Mutex<LocalMetadataReadHookSlot>> =
    LazyLock::new(|| Mutex::new(None));

pub(super) fn write_json_atomically<T>(
    root: &Path,
    path: &Path,
    value: &T,
) -> Result<(), LocalIndexStoreError>
where
    T: serde::Serialize,
{
    ensure_directory_path_components_are_not_symlinked(root)?;
    ensure_parent_directory_path_components_are_not_symlinked(path)?;
    let bytes = serde_json::to_vec(value)?;
    write_file_atomically(root, path, &bytes).map_err(LocalIndexStoreError::Io)
}

pub(super) fn read_json_if_exists<T>(
    path: &Path,
    maximum_bytes: u64,
) -> Result<Option<T>, LocalIndexStoreError>
where
    T: for<'de> Deserialize<'de>,
{
    read_file_if_exists_bounded(path, maximum_bytes)?
        .map(|bytes| from_slice(&bytes))
        .transpose()
        .map_err(LocalIndexStoreError::from)
}

pub(super) fn read_quarantine_candidate_if_exists(
    path: &Path,
) -> Result<Option<QuarantineCandidate>, LocalIndexStoreError> {
    let Some(bytes) = read_file_if_exists_bounded(path, MAX_CONTROL_PLANE_METADATA_BYTES)? else {
        return Ok(None);
    };
    if let Ok(record) = from_slice::<QuarantineCandidateRecord>(&bytes) {
        return Ok(Some(record.into_domain()?));
    }

    if let Ok(record) = from_slice::<LegacyQuarantineCandidateRecord>(&bytes) {
        let object_key = legacy_quarantine_object_key(&record.hash)?;
        let candidate = QuarantineCandidate::new(
            object_key,
            record.bytes,
            record.first_seen_unreachable_at_unix_seconds,
            record.delete_after_unix_seconds,
        )?;
        return Ok(Some(candidate));
    }

    Err(from_slice::<QuarantineCandidateRecord>(&bytes).unwrap_err().into())
}

pub(super) fn visit_quarantine_candidates_recursive<Visitor, VisitorError>(
    directory: &Path,
    visitor: &mut Visitor,
) -> Result<(), VisitorError>
where
    LocalIndexStoreError: Into<VisitorError>,
    Visitor: FnMut(QuarantineCandidate) -> Result<(), VisitorError>,
{
    let Some(entries) = read_dir_if_exists(directory).map_err(Into::into)? else {
        return Ok(());
    };

    for entry in entries {
        let entry = entry
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        if file_type.is_dir() {
            visit_quarantine_candidates_recursive(&path, visitor)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        if let Some(candidate) = read_quarantine_candidate_if_exists(&path).map_err(Into::into)? {
            visitor(candidate)?;
        }
    }

    Ok(())
}

pub(super) fn read_retention_hold_if_exists(
    path: &Path,
) -> Result<Option<RetentionHold>, LocalIndexStoreError> {
    read_json_if_exists::<RetentionHoldRecord>(path, MAX_CONTROL_PLANE_METADATA_BYTES)?
        .map(RetentionHoldRecord::into_domain)
        .transpose()
}

pub(super) fn visit_retention_holds_recursive<Visitor, VisitorError>(
    directory: &Path,
    visitor: &mut Visitor,
) -> Result<(), VisitorError>
where
    LocalIndexStoreError: Into<VisitorError>,
    Visitor: FnMut(RetentionHold) -> Result<(), VisitorError>,
{
    let Some(entries) = read_dir_if_exists(directory).map_err(Into::into)? else {
        return Ok(());
    };

    for entry in entries {
        let entry = entry
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        if file_type.is_dir() {
            visit_retention_holds_recursive(&path, visitor)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        if let Some(hold) = read_retention_hold_if_exists(&path).map_err(Into::into)? {
            visitor(hold)?;
        }
    }

    Ok(())
}

pub(super) fn visit_dedupe_shard_mappings_recursive<Visitor, VisitorError>(
    directory: &Path,
    visitor: &mut Visitor,
) -> Result<(), VisitorError>
where
    LocalIndexStoreError: Into<VisitorError>,
    Visitor: FnMut(DedupeShardMapping) -> Result<(), VisitorError>,
{
    let Some(entries) = read_dir_if_exists(directory).map_err(Into::into)? else {
        return Ok(());
    };

    for entry in entries {
        let entry = entry
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        if file_type.is_dir() {
            visit_dedupe_shard_mappings_recursive(&path, visitor)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        if let Some(mapping) =
            read_json_if_exists::<DedupeShardRecord>(&path, MAX_CONTROL_PLANE_METADATA_BYTES)
                .map_err(Into::into)?
                .map(DedupeShardRecord::into_domain)
                .transpose()
                .map_err(Into::into)?
        {
            visitor(mapping)?;
        }
    }

    Ok(())
}

pub(super) fn visit_webhook_deliveries_recursive<Visitor, VisitorError>(
    directory: &Path,
    visitor: &mut Visitor,
) -> Result<(), VisitorError>
where
    LocalIndexStoreError: Into<VisitorError>,
    Visitor: FnMut(WebhookDelivery) -> Result<(), VisitorError>,
{
    let Some(entries) = read_dir_if_exists(directory).map_err(Into::into)? else {
        return Ok(());
    };

    for entry in entries {
        let entry = entry
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        if file_type.is_dir() {
            visit_webhook_deliveries_recursive(&path, visitor)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        if let Some(delivery) =
            read_json_if_exists::<WebhookDeliveryRecord>(&path, MAX_CONTROL_PLANE_METADATA_BYTES)
                .map_err(Into::into)?
                .map(WebhookDeliveryRecord::into_domain)
                .transpose()
                .map_err(Into::into)?
        {
            visitor(delivery)?;
        }
    }

    Ok(())
}

pub(super) fn visit_provider_repository_states_recursive<Visitor, VisitorError>(
    directory: &Path,
    visitor: &mut Visitor,
) -> Result<(), VisitorError>
where
    LocalIndexStoreError: Into<VisitorError>,
    Visitor: FnMut(ProviderRepositoryState) -> Result<(), VisitorError>,
{
    let Some(entries) = read_dir_if_exists(directory).map_err(Into::into)? else {
        return Ok(());
    };

    for entry in entries {
        let entry = entry
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(LocalIndexStoreError::Io)
            .map_err(Into::into)?;
        if file_type.is_dir() {
            visit_provider_repository_states_recursive(&path, visitor)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        if let Some(state) = read_json_if_exists::<ProviderRepositoryStateRecord>(
            &path,
            MAX_CONTROL_PLANE_METADATA_BYTES,
        )
        .map_err(Into::into)?
        .map(ProviderRepositoryStateRecord::into_domain)
        .transpose()
        .map_err(Into::into)?
        {
            visitor(state)?;
        }
    }

    Ok(())
}

pub(super) fn provider_repository_state_path(
    root: &Path,
    provider: shardline_protocol::RepositoryProvider,
    owner: &str,
    repo: &str,
) -> PathBuf {
    root.join(provider.as_str())
        .join(hex_encode_component(owner))
        .join(format!("{}.json", hex_encode_component(repo)))
}

pub(super) fn read_dir_if_exists(
    directory: &Path,
) -> Result<Option<fs::ReadDir>, LocalIndexStoreError> {
    ensure_directory_path_components_are_not_symlinked(directory)?;
    match fs::read_dir(directory) {
        Ok(entries) => Ok(Some(entries)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(LocalIndexStoreError::Io(error)),
    }
}

pub(super) fn read_file_if_exists_bounded(
    path: &Path,
    maximum_bytes: u64,
) -> Result<Option<Vec<u8>>, LocalIndexStoreError> {
    ensure_parent_directory_path_components_are_not_symlinked(path)?;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(LocalIndexStoreError::Io(error)),
    };
    ensure_regular_metadata_file(&metadata)?;
    ensure_metadata_size_within_limit(metadata.len(), maximum_bytes)?;

    let mut file = match open_metadata_file(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(LocalIndexStoreError::Io(error)),
    };
    let opened_metadata = file.metadata()?;
    ensure_regular_metadata_file(&opened_metadata)?;
    ensure_metadata_size_within_limit(opened_metadata.len(), maximum_bytes)?;

    run_before_local_metadata_read_hook_for_tests(path);

    let bytes = read_bounded_metadata_file(&mut file, opened_metadata.len())?;
    let observed_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    ensure_metadata_size_within_limit(observed_bytes, maximum_bytes)?;

    Ok(Some(bytes))
}

fn read_bounded_metadata_file(
    file: &mut fs::File,
    expected_length: u64,
) -> Result<Vec<u8>, LocalIndexStoreError> {
    let capacity = usize::try_from(expected_length).unwrap_or(usize::MAX);
    let mut bytes = Vec::with_capacity(capacity);
    let mut limited = Read::by_ref(file).take(expected_length);
    limited.read_to_end(&mut bytes)?;

    let read_length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if read_length != expected_length {
        return Err(LocalIndexStoreError::MetadataLengthMismatch {
            expected_bytes: expected_length,
            observed_bytes: read_length,
        });
    }

    let mut trailing_byte = [0_u8; 1];
    if file.read(&mut trailing_byte)? != 0 {
        return Err(LocalIndexStoreError::MetadataLengthMismatch {
            expected_bytes: expected_length,
            observed_bytes: expected_length.saturating_add(1),
        });
    }

    let observed_metadata = file.metadata()?;
    let metadata_length = observed_metadata.len();
    if metadata_length != expected_length {
        return Err(LocalIndexStoreError::MetadataLengthMismatch {
            expected_bytes: expected_length,
            observed_bytes: metadata_length,
        });
    }

    Ok(bytes)
}

const fn ensure_metadata_size_within_limit(
    observed_bytes: u64,
    maximum_bytes: u64,
) -> Result<(), LocalIndexStoreError> {
    if observed_bytes > maximum_bytes {
        return Err(LocalIndexStoreError::MetadataTooLarge {
            observed_bytes,
            maximum_bytes,
        });
    }

    Ok(())
}

#[cfg(unix)]
fn open_metadata_file(path: &Path) -> Result<fs::File, IoError> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

#[cfg(not(unix))]
fn open_metadata_file(path: &Path) -> Result<fs::File, IoError> {
    OpenOptions::new().read(true).open(path)
}

fn ensure_regular_metadata_file(metadata: &fs::Metadata) -> Result<(), LocalIndexStoreError> {
    if !metadata.file_type().is_file() {
        return Err(invalid_metadata_path_error());
    }
    Ok(())
}

fn invalid_metadata_path_error() -> LocalIndexStoreError {
    LocalIndexStoreError::Io(IoError::new(
        ErrorKind::InvalidData,
        "local index metadata path must be a regular file and must not be a symlink",
    ))
}

pub(super) fn ensure_parent_directory_path_components_are_not_symlinked(
    path: &Path,
) -> Result<(), LocalIndexStoreError> {
    let parent = path.parent().ok_or_else(invalid_metadata_path_error)?;
    ensure_directory_path_components_are_not_symlinked(parent)
}

fn ensure_directory_path_components_are_not_symlinked(
    path: &Path,
) -> Result<(), LocalIndexStoreError> {
    ensure_directory_path_components_are_not_symlinked_shared(path)
        .map_err(map_directory_path_error)
}

fn map_directory_path_error(error: DirectoryPathError) -> LocalIndexStoreError {
    match error {
        DirectoryPathError::UnsupportedPrefix
        | DirectoryPathError::SymlinkedComponent(_)
        | DirectoryPathError::NonDirectoryComponent(_) => invalid_metadata_path_error(),
        DirectoryPathError::Io(error) => LocalIndexStoreError::Io(error),
    }
}

#[cfg(test)]
pub(super) fn set_before_local_metadata_read_hook(
    path: PathBuf,
    hook: impl FnOnce() + Send + 'static,
) {
    let mut slot = match BEFORE_LOCAL_METADATA_READ_HOOK.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *slot = Some(LocalMetadataReadHookRegistration {
        path,
        hook: Box::new(hook),
    });
}

#[cfg(test)]
fn run_before_local_metadata_read_hook_for_tests(path: &Path) {
    let hook = match BEFORE_LOCAL_METADATA_READ_HOOK.lock() {
        Ok(mut guard) => take_matching_local_metadata_read_hook(&mut guard, path),
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            take_matching_local_metadata_read_hook(&mut guard, path)
        }
    };

    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn take_matching_local_metadata_read_hook(
    slot: &mut LocalMetadataReadHookSlot,
    path: &Path,
) -> Option<LocalMetadataReadHook> {
    if slot
        .as_ref()
        .is_none_or(|registration| registration.path != path)
    {
        return None;
    }

    slot.take().map(|registration| registration.hook)
}

#[cfg(not(test))]
const fn run_before_local_metadata_read_hook_for_tests(_path: &Path) {}

fn legacy_quarantine_object_key(hash: &str) -> Result<ObjectKey, LocalIndexStoreError> {
    let prefix = hash.get(..2).ok_or(ObjectKeyError::UnsafePath)?;
    let value = format!("{prefix}/{hash}");
    ObjectKey::parse(&value).map_err(LocalIndexStoreError::from)
}

pub(super) fn remove_empty_ancestors(
    path: &Path,
    root: &Path,
) -> Result<(), LocalIndexStoreError> {
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
            Err(error) => return Err(LocalIndexStoreError::Io(error)),
        }
    }

    Ok(())
}

pub(super) fn hex_encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len().saturating_mul(2));
    for byte in value.as_bytes() {
        let high = byte >> 4;
        let low = byte & 0x0f;
        encoded.push(char::from(nibble_to_hex(high)));
        encoded.push(char::from(nibble_to_hex(low)));
    }
    encoded
}

const fn nibble_to_hex(value: u8) -> u8 {
    match value {
        0 => b'0',
        1 => b'1',
        2 => b'2',
        3 => b'3',
        4 => b'4',
        5 => b'5',
        6 => b'6',
        7 => b'7',
        8 => b'8',
        9 => b'9',
        10 => b'a',
        11 => b'b',
        12 => b'c',
        13 => b'd',
        14 => b'e',
        15 => b'f',
        _other => b'0',
    }
}
