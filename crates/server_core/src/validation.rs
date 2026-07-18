use shardline_protocol::ShardlineHash;
use shardline_storage::{ObjectKey, ObjectKeyError};
use thiserror::Error;

use crate::object_store::ServerObjectStoreError;

/// Maximum byte length for a validated file identifier.
const MAX_IDENTIFIER_BYTES: usize = 1024;

/// Validates that a content hash is exactly 64 lowercase hex characters.
///
/// # Errors
///
/// Returns an error with the given `error_fn` when the hash is malformed.
pub fn validate_content_hash_with<E>(value: &str, error_fn: fn() -> E) -> Result<(), E> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(error_fn());
    }
    Ok(())
}

pub(crate) const fn map_object_key_error(error: ObjectKeyError) -> ServerObjectStoreError {
    match error {
        ObjectKeyError::Empty
        | ObjectKeyError::UnsafePath
        | ObjectKeyError::ControlCharacter
        | ObjectKeyError::TooLong => ServerObjectStoreError::Overflow,
    }
}

/// Returns the chunk object key for a hex-encoded content hash.
///
/// # Errors
///
/// Returns [`ServerObjectStoreError`] when the hash is malformed or the key cannot be created.
pub fn chunk_object_key(hash_hex: &str) -> Result<ObjectKey, ServerObjectStoreError> {
    validate_content_hash_with(hash_hex, || ServerObjectStoreError::InvalidContentHash)?;
    let prefix = hash_hex.get(..2).ok_or(ServerObjectStoreError::Overflow)?;
    let key = format!("{prefix}/{hash_hex}");
    ObjectKey::parse(&key).map_err(map_object_key_error)
}

/// Extracts the chunk hash from a chunk object key if the key matches the expected layout.
///
/// Returns `Some(hash_hex)` if the key is in the format `<2-char-prefix>/<64-char-hash>`,
/// `None` otherwise.
///
/// # Errors
///
/// Returns [`ServerObjectStoreError::InvalidContentHash`] if the extracted hash fails validation.
pub fn chunk_hash_from_chunk_object_key_if_present(
    key: &ObjectKey,
) -> Result<Option<&str>, ServerObjectStoreError> {
    let mut segments = key.as_str().split('/');
    let Some(prefix) = segments.next() else {
        return Ok(None);
    };
    let Some(candidate_hash_hex) = segments.next() else {
        return Ok(None);
    };
    if segments.next().is_some() {
        return Ok(None);
    }
    if prefix.len() != 2 || !prefix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(None);
    }
    if !candidate_hash_hex.starts_with(prefix) {
        return Ok(None);
    }
    validate_content_hash_with(candidate_hash_hex, || {
        ServerObjectStoreError::InvalidContentHash
    })?;
    Ok(Some(candidate_hash_hex))
}

/// Computes a blake3 content hash for the given bytes.
#[must_use]
pub fn chunk_hash(bytes: &[u8]) -> ShardlineHash {
    let digest = blake3::hash(bytes);
    ShardlineHash::from_bytes(*digest.as_bytes())
}

/// Computes a blake3 content hash for a file record's chunk layout.
#[must_use]
pub fn content_hash(
    total_bytes: u64,
    chunk_size: u64,
    chunks: &[shardline_index::FileChunkRecord],
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&total_bytes.to_le_bytes());
    hasher.update(&chunk_size.to_le_bytes());
    for chunk in chunks {
        hasher.update(chunk.hash.as_bytes());
        hasher.update(&chunk.offset.to_le_bytes());
        hasher.update(&chunk.length.to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

/// Validates that a file identifier is safe for use as a single path component.
///
/// # Errors
///
/// Returns [`ValidateIdentifierError`] if the identifier is empty, contains
/// path separators, traversal sequences, control characters, or exceeds the
/// maximum byte length.
pub fn validate_identifier(value: &str) -> Result<(), ValidateIdentifierError> {
    if value.trim().is_empty()
        || value == "."
        || value.len() > MAX_IDENTIFIER_BYTES
        || value.starts_with('/')
        || value.contains("..")
        || value.contains('\\')
        || value.contains('/')
        || value.chars().any(char::is_control)
    {
        return Err(ValidateIdentifierError);
    }

    Ok(())
}

/// File identifier validation failure.
#[derive(Debug, Clone, Copy, Error)]
#[error("file identifier must be relative and must not contain traversal or control characters")]
pub struct ValidateIdentifierError;

/// Validates that a content hash is exactly 64 lowercase hex characters.
///
/// # Errors
///
/// Returns [`ValidateContentHashError`] if the hash is malformed.
pub fn validate_content_hash(value: &str) -> Result<(), ValidateContentHashError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(ValidateContentHashError);
    }

    Ok(())
}

/// Content hash validation failure.
#[derive(Debug, Clone, Copy, Error)]
#[error("content hash must be 64 hexadecimal characters")]
pub struct ValidateContentHashError;
