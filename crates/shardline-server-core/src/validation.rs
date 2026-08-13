use shardline_protocol::ShardlineHash;
use shardline_storage::{ObjectKey, ObjectKeyError};

use crate::object_store::ServerObjectStoreError;

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
/// The key is laid out as `<2-char-prefix>/<64-char-hash>`, which mirrors how
/// chunk objects are addressed in storage.
///
/// # Examples
///
/// ```
/// use shardline_server_core::chunk_object_key;
///
/// let hash_hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
/// let key = chunk_object_key(hash_hex)?;
/// assert_eq!(
///     key.as_str(),
///     "01/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Errors
///
/// Returns [`ServerObjectStoreError`] when the hash is malformed or the key cannot be created.
pub fn chunk_object_key(hash_hex: &str) -> Result<ObjectKey, ServerObjectStoreError> {
    shardline_validation::validate_content_hash_with(hash_hex, || {
        ServerObjectStoreError::InvalidContentHash
    })?;
    let prefix = hash_hex.get(..2).ok_or(ServerObjectStoreError::Overflow)?;
    let key = format!("{prefix}/{hash_hex}");
    ObjectKey::parse(&key).map_err(map_object_key_error)
}

/// Extracts the chunk hash from a chunk object key if the key matches the expected layout.
///
/// Returns `Some(hash_hex)` if the key is in the format `<2-char-prefix>/<64-char-hash>`,
/// `None` otherwise.
///
/// # Examples
///
/// ```
/// use shardline_server_core::chunk_hash_from_chunk_object_key_if_present;
/// use shardline_storage::ObjectKey;
///
/// let key = ObjectKey::parse(
///     "01/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
/// )?;
/// assert_eq!(
///     chunk_hash_from_chunk_object_key_if_present(&key)?,
///     Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
/// );
///
/// // Keys with a different layout yield `None`.
/// let unrelated = ObjectKey::parse("xorbs/default/aa/bb/example.xorb")?;
/// assert_eq!(chunk_hash_from_chunk_object_key_if_present(&unrelated)?, None);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
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
    shardline_validation::validate_content_hash_with(candidate_hash_hex, || {
        ServerObjectStoreError::InvalidContentHash
    })?;
    Ok(Some(candidate_hash_hex))
}

/// Computes a blake3 content hash for the given bytes.
///
/// # Examples
///
/// ```
/// use shardline_server_core::chunk_hash;
///
/// let hash = chunk_hash(b"payload");
/// assert_eq!(hash.as_bytes().len(), 32);
/// assert_eq!(hash, chunk_hash(b"payload"));
/// assert_ne!(hash, chunk_hash(b"different"));
/// ```
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
