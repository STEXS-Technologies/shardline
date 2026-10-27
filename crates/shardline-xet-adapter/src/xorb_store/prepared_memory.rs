//! Request-owned verified bytes carried from identity to immutable publication.

use super::{
    PutOutcome, ServerObjectStore, StoredXorbUpload, ValidatedXorb, XetAdapterError, blocking,
    canonicalize_uploaded_xorb, encoding, parse_xet_hash_hex, xorb_object_key,
};
use shardline_server_core::chunk_hash;
use shardline_storage::{ObjectBody, ObjectIntegrity};

/// Owned canonical bytes and their complete validation result. Callers cannot
/// replace the bytes or construct a validation result independently of them.
pub struct PreparedXorbMemory {
    expected_hash: String,
    bytes: Vec<u8>,
    validated: ValidatedXorb,
    integrity: ObjectIntegrity,
}

impl PreparedXorbMemory {
    /// Authenticated canonical serialized length used for upload identity.
    #[must_use]
    pub const fn length(&self) -> u64 {
        self.integrity.length()
    }
}

/// Fully validate and normalize a bounded in-memory upload before deriving its
/// identity. Retain this owned result through publication instead of repeating
/// normalization and native-content validation afterwards.
///
/// # Errors
/// Returns malformed-content, content-hash, format or length errors.
pub fn prepare_xorb_memory(
    expected_hash: &str,
    uploaded: &[u8],
) -> Result<PreparedXorbMemory, XetAdapterError> {
    let (bytes, validated) =
        canonicalize_uploaded_xorb(parse_xet_hash_hex(expected_hash)?, uploaded)?;
    let length = u64::try_from(bytes.len())?;
    let integrity = ObjectIntegrity::new(chunk_hash(bytes.as_ref()), length);
    Ok(PreparedXorbMemory {
        expected_hash: expected_hash.to_owned(),
        bytes: bytes.into_owned(),
        validated,
        integrity,
    })
}

/// Publish owned prepared bytes while retaining admission through synchronous
/// storage work. Storage rechecks their serialized digest before publication;
/// conflicting encodings still require complete verification of retained bytes.
///
/// # Errors
/// Returns immutable-storage or conflicting-content verification errors.
pub async fn store_prepared_xorb_memory<R: Send + 'static>(
    objects: &ServerObjectStore,
    prepared: PreparedXorbMemory,
    resources: R,
) -> Result<StoredXorbUpload, XetAdapterError> {
    let key = xorb_object_key(&prepared.expected_hash)?;
    let length = prepared.length();
    let store = objects.clone();
    let outcome = blocking::cancellable_write(resources, move || {
        let result = shardline_storage::ObjectStore::put_if_absent(
            &store,
            &key,
            ObjectBody::from_vec(prepared.bytes),
            &prepared.integrity,
        );
        encoding::resolve_conflict(&store, &key, &prepared.validated, result)
    })
    .await?;
    let was_inserted = matches!(outcome, PutOutcome::Inserted);
    Ok(StoredXorbUpload {
        was_inserted,
        stored_bytes: if was_inserted { length } else { 0 },
    })
}

#[cfg(test)]
mod tests;
