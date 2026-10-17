//! Durable installation and complete read-back for local protocol containers.
use shardline_storage::{
    LocalObjectStore, ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome,
};

use crate::ServerError;

pub(super) fn store_verified(
    store: &LocalObjectStore,
    key: &ObjectKey,
    bytes: &[u8],
    integrity: &ObjectIntegrity,
) -> Result<PutOutcome, ServerError> {
    // Keep the adapter's anchored install, bytewise duplicate comparison and
    // file/parent durability barriers. Read the installed name back, not input.
    let outcome = ObjectStore::put_if_absent(store, key, ObjectBody::from_slice(bytes), integrity)?;
    let mut file = store.open_object_file(key)?;
    let mut hasher = blake3::Hasher::new();
    let length = std::io::copy(&mut file, &mut hasher)?;
    if length != integrity.length() || hasher.finalize().as_bytes() != integrity.hash().as_bytes() {
        return Err(ServerError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "stored protocol container failed complete read-back verification",
        )));
    }
    Ok(outcome)
}
