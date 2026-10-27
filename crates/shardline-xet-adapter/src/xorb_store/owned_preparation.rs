//! Request-owned validation carried from upload identity to immutable storage.
use super::{ServerObjectStore, StoredXorbUpload, XetAdapterError, file_preparation};
use std::path::Path;

#[cfg(test)]
mod tests;

/// An owned, fully verified native container ready for immutable persistence.
/// The validation result cannot be constructed or changed by callers. Storage
/// rechecks the serialized digest before publication; compatibility-copy writes
/// also recheck decoded members. This is not a cache of the health of an already
/// published object.
pub struct PreparedXorbFile {
    expected_hash: String,
    staging: tempfile::NamedTempFile,
    prepared: file_preparation::PreparedFile,
}

impl PreparedXorbFile {
    /// The canonical serialized length authenticated during preparation.
    #[must_use]
    pub const fn length(&self) -> u64 {
        self.prepared.length
    }

    /// The serialized storage digest, distinct from the native content hash.
    #[must_use]
    pub const fn integrity(&self) -> &shardline_storage::ObjectIntegrity {
        &self.prepared.integrity
    }

    /// The privately owned staging path, retained until storage finishes.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.prepared.path
    }
}

/// Authenticate and normalize owned staging once, preserving its validation
/// result for persistence. Call this synchronous operation under bounded
/// blocking admission and retain ownership until its checkpoint reports stop.
///
/// # Errors
/// Returns validation, normalization, source I/O or cancellation errors.
pub fn prepare_owned_xorb_file_with_checkpoint(
    expected_hash: &str,
    staging: tempfile::NamedTempFile,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<PreparedXorbFile, XetAdapterError> {
    let prepared = file_preparation::prepare_source(
        expected_hash,
        staging.path().to_owned(),
        file_preparation::SourceOwnership::OwnedStaging,
        checkpoint,
    )?;
    Ok(PreparedXorbFile {
        expected_hash: expected_hash.to_owned(),
        staging,
        prepared,
    })
}

/// Persist a prepared owned container without repeating its complete format
/// validation. Member rereads and immutable publication retain their integrity
/// checks. Started writes keep staging and admission after caller cancellation.
///
/// # Errors
/// Returns source mutation, member validation or immutable-storage errors.
pub async fn store_prepared_xorb_file<R: Send + 'static>(
    objects: &ServerObjectStore,
    file: PreparedXorbFile,
    resources: R,
) -> Result<StoredXorbUpload, XetAdapterError> {
    let PreparedXorbFile {
        expected_hash,
        staging,
        prepared,
    } = file;
    super::store_prepared_file_with_resources(
        objects,
        &expected_hash,
        prepared,
        (staging, resources),
    )
    .await
}

/// Persist only the authenticated container, without standalone decoded chunks.
///
/// The caller must publish verified member discovery metadata before promising
/// independent chunk reads. Readers that require standalone chunk objects cannot
/// read those members. This method does not change the compatibility behavior of
/// [`store_prepared_xorb_file`]. The serialized digest is rechecked during durable
/// immutable publication, including any mutation after preparation. Staging and
/// admission remain owned until started synchronous writes finish.
///
/// # Errors
/// Returns source mutation, immutable-storage or retained-container validation errors.
pub async fn store_prepared_xorb_container<R: Send + 'static>(
    objects: &ServerObjectStore,
    file: PreparedXorbFile,
    resources: R,
) -> Result<StoredXorbUpload, XetAdapterError> {
    let PreparedXorbFile {
        expected_hash,
        staging,
        prepared,
    } = file;
    super::persist_prepared_file(
        objects,
        &expected_hash,
        prepared,
        (staging, resources),
        super::MemberPersistence::ContainerOnly,
    )
    .await
}
