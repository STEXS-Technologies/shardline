//! Discover length and stream a complete object with one GET.
use std::fs::File;

use object_store::{GetResult, ObjectStoreExt};

use super::{S3ObjectStore, S3ObjectStoreError, copy_snapshot_to_writer};
use crate::ObjectKey;

impl S3ObjectStore {
    /// Reads a complete small object with one GET, returning `None` for absence.
    ///
    /// # Errors
    /// Rejects objects larger than `maximum_bytes`, incomplete bodies, partial
    /// ranges and provider failures. No partial output is returned.
    pub fn read_object_bytes(
        &self,
        key: &ObjectKey,
        maximum_bytes: u64,
    ) -> Result<Option<Vec<u8>>, S3ObjectStoreError> {
        let location = self.location_for_key(key)?;
        self.block_on_result(async {
            let result = match self.inner.get(&location).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(S3ObjectStoreError::External(error)),
            };
            read_payload_bytes(result, maximum_bytes).await.map(Some)
        })
    }

    /// Copies a complete object to a file and returns its stored length, or
    /// `None` when the object does not exist.
    ///
    /// The GET supplies the length, avoiding a separate metadata request. The
    /// body must match that length and cover the complete object. Memory stays
    /// bounded to transport frames. Callers must discard output on failure and
    /// validate any format-specific content commitments before exposing bytes.
    ///
    /// # Errors
    /// Returns an error on provider, length, range or file-write failures.
    pub fn copy_object_to_file(
        &self,
        key: &ObjectKey,
        output: &mut File,
    ) -> Result<Option<u64>, S3ObjectStoreError> {
        let location = self.location_for_key(key)?;
        self.block_on_result(async {
            let result = match self.inner.get(&location).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(S3ObjectStoreError::External(error)),
            };
            copy_payload_to_writer(result, output).await.map(Some)
        })
    }
}

async fn copy_payload_to_writer(
    result: GetResult,
    output: &mut impl std::io::Write,
) -> Result<u64, S3ObjectStoreError> {
    copy_snapshot_to_writer(result, output, || S3ObjectStoreError::RangeOutOfBounds).await
}

async fn read_payload_bytes(
    result: GetResult,
    maximum_bytes: u64,
) -> Result<Vec<u8>, S3ObjectStoreError> {
    if result.meta.size > maximum_bytes {
        return Err(S3ObjectStoreError::RangeOutOfBounds);
    }
    let capacity =
        usize::try_from(result.meta.size).map_err(|_error| S3ObjectStoreError::RangeOutOfBounds)?;
    let mut output = Vec::with_capacity(capacity);
    copy_payload_to_writer(result, &mut output).await?;
    Ok(output)
}

#[cfg(test)]
mod tests;
