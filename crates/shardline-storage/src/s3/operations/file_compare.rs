//! Compare one complete remote snapshot with a local file using bounded buffers.
use std::path::Path;

use futures_util::StreamExt;
use object_store::{GetOptions, ObjectStore};
use tokio::io::AsyncReadExt;

use super::{S3ObjectStore, S3ObjectStoreError, stream_payload_for_range};
use crate::{ObjectKey, PutOutcome};

pub(super) fn compare_existing_file(
    store: &S3ObjectStore,
    key: &ObjectKey,
    path: &Path,
    expected_length: u64,
) -> Result<PutOutcome, S3ObjectStoreError> {
    let location = store.location_for_key(key)?;
    store.block_on_result(async {
        let file = tokio::fs::File::open(path)
            .await
            .map_err(S3ObjectStoreError::Io)?;
        if file.metadata().await.map_err(S3ObjectStoreError::Io)?.len() != expected_length {
            return Err(S3ObjectStoreError::ExistingObjectConflict);
        }
        // Transport frames need not align with file blocks. Buffer local reads
        // so tiny network frames do not turn into tiny disk reads or jobs.
        let mut file =
            tokio::io::BufReader::with_capacity(crate::s3::STREAM_COMPARE_CHUNK_BYTES, file);
        let result = store
            .inner
            .get_opts(
                &location,
                GetOptions::new().with_range(Some(0..expected_length)),
            )
            .await
            .map_err(S3ObjectStoreError::External)?;
        if result.meta.size != expected_length {
            return Err(S3ObjectStoreError::ExistingObjectConflict);
        }
        let mut stream = stream_payload_for_range(result, 0..expected_length)?;
        let mut buffer = vec![0_u8; crate::s3::STREAM_COMPARE_CHUNK_BYTES];
        let mut compared = 0_u64;
        while let Some(bytes) = stream.next().await {
            let bytes = bytes?;
            compared = compared
                .checked_add(
                    u64::try_from(bytes.len())
                        .map_err(|_error| S3ObjectStoreError::ExistingObjectConflict)?,
                )
                .filter(|total| *total <= expected_length)
                .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
            for part in bytes.chunks(buffer.len()) {
                let expected = buffer
                    .get_mut(..part.len())
                    .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
                file.read_exact(expected)
                    .await
                    .map_err(S3ObjectStoreError::Io)?;
                if expected != part {
                    return Err(S3ObjectStoreError::ExistingObjectConflict);
                }
            }
        }
        let mut extra = [0_u8; 1];
        if compared != expected_length
            || file
                .read(&mut extra)
                .await
                .map_err(S3ObjectStoreError::Io)?
                != 0
        {
            return Err(S3ObjectStoreError::ExistingObjectConflict);
        }
        Ok(PutOutcome::AlreadyExists)
    })
}
