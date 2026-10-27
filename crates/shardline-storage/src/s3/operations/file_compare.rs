//! Compare one complete remote snapshot with a local file using bounded buffers.
use std::path::Path;

use futures_util::StreamExt;
use object_store::GetResult;
use tokio::io::AsyncReadExt;

use super::{S3ObjectStore, S3ObjectStoreError, stream_payload_for_range};
use crate::{ObjectIntegrity, PutOutcome};

pub(super) fn compare_existing_file(
    store: &S3ObjectStore,
    result: GetResult,
    path: &Path,
    integrity: &ObjectIntegrity,
) -> Result<PutOutcome, S3ObjectStoreError> {
    let expected_length = integrity.length();
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
        if result.meta.size != expected_length {
            return Err(S3ObjectStoreError::ExistingObjectConflict);
        }
        let mut stream = stream_payload_for_range(result, 0..expected_length)?;
        let mut buffer = vec![0_u8; crate::s3::STREAM_COMPARE_CHUNK_BYTES];
        let mut compared = 0_u64;
        let mut digest = blake3::Hasher::new();
        while let Some(bytes) = stream.next().await {
            let bytes = bytes?;
            digest.update(&bytes);
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
        if shardline_protocol::ShardlineHash::from_bytes(*digest.finalize().as_bytes())
            != integrity.hash()
        {
            return Err(S3ObjectStoreError::ExistingObjectConflict);
        }
        Ok(PutOutcome::AlreadyExists)
    })
}
