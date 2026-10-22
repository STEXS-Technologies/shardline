use std::path::Path;

use futures_util::{TryStreamExt, stream::FuturesOrdered};
use object_store::{PutPayload, multipart::MultipartStore, path::Path as ObjectStorePath};
use tokio::{fs::File, io::AsyncReadExt};

use super::{S3ObjectStore, S3ObjectStoreError, STREAM_UPLOAD_CHUNK_BYTES};

impl S3ObjectStore {
    /// Streams into an unpublished multipart upload at the canonical key.
    /// Completion atomically checks absence; no temporary object is published.
    pub(super) fn stream_file_if_absent(
        &self,
        location: &ObjectStorePath,
        path: &Path,
        expected_length: u64,
    ) -> Result<(), S3ObjectStoreError> {
        self.block_on_result(async {
            // Do not create remote state if the source cannot be opened.
            let mut file = File::open(path).await?;
            let upload_id = self.inner.create_multipart(location).await?;
            let result = async {
                let mut pending = FuturesOrdered::new();
                let mut parts = Vec::new();
                let mut total = 0_u64;
                let mut part_index = 0_usize;
                loop {
                    // Bound submitted payloads before allocating the next part.
                    if pending.len() == 4 {
                        parts.push(
                            pending
                                .try_next()
                                .await?
                                .ok_or(S3ObjectStoreError::InvalidUploadParts)?,
                        );
                    }
                    let mut bytes = Vec::with_capacity(STREAM_UPLOAD_CHUNK_BYTES);
                    while bytes.len() < STREAM_UPLOAD_CHUNK_BYTES {
                        if file.read_buf(&mut bytes).await? == 0 {
                            break;
                        }
                    }
                    if bytes.is_empty() && part_index != 0 {
                        break;
                    }
                    total = total
                        .checked_add(bytes.len() as u64)
                        .filter(|length| *length <= expected_length)
                        .ok_or(S3ObjectStoreError::IntegrityLengthMismatch)?;
                    let final_part = bytes.len() < STREAM_UPLOAD_CHUNK_BYTES;
                    let index = part_index;
                    part_index = part_index
                        .checked_add(1)
                        .filter(|count| *count <= 10_000)
                        .ok_or(S3ObjectStoreError::InvalidUploadParts)?;
                    let store = self.inner.clone();
                    let location = location.clone();
                    let upload_id = upload_id.clone();
                    pending.push_back(async move {
                        store
                            .put_part(&location, &upload_id, index, PutPayload::from(bytes))
                            .await
                    });
                    if final_part {
                        break;
                    }
                }
                if total != expected_length {
                    return Err(S3ObjectStoreError::IntegrityLengthMismatch);
                }
                while let Some(part) = pending.try_next().await? {
                    parts.push(part);
                }
                self.conditional_completion_inner
                    .complete_multipart(location, &upload_id, parts)
                    .await?;
                Ok(())
            }
            .await;
            if result.is_err() {
                // Preserve the original failure, including conditional conflict.
                // An unsuccessful abort leaves only unpublished parts for the
                // bucket's incomplete-multipart lifecycle rule to reclaim.
                self.inner.abort_multipart(location, &upload_id).await.ok();
            }
            result
        })
    }
}
