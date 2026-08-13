use bytes::Bytes;
use object_store::{
    ObjectStoreExt, WriteMultipart,
    multipart::{MultipartStore, PartId},
};

use super::{
    BeginMultipartUploadResult, S3MultipartUploadWriter, S3ObjectStore, S3ObjectStoreError,
    STREAM_UPLOAD_CHUNK_BYTES, temp_key_for,
};
use crate::{ObjectKey, ObjectStore, PutOutcome};

impl S3ObjectStore {
    /// Begins a direct multipart upload to a content-addressed destination key.
    ///
    /// This path is intended for immutable digest-addressed objects, where callers
    /// validate the stream contents independently and concurrent writers for the same
    /// key can only be writing identical bytes.
    ///
    /// # TOCTOU Safety
    ///
    /// The initial existence check is a fast-path optimization only.  The multipart
    /// upload is started on a **temp key** derived from the canonical key.  After the
    /// caller streams data and calls [`S3ObjectStore::finish_content_addressed_upload`],
    /// the content is atomically promoted to the canonical key via a conditional copy
    /// (`CopyMode::Create`).  If a concurrent writer has already promoted the same
    /// canonical key, the copy returns [`PutOutcome::AlreadyExists`] and the temp
    /// content is discarded — eliminating the TOCTOU window.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the destination lookup or multipart
    /// initialization fails.
    pub async fn begin_content_addressed_upload(
        &self,
        key: &ObjectKey,
    ) -> Result<BeginMultipartUploadResult, S3ObjectStoreError> {
        if self.metadata(key)?.is_some() {
            return Ok(BeginMultipartUploadResult::AlreadyExists);
        }

        let temp_key = temp_key_for(key)?;
        let location = self.location_for_key(&temp_key)?;
        let upload = self
            .inner
            .put_multipart(&location)
            .await
            .map_err(S3ObjectStoreError::External)?;
        Ok(BeginMultipartUploadResult::Upload(
            S3MultipartUploadWriter {
                writer: WriteMultipart::new_with_chunk_size(upload, STREAM_UPLOAD_CHUNK_BYTES),
            },
            temp_key,
        ))
    }

    /// Finishes a content-addressed upload and atomically promotes the temp content
    /// to the canonical key.
    ///
    /// After the caller has streamed all bytes through the writer returned by
    /// [`S3ObjectStore::begin_content_addressed_upload`], this method:
    /// 1. Finalizes the multipart upload to the temp key.
    /// 2. Atomically copies temp → canonical using `CopyMode::Create`.
    /// 3. Deletes the temp key.
    ///
    /// If the canonical key already exists (concurrent writer finished first),
    /// returns [`PutOutcome::AlreadyExists`].  The content is identical since it
    /// is content-addressed.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the multipart finalization, conditional
    /// copy, or cleanup fails.
    pub async fn finish_content_addressed_upload(
        &self,
        upload: S3MultipartUploadWriter,
        temp_key: &ObjectKey,
        canonical_key: &ObjectKey,
    ) -> Result<PutOutcome, S3ObjectStoreError> {
        upload.finish().await?;

        // Conditional copy — fails if canonical already exists.
        match self.copy_object_if_absent(temp_key, canonical_key) {
            Ok(outcome) => {
                self.delete_if_present(temp_key).ok();
                Ok(outcome)
            }
            Err(error) => {
                self.delete_if_present(temp_key).ok();
                Err(error)
            }
        }
    }

    /// Starts a resumable multipart upload at a temporary S3 location.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the multipart upload cannot be
    /// initialized.
    pub async fn create_resumable_upload(
        &self,
        key: &ObjectKey,
    ) -> Result<String, S3ObjectStoreError> {
        let location = self.location_for_key(key)?;
        self.inner
            .create_multipart(&location)
            .await
            .map_err(S3ObjectStoreError::External)
    }

    /// Uploads one part into an existing resumable multipart upload.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the object key is invalid or the part
    /// upload fails.
    pub async fn upload_resumable_part(
        &self,
        key: &ObjectKey,
        upload_id: &str,
        part_idx: usize,
        bytes: Bytes,
    ) -> Result<String, S3ObjectStoreError> {
        let location = self.location_for_key(key)?;
        let multipart_id = upload_id.to_owned();
        let part = self
            .inner
            .put_part(&location, &multipart_id, part_idx, bytes.into())
            .await
            .map_err(S3ObjectStoreError::External)?;
        Ok(part.content_id)
    }

    /// Completes a resumable multipart upload once all parts are uploaded.
    ///
    /// The `parts` parameter is a vector of `(part_number, etag)` tuples.  Part
    /// numbers must be 0-indexed and consecutive from 0 (inclusive) to
    /// `parts.len() - 1` (inclusive).  Duplicate or missing part numbers are
    /// rejected.  Parts are sorted by part number before the S3 CompleteMultipartUpload
    /// request is sent.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the final completion request fails or
    /// part numbering is invalid.
    pub async fn complete_resumable_upload(
        &self,
        key: &ObjectKey,
        upload_id: &str,
        parts: Vec<(usize, String)>,
    ) -> Result<(), S3ObjectStoreError> {
        let count = parts.len();
        if count == 0 {
            return Err(S3ObjectStoreError::InvalidUploadParts);
        }

        // Validate consecutive 0..count numbering.
        let mut indexed: Vec<(usize, String)> = parts;
        indexed.sort_by_key(|(part_number, _etag)| *part_number);
        for (expected, (part_number, _etag)) in indexed.iter().enumerate() {
            if *part_number != expected {
                return Err(S3ObjectStoreError::InvalidUploadParts);
            }
        }

        let location = self.location_for_key(key)?;
        let multipart_id = upload_id.to_owned();
        let part_ids = indexed
            .into_iter()
            .map(|(_part_number, content_id)| PartId { content_id })
            .collect();
        let _result = self
            .inner
            .complete_multipart(&location, &multipart_id, part_ids)
            .await
            .map_err(S3ObjectStoreError::External)?;
        Ok(())
    }

    /// Aborts a resumable multipart upload and discards any uploaded parts.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the abort request fails.
    pub async fn abort_resumable_upload(
        &self,
        key: &ObjectKey,
        upload_id: &str,
    ) -> Result<(), S3ObjectStoreError> {
        let location = self.location_for_key(key)?;
        let multipart_id = upload_id.to_owned();
        self.inner
            .abort_multipart(&location, &multipart_id)
            .await
            .map_err(S3ObjectStoreError::External)
    }
}
