use std::future::Future;

use bytes::Bytes;
use shardline_storage::{DeleteOutcome, ObjectKey, PutOutcome};

use crate::OciAdapterError;

/// Backend operations required by the OCI adapter.
///
/// Implemented by the server crate for its `ServerBackend` enum.
pub trait OciBackend: Send + Sync {
    /// Creates a resumable S3 upload, returning an upload ID if S3 is available.
    ///
    /// # Errors
    ///
    /// Returns [`OciAdapterError`] on storage backend failures.
    fn create_resumable_object_upload(
        &self,
        object_key: &ObjectKey,
    ) -> impl Future<Output = Result<Option<String>, OciAdapterError>> + Send;

    /// Uploads one part of a resumable S3 multipart upload.
    ///
    /// # Errors
    ///
    /// Returns [`OciAdapterError`] on storage backend failures.
    fn upload_resumable_object_part(
        &self,
        object_key: &ObjectKey,
        upload_id: &str,
        part_idx: usize,
        bytes: Bytes,
    ) -> impl Future<Output = Result<String, OciAdapterError>> + Send;

    /// Completes a resumable S3 multipart upload.
    ///
    /// `parts` is a vector of `(part_number, etag)` tuples.  Part numbers must be
    /// consecutive 0..n without gaps or duplicates.
    ///
    /// # Errors
    ///
    /// Returns [`OciAdapterError`] on storage backend failures or invalid part
    /// numbering.
    fn complete_resumable_object_upload(
        &self,
        object_key: &ObjectKey,
        upload_id: &str,
        parts: Vec<(usize, String)>,
    ) -> impl Future<Output = Result<(), OciAdapterError>> + Send;

    /// Aborts a resumable S3 multipart upload.
    ///
    /// # Errors
    ///
    /// Returns [`OciAdapterError`] on storage backend failures.
    fn abort_resumable_object_upload(
        &self,
        object_key: &ObjectKey,
        upload_id: &str,
    ) -> impl Future<Output = Result<(), OciAdapterError>> + Send;

    /// Stores bytes for a content-addressed object if no object exists at the key.
    ///
    /// # Errors
    ///
    /// Returns [`OciAdapterError`] on storage backend failures.
    fn put_sha256_addressed_object_bytes_if_absent(
        &self,
        object_key: &ObjectKey,
        digest_hex: &str,
        bytes: Vec<u8>,
    ) -> Result<PutOutcome, OciAdapterError>;

    /// Copies an object from source to destination if no object exists at the destination.
    ///
    /// # Errors
    ///
    /// Returns [`OciAdapterError`] on storage backend failures.
    fn copy_object_if_absent(
        &self,
        source: &ObjectKey,
        destination: &ObjectKey,
    ) -> Result<PutOutcome, OciAdapterError>;

    /// Deletes an object if it exists.
    ///
    /// # Errors
    ///
    /// Returns [`OciAdapterError`] on storage backend failures.
    fn delete_object_if_present(
        &self,
        object_key: &ObjectKey,
    ) -> impl Future<Output = Result<DeleteOutcome, OciAdapterError>> + Send;
}
