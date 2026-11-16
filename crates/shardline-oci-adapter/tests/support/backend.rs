use bytes::Bytes;
use shardline_storage::{DeleteOutcome, ObjectKey, PutOutcome};

use crate::{OciAdapterError, OciBackend};

pub struct TestBackend;

impl OciBackend for TestBackend {
    async fn create_resumable_object_upload(
        &self,
        _object_key: &ObjectKey,
    ) -> Result<Option<String>, OciAdapterError> {
        Ok(None)
    }

    async fn upload_resumable_object_part(
        &self,
        _object_key: &ObjectKey,
        _upload_id: &str,
        _part_idx: usize,
        _bytes: Bytes,
    ) -> Result<String, OciAdapterError> {
        Err(OciAdapterError::NotFound)
    }

    async fn complete_resumable_object_upload(
        &self,
        _object_key: &ObjectKey,
        _upload_id: &str,
        _parts: Vec<(usize, String)>,
    ) -> Result<(), OciAdapterError> {
        Err(OciAdapterError::NotFound)
    }

    async fn abort_resumable_object_upload(
        &self,
        _object_key: &ObjectKey,
        _upload_id: &str,
    ) -> Result<(), OciAdapterError> {
        Ok(())
    }

    fn put_sha256_addressed_object_bytes_if_absent(
        &self,
        _object_key: &ObjectKey,
        _digest_hex: &str,
        _bytes: Vec<u8>,
    ) -> Result<PutOutcome, OciAdapterError> {
        Ok(PutOutcome::Inserted)
    }

    fn copy_object_if_absent(
        &self,
        _source: &ObjectKey,
        _destination: &ObjectKey,
    ) -> Result<PutOutcome, OciAdapterError> {
        Ok(PutOutcome::Inserted)
    }

    async fn delete_object_if_present(
        &self,
        _object_key: &ObjectKey,
    ) -> Result<DeleteOutcome, OciAdapterError> {
        Ok(DeleteOutcome::Deleted)
    }
}

pub const NO_BACKEND: Option<&TestBackend> = None;
