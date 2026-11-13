//! Protocol listing, tag and tombstone index dispatch.
use shardline_index::{
    OciObjectKey, OciObjectStore, OciTagEntry, OciTagStore, S3ObjectEntry, S3ObjectIndexStore,
};

use super::ServerBackend;
use crate::ServerError;

impl ServerBackend {
    pub(crate) async fn upsert_s3_object_with_multipart_count(
        &self,
        entry: &S3ObjectEntry,
        part_count: u32,
    ) -> Result<(), ServerError> {
        match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .upsert_s3_object_with_multipart_count(entry, part_count)
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .upsert_s3_object_with_multipart_count(entry, part_count)
                    .await?
            }
        };
        Ok(())
    }

    pub(crate) async fn compare_and_swap_s3_object_with_multipart_count(
        &self,
        expected: Option<&S3ObjectEntry>,
        replacement: &S3ObjectEntry,
        part_count: u32,
    ) -> Result<bool, ServerError> {
        Ok(match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .compare_and_swap_s3_object_with_multipart_count(
                        expected,
                        replacement,
                        part_count,
                    )
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .compare_and_swap_s3_object_with_multipart_count(
                        expected,
                        replacement,
                        part_count,
                    )
                    .await?
            }
        })
    }

    pub(crate) async fn compare_and_swap_s3_object_metadata(
        &self,
        expected: &S3ObjectEntry,
        replacement: &S3ObjectEntry,
    ) -> Result<bool, ServerError> {
        Ok(match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .compare_and_swap_s3_object_metadata(expected, replacement)
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .compare_and_swap_s3_object_metadata(expected, replacement)
                    .await?
            }
        })
    }

    /// Deletes one S3 object listing-index row, returning whether a row was removed.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index delete fails.
    pub(crate) async fn delete_s3_object(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<bool, ServerError> {
        Ok(match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .delete_s3_object(scope_namespace, object_key)
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .delete_s3_object(scope_namespace, object_key)
                    .await?
            }
        })
    }

    /// Scans S3 object listing rows for a scope namespace.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index scan fails.
    pub(crate) async fn scan_s3_objects(
        &self,
        scope_namespace: &str,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<S3ObjectEntry>, ServerError> {
        Ok(match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .scan_s3_objects(scope_namespace, prefix, cursor, limit)
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .scan_s3_objects(scope_namespace, prefix, cursor, limit)
                    .await?
            }
        })
    }

    pub(crate) async fn scan_s3_objects_from(
        &self,
        scope_namespace: &str,
        prefix: &str,
        start: Option<shardline_index::S3ObjectScanStart<'_>>,
        limit: usize,
    ) -> Result<Vec<S3ObjectEntry>, ServerError> {
        Ok(match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .scan_s3_objects_from(scope_namespace, prefix, start, limit)
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .scan_s3_objects_from(scope_namespace, prefix, start, limit)
                    .await?
            }
        })
    }

    /// Resolves exactly one S3 object listing row by its full raw key (no
    /// prefix matching), for the S3 frontend's conditional-object semantics.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index lookup fails.
    pub(crate) async fn scan_s3_object_exact(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<Option<S3ObjectEntry>, ServerError> {
        Ok(match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .scan_s3_object_exact(scope_namespace, object_key)
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .scan_s3_object_exact(scope_namespace, object_key)
                    .await?
            }
        })
    }

    pub(super) async fn scan_s3_object_exact_with_multipart_count(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<Option<(S3ObjectEntry, Option<u32>)>, ServerError> {
        Ok(match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .scan_s3_object_exact_with_multipart_count(scope_namespace, object_key)
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .scan_s3_object_exact_with_multipart_count(scope_namespace, object_key)
                    .await?
            }
        })
    }

    pub(crate) async fn insert_oci_tag_if_absent(
        &self,
        entry: &OciTagEntry,
    ) -> Result<bool, ServerError> {
        Ok(match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .insert_oci_tag_if_absent(entry)
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .insert_oci_tag_if_absent(entry)
                    .await?
            }
        })
    }

    pub(crate) async fn oci_tag(
        &self,
        scope_namespace: &str,
        repository: &str,
        tag: &str,
    ) -> Result<Option<OciTagEntry>, ServerError> {
        Ok(match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .oci_tag(scope_namespace, repository, tag)
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .oci_tag(scope_namespace, repository, tag)
                    .await?
            }
        })
    }

    pub(crate) async fn list_oci_tags(
        &self,
        scope_namespace: &str,
        repository: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<OciTagEntry>, ServerError> {
        Ok(match self {
            Self::Local(backend) => {
                backend
                    .index_store()
                    .list_oci_tags(scope_namespace, repository, cursor, limit)
                    .await?
            }
            Self::Postgres(backend) => {
                backend
                    .index_store()
                    .list_oci_tags(scope_namespace, repository, cursor, limit)
                    .await?
            }
        })
    }

    /// Returns whether an immutable OCI object is hidden by a durable tombstone.
    pub(crate) async fn oci_object_is_deleted(
        &self,
        key: &OciObjectKey,
    ) -> Result<bool, ServerError> {
        Ok(match self {
            Self::Local(backend) => backend.index_store().oci_object_is_deleted(key).await?,
            Self::Postgres(backend) => backend.index_store().oci_object_is_deleted(key).await?,
        })
    }
}
