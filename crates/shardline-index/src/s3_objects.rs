/// One indexed S3 object row for the S3 frontend's listing index.
///
/// Rows are keyed by `(scope_namespace, object_key)`. `object_key` is the
/// client-facing S3 key (the `{key}` remainder of the storage layout
/// `protocols/s3/{scope_namespace}/{key}`), not the storage object key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ObjectEntry {
    /// The sha256 repository-scope namespace the object belongs to.
    pub scope_namespace: String,
    /// The client-facing S3 object key (without the `protocols/s3/` prefix).
    pub object_key: String,
    /// The `file_id` of the content-addressed record backing the object.
    pub file_id: String,
    /// Snapshot of the record's size in bytes at upsert time.
    pub size_bytes: u64,
    /// The BLAKE3 root content hash (served as the opaque S3 ETag).
    pub content_hash: String,
    /// Unix seconds when the row was last updated.
    pub updated_at_unix_seconds: i64,
}

/// S3 object listing-index storage contract.
///
/// Implementations store per-`(scope_namespace, object_key)` rows for the S3
/// frontend's object listing. Deleting an index row never touches the
/// referenced file record or CAS objects.
///
/// # Notes
///
/// The scan is ordered by raw `object_key` within a scope namespace so callers
/// can paginate with an opaque keyset cursor over the raw key, matching the
/// `scan_tree` contract.
#[async_trait::async_trait]
pub trait S3ObjectIndexStore: Send + Sync {
    /// Adapter-specific error type.
    type Error: Send + Sync;

    /// Inserts or replaces an S3 object index row.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when persistence fails.
    async fn upsert_s3_object(&self, entry: &S3ObjectEntry) -> Result<(), Self::Error>;

    /// Deletes one S3 object index row, returning whether a row was removed.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when deletion fails.
    async fn delete_s3_object(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<bool, Self::Error>;

    /// Scans S3 object rows for a scope namespace whose raw key starts with
    /// `prefix`, ordered by key, resuming after `cursor` (keyset on the raw
    /// key), returning at most `limit` rows.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the scan fails.
    async fn scan_s3_objects(
        &self,
        scope_namespace: &str,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<S3ObjectEntry>, Self::Error>;
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_in_result,
        clippy::arithmetic_side_effects,
        clippy::option_if_let_else,
        clippy::unreachable,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use
    )]

    use super::S3ObjectEntry;

    #[test]
    fn s3_object_entry_equality_and_fields() {
        let entry = S3ObjectEntry {
            scope_namespace: "global".to_owned(),
            object_key: "data/model.pt".to_owned(),
            file_id: "ab".repeat(32),
            size_bytes: 1234,
            content_hash: "cd".repeat(32),
            updated_at_unix_seconds: 1700000000,
        };
        assert_eq!(entry.scope_namespace, "global");
        assert_eq!(entry.object_key, "data/model.pt");
        assert_eq!(entry.size_bytes, 1234);
        assert_eq!(entry.updated_at_unix_seconds, 1700000000);
        assert_eq!(entry, entry.clone());
        let other = S3ObjectEntry {
            object_key: "other.pt".to_owned(),
            ..entry.clone()
        };
        assert_ne!(entry, other);
    }
}
