/// Inclusive or exclusive lower bound for a raw S3 key scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3ObjectScanStart<'key> {
    /// Include a key exactly equal to the bound.
    Inclusive(&'key str),
    /// Resume strictly after the bound.
    Exclusive(&'key str),
}

/// Returns the exclusive UTF-8 lexical upper bound of a prefix.
/// Empty or all-largest-scalar prefixes have no upper bound.
#[must_use]
pub fn s3_prefix_successor(prefix: &str) -> Option<String> {
    crate::hub::prefix_successor(prefix)
}

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
    /// The BLAKE3 root content hash (used to pin reads to a record version).
    pub content_hash: String,
    /// The S3 ETag served to clients: the hex MD5 of the object bytes for
    /// single-part uploads (S3 convention; checksum-verifying clients such as
    /// `s3cmd` depend on it). Multipart completions hash the assembled object.
    pub etag: String,
    /// S3 user metadata (`x-amz-meta-*`), stored as sorted `(name, value)`
    /// pairs with the `x-amz-meta-` prefix stripped and names lowercased.
    pub user_metadata: Vec<(String, String)>,
    /// Unix seconds when the row was last updated.
    pub updated_at_unix_seconds: i64,
}

/// Metadata precondition applied at the S3 object's publication transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3PublishCondition {
    /// Replace any current value.
    Unconditional,
    /// Publish only when the current row still equals this snapshot. `None`
    /// requires the key to remain absent.
    IfUnchanged(Option<S3ObjectEntry>),
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

    /// Atomically replaces an S3 object row only when its current value equals
    /// `expected`. `None` means create only when the key is absent.
    ///
    /// Returns `true` when the replacement became visible and `false` when a
    /// concurrent writer changed or created/deleted the row first. This is the
    /// database linearization point for S3 conditional writes across replicas.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when persistence fails.
    async fn compare_and_swap_s3_object(
        &self,
        expected: Option<&S3ObjectEntry>,
        replacement: &S3ObjectEntry,
    ) -> Result<bool, Self::Error>;

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
    /// The `object_key` column is matched as a string **prefix**, which is the
    /// listing/pagination contract (keyset cursors walk prefix pages). It must
    /// NOT be used for exact-key object resolution — a sibling key that merely
    /// has the target as a string prefix (e.g. `a` vs `a/b`) would be returned
    /// as if it were the object. Use [`Self::scan_s3_object_exact`] instead.
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

    /// Scans a bounded prefix page from an inclusive or exclusive raw-key bound.
    ///
    /// Builtin stores use indexed UTF-8 byte ordering. The compatibility default
    /// adds an exact lookup for an inclusive bound before the existing exclusive
    /// scan, without loading the whole namespace.
    ///
    /// # Errors
    /// Returns the adapter error when either lookup or scan fails.
    async fn scan_s3_objects_from(
        &self,
        scope_namespace: &str,
        prefix: &str,
        start: Option<S3ObjectScanStart<'_>>,
        limit: usize,
    ) -> Result<Vec<S3ObjectEntry>, Self::Error> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        match start {
            None => {
                self.scan_s3_objects(scope_namespace, prefix, None, limit)
                    .await
            }
            Some(S3ObjectScanStart::Exclusive(key)) => {
                self.scan_s3_objects(scope_namespace, prefix, Some(key), limit)
                    .await
            }
            Some(S3ObjectScanStart::Inclusive(key)) => {
                let mut values = Vec::new();
                if key.starts_with(prefix)
                    && let Some(entry) = self.scan_s3_object_exact(scope_namespace, key).await?
                {
                    values.push(entry);
                }
                let remaining = limit.saturating_sub(values.len());
                if remaining > 0 {
                    values.extend(
                        self.scan_s3_objects(scope_namespace, prefix, Some(key), remaining)
                            .await?,
                    );
                }
                Ok(values)
            }
        }
    }

    /// Resolves exactly one S3 object row by its full raw key — no prefix
    /// matching — returning `None` when the exact key is absent.
    ///
    /// This is the exact-key path for conditional semantics (`If-Match` /
    /// `If-None-Match` on the object's ETag): a prefix-shadowed sibling row
    /// (e.g. `a/b` when looking up `a`) must never satisfy the lookup (F-33).
    /// The `shardline_s3_objects` table keys rows on the unique
    /// `(scope_namespace, object_key)` primary key, so the lookup hits that
    /// index directly.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the lookup fails.
    async fn scan_s3_object_exact(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<Option<S3ObjectEntry>, Self::Error>;
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
            etag: "ef".repeat(16),
            user_metadata: vec![("purpose".to_owned(), "model".to_owned())],
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

#[cfg(test)]
mod range_contract_tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    struct CustomStore(Vec<S3ObjectEntry>);
    #[async_trait::async_trait]
    impl S3ObjectIndexStore for CustomStore {
        type Error = std::convert::Infallible;
        async fn upsert_s3_object(&self, _: &S3ObjectEntry) -> Result<(), Self::Error> {
            Ok(())
        }
        async fn compare_and_swap_s3_object(
            &self,
            _: Option<&S3ObjectEntry>,
            _: &S3ObjectEntry,
        ) -> Result<bool, Self::Error> {
            Ok(false)
        }
        async fn delete_s3_object(&self, _: &str, _: &str) -> Result<bool, Self::Error> {
            Ok(false)
        }
        async fn scan_s3_objects(
            &self,
            scope: &str,
            prefix: &str,
            cursor: Option<&str>,
            limit: usize,
        ) -> Result<Vec<S3ObjectEntry>, Self::Error> {
            Ok(self
                .0
                .iter()
                .filter(|entry| {
                    entry.scope_namespace == scope
                        && entry.object_key.starts_with(prefix)
                        && cursor.is_none_or(|cursor| entry.object_key.as_str() > cursor)
                })
                .take(limit)
                .cloned()
                .collect())
        }
        async fn scan_s3_object_exact(
            &self,
            scope: &str,
            key: &str,
        ) -> Result<Option<S3ObjectEntry>, Self::Error> {
            Ok(self
                .0
                .iter()
                .find(|entry| entry.scope_namespace == scope && entry.object_key == key)
                .cloned())
        }
    }
    #[tokio::test]
    async fn inclusive_default_preserves_exact_successor_and_limit() {
        let store = CustomStore(
            ["a/child", "a0", "a1", "z"]
                .into_iter()
                .map(|key| S3ObjectEntry {
                    scope_namespace: "scope".into(),
                    object_key: key.into(),
                    file_id: "file".into(),
                    size_bytes: 1,
                    content_hash: "hash".into(),
                    etag: "etag".into(),
                    user_metadata: Vec::new(),
                    updated_at_unix_seconds: 0,
                })
                .collect(),
        );
        let inclusive = store
            .scan_s3_objects_from("scope", "a", Some(S3ObjectScanStart::Inclusive("a0")), 2)
            .await
            .unwrap();
        assert_eq!(
            inclusive
                .iter()
                .map(|entry| entry.object_key.as_str())
                .collect::<Vec<_>>(),
            ["a0", "a1"]
        );
        let exclusive = store
            .scan_s3_objects_from("scope", "a", Some(S3ObjectScanStart::Exclusive("a0")), 2)
            .await
            .unwrap();
        assert_eq!(
            exclusive
                .iter()
                .map(|entry| entry.object_key.as_str())
                .collect::<Vec<_>>(),
            ["a1"]
        );
        assert!(
            store
                .scan_s3_objects_from("scope", "a", Some(S3ObjectScanStart::Inclusive("a0")), 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .scan_s3_objects_from("scope", "a", Some(S3ObjectScanStart::Inclusive("z")), 2)
                .await
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn utf8_prefix_boundaries_skip_surrogates_and_carry_maximum_scalar() {
        assert_eq!(s3_prefix_successor("a/").as_deref(), Some("a0"));
        assert_eq!(s3_prefix_successor("é・").as_deref(), Some("éー"));
        assert_eq!(
            s3_prefix_successor("a\u{d7ff}").as_deref(),
            Some("a\u{e000}")
        );
        assert_eq!(s3_prefix_successor("a\u{10ffff}").as_deref(), Some("b"));
        assert_eq!(s3_prefix_successor("\u{10ffff}\u{10ffff}"), None);
        assert_eq!(s3_prefix_successor(""), None);
    }
}
