use std::{io::Cursor, path::PathBuf};

use serde::Serialize;
use shardline_index::{parse_xet_hash_hex, xet_hash_hex_string};
use shardline_storage::{
    ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix, ObjectStore, PutOutcome,
    S3ObjectStoreConfig,
};
use shardline_xet_core::merklehash::compute_data_hash;

use crate::{
    ServerError,
    chunk_store::chunk_hash_from_chunk_object_key_if_present,
    error::ObjectStoreError,
    local_backend::chunk_hash,
    object_store::{ServerObjectStore, read_full_object},
    overflow::checked_add,
    xet_adapter::{
        shard_hash_from_object_key_if_present, validate_serialized_xorb,
        xorb_hash_from_object_key_if_present,
    },
};

/// Object-storage endpoint used by storage migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageMigrationEndpoint {
    /// Local Shardline state root. Object bytes live below `chunks/`.
    LocalStateRoot(PathBuf),
    /// S3-compatible object storage.
    S3(S3ObjectStoreConfig),
}

impl StorageMigrationEndpoint {
    const fn backend_name(&self) -> &'static str {
        match self {
            Self::LocalStateRoot(_root) => "local",
            Self::S3(_config) => "s3",
        }
    }
}

/// Storage-migration runtime options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMigrationOptions {
    source: StorageMigrationEndpoint,
    destination: StorageMigrationEndpoint,
    prefix: String,
    dry_run: bool,
}

impl StorageMigrationOptions {
    /// Creates storage-migration options.
    #[must_use]
    pub const fn new(
        source: StorageMigrationEndpoint,
        destination: StorageMigrationEndpoint,
    ) -> Self {
        Self {
            source,
            destination,
            prefix: String::new(),
            dry_run: false,
        }
    }

    /// Limits migration to objects under one object-key prefix.
    #[must_use]
    pub fn with_prefix(mut self, prefix: String) -> Self {
        self.prefix = prefix;
        self
    }

    /// Enables inventory-only mode.
    #[must_use]
    pub const fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }
}

/// Storage-migration summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StorageMigrationReport {
    /// Source object-storage adapter name.
    pub source_backend: String,
    /// Destination object-storage adapter name.
    pub destination_backend: String,
    /// Object-key prefix scanned by the migration.
    pub prefix: String,
    /// Whether the run avoided writes.
    pub dry_run: bool,
    /// Number of source objects scanned.
    pub scanned_objects: u64,
    /// Number of destination writes that inserted a new object.
    pub inserted_objects: u64,
    /// Number of destination writes that found an identical object already present.
    pub already_present_objects: u64,
    /// Number of bytes copied into newly inserted destination objects.
    pub copied_bytes: u64,
    /// Number of source bytes inventoried.
    pub scanned_bytes: u64,
}

impl StorageMigrationReport {
    fn new(options: &StorageMigrationOptions) -> Self {
        Self {
            source_backend: options.source.backend_name().to_owned(),
            destination_backend: options.destination.backend_name().to_owned(),
            prefix: options.prefix.clone(),
            dry_run: options.dry_run,
            scanned_objects: 0,
            inserted_objects: 0,
            already_present_objects: 0,
            copied_bytes: 0,
            scanned_bytes: 0,
        }
    }
}

/// Copies immutable payload objects between object-storage adapters.
///
/// The command migrates object bytes only. Metadata, provider state, lifecycle state, and
/// reconstruction rows remain in their configured metadata stores.
///
/// # Errors
///
/// Returns [`ServerError`] when inventory, reads, integrity checks, or destination writes
/// fail.
pub fn run_storage_migration(
    options: &StorageMigrationOptions,
) -> Result<StorageMigrationReport, ServerError> {
    let source = endpoint_store(&options.source)?;
    let destination = endpoint_store(&options.destination)?;
    let prefix = ObjectPrefix::parse(&options.prefix)?;
    let mut report = StorageMigrationReport::new(options);

    crate::object_store::visit_object_prefix(&source, &prefix, |metadata| {
        report.scanned_objects = checked_add(report.scanned_objects, 1)?;
        report.scanned_bytes = checked_add(report.scanned_bytes, metadata.length())?;
        if options.dry_run {
            return Ok(());
        }

        let bytes = read_full_object(&source, metadata.key(), metadata.length())?;
        let observed_length = u64::try_from(bytes.len())?;
        validate_source_object_matches_content_addressed_key(metadata.key(), &bytes)?;
        let integrity = ObjectIntegrity::new(chunk_hash(&bytes), observed_length);
        let outcome =
            destination.put_if_absent(metadata.key(), ObjectBody::from_vec(bytes), &integrity)?;
        match outcome {
            PutOutcome::Inserted => {
                report.inserted_objects = checked_add(report.inserted_objects, 1)?;
                report.copied_bytes = checked_add(report.copied_bytes, observed_length)?;
            }
            PutOutcome::AlreadyExists => {
                report.already_present_objects = checked_add(report.already_present_objects, 1)?;
            }
        }

        Ok(())
    })?;

    Ok(report)
}

fn validate_source_object_matches_content_addressed_key(
    key: &ObjectKey,
    bytes: &[u8],
) -> Result<(), ServerError> {
    if let Some(expected_hash) = chunk_hash_from_chunk_object_key_if_present(key)? {
        let observed_hash = xet_hash_hex_string(chunk_hash(bytes));
        ensure_observed_hash_matches_key(key, expected_hash, &observed_hash)?;
    }

    if let Some(expected_hash) = xorb_hash_from_object_key_if_present(key)? {
        let expected_hash = parse_xet_hash_hex(expected_hash)?;
        let mut cursor = Cursor::new(bytes);
        validate_serialized_xorb(&mut cursor, expected_hash)?;
    }

    if let Some(expected_hash) = shard_hash_from_object_key_if_present(key)? {
        let observed_hash = compute_data_hash(bytes).hex();
        ensure_observed_hash_matches_key(key, expected_hash, &observed_hash)?;
    }

    Ok(())
}

fn ensure_observed_hash_matches_key(
    key: &ObjectKey,
    expected_hash: &str,
    observed_hash: &str,
) -> Result<(), ServerError> {
    if observed_hash == expected_hash {
        return Ok(());
    }

    Err(ServerError::ObjectStore(
        ObjectStoreError::MigrationSourceHashMismatch {
            key: key.as_str().to_owned(),
            expected_hash: expected_hash.to_owned(),
            observed_hash: observed_hash.to_owned(),
        },
    ))
}

fn endpoint_store(endpoint: &StorageMigrationEndpoint) -> Result<ServerObjectStore, ServerError> {
    match endpoint {
        StorageMigrationEndpoint::LocalStateRoot(root) => {
            Ok(ServerObjectStore::local(root.join("chunks"))?)
        }
        StorageMigrationEndpoint::S3(config) => Ok(ServerObjectStore::s3(config.clone())?),
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};

    use super::{
        StorageMigrationEndpoint, StorageMigrationOptions, StorageMigrationReport,
        endpoint_store, ensure_observed_hash_matches_key, run_storage_migration,
    };
    use crate::{
        ServerError,
        chunk_store::chunk_object_key_for_computed_hash, local_backend::chunk_hash,
        object_store::ServerObjectStore,
    };

    // ── StorageMigrationEndpoint ──────────────────────────────────────────

    #[test]
    fn storage_migration_endpoint_backend_name_local() {
        let endpoint = StorageMigrationEndpoint::LocalStateRoot(PathBuf::from("/tmp"));
        assert_eq!(endpoint.backend_name(), "local");
    }

    #[test]
    fn storage_migration_endpoint_backend_name_s3() {
        let config =
            shardline_storage::S3ObjectStoreConfig::new("bucket".to_owned(), "us-east-1".to_owned());
        let endpoint = StorageMigrationEndpoint::S3(config);
        assert_eq!(endpoint.backend_name(), "s3");
    }

    // ── StorageMigrationOptions ───────────────────────────────────────────

    #[test]
    fn storage_migration_options_constructors() {
        let source = StorageMigrationEndpoint::LocalStateRoot(PathBuf::from("/src"));
        let dest = StorageMigrationEndpoint::LocalStateRoot(PathBuf::from("/dst"));

        let options = StorageMigrationOptions::new(source, dest);
        assert!(!options.dry_run);
        assert!(options.prefix.is_empty());

        let with_prefix = options.with_prefix("xorbs/default/".to_owned());
        assert_eq!(with_prefix.prefix, "xorbs/default/");

        let with_dry_run = with_prefix.with_dry_run(true);
        assert!(with_dry_run.dry_run);
    }

    #[test]
    fn storage_migration_options_debug_and_clone() {
        let source = StorageMigrationEndpoint::LocalStateRoot(PathBuf::from("/src"));
        let dest = StorageMigrationEndpoint::LocalStateRoot(PathBuf::from("/dst"));
        let options = StorageMigrationOptions::new(source, dest);
        let debug = format!("{options:?}");
        assert!(debug.contains("dry_run"));
    }

    // ── StorageMigrationReport ────────────────────────────────────────────

    #[test]
    fn storage_migration_report_new_initializes_fields() {
        let source = StorageMigrationEndpoint::LocalStateRoot(PathBuf::from("/src"));
        let dest = StorageMigrationEndpoint::LocalStateRoot(PathBuf::from("/dst"));
        let options = StorageMigrationOptions::new(source, dest);
        let report = StorageMigrationReport::new(&options);

        assert_eq!(report.source_backend, "local");
        assert_eq!(report.destination_backend, "local");
        assert!(!report.dry_run);
        assert_eq!(report.scanned_objects, 0);
        assert_eq!(report.inserted_objects, 0);
        assert_eq!(report.already_present_objects, 0);
        assert_eq!(report.copied_bytes, 0);
        assert_eq!(report.scanned_bytes, 0);
    }

    #[test]
    fn storage_migration_report_new_dry_run() {
        let source = StorageMigrationEndpoint::S3(shardline_storage::S3ObjectStoreConfig::new(
            "bucket".to_owned(),
            "us-east-1".to_owned(),
        ));
        let dest = StorageMigrationEndpoint::LocalStateRoot(PathBuf::from("/dst"));
        let options = StorageMigrationOptions::new(source, dest).with_dry_run(true);
        let report = StorageMigrationReport::new(&options);

        assert_eq!(report.source_backend, "s3");
        assert_eq!(report.destination_backend, "local");
        assert!(report.dry_run);
    }

    #[test]
    fn storage_migration_report_serialize() {
        let source = StorageMigrationEndpoint::LocalStateRoot(PathBuf::from("/src"));
        let dest = StorageMigrationEndpoint::LocalStateRoot(PathBuf::from("/dst"));
        let options = StorageMigrationOptions::new(source, dest);
        let report = StorageMigrationReport::new(&options);
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains(r#""scanned_objects":0"#));
    }

    // ── endpoint_store ────────────────────────────────────────────────────

    #[test]
    fn endpoint_store_local_creates_store() {
        let tmp = tempfile::tempdir().unwrap();
        let endpoint = StorageMigrationEndpoint::LocalStateRoot(tmp.path().to_path_buf());
        let store = endpoint_store(&endpoint);
        assert!(store.is_ok());
    }

    #[test]
    fn endpoint_store_local_rejects_invalid_path() {
        let endpoint =
            StorageMigrationEndpoint::LocalStateRoot(PathBuf::from("/nonexistent/path/chunks"));
        let store = endpoint_store(&endpoint);
        assert!(store.is_err());
    }

    // ── ensure_observed_hash_matches_key ──────────────────────────────────

    #[test]
    fn ensure_observed_hash_matches_key_ok_when_equal() {
        let key = ObjectKey::parse("aa/aaa").unwrap();
        let result = ensure_observed_hash_matches_key(&key, "abc", "abc");
        assert!(result.is_ok());
    }

    #[test]
    fn ensure_observed_hash_matches_key_err_when_not_equal() {
        let key = ObjectKey::parse("aa/aaa").unwrap();
        let result = ensure_observed_hash_matches_key(&key, "abc", "xyz");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ServerError::ObjectStore(_)));
        let msg = format!("{err}");
        assert!(msg.contains("object storage operation failed"));
    }

    #[test]
    fn storage_migration_copies_local_objects_idempotently() {
        let source = tempfile::tempdir();
        assert!(source.is_ok());
        let Ok(source) = source else {
            return;
        };
        let destination = tempfile::tempdir();
        assert!(destination.is_ok());
        let Ok(destination) = destination else {
            return;
        };
        let source_store = ServerObjectStore::local(source.path().join("chunks"));
        assert!(source_store.is_ok());
        let Ok(source_store) = source_store else {
            return;
        };
        let body = b"payload";
        let key = chunk_object_key_for_computed_hash(chunk_hash(body)).map(|(_hash, key)| key);
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let integrity = ObjectIntegrity::new(chunk_hash(body), 7);
        let put = source_store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
        assert!(put.is_ok());

        let options = StorageMigrationOptions::new(
            StorageMigrationEndpoint::LocalStateRoot(source.path().to_path_buf()),
            StorageMigrationEndpoint::LocalStateRoot(destination.path().to_path_buf()),
        );
        let first = run_storage_migration(&options);
        assert!(first.is_ok());
        if let Ok(first) = first {
            assert_eq!(first.scanned_objects, 1);
            assert_eq!(first.inserted_objects, 1);
            assert_eq!(first.already_present_objects, 0);
            assert_eq!(first.copied_bytes, 7);
        }

        let second = run_storage_migration(&options);
        assert!(second.is_ok());
        if let Ok(second) = second {
            assert_eq!(second.scanned_objects, 1);
            assert_eq!(second.inserted_objects, 0);
            assert_eq!(second.already_present_objects, 1);
        }
    }

    // ── validate_source_object_matches_content_addressed_key ─────────────

    #[test]
    fn validate_source_ok_for_non_content_addressed_key() {
        // A key that does not match any content-addressed pattern should pass
        let key = ObjectKey::parse("some/arbitrary/key.bin").unwrap();
        let result = crate::storage_migration::validate_source_object_matches_content_addressed_key(
            &key, b"any bytes",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn validate_source_rejects_chunk_key_hash_mismatch() {
        use crate::chunk_store::chunk_hash_from_chunk_object_key_if_present;
        use crate::chunk_store::chunk_object_key;
        use crate::local_backend::chunk_hash;

        // Create a chunk key where the hash in the key doesn't match the body
        let body = b"test payload";
        let correct_hash = chunk_hash(body);
        let correct_hex = hex::encode(correct_hash.as_bytes());
        let key = chunk_object_key(&correct_hex).unwrap();
        // Verify key parses to extract hash
        let parsed = chunk_hash_from_chunk_object_key_if_present(&key).unwrap();
        assert!(parsed.is_some(), "key should have a parseable chunk hash: {key:?}");

        // Call with wrong bytes (should trigger hash mismatch)
        let result = crate::storage_migration::validate_source_object_matches_content_addressed_key(
            &key, b"wrong-bytes",
        );
        assert!(result.is_err(), "expected err for hash mismatch, got ok");
        let err = result.unwrap_err();
        assert!(matches!(err, crate::ServerError::ObjectStore(_)));
    }

    #[test]
    fn validate_source_rejects_shard_key_hash_mismatch() {
        use crate::xet_adapter::shard_hash_from_object_key_if_present;

        // Create a shard object key in the shards namespace with .shard suffix
        let hash_hex = "aa".repeat(32);
        let key_str = format!("shards/{}/{}.shard", &hash_hex[..2], hash_hex);
        let key = ObjectKey::parse(&key_str).unwrap();
        let parsed = shard_hash_from_object_key_if_present(&key).unwrap();
        assert!(parsed.is_some(), "key should parse shard hash: {key_str}");

        let result = crate::storage_migration::validate_source_object_matches_content_addressed_key(
            &key, b"wrong-bytes",
        );
        assert!(result.is_err(), "expected err for shard hash mismatch, got ok");
        let err = result.unwrap_err();
        assert!(matches!(err, crate::ServerError::ObjectStore(_)));
    }

    // ── endpoint_store ────────────────────────────────────────────────────

    #[test]
    fn endpoint_store_s3_accepts_valid_config() {
        use shardline_storage::S3ObjectStoreConfig;
        let config = S3ObjectStoreConfig::new("bucket".into(), "region".into());
        let endpoint = StorageMigrationEndpoint::S3(config);
        let store = endpoint_store(&endpoint);
        assert!(store.is_ok());
    }

    #[test]
    fn storage_migration_report_serialize_with_counts() {
        use shardline_storage::S3ObjectStoreConfig;
        let source = StorageMigrationEndpoint::S3(S3ObjectStoreConfig::new("src".into(), "us-east-1".into()));
        let dest = StorageMigrationEndpoint::S3(S3ObjectStoreConfig::new("dst".into(), "us-east-1".into()));
        let opts = StorageMigrationOptions::new(source, dest).with_prefix("test/".into());
        let mut report = StorageMigrationReport::new(&opts);
        report.scanned_objects = 10;
        report.inserted_objects = 5;
        report.already_present_objects = 5;
        report.copied_bytes = 1000;
        report.scanned_bytes = 2000;
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains(r#""scanned_objects":10"#));
        assert!(json.contains(r#""inserted_objects":5"#));
        assert!(json.contains(r#""copied_bytes":1000"#));
    }

    #[test]
    fn storage_migration_rejects_corrupt_source_chunk_key() {
        let source = tempfile::tempdir();
        assert!(source.is_ok());
        let Ok(source) = source else {
            return;
        };
        let destination = tempfile::tempdir();
        assert!(destination.is_ok());
        let Ok(destination) = destination else {
            return;
        };
        let key =
            ObjectKey::parse("aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let source_path = source.path().join("chunks").join(key.as_str());
        let parent = source_path.parent();
        assert!(parent.is_some());
        let Some(parent) = parent else {
            return;
        };
        let created = fs::create_dir_all(parent);
        assert!(created.is_ok());
        let written = fs::write(&source_path, b"corrupt-source-bytes");
        assert!(written.is_ok());

        let options = StorageMigrationOptions::new(
            StorageMigrationEndpoint::LocalStateRoot(source.path().to_path_buf()),
            StorageMigrationEndpoint::LocalStateRoot(destination.path().to_path_buf()),
        );
        let migrated = run_storage_migration(&options);

        assert!(
            migrated.is_err(),
            "storage migration copied a source chunk whose bytes did not match its content-addressed key"
        );
        assert!(
            !destination
                .path()
                .join("chunks")
                .join(key.as_str())
                .exists(),
            "storage migration wrote corrupt source bytes into the destination under the original key"
        );
    }
}
