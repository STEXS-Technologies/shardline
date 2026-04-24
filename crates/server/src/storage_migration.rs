use std::{io::Cursor, path::PathBuf};

use serde::Serialize;
use shardline_index::{parse_xet_hash_hex, xet_hash_hex_string};
use shardline_storage::{
    ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix, PutOutcome, S3ObjectStoreConfig,
};
use xet_core_structures::merklehash::compute_data_hash;

use crate::{
    ServerError,
    chunk_store::chunk_hash_from_chunk_object_key_if_present,
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

    source.visit_prefix(&prefix, |metadata| {
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

    Err(ServerError::StorageMigrationSourceHashMismatch {
        key: key.as_str().to_owned(),
        expected_hash: expected_hash.to_owned(),
        observed_hash: observed_hash.to_owned(),
    })
}

fn endpoint_store(endpoint: &StorageMigrationEndpoint) -> Result<ServerObjectStore, ServerError> {
    match endpoint {
        StorageMigrationEndpoint::LocalStateRoot(root) => {
            ServerObjectStore::local(root.join("chunks"))
        }
        StorageMigrationEndpoint::S3(config) => ServerObjectStore::s3(config.clone()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey};

    use super::{StorageMigrationEndpoint, StorageMigrationOptions, run_storage_migration};
    use crate::{
        chunk_store::chunk_object_key_for_computed_hash, local_backend::chunk_hash,
        object_store::ServerObjectStore,
    };

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
