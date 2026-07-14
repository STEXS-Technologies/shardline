use std::io::Write;

use serde::Serialize;
use serde_json::to_writer;
use shardline_index::{
    AsyncIndexStore, LocalIndexStore, PostgresIndexStore, PostgresRecordStore, RecordTraversal,
    xet_hash_hex_string,
};
use shardline_storage::{ObjectMetadata, ObjectPrefix};

use crate::{
    ServerConfig, ServerError,
    object_store::{ServerObjectStore, object_store_from_config},
    ops_record_store::OpsRecordStore,
    overflow::{checked_add, checked_increment},
    postgres_backend::connect_postgres_metadata_pool,
    record_store::LocalRecordStore,
};

/// Adapter-neutral backup manifest summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BackupManifestReport {
    /// Stable manifest format version.
    pub manifest_version: u64,
    /// Metadata backend used by the deployment.
    pub metadata_backend: String,
    /// Object backend used by the deployment.
    pub object_backend: String,
    /// Number of object-store entries written to the manifest.
    pub object_count: u64,
    /// Total bytes reported by object-store metadata.
    pub object_bytes: u64,
    /// Number of visible latest records in metadata storage.
    pub latest_records: u64,
    /// Number of immutable version records in metadata storage.
    pub version_records: u64,
    /// Number of durable reconstruction rows in metadata storage.
    pub reconstruction_rows: u64,
    /// Number of retained dedupe-shard mappings in metadata storage.
    pub dedupe_shard_mappings: u64,
    /// Number of durable quarantine candidates in metadata storage.
    pub quarantine_candidates: u64,
    /// Number of durable retention holds in metadata storage.
    pub retention_holds: u64,
    /// Number of processed provider webhook delivery claims in metadata storage.
    pub webhook_deliveries: u64,
    /// Number of provider repository lifecycle states in metadata storage.
    pub provider_repository_states: u64,
}

impl BackupManifestReport {
    fn new(metadata_backend: &str, object_backend: &str) -> Self {
        Self {
            manifest_version: 1,
            metadata_backend: metadata_backend.to_owned(),
            object_backend: object_backend.to_owned(),
            object_count: 0,
            object_bytes: 0,
            latest_records: 0,
            version_records: 0,
            reconstruction_rows: 0,
            dedupe_shard_mappings: 0,
            quarantine_candidates: 0,
            retention_holds: 0,
            webhook_deliveries: 0,
            provider_repository_states: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct BackupManifestObjectEntry {
    key: String,
    length: u64,
    checksum: Option<String>,
}

impl BackupManifestObjectEntry {
    fn from_metadata(metadata: &ObjectMetadata) -> Self {
        Self {
            key: metadata.key().as_str().to_owned(),
            length: metadata.length(),
            checksum: metadata.checksum().map(xet_hash_hex_string),
        }
    }
}

/// Writes an adapter-neutral backup manifest for the configured deployment.
///
/// The manifest inventories object metadata and durable index state without reading object
/// bodies. Payload bytes remain in the configured object store; operators should combine
/// this manifest with the storage backend's native backup or replication mechanism.
///
/// # Errors
///
/// Returns [`ServerError`] when metadata enumeration, object inventory, or manifest
/// writing fails.
pub async fn write_backup_manifest<Writer>(
    config: ServerConfig,
    writer: Writer,
) -> Result<BackupManifestReport, ServerError>
where
    Writer: Write,
{
    let object_store = object_store_from_config(&config)?;
    let object_backend = object_store.backend_name();

    if let Some(index_postgres_url) = config.index_postgres_url() {
        let pool = connect_postgres_metadata_pool(index_postgres_url, 4)?;
        let index_store = PostgresIndexStore::new(pool.clone());
        let record_store = PostgresRecordStore::new(pool);
        let mut report = BackupManifestReport::new("postgres", object_backend);
        collect_metadata_counts(&record_store, &index_store, &mut report).await?;
        write_manifest_body(writer, &object_store, report)
    } else {
        let index_store = LocalIndexStore::open(config.root_dir().to_path_buf());
        let record_store = LocalRecordStore::open(config.root_dir().to_path_buf());
        let mut report = BackupManifestReport::new("local", object_backend);
        collect_metadata_counts(&record_store, &index_store, &mut report).await?;
        write_manifest_body(writer, &object_store, report)
    }
}

async fn collect_metadata_counts<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    report: &mut BackupManifestReport,
) -> Result<(), ServerError>
where
    RecordAdapter: OpsRecordStore + Sync,
    RecordAdapter::Error: Into<ServerError>,
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<ServerError>,
{
    RecordTraversal::visit_latest_records(record_store, |_entry| {
        report.latest_records = checked_increment(report.latest_records)?;
        Ok::<(), ServerError>(())
    })
    .await?;

    RecordTraversal::visit_version_records(record_store, |_entry| {
        report.version_records = checked_increment(report.version_records)?;
        Ok::<(), ServerError>(())
    })
    .await?;

    report.reconstruction_rows = u64::try_from(
        index_store
            .list_reconstruction_file_ids()
            .await
            .map_err(Into::into)?
            .len(),
    )?;

    index_store
        .visit_dedupe_shard_mappings(|_mapping| {
            report.dedupe_shard_mappings = checked_increment(report.dedupe_shard_mappings)?;
            Ok::<(), ServerError>(())
        })
        .await?;
    index_store
        .visit_quarantine_candidates(|_candidate| {
            report.quarantine_candidates = checked_increment(report.quarantine_candidates)?;
            Ok::<(), ServerError>(())
        })
        .await?;
    index_store
        .visit_retention_holds(|_hold| {
            report.retention_holds = checked_increment(report.retention_holds)?;
            Ok::<(), ServerError>(())
        })
        .await?;
    index_store
        .visit_webhook_deliveries(|_delivery| {
            report.webhook_deliveries = checked_increment(report.webhook_deliveries)?;
            Ok::<(), ServerError>(())
        })
        .await?;
    index_store
        .visit_provider_repository_states(|_state| {
            report.provider_repository_states =
                checked_increment(report.provider_repository_states)?;
            Ok::<(), ServerError>(())
        })
        .await?;

    Ok(())
}

fn write_manifest_body<Writer>(
    mut writer: Writer,
    object_store: &ServerObjectStore,
    mut report: BackupManifestReport,
) -> Result<BackupManifestReport, ServerError>
where
    Writer: Write,
{
    let prefix = ObjectPrefix::parse("")?;
    let mut first_field = true;

    writer.write_all(b"{")?;
    write_named_value(
        &mut writer,
        "manifest_version",
        &report.manifest_version,
        &mut first_field,
    )?;
    write_named_value(
        &mut writer,
        "metadata_backend",
        &report.metadata_backend,
        &mut first_field,
    )?;
    write_named_value(
        &mut writer,
        "object_backend",
        &report.object_backend,
        &mut first_field,
    )?;
    write_named_value(
        &mut writer,
        "latest_records",
        &report.latest_records,
        &mut first_field,
    )?;
    write_named_value(
        &mut writer,
        "version_records",
        &report.version_records,
        &mut first_field,
    )?;
    write_named_value(
        &mut writer,
        "reconstruction_rows",
        &report.reconstruction_rows,
        &mut first_field,
    )?;
    write_named_value(
        &mut writer,
        "dedupe_shard_mappings",
        &report.dedupe_shard_mappings,
        &mut first_field,
    )?;
    write_named_value(
        &mut writer,
        "quarantine_candidates",
        &report.quarantine_candidates,
        &mut first_field,
    )?;
    write_named_value(
        &mut writer,
        "retention_holds",
        &report.retention_holds,
        &mut first_field,
    )?;
    write_named_value(
        &mut writer,
        "webhook_deliveries",
        &report.webhook_deliveries,
        &mut first_field,
    )?;
    write_named_value(
        &mut writer,
        "provider_repository_states",
        &report.provider_repository_states,
        &mut first_field,
    )?;
    write_field_name(&mut writer, "objects", &mut first_field)?;
    writer.write_all(b"[")?;

    let mut first_object = true;
    crate::object_store::visit_object_prefix(object_store, &prefix, |metadata| {
        if first_object {
            first_object = false;
        } else {
            writer.write_all(b",")?;
        }

        let entry = BackupManifestObjectEntry::from_metadata(&metadata);
        report.object_count = checked_increment(report.object_count)?;
        report.object_bytes = checked_add(report.object_bytes, entry.length)?;
        to_writer(&mut writer, &entry)?;
        Ok(())
    })?;

    writer.write_all(b"]")?;
    write_named_value(
        &mut writer,
        "object_count",
        &report.object_count,
        &mut first_field,
    )?;
    write_named_value(
        &mut writer,
        "object_bytes",
        &report.object_bytes,
        &mut first_field,
    )?;
    writer.write_all(b"}\n")?;

    Ok(report)
}

fn write_named_value<Writer, Value>(
    writer: &mut Writer,
    name: &str,
    value: &Value,
    first_field: &mut bool,
) -> Result<(), ServerError>
where
    Writer: Write,
    Value: Serialize,
{
    write_field_name(writer, name, first_field)?;
    to_writer(writer, value)?;
    Ok(())
}

fn write_field_name<Writer>(
    writer: &mut Writer,
    name: &str,
    first_field: &mut bool,
) -> Result<(), ServerError>
where
    Writer: Write,
{
    if *first_field {
        *first_field = false;
    } else {
        writer.write_all(b",")?;
    }

    to_writer(&mut *writer, name)?;
    writer.write_all(b":")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use shardline_storage::ObjectKey;

    use super::*;

    // -----------------------------------------------------------------------
    // write_field_name
    // -----------------------------------------------------------------------

    #[test]
    fn write_field_name_first_field() {
        let mut buf = Vec::new();
        let mut first = true;
        write_field_name(&mut buf, "key", &mut first).unwrap();
        assert!(!first);
        assert_eq!(String::from_utf8(buf).unwrap(), r#""key":"#);
    }

    #[test]
    fn write_field_name_second_field_adds_comma() {
        let mut buf = Vec::new();
        let mut first = true;

        write_field_name(&mut buf, "first", &mut first).unwrap();
        write_field_name(&mut buf, "second", &mut first).unwrap();

        assert!(!first);
        assert_eq!(String::from_utf8(buf).unwrap(), r#""first":,"second":"#);
    }

    #[test]
    fn write_field_name_three_fields() {
        let mut buf = Vec::new();
        let mut first = true;

        write_field_name(&mut buf, "a", &mut first).unwrap();
        write_field_name(&mut buf, "b", &mut first).unwrap();
        write_field_name(&mut buf, "c", &mut first).unwrap();

        assert_eq!(String::from_utf8(buf).unwrap(), r#""a":,"b":,"c":"#);
    }

    // -----------------------------------------------------------------------
    // write_named_value
    // -----------------------------------------------------------------------

    #[test]
    fn write_named_value_string() {
        let mut buf = Vec::new();
        let mut first = true;
        write_named_value(&mut buf, "name", &"Alice".to_owned(), &mut first).unwrap();

        assert!(!first);
        assert_eq!(String::from_utf8(buf).unwrap(), r#""name":"Alice""#);
    }

    #[test]
    fn write_named_value_integer() {
        let mut buf = Vec::new();
        let mut first = true;
        write_named_value(&mut buf, "count", &42u64, &mut first).unwrap();

        assert!(!first);
        assert_eq!(String::from_utf8(buf).unwrap(), r#""count":42"#);
    }

    #[test]
    fn write_named_value_multiple_produces_comma_separation() {
        let mut buf = Vec::new();
        let mut first = true;

        write_named_value(&mut buf, "x", &1u64, &mut first).unwrap();
        write_named_value(&mut buf, "y", &2u64, &mut first).unwrap();

        assert_eq!(String::from_utf8(buf).unwrap(), r#""x":1,"y":2"#);
    }

    // -----------------------------------------------------------------------
    // BackupManifestReport
    // -----------------------------------------------------------------------

    #[test]
    fn backup_manifest_report_new_defaults() {
        let report = BackupManifestReport::new("postgres", "fs");
        assert_eq!(report.manifest_version, 1);
        assert_eq!(report.metadata_backend, "postgres");
        assert_eq!(report.object_backend, "fs");
        assert_eq!(report.object_count, 0);
        assert_eq!(report.object_bytes, 0);
        assert_eq!(report.latest_records, 0);
        assert_eq!(report.version_records, 0);
        assert_eq!(report.reconstruction_rows, 0);
        assert_eq!(report.dedupe_shard_mappings, 0);
        assert_eq!(report.quarantine_candidates, 0);
        assert_eq!(report.retention_holds, 0);
        assert_eq!(report.webhook_deliveries, 0);
        assert_eq!(report.provider_repository_states, 0);
    }

    #[test]
    fn backup_manifest_report_new_local_backend() {
        let report = BackupManifestReport::new("local", "s3");
        assert_eq!(report.metadata_backend, "local");
        assert_eq!(report.object_backend, "s3");
    }

    // -----------------------------------------------------------------------
    // BackupManifestObjectEntry
    // -----------------------------------------------------------------------

    #[test]
    fn backup_manifest_object_entry_fields() {
        let entry = BackupManifestObjectEntry {
            key: "objects/abc".to_owned(),
            length: 1024,
            checksum: Some("deadbeef".to_owned()),
        };

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"key\":\"objects/abc\""));
        assert!(json.contains("\"length\":1024"));
        assert!(json.contains("\"checksum\":\"deadbeef\""));
    }

    #[test]
    fn backup_manifest_object_entry_from_metadata_with_checksum() {
        let key = ObjectKey::parse("xorbs/default/ab/abcdef.shard").unwrap();
        let hash = shardline_protocol::ShardlineHash::from_bytes([0xab; 32]);
        let metadata = ObjectMetadata::new(key, 4096, Some(hash));
        let entry = BackupManifestObjectEntry::from_metadata(&metadata);

        assert_eq!(entry.key, "xorbs/default/ab/abcdef.shard");
        assert_eq!(entry.length, 4096);
        assert_eq!(entry.checksum, Some("ab".repeat(32)));
    }

    #[test]
    fn backup_manifest_object_entry_from_metadata_without_checksum() {
        let key = ObjectKey::parse("some/object.bin").unwrap();
        let metadata = ObjectMetadata::new(key, 0, None);
        let entry = BackupManifestObjectEntry::from_metadata(&metadata);

        assert_eq!(entry.key, "some/object.bin");
        assert_eq!(entry.length, 0);
        assert!(entry.checksum.is_none());
    }

    #[test]
    fn backup_manifest_object_entry_no_checksum() {
        let entry = BackupManifestObjectEntry {
            key: "objects/xyz".to_owned(),
            length: 0,
            checksum: None,
        };

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"checksum\":null"));
    }

    // -----------------------------------------------------------------------
    // BackupManifestReport serialization
    // -----------------------------------------------------------------------

    #[test]
    fn backup_manifest_report_serializable() {
        let report = BackupManifestReport {
            manifest_version: 1,
            metadata_backend: "test".to_owned(),
            object_backend: "test".to_owned(),
            object_count: 5,
            object_bytes: 100,
            latest_records: 1,
            version_records: 2,
            reconstruction_rows: 3,
            dedupe_shard_mappings: 4,
            quarantine_candidates: 5,
            retention_holds: 6,
            webhook_deliveries: 7,
            provider_repository_states: 8,
        };

        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("\"manifest_version\":1"));
        assert!(json.contains("\"metadata_backend\":\"test\""));
        assert!(json.contains("\"object_backend\":\"test\""));
        assert!(json.contains("\"object_count\":5"));
        assert!(json.contains("\"object_bytes\":100"));
        assert!(json.contains("\"latest_records\":1"));
        assert!(json.contains("\"version_records\":2"));
        assert!(json.contains("\"reconstruction_rows\":3"));
        assert!(json.contains("\"dedupe_shard_mappings\":4"));
        assert!(json.contains("\"quarantine_candidates\":5"));
        assert!(json.contains("\"retention_holds\":6"));
        assert!(json.contains("\"webhook_deliveries\":7"));
        assert!(json.contains("\"provider_repository_states\":8"));
    }

    // -----------------------------------------------------------------------
    // write_backup_manifest integration test
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_backup_manifest_writes_valid_json_with_local_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let config = crate::ServerConfig::new(
            std::net::SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                8080,
            ),
            "http://127.0.0.1:8080".to_owned(),
            tmp.path().to_path_buf(),
            std::num::NonZeroUsize::new(65536).unwrap_or(std::num::NonZeroUsize::MIN),
        )
        .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
        .unwrap();
        let mut buffer = Vec::new();
        let report = write_backup_manifest(config, &mut buffer).await.unwrap();
        assert_eq!(report.manifest_version, 1);
        assert_eq!(report.metadata_backend, "local");
        assert_eq!(report.object_backend, "local");
        let json = String::from_utf8(buffer).unwrap();
        assert!(json.contains("manifest_version"));
        assert!(json.contains("object_backend"));
    }
}
