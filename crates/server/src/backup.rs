use std::io::Write;

use serde::Serialize;
use serde_json::to_writer;
use shardline_index::{
    AsyncIndexStore, LocalIndexStore, PostgresIndexStore, PostgresRecordStore, RecordStore,
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
    RecordStore::visit_latest_records(record_store, |_entry| {
        report.latest_records = checked_increment(report.latest_records)?;
        Ok::<(), ServerError>(())
    })
    .await?;

    RecordStore::visit_version_records(record_store, |_entry| {
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
    object_store.visit_prefix(&prefix, |metadata| {
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
