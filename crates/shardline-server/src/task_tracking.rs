//! Deployment-backed task evidence using existing storage resources.
use crate::{ServerConfig, ServerError, ServerObjectStore, object_store_from_config};
use serde::{Deserialize, Serialize};
use shardline_protocol::{ByteRange, ShardlineHash};
use shardline_storage::{
    ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata, ObjectPrefix, ObjectStore,
};
use std::ops::ControlFlow;
pub(crate) mod embedded;
pub use embedded::EmbeddedTaskTracker;
mod owner;
pub use owner::TaskOwner;
mod evidence_reader;
mod history_observation;
mod history_scope_binding;
mod postgres_metadata;
mod relocation;
mod retention;
pub(crate) use history_observation::observe_work_pool;
mod status;
pub use status::{TaskRunState, TaskRunSummary};
#[cfg(test)]
mod tests;

/// Reserved namespace for host task evidence; domain metadata remains authoritative.
pub const TASK_EVIDENCE_PREFIX: &str = "_shardline_tasks/v1/";

/// Whether the host can physically fence this work independently of serving.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskExecutionMode {
    /// A Shardline-managed process worker.
    Isolated,
    /// Server-owned work; observation does not authorize automatic retries.
    Embedded,
    /// An ordinary CLI invocation owns its work directly. Domain fences apply;
    /// this tracker does not claim independent OS fencing of its own process.
    Inline,
}

/// Immutable identity and trust input for one intentional run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskRunIdentity {
    /// Evidence codec version.
    pub schema: u32,
    /// Internally allocated opaque run identifier.
    pub id: String,
    /// Human-readable operation name.
    pub operation: String,
    /// Creation time, used for retention rather than execution deadlines.
    pub created_at: u64,
    /// The pinned Penelope scope.
    pub scope: penelope_core::task::TaskScope,
    /// The complete execution contract.
    pub definition: penelope_core::task::TaskDefinition,
    /// Verification key pinned before dispatch. Never a private signing key.
    pub public_key: [u8; 32],
    /// Logical deployment target, independent of password and S3 secret
    /// rotation. PostgreSQL login names remain part of the legacy scope.
    pub deployment: penelope_core::task::Digest,
    /// Runtime protection model.
    pub execution: TaskExecutionMode,
    /// Host identity for observation. This is not a distributed lease or a fence.
    pub owner: TaskOwner,
}

/// Hashes logical metadata and object targets without retaining credentials.
/// PostgreSQL login and session options remain in the legacy scope because
/// different roles can resolve to different schemas on the same database.
/// # Errors
/// Rejects malformed Postgres connection configuration.
pub fn deployment_fingerprint(
    config: &ServerConfig,
) -> Result<penelope_core::task::Digest, ServerError> {
    use std::str::FromStr;
    let root = config
        .root_dir()
        .canonicalize()
        .or_else(|_| std::path::absolute(config.root_dir()))?;
    let metadata = if let Some(url) = config.index_postgres_url() {
        let options = sqlx::postgres::PgConnectOptions::from_str(url)
            .map_err(|error| ServerError::Io(std::io::Error::other(error)))?;
        format!(
            "{}|{}|{:?}|{}|{:?}",
            options.get_host(),
            options.get_port(),
            options.get_database(),
            options.get_username(),
            options.get_options()
        )
    } else {
        format!("local:{root:?}")
    };
    let objects = deployment_object_target(config, &root);
    Ok(penelope_core::task::Digest::of(
        format!("shardline.deployment-target.v1|{metadata}|{objects}").as_bytes(),
    ))
}

fn deployment_object_target(config: &ServerConfig, root: &std::path::Path) -> String {
    match config.object_storage_adapter() {
        crate::ObjectStorageAdapter::Local => {
            format!("local:{:?}", root.join("chunks"))
        }
        crate::ObjectStorageAdapter::S3 => format!(
            "s3:{:?}",
            config
                .s3_object_store_config()
                .map(|s3| s3.clone().with_credentials(None, None, None))
        ),
    }
}

/// Resolves a PostgreSQL history scope on an existing metadata connection.
///
/// Roles using the same configured endpoint, initialized metadata identity,
/// visible schema and object target agree on this scope. Different schemas and
/// independently initialized stores behind a proxy stay separate,
/// even when their connection options match. The connection must belong to the
/// configured metadata target. No connection is opened or replaced here.
///
/// This additive identity does not rewrite legacy task manifests or change
/// [`deployment_fingerprint`]. Callers must explicitly bind new evidence to it;
/// it does not authorize merging historical scopes, retries or health claims.
///
/// # Errors
///
/// Returns missing PostgreSQL configuration, an unavailable or uninitialized
/// schema, unsupported row-level security,
/// database query failure, or invalid local object-target resolution.
pub async fn resolved_postgres_deployment_fingerprint(
    config: &ServerConfig,
    connection: &mut sqlx::PgConnection,
) -> Result<penelope_core::task::Digest, ServerError> {
    use std::str::FromStr;
    let url = config.index_postgres_url().ok_or_else(|| {
        shardline_index::PostgresMetadataStoreError::Unsupported(
            "resolved history scope requires PostgreSQL configuration".to_owned(),
        )
    })?;
    let options = sqlx::postgres::PgConnectOptions::from_str(url)
        .map_err(|error| ServerError::Io(std::io::Error::other(error)))?;
    let (database, schema): (String, String) =
        sqlx::query_as("SELECT current_database(), current_schema()")
            .fetch_one(&mut *connection)
            .await
            .map_err(shardline_index::PostgresMetadataStoreError::from)?;
    let quoted_schema = format!("\"{}\"", schema.replace('"', "\"\""));
    let identity: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT {quoted_schema}.shardline_task_history_scope_v1($1)"
    )))
    .bind(&schema)
    .fetch_one(connection)
    .await
    .map_err(shardline_index::PostgresMetadataStoreError::from)?;
    let root = config
        .root_dir()
        .canonicalize()
        .or_else(|_| std::path::absolute(config.root_dir()))?;
    // Tuple encoding makes identifier boundaries unambiguous, including names
    // containing separators or quotes. Credentials never enter this identity.
    let target = serde_json::to_vec(&(
        "shardline.deployment-target.postgres-schema.v1",
        options.get_host(),
        options.get_port(),
        options.get_socket(),
        database,
        schema,
        identity,
        deployment_object_target(config, &root),
    ))?;
    Ok(penelope_core::task::Digest::of(&target))
}

/// Immutable publication in existing deployment storage. Local deployments use
/// the metadata database with legacy object-store reads. Server-backed stores
/// can use owner-installed PostgreSQL evidence storage on the existing pool;
/// configuration-only stores retain legacy PostgreSQL/S3 publication.
/// Instantiate on a blocking thread when using the synchronous methods.
#[derive(Debug, Clone)]
pub struct TaskEvidenceStore {
    objects: ServerObjectStore,
    local_metadata: Option<shardline_index::LocalIndexStore>,
    postgres_metadata: Option<postgres_metadata::PostgresTaskMetadata>,
    metadata_only_publication: bool,
}

impl TaskEvidenceStore {
    pub(crate) const fn from_objects(objects: ServerObjectStore) -> Self {
        Self {
            objects,
            local_metadata: None,
            postgres_metadata: None,
            metadata_only_publication: false,
        }
    }
    /// Reuses existing metadata for local payload storage without PostgreSQL.
    /// Other configurations retain object-backed evidence and its existing permissions.
    /// For upgraded PostgreSQL history, reuse a running backend with
    /// [`Self::from_backend`] and its verified history scope instead.
    /// # Errors
    /// Returns adapter initialization failures.
    pub fn from_config(config: &ServerConfig) -> Result<Self, ServerError> {
        Ok(Self::from_backend_objects(
            config,
            object_store_from_config(config)?,
        ))
    }

    pub(crate) fn from_backend_objects(config: &ServerConfig, objects: ServerObjectStore) -> Self {
        Self {
            objects,
            postgres_metadata: None,
            metadata_only_publication: false,
            local_metadata: (config.index_postgres_url().is_none()
                && config.object_storage_adapter() == crate::ObjectStorageAdapter::Local)
                .then(|| shardline_index::LocalIndexStore::open(config.root_dir().to_owned())),
        }
    }

    /// Prevents new history from entering payload storage when metadata is unavailable.
    /// Legacy reads and retention remain available. Publication then returns an
    /// error instead of adding control objects to the payload inventory.
    /// Existing constructors retain their legacy publication behavior by default.
    #[must_use]
    pub const fn require_metadata_publication(mut self) -> Self {
        self.metadata_only_publication = true;
        self
    }

    /// Whether metadata was selected for history; this is not a write-health probe.
    pub(crate) const fn uses_metadata_history(&self) -> bool {
        self.local_metadata.is_some() || self.postgres_metadata.is_some()
    }

    /// Publishes an immutable record, checking exact bytes even on repeated writes.
    /// # Errors
    /// Returns invalid keys, unavailable storage or a conflicting publication.
    pub fn publish(&self, suffix: &str, bytes: &[u8]) -> Result<(), ServerError> {
        let key = Self::key(suffix)?;
        if let Some(metadata) = &self.postgres_metadata {
            if self.objects.metadata(&key)?.is_some() {
                if self.read(suffix, u64::try_from(bytes.len())?)?.as_deref() != Some(bytes) {
                    return Err(ServerError::InvalidContentHash);
                }
                return Ok(());
            }
            return metadata.publish(key.as_str(), bytes);
        }
        if let Some(metadata) = &self.local_metadata {
            // Preserve existing immutable legacy publications. New runs use the
            // metadata table; old history is neither rewritten nor discarded.
            if self.objects.metadata(&key)?.is_some() {
                let existing = self.read(suffix, u64::try_from(bytes.len())?)?;
                if existing.as_deref() != Some(bytes) {
                    return Err(ServerError::Io(std::io::Error::other(
                        "task evidence publication conflict",
                    )));
                }
                return Ok(());
            }
            metadata.publish_task_artifact(key.as_str(), bytes)?;
            return Ok(());
        }
        if self.metadata_only_publication {
            return Err(ServerError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "task metadata unavailable; history cannot be published into payload storage",
            )));
        }
        let hash = ShardlineHash::from_bytes(*blake3::hash(bytes).as_bytes());
        let length = u64::try_from(bytes.len())?;
        self.objects.put_if_absent(
            &key,
            ObjectBody::Borrowed(bytes),
            &ObjectIntegrity::new(hash, length),
        )?;
        // Resolve lost acknowledgements and existing objects against exact bytes.
        if self.read(suffix, length)? != Some(bytes.to_vec()) {
            return Err(ServerError::Io(std::io::Error::other(
                "task evidence publication was not confirmed",
            )));
        }
        Ok(())
    }

    /// Reads a bounded immutable record. Absence is distinct from a read failure.
    /// # Errors
    /// Returns unavailable storage or evidence larger than the requested bound.
    pub fn read(&self, suffix: &str, maximum: u64) -> Result<Option<Vec<u8>>, ServerError> {
        if let Some(metadata) = &self.postgres_metadata
            && let Some(artifact) = metadata.read(Self::key(suffix)?.as_str(), maximum)?
        {
            let bytes = artifact.bytes.ok_or(ServerError::RequestBodyTooLarge)?;
            if self
                .read_objects(suffix, maximum)?
                .is_some_and(|legacy| legacy != bytes)
            {
                return Err(ServerError::InvalidContentHash);
            }
            return Ok(Some(bytes));
        }
        if let Some(metadata) = &self.local_metadata {
            let bytes = match metadata.read_task_artifact(Self::key(suffix)?.as_str(), maximum) {
                Err(shardline_index::LocalIndexStoreError::MetadataTooLarge { .. }) => {
                    return Err(ServerError::RequestBodyTooLarge);
                }
                result => result?,
            };
            if let Some(bytes) = bytes {
                if self
                    .read_objects(suffix, maximum)?
                    .is_some_and(|legacy| legacy != bytes)
                {
                    return Err(ServerError::InvalidContentHash);
                }
                return Ok(Some(bytes));
            }
        }
        self.read_objects(suffix, maximum)
    }

    fn read_objects(&self, suffix: &str, maximum: u64) -> Result<Option<Vec<u8>>, ServerError> {
        let key = Self::key(suffix)?;
        let Some(metadata) = self.objects.metadata(&key)? else {
            return Ok(None);
        };
        let length = metadata.length();
        if length > maximum {
            return Err(ServerError::RequestBodyTooLarge);
        }
        if length == 0 {
            return Ok(Some(Vec::new()));
        }
        let bytes = self.objects.read_range(
            &key,
            ByteRange::new(0, length.saturating_sub(1))
                .map_err(|_error| ServerError::InvalidPath)?,
        )?;
        if u64::try_from(bytes.len())? != length {
            return Err(ServerError::InvalidContentHash);
        }
        Ok(Some(bytes))
    }

    fn visit_prefix<F>(&self, prefix: &ObjectPrefix, visit: F) -> Result<(), ServerError>
    where
        F: FnMut(&ObjectMetadata) -> Result<(), ServerError>,
    {
        self.visit_prefix_after(prefix, None, visit)
    }

    fn visit_prefix_after<F>(
        &self,
        prefix: &ObjectPrefix,
        after: Option<&str>,
        mut visit: F,
    ) -> Result<(), ServerError>
    where
        F: FnMut(&ObjectMetadata) -> Result<(), ServerError>,
    {
        self.visit_prefix_until(prefix, after, |object| {
            visit(object)?;
            Ok(ControlFlow::Continue(()))
        })
    }

    // Only ordered metadata honors Break. Legacy inventory can be unordered,
    // so all of it must still be considered for the globally earliest page.
    fn visit_prefix_until<F>(
        &self,
        prefix: &ObjectPrefix,
        after: Option<&str>,
        mut visit: F,
    ) -> Result<(), ServerError>
    where
        F: FnMut(&ObjectMetadata) -> Result<ControlFlow<()>, ServerError>,
    {
        if let Some(metadata) = &self.postgres_metadata {
            let mut after = after.map(str::to_owned);
            'pages: loop {
                let page = metadata.page(prefix.as_str(), after.as_deref())?;
                if page.is_empty() {
                    break;
                }
                for (key, length) in page {
                    if !key.starts_with(prefix.as_str()) {
                        break 'pages;
                    }
                    let object_key =
                        ObjectKey::parse(&key).map_err(|_error| ServerError::InvalidPath)?;
                    after = Some(key);
                    if visit(&ObjectMetadata::new(object_key, length, None))?.is_break() {
                        break 'pages;
                    }
                }
            }
        }
        if let Some(metadata) = &self.local_metadata {
            metadata.visit_task_artifacts_until::<ServerError, _>(
                prefix.as_str(),
                after,
                |key, length| {
                    let key = ObjectKey::parse(key).map_err(|_error| ServerError::InvalidPath)?;
                    visit(&ObjectMetadata::new(key, length, None))
                },
            )?;
        }
        self.objects.visit_prefix(prefix, |object| {
            if after.is_some_and(|after| object.key().as_str() <= after) {
                return Ok(());
            }
            if let Some(metadata) = &self.postgres_metadata
                && metadata.read(object.key().as_str(), 0)?.is_some()
            {
                return Ok(());
            }
            if let Some(metadata) = &self.local_metadata {
                // Do not allocate evidence while suppressing duplicate inventory.
                if metadata.contains_task_artifact(object.key().as_str())? {
                    return Ok(());
                }
            }
            visit(&object).map(|_| ())
        })?;
        Ok(())
    }

    fn delete_artifact(&self, key: &ObjectKey) -> Result<(), ServerError> {
        // Remove the legacy copy first so a partially completed cleanup cannot
        // reveal it again after the metadata copy disappears.
        self.objects.delete_if_present(key)?;
        if let Some(metadata) = &self.postgres_metadata {
            metadata.delete(key.as_str())?;
        }
        if let Some(metadata) = &self.local_metadata {
            metadata.delete_task_artifact(key.as_str())?;
        }
        Ok(())
    }

    fn key(suffix: &str) -> Result<ObjectKey, ServerError> {
        ObjectKey::parse(&format!("{TASK_EVIDENCE_PREFIX}{suffix}"))
            .map_err(|_error| ServerError::InvalidPath)
    }
}
