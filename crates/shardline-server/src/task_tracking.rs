//! Deployment-backed task evidence. Uses the existing object adapter, not a new service.
use crate::{ServerConfig, ServerError, ServerObjectStore, object_store_from_config};
use serde::{Deserialize, Serialize};
use shardline_protocol::{ByteRange, ShardlineHash};
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
pub(crate) mod embedded;
pub use embedded::EmbeddedTaskTracker;
mod owner;
pub use owner::TaskOwner;
mod history_observation;
mod history_scope_binding;
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

pub(crate) async fn backend_history_scope(
    config: &ServerConfig,
    backend: &crate::ServerBackend,
) -> Option<penelope_core::task::Digest> {
    let crate::ServerBackend::Postgres(backend) = backend else {
        return None;
    };
    // Optional observation must not hold startup or admin inspection indefinitely.
    // Use the existing metadata pool and perform only reads; no upgrade here.
    let resolved = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut connection = backend
            .index_store()
            .pool()
            .acquire()
            .await
            .map_err(shardline_index::PostgresMetadataStoreError::from)?;
        resolved_postgres_deployment_fingerprint(config, &mut connection).await
    })
    .await;
    match resolved {
        Ok(Ok(scope)) => Some(scope),
        Ok(Err(error)) => {
            tracing::warn!(%error, "shared task history unavailable; legacy history continues");
            None
        }
        Err(error) => {
            tracing::warn!(%error, "shared task history timed out; legacy history continues");
            None
        }
    }
}

/// Immutable publication in the configured local or S3-compatible object store.
/// Instantiate on a blocking thread when using the synchronous methods.
#[derive(Debug, Clone)]
pub struct TaskEvidenceStore {
    objects: ServerObjectStore,
}

impl TaskEvidenceStore {
    pub(crate) const fn from_objects(objects: ServerObjectStore) -> Self {
        Self { objects }
    }
    /// Reuses the configured object adapter without requiring a writable metadata root.
    /// # Errors
    /// Returns adapter initialization failures.
    pub fn from_config(config: &ServerConfig) -> Result<Self, ServerError> {
        Ok(Self {
            objects: object_store_from_config(config)?,
        })
    }

    /// Publishes an immutable record, checking exact bytes even on repeated writes.
    /// # Errors
    /// Returns invalid keys, unavailable storage or a conflicting publication.
    pub fn publish(&self, suffix: &str, bytes: &[u8]) -> Result<(), ServerError> {
        let key = Self::key(suffix)?;
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

    fn key(suffix: &str) -> Result<ObjectKey, ServerError> {
        ObjectKey::parse(&format!("{TASK_EVIDENCE_PREFIX}{suffix}"))
            .map_err(|_error| ServerError::InvalidPath)
    }
}
