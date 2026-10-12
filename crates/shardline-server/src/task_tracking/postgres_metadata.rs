//! Evidence I/O on an existing server pool, separate from execution authority.
use std::{fmt, future::Future, time::Duration};

use penelope_core::task::Digest;
use shardline_index::{PostgresIndexStore, PostgresMetadataStoreError, PostgresTaskArtifact};
use sqlx::{PgPool, Postgres, pool::PoolConnection};

use super::{TaskEvidenceStore, resolved_postgres_deployment_fingerprint};
use crate::{ServerBackend, ServerConfig, ServerError};

#[derive(Clone)]
pub(super) struct PostgresTaskMetadata {
    pool: PgPool,
    handle: tokio::runtime::Handle,
    config: ServerConfig,
    pub(super) scope: Digest,
}

impl fmt::Debug for PostgresTaskMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresTaskMetadata")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl TaskEvidenceStore {
    /// Reuses a backend and the configuration used to create it. Selects
    /// metadata only after resolving an actual backend session. Older
    /// owner installations retain legacy reads and publication. No pool,
    /// runtime, process, runtime DDL or new connection configuration is created.
    ///
    /// PostgreSQL needs an active Tokio runtime. Use the synchronous evidence
    /// methods on a blocking thread while that runtime remains alive. Pass the
    /// returned scope to [`Self::summaries_with_scope`] and [`Self::embedded_report`].
    ///
    /// # Examples
    /// ```no_run
    /// # async fn example(config: shardline_server::ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    /// use shardline_server::{ServerBackend, task_tracking::{TaskEvidenceStore, deployment_fingerprint}};
    /// let backend = ServerBackend::from_config(&config).await?;
    /// let (store, scope) = TaskEvidenceStore::from_backend(&config, &backend).await?;
    /// let deployment = deployment_fingerprint(&config)?;
    /// let history = tokio::task::spawn_blocking(move || {
    ///     store.summaries_with_scope(deployment, scope, None, 20)
    /// }).await??;
    /// # let _ = history;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    /// Returns a missing runtime, unexpected metadata failures or a timeout.
    /// Inspection must expose these failures; startup may preserve optional
    /// legacy tracking instead.
    pub async fn from_backend(
        config: &ServerConfig,
        backend: &ServerBackend,
    ) -> Result<(Self, Option<Digest>), ServerError> {
        let mut store = Self::from_backend_objects(config, backend.object_store());
        let ServerBackend::Postgres(postgres) = backend else {
            return Ok((store, None));
        };
        let pool = postgres.index_store().pool();
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|error| ServerError::Io(std::io::Error::other(error)))?;
        let resolved = tokio::time::timeout(Duration::from_secs(5), async {
            let mut connection = pool.acquire().await
                .map_err(PostgresMetadataStoreError::from)?;
            let scope = resolved_postgres_deployment_fingerprint(config, &mut connection).await?;
            let installed: bool = sqlx::query_scalar(
                "SELECT to_regprocedure(format('%I.shardline_task_artifact_read_v1(text,text,bigint)',current_schema())) IS NOT NULL",
            ).fetch_one(&mut *connection).await
                .map_err(PostgresMetadataStoreError::from)?;
            let usable = if installed {
                match PostgresIndexStore::read_task_artifact_on_connection(
                    &mut connection, super::TASK_EVIDENCE_PREFIX, 0,
                ).await {
                    Ok(_) => true,
                    Err(error) => {
                        if let PostgresMetadataStoreError::Sqlx(database) = &error
                            && database.as_database_error().is_some_and(|database| database.code().as_deref() == Some("42501"))
                        {
                            // Existing history grouping requires schema access,
                            // not artifact-read privileges. Keep that grouping
                            // and its legacy storage for a restricted account.
                            false
                        } else {
                            return Err(ServerError::from(error));
                        }
                    }
                }
            } else { false };
            Ok::<_, ServerError>((scope, usable))
        }).await;
        match resolved {
            Ok(Ok((scope, installed))) => {
                if installed {
                    store.postgres_metadata = Some(PostgresTaskMetadata {
                        pool: pool.clone(),
                        handle,
                        config: config.clone(),
                        scope,
                    });
                }
                Ok((store, Some(scope)))
            }
            Ok(Err(error)) => {
                if let ServerError::Index(crate::error::IndexError::PostgresMetadata(
                    PostgresMetadataStoreError::Sqlx(database),
                )) = &error
                    && database.as_database_error().is_some_and(|database| {
                        matches!(
                            database.code().as_deref(),
                            Some("42883" | "SLD04" | "42501")
                        )
                    })
                {
                    tracing::warn!(%error, "task metadata unsupported; legacy history continues");
                    return Ok((store, None));
                }
                Err(error)
            }
            Err(error) => Err(ServerError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                error,
            ))),
        }
    }
}

impl PostgresTaskMetadata {
    async fn connection(&self) -> Result<PoolConnection<Postgres>, ServerError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(PostgresMetadataStoreError::from)?;
        // A proxy or changed session default must not move a run's history to
        // another database or schema. Resolve and use the very same checkout.
        let actual =
            resolved_postgres_deployment_fingerprint(&self.config, &mut connection).await?;
        if actual != self.scope {
            return Err(ServerError::Io(std::io::Error::other(
                "task metadata target changed",
            )));
        }
        Ok(connection)
    }

    fn run<T>(&self, work: impl Future<Output = Result<T, ServerError>>) -> Result<T, ServerError> {
        // All host call sites use blocking inspection/receipt threads. The
        // captured server handle also works there without a thread-local handle.
        self.handle.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), work)
                .await
                .map_err(|error| {
                    ServerError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, error))
                })?
        })
    }

    pub(super) fn publish(&self, key: &str, bytes: &[u8]) -> Result<(), ServerError> {
        self.run(async {
            let mut connection = self.connection().await?;
            PostgresIndexStore::publish_task_artifact_on_connection(&mut connection, key, bytes)
                .await
                .map_err(ServerError::from)
        })
    }

    pub(super) fn read(
        &self,
        key: &str,
        maximum: u64,
    ) -> Result<Option<PostgresTaskArtifact>, ServerError> {
        self.run(async {
            let mut connection = self.connection().await?;
            PostgresIndexStore::read_task_artifact_on_connection(&mut connection, key, maximum)
                .await
                .map_err(ServerError::from)
        })
    }

    pub(super) fn page(
        &self,
        prefix: &str,
        after: Option<&str>,
    ) -> Result<Vec<(String, u64)>, ServerError> {
        self.run(async {
            let mut connection = self.connection().await?;
            PostgresIndexStore::task_artifact_page_on_connection(&mut connection, prefix, after)
                .await
                .map_err(ServerError::from)
        })
    }

    pub(super) fn delete(&self, key: &str) -> Result<(), ServerError> {
        self.run(async {
            let mut connection = self.connection().await?;
            PostgresIndexStore::delete_task_artifact_on_connection(&mut connection, key)
                .await
                .map_err(ServerError::from)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests;
