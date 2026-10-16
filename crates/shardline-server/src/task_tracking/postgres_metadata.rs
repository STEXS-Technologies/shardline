//! Evidence I/O on an existing server pool, separate from execution authority.
use std::{fmt, future::Future, time::Duration};

use penelope::core::task::Digest;
use shardline_index::{PostgresIndexStore, PostgresMetadataStoreError, PostgresTaskArtifact};
use sqlx::{Connection, PgPool, Postgres, pool::PoolConnection};

use super::{TaskEvidenceStore, resolved_postgres_deployment_fingerprint};
use crate::{ServerBackend, ServerConfig, ServerError};

#[derive(Clone)]
pub(super) struct PostgresTaskMetadata {
    pool: PgPool,
    handle: tokio::runtime::Handle,
    config: ServerConfig,
    pub(super) scope: Digest,
    close_after_io: bool,
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
    /// Opens existing history without local initialization or application work.
    /// PostgreSQL inspection uses a caller-owned pool and resolves its real scope.
    /// Keep its runtime alive and run synchronous reads on a blocking thread.
    ///
    /// # Errors
    /// Returns configuration, storage or metadata resolution failures.
    pub async fn for_inspection(
        config: &ServerConfig,
        pool: Option<&PgPool>,
    ) -> Result<(Self, Option<Digest>), ServerError> {
        let objects = match config.object_storage_adapter() {
            crate::ObjectStorageAdapter::Local => crate::ServerObjectStore::Local(
                shardline_storage::LocalObjectStore::open(config.root_dir().join("chunks")),
            ),
            crate::ObjectStorageAdapter::S3 => crate::object_store_from_config(config)?,
        };
        let mut store = Self::from_objects(objects);
        if config.index_postgres_url().is_some() {
            let pool = pool.ok_or_else(|| {
                ServerError::Io(std::io::Error::other(
                    "history inspection requires the configured PostgreSQL pool",
                ))
            })?;
            store.with_postgres_pool(config, pool, true).await
        } else {
            if config.root_dir().join("metadata.sqlite3").try_exists()? {
                store.local_metadata = Some(shardline_index::LocalIndexStore::open_read_only(
                    config.root_dir().to_owned(),
                ));
            }
            Ok((store, None))
        }
    }

    /// Keeps the verified target while separating final evidence from the work
    /// session. The lazy pool opens no connection until the work pool is closed.
    pub(super) fn detach_work_history(&mut self) -> Result<(), ServerError> {
        let Some(metadata) = &mut self.postgres_metadata else {
            return Ok(());
        };
        let url = metadata.config.index_postgres_url().ok_or_else(|| {
            ServerError::Io(std::io::Error::other("missing task metadata target"))
        })?;
        metadata.pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect_lazy(url)
            .map_err(PostgresMetadataStoreError::from)?;
        metadata.close_after_io = true;
        Ok(())
    }

    /// Reuses a backend and the configuration used to create it. Selects
    /// metadata only after resolving an actual backend session. Older
    /// owner installations retain legacy reads and publication. No pool,
    /// runtime, process, runtime DDL or new connection configuration is created.
    ///
    /// PostgreSQL needs an active Tokio runtime. Use the synchronous evidence
    /// methods on a blocking thread while that runtime remains alive. Pass the
    /// returned scope to [`Self::summaries_with_scope`].
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
        if let ServerBackend::Local(local) = backend {
            // Metadata ownership is independent of the payload adapter. Use
            // the actual backend root, including SQLite with S3 payloads.
            store.local_metadata = Some(shardline_index::LocalIndexStore::open(
                local.metadata_root.clone(),
            ));
        }
        let ServerBackend::Postgres(postgres) = backend else {
            return Ok((store, None));
        };
        let pool = postgres.index_store().pool();
        store.with_postgres_pool(config, pool, false).await
    }

    /// Reuses a caller-owned evidence pool, independent of a maintenance work
    /// session. The caller must keep the pool and Tokio runtime alive until all
    /// synchronous history I/O has finished. This pool never authorizes work.
    /// Uses the same target checks and legacy compatibility as [`Self::from_backend`].
    /// Connections are closed after each history operation so this evidence
    /// pool does not retain an idle session needed by the actual work.
    ///
    /// # Errors
    /// Returns adapter, runtime, metadata or timeout failures.
    pub async fn from_postgres_pool(
        config: &ServerConfig,
        pool: &PgPool,
    ) -> Result<(Self, Option<Digest>), ServerError> {
        Self::from_config(config)?
            .with_postgres_pool(config, pool, true)
            .await
    }

    pub(super) async fn with_postgres_pool(
        mut self,
        config: &ServerConfig,
        pool: &PgPool,
        close_after_io: bool,
    ) -> Result<(Self, Option<Digest>), ServerError> {
        if config.index_postgres_url().is_none() {
            return Ok((self, None));
        }
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|error| ServerError::Io(std::io::Error::other(error)))?;
        let resolved = tokio::time::timeout(Duration::from_secs(5), async {
            let mut connection = pool.acquire().await
                .map_err(PostgresMetadataStoreError::from)?;
            if close_after_io {
                connection.close_on_drop();
            }
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
            if close_after_io {
                connection.close().await.map_err(PostgresMetadataStoreError::from)?;
            }
            Ok::<_, ServerError>((scope, usable))
        }).await;
        match resolved {
            Ok(Ok((scope, installed))) => {
                if installed {
                    self.postgres_metadata = Some(PostgresTaskMetadata {
                        pool: pool.clone(),
                        handle,
                        config: config.clone(),
                        scope,
                        close_after_io,
                    });
                }
                Ok((self, Some(scope)))
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
                    tracing::warn!(%error, "task metadata unsupported; existing history remains available");
                    return Ok((self, None));
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
        if self.close_after_io {
            connection.close_on_drop();
        }
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

    async fn finish<T>(
        &self,
        connection: PoolConnection<Postgres>,
        result: Result<T, ServerError>,
    ) -> Result<T, ServerError> {
        if self.close_after_io {
            let closed = connection
                .close()
                .await
                .map_err(PostgresMetadataStoreError::from);
            if result.is_ok() {
                closed?;
            }
        }
        result
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
            let result = PostgresIndexStore::publish_task_artifact_on_connection(
                &mut connection,
                key,
                bytes,
            )
            .await
            .map_err(ServerError::from);
            self.finish(connection, result).await
        })
    }

    pub(super) fn publish_bounded(
        &self,
        key: &str,
        bytes: &[u8],
        maximum_runs: u64,
    ) -> Result<bool, ServerError> {
        self.run(async {
            let mut connection = self.connection().await?;
            let result = PostgresIndexStore::publish_task_artifact_bounded_on_connection(
                &mut connection,
                key,
                bytes,
                maximum_runs,
            )
            .await
            .map_err(ServerError::from);
            self.finish(connection, result).await
        })
    }

    pub(super) fn admission_full(&self, maximum_runs: u64) -> Result<bool, ServerError> {
        self.run(async {
            let mut connection = self.connection().await?;
            let result = PostgresIndexStore::task_history_admission_full_on_connection(
                &mut connection,
                maximum_runs,
            )
            .await
            .map_err(ServerError::from);
            self.finish(connection, result).await
        })
    }

    pub(super) fn read(
        &self,
        key: &str,
        maximum: u64,
    ) -> Result<Option<PostgresTaskArtifact>, ServerError> {
        self.run(async {
            let mut connection = self.connection().await?;
            let result =
                PostgresIndexStore::read_task_artifact_on_connection(&mut connection, key, maximum)
                    .await
                    .map_err(ServerError::from);
            self.finish(connection, result).await
        })
    }

    pub(super) fn page(
        &self,
        prefix: &str,
        after: Option<&str>,
    ) -> Result<Vec<(String, u64)>, ServerError> {
        self.run(async {
            let mut connection = self.connection().await?;
            let result = PostgresIndexStore::task_artifact_page_on_connection(
                &mut connection,
                prefix,
                after,
            )
            .await
            .map_err(ServerError::from);
            self.finish(connection, result).await
        })
    }

    pub(super) fn bounded_page(
        &self,
        prefix: &str,
        after: Option<&str>,
    ) -> Result<Vec<(String, PostgresTaskArtifact)>, ServerError> {
        self.run(async {
            let mut connection = self.connection().await?;
            let result = PostgresIndexStore::task_artifact_bounded_page_on_connection(
                &mut connection,
                prefix,
                after,
            )
            .await
            .map_err(ServerError::from);
            self.finish(connection, result).await
        })
    }

    pub(super) fn delete(&self, key: &str) -> Result<(), ServerError> {
        self.run(async {
            let mut connection = self.connection().await?;
            let result =
                PostgresIndexStore::delete_task_artifact_on_connection(&mut connection, key)
                    .await
                    .map(|_| ())
                    .map_err(ServerError::from);
            self.finish(connection, result).await
        })
    }

    pub(super) fn delete_batch(&self, keys: &[String]) -> Result<(), ServerError> {
        if keys.is_empty() {
            return Ok(());
        }
        self.run(async {
            let mut connection = self.connection().await?;
            let result =
                PostgresIndexStore::delete_task_artifacts_on_connection(&mut connection, keys)
                    .await
                    .map(|_| ())
                    .map_err(ServerError::from);
            self.finish(connection, result).await
        })
    }

    pub(super) fn finish_retirement(&self, id: &str) -> Result<(), ServerError> {
        self.run(async {
            let mut connection = self.connection().await?;
            let result = async {
                let mut transaction = connection
                    .begin()
                    .await
                    .map_err(PostgresMetadataStoreError::from)?;
                for category in ["scope-bindings", "retiring"] {
                    PostgresIndexStore::delete_task_artifact_on_connection(
                        &mut transaction,
                        &format!("{}{category}/{id}", super::TASK_EVIDENCE_PREFIX),
                    )
                    .await?;
                }
                transaction
                    .commit()
                    .await
                    .map_err(PostgresMetadataStoreError::from)?;
                Ok::<(), ServerError>(())
            }
            .await;
            self.finish(connection, result).await
        })
    }
}

#[cfg(test)]
mod tests;
