//! Operator entry point for publishing an authenticated Hub state checkpoint.

use std::io::Error;

use shardline_index::{
    HubRecoveryOutboxStore, HubRefStateCheckpointArchiveObject, LocalIndexStore,
    PostgresIndexStore, put_hub_ref_state_checkpoint,
};
use sqlx::postgres::PgPoolOptions;

use crate::{ServerConfig, ServerError, hub_recovery_resources::HubRecoveryResources};

/// Publishes a signed checkpoint for one repository using the configured Hub
/// recovery archive. This command reads metadata without changing the durable
/// recovery mode; it is available only when SHADOW is already configured.
///
/// # Errors
/// Returns an error when recovery is not configured, the repository is not at
/// an archived watermark, or SQL, signature, and object-store verification
/// fails.
pub async fn create_hub_ref_state_checkpoint(
    config: &ServerConfig,
    repository_id: &str,
) -> Result<HubRefStateCheckpointArchiveObject, ServerError> {
    if config.hub_recovery_mode() != Some(shardline_index::HubRecoveryMode::Shadow) {
        return Err(ServerError::Config(
            crate::ServerConfigError::ConfigFileError(
                "Hub checkpoint creation requires configured SHADOW recovery".into(),
            ),
        ));
    }
    let outbox: Box<dyn HubRecoveryOutboxStore> = match config.index_postgres_url() {
        Some(url) => {
            let pool = PgPoolOptions::new()
                .max_connections(4)
                .connect_lazy(url)
                .map_err(|error| ServerError::Io(Error::other(error)))?;
            Box::new(PostgresIndexStore::new(pool))
        }
        None => Box::new(LocalIndexStore::open_read_only(
            config.root_dir().join("hub"),
        )),
    };

    let runtime = config.hub_recovery_runtime().ok_or_else(|| {
        ServerError::Config(crate::ServerConfigError::ConfigFileError(
            "Hub SHADOW recovery runtime configuration is missing".into(),
        ))
    })?;
    let resources = HubRecoveryResources::from_config(runtime)?;

    put_hub_ref_state_checkpoint(
        outbox.as_ref(),
        &resources.archive,
        repository_id,
        &resources.signing_key,
        &runtime.key_id,
    )
    .await
    .map_err(|error| ServerError::Io(Error::other(error)))
}
