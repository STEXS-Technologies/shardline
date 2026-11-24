//! Shared construction of the configured Hub recovery archive and signer.

use std::io::Error;

use ed25519_dalek::SigningKey;
use shardline_server_core::ServerObjectStore;

use crate::{ServerConfigError, config::HubRecoveryRuntimeConfig};

pub(crate) struct HubRecoveryResources {
    pub(crate) archive: ServerObjectStore,
    pub(crate) signing_key: SigningKey,
}

impl HubRecoveryResources {
    pub(crate) fn from_config(
        runtime: &HubRecoveryRuntimeConfig,
    ) -> Result<Self, crate::ServerError> {
        let archive = ServerObjectStore::s3(runtime.archive_s3.clone())
            .map_err(|error| crate::ServerError::Io(Error::other(error)))?;
        let signing_seed: [u8; 32] =
            runtime
                .signing_key
                .expose_secret()
                .try_into()
                .map_err(|_error| {
                    crate::ServerError::Config(ServerConfigError::ConfigFileError(
                        "Hub recovery signing key must be 32 bytes".into(),
                    ))
                })?;
        Ok(Self {
            archive,
            signing_key: SigningKey::from_bytes(&signing_seed),
        })
    }
}
