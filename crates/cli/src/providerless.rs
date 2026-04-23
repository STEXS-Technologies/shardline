use std::{
    env::current_dir,
    fs::File,
    io::{Error as IoError, Read},
    path::{Path, PathBuf},
};

use shardline_server::{ServerConfig, ServerConfigError};
use thiserror::Error;

use crate::{
    admin::{AdminTokenError, read_signing_key_bytes},
    config::load_server_config,
    local_output::{ensure_output_directory, write_output_bytes},
};

const PROVIDERLESS_STATE_DIR_NAME: &str = ".shardline";
const PROVIDERLESS_DATA_DIR_NAME: &str = "data";
const PROVIDERLESS_ENV_FILE_NAME: &str = "providerless.env";
const PROVIDERLESS_SIGNING_KEY_FILE_NAME: &str = "token-signing-key";
const PROVIDERLESS_BIND_ADDR: &str = "0.0.0.0:8080";
const PROVIDERLESS_PUBLIC_BASE_URL: &str = "http://127.0.0.1:8080";

/// Result of bootstrapping providerless source-checkout state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderlessSetupReport {
    /// The providerless state directory, usually `.shardline`.
    pub state_dir: PathBuf,
    /// The providerless local data root, usually `.shardline/data`.
    pub data_dir: PathBuf,
    /// The local bearer-token signing key path.
    pub key_file: PathBuf,
    /// The generated environment file path.
    pub env_file: PathBuf,
}

/// Failure while creating or loading providerless source-checkout state.
#[derive(Debug, Error)]
pub enum ProviderlessRuntimeError {
    /// The current directory could not be resolved.
    #[error("providerless bootstrap could not resolve the current directory")]
    CurrentDirectory(#[source] IoError),
    /// Local bootstrap filesystem I/O failed.
    #[error("providerless bootstrap failed")]
    Io(#[from] IoError),
    /// The configured signing key file could not be parsed safely.
    #[error("providerless signing key could not be loaded")]
    SigningKey(#[from] AdminTokenError),
    /// Server configuration parsing or validation failed.
    #[error(transparent)]
    Config(#[from] ServerConfigError),
}

/// Bootstraps local providerless source-checkout state under `.shardline`.
///
/// # Errors
///
/// Returns [`ProviderlessRuntimeError`] when the local state directory cannot be created,
/// the signing key cannot be generated or validated, or the generated configuration cannot
/// be applied safely.
pub fn run_providerless_setup(
    state_dir_override: Option<&Path>,
) -> Result<ProviderlessSetupReport, ProviderlessRuntimeError> {
    let state_dir = resolve_providerless_state_dir(state_dir_override)?;
    ensure_providerless_state(&state_dir)
}

/// Loads the runtime server configuration and automatically bootstraps providerless local
/// source-checkout state when the active configuration matches the `.shardline/data`
/// convention and does not already provide a signing key.
///
/// # Errors
///
/// Returns [`ProviderlessRuntimeError`] when configuration loading fails or when local
/// providerless bootstrap cannot complete safely.
pub fn load_runtime_server_config(
    root_override: Option<&Path>,
) -> Result<ServerConfig, ProviderlessRuntimeError> {
    let config = load_server_config(root_override)?;
    apply_providerless_source_checkout_defaults(config)
}

fn apply_providerless_source_checkout_defaults(
    config: ServerConfig,
) -> Result<ServerConfig, ProviderlessRuntimeError> {
    if config.token_signing_key().is_some()
        || config.index_postgres_url().is_some()
        || config.provider_config_path().is_some()
        || config.provider_api_key().is_some()
    {
        return Ok(config);
    }

    let Some(state_dir) = state_dir_for_source_checkout_root(config.root_dir()) else {
        return Ok(config);
    };

    let report = ensure_providerless_state(&state_dir)?;
    let signing_key = read_signing_key_bytes(&report.key_file)?;
    Ok(config
        .with_root_dir(report.data_dir)
        .with_token_signing_key(signing_key)?)
}

fn ensure_providerless_state(
    state_dir: &Path,
) -> Result<ProviderlessSetupReport, ProviderlessRuntimeError> {
    ensure_output_directory(state_dir)?;
    let data_dir = state_dir.join(PROVIDERLESS_DATA_DIR_NAME);
    ensure_output_directory(&data_dir)?;

    let key_file = state_dir.join(PROVIDERLESS_SIGNING_KEY_FILE_NAME);
    ensure_providerless_signing_key(&key_file)?;

    let env_file = state_dir.join(PROVIDERLESS_ENV_FILE_NAME);
    let env_contents = render_providerless_env_file(&data_dir, &key_file);
    write_output_bytes(&env_file, env_contents.as_bytes(), false)?;

    Ok(ProviderlessSetupReport {
        state_dir: state_dir.to_path_buf(),
        data_dir,
        key_file,
        env_file,
    })
}

fn resolve_providerless_state_dir(
    state_dir_override: Option<&Path>,
) -> Result<PathBuf, ProviderlessRuntimeError> {
    if let Some(path) = state_dir_override {
        return Ok(path.to_path_buf());
    }

    Ok(current_dir()
        .map_err(ProviderlessRuntimeError::CurrentDirectory)?
        .join(PROVIDERLESS_STATE_DIR_NAME))
}

fn state_dir_for_source_checkout_root(root_dir: &Path) -> Option<PathBuf> {
    if root_dir.file_name()? != PROVIDERLESS_DATA_DIR_NAME {
        return None;
    }
    let state_dir = root_dir.parent()?;
    if state_dir.file_name()? != PROVIDERLESS_STATE_DIR_NAME {
        return None;
    }

    Some(state_dir.to_path_buf())
}

fn ensure_providerless_signing_key(path: &Path) -> Result<(), ProviderlessRuntimeError> {
    if path.exists() {
        let _bytes = read_signing_key_bytes(path)?;
        return Ok(());
    }

    let mut random = [0_u8; 32];
    let mut urandom = File::open("/dev/urandom")?;
    urandom.read_exact(&mut random)?;
    let encoded = hex::encode(random);
    write_output_bytes(path, encoded.as_bytes(), false)?;
    let _bytes = read_signing_key_bytes(path)?;
    Ok(())
}

fn render_providerless_env_file(data_dir: &Path, key_file: &Path) -> String {
    format!(
        "SHARDLINE_BIND_ADDR={PROVIDERLESS_BIND_ADDR}\n\
SHARDLINE_SERVER_ROLE=all\n\
SHARDLINE_PUBLIC_BASE_URL={PROVIDERLESS_PUBLIC_BASE_URL}\n\
SHARDLINE_ROOT_DIR={}\n\
SHARDLINE_OBJECT_STORAGE_ADAPTER=local\n\
SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=memory\n\
SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS=30\n\
SHARDLINE_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES=4096\n\
SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS=64\n\
SHARDLINE_TOKEN_SIGNING_KEY_FILE={}\n",
        data_dir.display(),
        key_file.display()
    )
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{create_dir, create_dir_all, read_to_string},
        net::SocketAddr,
        num::NonZeroUsize,
        path::PathBuf,
    };

    use shardline_server::ServerConfig;

    use super::{
        PROVIDERLESS_DATA_DIR_NAME, PROVIDERLESS_ENV_FILE_NAME, PROVIDERLESS_SIGNING_KEY_FILE_NAME,
        ProviderlessRuntimeError, apply_providerless_source_checkout_defaults,
        run_providerless_setup, state_dir_for_source_checkout_root,
    };

    #[test]
    fn providerless_setup_creates_expected_local_state() {
        let temp = tempfile::tempdir();
        assert!(temp.is_ok());
        let Ok(temp) = temp else {
            return;
        };
        let state_dir = temp.path().join(".shardline");

        let report = run_providerless_setup(Some(&state_dir));
        assert!(report.is_ok());
        let Ok(report) = report else {
            return;
        };

        assert_eq!(report.state_dir, state_dir);
        assert_eq!(report.data_dir, state_dir.join(PROVIDERLESS_DATA_DIR_NAME));
        assert_eq!(
            report.key_file,
            state_dir.join(PROVIDERLESS_SIGNING_KEY_FILE_NAME)
        );
        assert_eq!(report.env_file, state_dir.join(PROVIDERLESS_ENV_FILE_NAME));
        assert!(report.data_dir.is_dir());
        assert!(report.key_file.is_file());
        assert!(report.env_file.is_file());

        let env_contents = read_to_string(&report.env_file);
        assert!(env_contents.is_ok());
        let Ok(env_contents) = env_contents else {
            return;
        };
        assert!(env_contents.contains("SHARDLINE_ROOT_DIR="));
        assert!(env_contents.contains("SHARDLINE_TOKEN_SIGNING_KEY_FILE="));
    }

    #[test]
    fn providerless_runtime_defaults_bootstrap_source_checkout_root() {
        let temp = tempfile::tempdir();
        assert!(temp.is_ok());
        let Ok(temp) = temp else {
            return;
        };
        let data_root = temp.path().join(".shardline").join("data");
        let bind_addr: Result<SocketAddr, _> = "127.0.0.1:8080".parse();
        assert!(bind_addr.is_ok());
        let Ok(bind_addr) = bind_addr else {
            return;
        };
        let config = ServerConfig::new(
            bind_addr,
            "http://127.0.0.1:8080".to_owned(),
            data_root.clone(),
            NonZeroUsize::MIN,
        );

        let config = apply_providerless_source_checkout_defaults(config);
        assert!(config.is_ok());
        let Ok(config) = config else {
            return;
        };

        assert_eq!(config.root_dir(), data_root.as_path());
        assert!(config.token_signing_key().is_some());
        let key_file = temp
            .path()
            .join(".shardline")
            .join(PROVIDERLESS_SIGNING_KEY_FILE_NAME);
        assert!(key_file.is_file());
    }

    #[test]
    fn providerless_runtime_defaults_ignore_non_checkout_roots() {
        let temp = tempfile::tempdir();
        assert!(temp.is_ok());
        let Ok(temp) = temp else {
            return;
        };
        let bind_addr: Result<SocketAddr, _> = "127.0.0.1:8080".parse();
        assert!(bind_addr.is_ok());
        let Ok(bind_addr) = bind_addr else {
            return;
        };
        let config = ServerConfig::new(
            bind_addr,
            "http://127.0.0.1:8080".to_owned(),
            temp.path().join("srv").join("assets"),
            NonZeroUsize::MIN,
        );

        let config = apply_providerless_source_checkout_defaults(config);
        assert!(config.is_ok());
        let Ok(config) = config else {
            return;
        };

        assert!(config.token_signing_key().is_none());
    }

    #[test]
    fn setup_rejects_existing_invalid_signing_key() {
        let temp = tempfile::tempdir();
        assert!(temp.is_ok());
        let Ok(temp) = temp else {
            return;
        };
        let state_dir = temp.path().join(".shardline");
        let created = create_dir_all(&state_dir);
        assert!(created.is_ok());
        let key_file = state_dir.join(PROVIDERLESS_SIGNING_KEY_FILE_NAME);
        let wrote = create_dir(&key_file);
        assert!(wrote.is_ok());

        let report = run_providerless_setup(Some(&state_dir));

        assert!(matches!(
            report,
            Err(ProviderlessRuntimeError::SigningKey(_))
        ));
    }

    #[test]
    fn source_checkout_root_detection_requires_hidden_state_layout() {
        let root = PathBuf::from("/tmp/project/.shardline/data");

        assert_eq!(
            state_dir_for_source_checkout_root(&root),
            Some(PathBuf::from("/tmp/project/.shardline"))
        );
        assert_eq!(
            state_dir_for_source_checkout_root(PathBuf::from("/tmp/project/data").as_path()),
            None
        );
    }
}
