use std::{
    env::{current_dir, var_os},
    path::{Path, PathBuf},
};

use shardline_server::{
    ConfigCheckReport, ServerConfig, ServerConfigError, ServerError, run_config_check,
};
use thiserror::Error;

use crate::{
    local_path::ensure_directory_path_components_are_not_symlinked,
    providerless::{ProviderlessRuntimeError, load_runtime_server_config},
};

const LOCAL_STATE_DIR: &str = ".shardline";
const LOCAL_DATA_DIR: &str = "data";

/// Runtime failure while validating configuration.
#[derive(Debug, Error)]
pub enum ConfigRuntimeError {
    /// Configuration loading failed before runtime validation started.
    #[error(transparent)]
    Config(#[from] ServerConfigError),
    /// Providerless local source-checkout bootstrap failed.
    #[error(transparent)]
    Providerless(#[from] ProviderlessRuntimeError),
    /// Runtime validation failed after configuration loaded.
    #[error(transparent)]
    Server(#[from] ServerError),
}

/// Loads and validates the active Shardline server configuration.
///
/// # Errors
///
/// Returns [`ConfigRuntimeError`] when configuration cannot be parsed or the selected
/// backend fails readiness validation.
pub async fn run_config_check_from_env() -> Result<ConfigCheckReport, ConfigRuntimeError> {
    let config = load_runtime_server_config(None)?;
    Ok(run_config_check(config).await?)
}

/// Loads the active Shardline server configuration and applies an optional deployment-root override.
///
/// # Errors
///
/// Returns [`ServerConfigError`] when environment configuration cannot be parsed.
pub fn load_server_config(root_override: Option<&Path>) -> Result<ServerConfig, ServerConfigError> {
    let config = ServerConfig::from_env()?;
    let root_dir = resolve_root_dir(root_override, config.root_dir());
    ensure_directory_path_components_are_not_symlinked(&root_dir)
        .map_err(ServerConfigError::RootDir)?;
    Ok(config.with_root_dir(root_dir))
}

/// Resolves the effective deployment-root path after applying an optional CLI override.
///
/// # Errors
///
/// Returns [`ServerConfigError`] when environment configuration cannot be parsed.
pub fn effective_root(root_override: Option<&Path>) -> Result<PathBuf, ServerConfigError> {
    let config = load_server_config(root_override)?;
    Ok(config.root_dir().to_path_buf())
}

fn resolve_root_dir(root_override: Option<&Path>, configured_root: &Path) -> PathBuf {
    if let Some(root) = root_override {
        return root.to_path_buf();
    }

    if var_os("SHARDLINE_ROOT_DIR").is_some() {
        return configured_root.to_path_buf();
    }

    if let Ok(current_dir) = current_dir() {
        if let Some(root) = discover_project_root(&current_dir) {
            return root;
        }

        return default_project_root(&current_dir);
    }

    configured_root.to_path_buf()
}

fn discover_project_root(current_dir: &Path) -> Option<PathBuf> {
    for candidate in current_dir.ancestors() {
        let local_state = candidate.join(LOCAL_STATE_DIR);
        let local_data = local_state.join(LOCAL_DATA_DIR);
        if local_data.exists() || local_state.is_dir() {
            return Some(local_data);
        }

        if is_deployment_root(candidate) {
            return Some(candidate.to_path_buf());
        }
    }

    None
}

fn default_project_root(current_dir: &Path) -> PathBuf {
    current_dir.join(LOCAL_STATE_DIR).join(LOCAL_DATA_DIR)
}

fn is_deployment_root(path: &Path) -> bool {
    let has_object_storage = path.join("chunks").is_dir();
    let has_sqlite_metadata = path.join("metadata.sqlite3").is_file();
    let has_legacy_filesystem_metadata = path.join("files").is_dir()
        && path.join("file_versions").is_dir()
        && path.join("gc").is_dir();
    has_object_storage && (has_sqlite_metadata || has_legacy_filesystem_metadata)
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use std::{fs, path::PathBuf};

    use shardline_server::ServerConfigError;

    use super::{
        default_project_root, discover_project_root, is_deployment_root, resolve_root_dir,
    };

    #[test]
    fn deployment_root_detection_accepts_sqlite_metadata_layout() {
        let temp = tempfile::tempdir();
        assert!(temp.is_ok());
        let Ok(temp) = temp else {
            return;
        };

        assert!(!is_deployment_root(temp.path()));

        let chunks_dir = fs::create_dir(temp.path().join("chunks"));
        assert!(chunks_dir.is_ok());
        assert!(!is_deployment_root(temp.path()));

        let metadata = fs::write(temp.path().join("metadata.sqlite3"), []);
        assert!(metadata.is_ok());
        assert!(is_deployment_root(temp.path()));
    }

    #[test]
    fn deployment_root_detection_accepts_legacy_filesystem_metadata_layout() {
        let temp = tempfile::tempdir();
        assert!(temp.is_ok());
        let Ok(temp) = temp else {
            return;
        };

        let files_dir = fs::create_dir(temp.path().join("files"));
        assert!(files_dir.is_ok());
        let versions_dir = fs::create_dir(temp.path().join("file_versions"));
        assert!(versions_dir.is_ok());
        let gc_dir = fs::create_dir(temp.path().join("gc"));
        assert!(gc_dir.is_ok());
        let chunks_dir = fs::create_dir(temp.path().join("chunks"));
        assert!(chunks_dir.is_ok());

        assert!(is_deployment_root(temp.path()));
    }

    #[test]
    fn explicit_root_wins_over_cwd_detection() {
        let configured = PathBuf::from("/var/lib/shardline");
        let override_root = PathBuf::from("/tmp/shardline");

        let resolved = resolve_root_dir(Some(&override_root), &configured);

        assert_eq!(resolved, override_root);
    }

    #[test]
    fn project_state_directory_resolves_to_data_root() {
        let temp = tempfile::tempdir();
        assert!(temp.is_ok());
        let Ok(temp) = temp else {
            return;
        };
        let project = temp.path().join("asset-project");
        let nested = project.join("assets").join("characters");
        let nested_dir = fs::create_dir_all(&nested);
        assert!(nested_dir.is_ok());
        let state_dir = fs::create_dir(project.join(".shardline"));
        assert!(state_dir.is_ok());

        let resolved = discover_project_root(&nested);
        assert!(resolved.is_some());
        let Some(resolved) = resolved else {
            return;
        };

        assert_eq!(resolved, project.join(".shardline").join("data"));
    }

    #[test]
    fn default_project_root_uses_hidden_state_directory() {
        let project = PathBuf::from("/tmp/assets");

        assert_eq!(
            default_project_root(&project),
            PathBuf::from("/tmp/assets/.shardline/data")
        );
    }

    #[cfg(unix)]
    #[test]
    fn load_server_config_rejects_symlinked_root_override() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let target = sandbox.path().join("redirected-root");
        let create_target = fs::create_dir_all(&target);
        assert!(create_target.is_ok());
        let override_root = sandbox.path().join("root-link");
        let linked = symlink(&target, &override_root);
        assert!(linked.is_ok());

        let loaded = super::load_server_config(Some(&override_root));

        assert!(matches!(loaded, Err(ServerConfigError::RootDir(_))));
    }
}
