//! Credential resolution for the `sdx` token service: from environment
//! variables, token files, and the `shardline.toml` config file.
//!
//! Credentials are resolved in priority order:
//!
//! 1. `SHARDLINE_TOKEN` — opaque server bearer token
//! 2. `SHARDLINE_API_KEY` — provider bootstrap API key
//! 3. `SHARDLINE_TOKEN_FILE` — path to a file containing an opaque bearer token
//!
//! [`SdxConfig`] parses the `shardline.toml` config file: a `[default]`
//! section with endpoint/repository defaults and an `[auth]` section with
//! credential fields. Credential priority is CLI flags > env vars >
//! config `[auth]` section > none (see [`resolve_credential_from_config`]).

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use thiserror::Error;

/// Environment variable carrying an opaque server bearer token.
pub const SHARDLINE_TOKEN_ENV: &str = "SHARDLINE_TOKEN";
/// Environment variable carrying a provider bootstrap API key.
pub const SHARDLINE_API_KEY_ENV: &str = "SHARDLINE_API_KEY";
/// Environment variable carrying a path to a token file.
pub const SHARDLINE_TOKEN_FILE_ENV: &str = "SHARDLINE_TOKEN_FILE";
/// Environment variable carrying an explicit path to the config file.
pub const SHARDLINE_CONFIG_ENV: &str = "SHARDLINE_CONFIG";
/// Default config-file name relative to `$HOME/.config/shardline/`.
pub const CONFIG_FILE_NAME: &str = "shardline.toml";

/// A resolved credential for a Shardline Xet frontend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Credential {
    /// Opaque bearer token, sent as `Authorization: Bearer <token>`.
    Bearer(String),
    /// Provider bootstrap API key, sent as `X-Shardline-Provider-Key: <key>`.
    ProviderKey(String),
}

/// Failure to resolve a credential from the environment or config file.
#[derive(Debug, Error)]
pub enum CredentialResolutionError {
    /// A configured token file could not be read.
    #[error("failed to read token file {path:?}: {source}")]
    TokenFile {
        /// Path that could not be read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },
}

/// Failure to load or parse the `shardline.toml` config file.
#[derive(Debug, Error)]
pub enum SdxConfigError {
    /// The config file could not be read.
    #[error("failed to read config file {path:?}: {source}")]
    Io {
        /// Path that could not be read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },
    /// The config file could not be parsed as TOML.
    #[error("failed to parse config file {path:?}: {source}")]
    Parse {
        /// Path that could not be parsed.
        path: PathBuf,
        /// Underlying TOML error.
        #[source]
        source: toml::de::Error,
    },
    /// An explicitly requested config path was unusable (e.g. not a file).
    #[error("invalid config path: {0}")]
    InvalidPath(String),
}

/// `[default]` section of `shardline.toml`: repository / endpoint defaults.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
pub struct DefaultSection {
    /// Full `xet://` endpoint URL (overrides the other fields).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Provider name, used when `endpoint` is absent or lacks a provider.
    #[serde(default)]
    pub provider: Option<String>,
    /// Repository owner.
    #[serde(default)]
    pub owner: Option<String>,
    /// Repository name.
    #[serde(default)]
    pub repo: Option<String>,
    /// Revision / branch.
    #[serde(default)]
    pub revision: Option<String>,
}

/// `[auth]` section of `shardline.toml`: credential fields.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
pub struct AuthSection {
    /// Opaque bearer token (priority 1 within the config).
    #[serde(default)]
    pub token: Option<String>,
    /// Provider bootstrap API key (priority 2 within the config).
    #[serde(default)]
    pub api_key: Option<String>,
    /// Path to a token file (priority 3 within the config).
    #[serde(default)]
    pub token_file: Option<String>,
}

/// A parsed `shardline.toml` config file.
///
/// Unknown keys are tolerated (config must stay forward-compatible), and any
/// missing section/key defaults to `None`/empty.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
pub struct SdxConfig {
    /// `[default]` section.
    #[serde(default)]
    pub default: DefaultSection,
    /// `[auth]` section.
    #[serde(default)]
    pub auth: AuthSection,
}

impl SdxConfig {
    /// Loads the config file.
    ///
    /// When `path` is `Some`, it is treated as an explicit `--config` path and
    /// must exist and parse (a missing or invalid file is an error). When
    /// `path` is `None`, the `SHARDLINE_CONFIG` environment variable is
    /// consulted first, then the default `$HOME/.config/shardline/shardline.toml`;
    /// a missing file is not an error (`Ok(None)`), but a present-and-invalid
    /// file is.
    ///
    /// # Errors
    ///
    /// Returns [`SdxConfigError::Io`] when an explicit (or present) file cannot
    /// be read, [`SdxConfigError::Parse`] when it is not valid TOML, and
    /// [`SdxConfigError::InvalidPath`] for an unusable explicit path.
    pub fn load(path: Option<&Path>) -> Result<Option<SdxConfig>, SdxConfigError> {
        Self::load_inner(path, &|name| std::env::var(name).ok(), |name| {
            std::env::var_os(name)
        })
    }

    /// Loads the config file, injecting the environment lookups (so tests can
    /// control `SHARDLINE_CONFIG` and `$HOME` without mutating process state).
    fn load_inner(
        path: Option<&Path>,
        env: &dyn Fn(&str) -> Option<String>,
        home: impl Fn(&str) -> Option<std::ffi::OsString>,
    ) -> Result<Option<SdxConfig>, SdxConfigError> {
        let (resolved, explicit) = match path {
            Some(path) => (path.to_path_buf(), true),
            None => {
                let env_path = env(SHARDLINE_CONFIG_ENV).filter(|value| !value.trim().is_empty());
                match env_path {
                    Some(env_path) => (PathBuf::from(env_path), false),
                    None => {
                        let base = home("HOME").map(PathBuf::from);
                        match base {
                            Some(base) => (
                                base.join(".config")
                                    .join("shardline")
                                    .join(CONFIG_FILE_NAME),
                                false,
                            ),
                            None => return Ok(None),
                        }
                    }
                }
            }
        };
        if explicit && !resolved.is_file() {
            return Err(SdxConfigError::InvalidPath(resolved.display().to_string()));
        }
        let contents = match fs::read_to_string(&resolved) {
            Ok(contents) => contents,
            Err(source) if source.kind() == io::ErrorKind::NotFound && !explicit => {
                return Ok(None);
            }
            Err(source) => {
                return Err(SdxConfigError::Io {
                    path: resolved,
                    source,
                });
            }
        };
        let config: SdxConfig =
            toml::from_str(&contents).map_err(|source| SdxConfigError::Parse {
                path: resolved,
                source,
            })?;
        Ok(Some(config))
    }

    /// Loads the config file using the default discovery path
    /// (`SHARDLINE_CONFIG`, then `$HOME/.config/shardline/shardline.toml`).
    ///
    /// # Errors
    ///
    /// Returns [`SdxConfigError`] when a discovered config file is present but
    /// unreadable or invalid.
    pub fn load_default() -> Result<Option<SdxConfig>, SdxConfigError> {
        Self::load(None)
    }

    /// Returns the default config-file path
    /// (`$HOME/.config/shardline/shardline.toml`), or `None` when `$HOME` is
    /// unset.
    #[must_use]
    pub fn default_path() -> Option<PathBuf> {
        let home = std::env::var_os("HOME")?;
        Some(
            PathBuf::from(home)
                .join(".config")
                .join("shardline")
                .join(CONFIG_FILE_NAME),
        )
    }

    /// Returns the `[default].endpoint` value.
    #[must_use]
    pub fn endpoint(&self) -> Option<&str> {
        self.default.endpoint.as_deref()
    }

    /// Returns the `[default].provider` value.
    #[must_use]
    pub fn provider(&self) -> Option<&str> {
        self.default.provider.as_deref()
    }

    /// Returns the `[default].owner` value.
    #[must_use]
    pub fn owner(&self) -> Option<&str> {
        self.default.owner.as_deref()
    }

    /// Returns the `[default].repo` value.
    #[must_use]
    pub fn repo(&self) -> Option<&str> {
        self.default.repo.as_deref()
    }

    /// Returns the `[default].revision` value.
    #[must_use]
    pub fn revision(&self) -> Option<&str> {
        self.default.revision.as_deref()
    }

    /// Returns the `[auth]` section.
    #[must_use]
    pub const fn auth(&self) -> &AuthSection {
        &self.auth
    }
}

/// Reads an opaque bearer token from `path`, trimming surrounding whitespace.
///
/// # Errors
///
/// Returns an [`std::io::Error`] when the file cannot be read.
pub fn read_token_file(path: &Path) -> Result<String, io::Error> {
    let contents = fs::read_to_string(path)?;
    Ok(contents.trim().to_owned())
}

/// Resolves a credential from the environment, in priority order:
/// `SHARDLINE_TOKEN` > `SHARDLINE_API_KEY` > `SHARDLINE_TOKEN_FILE`.
///
/// `env` is injected so callers can substitute a test double; the production
/// lookup is `std::env::var`.
///
/// # Errors
///
/// Returns [`CredentialResolutionError::TokenFile`] when `SHARDLINE_TOKEN_FILE`
/// is set but the file cannot be read.
pub fn resolve_credential_from_env(
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<Option<Credential>, CredentialResolutionError> {
    if let Some(token) = env(SHARDLINE_TOKEN_ENV).filter(|value| !value.trim().is_empty()) {
        return Ok(Some(Credential::Bearer(token)));
    }
    if let Some(api_key) = env(SHARDLINE_API_KEY_ENV).filter(|value| !value.trim().is_empty()) {
        return Ok(Some(Credential::ProviderKey(api_key)));
    }
    if let Some(path) = env(SHARDLINE_TOKEN_FILE_ENV).filter(|value| !value.trim().is_empty()) {
        let contents = read_token_file(Path::new(&path)).map_err(|source| {
            CredentialResolutionError::TokenFile {
                path: PathBuf::from(&path),
                source,
            }
        })?;
        return Ok(Some(Credential::Bearer(contents)));
    }
    Ok(None)
}

/// Resolves a credential with the full priority order: env vars first (via
/// [`resolve_credential_from_env`]), then the config-file `[auth]` section
/// (`token` > `api_key` > `token_file`).
///
/// `env` is injected so callers can substitute a test double. CLI flags are
/// applied by the caller as explicit [`Auth`](crate::auth::Auth) builder args
/// and therefore outrank everything here.
///
/// # Errors
///
/// Returns [`CredentialResolutionError::TokenFile`] when a token-file path
/// (from the environment or the config) is set but the file cannot be read.
pub fn resolve_credential_from_config(
    config: Option<&SdxConfig>,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<Option<Credential>, CredentialResolutionError> {
    // Env vars win over the config file.
    if let Some(credential) = resolve_credential_from_env(env)? {
        return Ok(Some(credential));
    }
    let Some(config) = config else {
        return Ok(None);
    };
    let auth = &config.auth;
    if let Some(token) = auth
        .token
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return Ok(Some(Credential::Bearer(token.to_owned())));
    }
    if let Some(api_key) = auth
        .api_key
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return Ok(Some(Credential::ProviderKey(api_key.to_owned())));
    }
    if let Some(path) = auth
        .token_file
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        let contents = read_token_file(Path::new(path)).map_err(|source| {
            CredentialResolutionError::TokenFile {
                path: PathBuf::from(path),
                source,
            }
        })?;
        return Ok(Some(Credential::Bearer(contents)));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::Path};

    use super::{
        Credential, SHARDLINE_API_KEY_ENV, SHARDLINE_TOKEN_ENV, SHARDLINE_TOKEN_FILE_ENV,
        read_token_file, resolve_credential_from_env,
    };

    fn env_lookup(map: &HashMap<String, String>) -> impl Fn(&str) -> Option<String> + '_ {
        move |name| map.get(name).cloned()
    }

    #[test]
    fn resolve_prefers_shardline_token_over_api_key_and_token_file() {
        let mut map = HashMap::new();
        map.insert(SHARDLINE_TOKEN_ENV.to_owned(), "token".to_owned());
        map.insert(SHARDLINE_API_KEY_ENV.to_owned(), "key".to_owned());
        map.insert(
            SHARDLINE_TOKEN_FILE_ENV.to_owned(),
            "/tmp/not-used".to_owned(),
        );
        let credential = resolve_credential_from_env(&env_lookup(&map))
            .unwrap()
            .unwrap();
        assert_eq!(credential, Credential::Bearer("token".to_owned()));
    }

    #[test]
    fn resolve_falls_back_to_api_key_when_no_token() {
        let mut map = HashMap::new();
        map.insert(SHARDLINE_API_KEY_ENV.to_owned(), "key".to_owned());
        map.insert(
            SHARDLINE_TOKEN_FILE_ENV.to_owned(),
            "/tmp/not-used".to_owned(),
        );
        let credential = resolve_credential_from_env(&env_lookup(&map))
            .unwrap()
            .unwrap();
        assert_eq!(credential, Credential::ProviderKey("key".to_owned()));
    }

    #[test]
    fn resolve_falls_back_to_token_file_when_no_token_or_api_key() {
        let dir = tempfile::tempdir().unwrap();
        let token_file = dir.path().join("token");
        std::fs::write(&token_file, "  file-token\n").unwrap();
        let mut map = HashMap::new();
        map.insert(
            SHARDLINE_TOKEN_FILE_ENV.to_owned(),
            token_file.to_string_lossy().into_owned(),
        );
        let credential = resolve_credential_from_env(&env_lookup(&map))
            .unwrap()
            .unwrap();
        assert_eq!(credential, Credential::Bearer("file-token".to_owned()));
    }

    #[test]
    fn resolve_ignores_empty_values() {
        let mut map = HashMap::new();
        map.insert(SHARDLINE_TOKEN_ENV.to_owned(), String::new());
        map.insert(SHARDLINE_TOKEN_FILE_ENV.to_owned(), String::new());
        // Empty and whitespace-only values are treated as unset.
        let credential = resolve_credential_from_env(&env_lookup(&map)).unwrap();
        assert!(credential.is_none());

        // A non-empty api key is used when the token slot is empty.
        map.insert(SHARDLINE_API_KEY_ENV.to_owned(), "key".to_owned());
        let credential = resolve_credential_from_env(&env_lookup(&map))
            .unwrap()
            .unwrap();
        assert_eq!(credential, Credential::ProviderKey("key".to_owned()));
    }

    #[test]
    fn resolve_returns_none_when_all_unset() {
        let map = HashMap::new();
        let credential = resolve_credential_from_env(&env_lookup(&map)).unwrap();
        assert!(credential.is_none());
    }

    #[test]
    fn resolve_errors_when_token_file_unreadable() {
        let mut map = HashMap::new();
        map.insert(
            SHARDLINE_TOKEN_FILE_ENV.to_owned(),
            "/nonexistent/shardline-token".to_owned(),
        );
        let error = resolve_credential_from_env(&env_lookup(&map)).unwrap_err();
        assert!(error.to_string().contains("token file"));
    }

    #[test]
    fn read_token_file_trims_whitespace() {
        let dir = tempfile::tempdir().unwrap();
        let token_file = dir.path().join("token");
        std::fs::write(&token_file, "\n  opaque-token \n").unwrap();
        let contents = read_token_file(&token_file).unwrap();
        assert_eq!(contents, "opaque-token");
    }

    #[test]
    fn read_token_file_missing_file_errors() {
        let result = read_token_file(Path::new("/nonexistent/shardline-token"));
        assert!(result.is_err());
    }

    // ── M6a: shardline.toml parsing ────────────────────────────────────────

    use super::{
        AuthSection, DefaultSection, SdxConfig, SdxConfigError, resolve_credential_from_config,
    };

    const VALID_TOML: &str = r#"
[default]
endpoint = "xet://127.0.0.1:8080/generic/myorg/myrepo/main"
provider = "generic"
owner = "myorg"
repo = "myrepo"
revision = "main"

[auth]
token = "opaque-token"
api_key = "bootstrap-key"
token_file = "/some/path"
"#;

    /// Runs `load(None)` with injected `SHARDLINE_CONFIG` / `HOME` lookups.
    fn load_discovered(
        env: &HashMap<String, String>,
        home: Option<&Path>,
    ) -> Result<Option<SdxConfig>, SdxConfigError> {
        let home_os = home.map(std::ffi::OsString::from);
        SdxConfig::load_inner(None, &env_lookup(env), move |_| home_os.clone())
    }

    #[test]
    fn parse_all_sections() {
        let config: SdxConfig = toml::from_str(VALID_TOML).unwrap();
        assert_eq!(
            config.endpoint(),
            Some("xet://127.0.0.1:8080/generic/myorg/myrepo/main")
        );
        assert_eq!(config.provider(), Some("generic"));
        assert_eq!(config.owner(), Some("myorg"));
        assert_eq!(config.repo(), Some("myrepo"));
        assert_eq!(config.revision(), Some("main"));
        assert_eq!(config.auth().token.as_deref(), Some("opaque-token"));
        assert_eq!(config.auth().api_key.as_deref(), Some("bootstrap-key"));
        assert_eq!(config.auth().token_file.as_deref(), Some("/some/path"));
    }

    #[test]
    fn unknown_keys_tolerated_and_missing_sections_default() {
        let config: SdxConfig = toml::from_str(
            r#"
[default]
owner = "o"
repo = "r"
unknown_future_key = "ignored"

[extra_section]
whatever = true
"#,
        )
        .unwrap();
        assert_eq!(config.owner(), Some("o"));
        assert_eq!(config.auth().token, None);
        assert_eq!(config.provider(), None);
    }

    #[test]
    fn empty_config_defaults() {
        let config: SdxConfig = toml::from_str("").unwrap();
        assert_eq!(config.endpoint(), None);
        assert_eq!(config.auth().token, None);
    }

    #[test]
    fn load_explicit_path_parses() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shardline.toml");
        std::fs::write(&path, VALID_TOML).unwrap();
        let config = SdxConfig::load(Some(&path)).unwrap().unwrap();
        assert_eq!(config.repo(), Some("myrepo"));
    }

    #[test]
    fn load_explicit_missing_path_is_error() {
        let err = SdxConfig::load(Some(Path::new("/nonexistent/shardline.toml"))).unwrap_err();
        assert!(matches!(err, SdxConfigError::InvalidPath(_)));
    }

    #[test]
    fn load_explicit_parse_error_is_typed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.toml");
        std::fs::write(&path, "not [valid toml ===").unwrap();
        let err = SdxConfig::load(Some(&path)).unwrap_err();
        assert!(matches!(err, SdxConfigError::Parse { .. }));
    }

    #[test]
    fn load_none_uses_shardline_config_env() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cfg.toml");
        std::fs::write(&path, VALID_TOML).unwrap();
        let mut env = HashMap::new();
        env.insert(
            super::SHARDLINE_CONFIG_ENV.to_owned(),
            path.to_string_lossy().into_owned(),
        );
        let config = load_discovered(&env, None).unwrap().unwrap();
        assert_eq!(config.repo(), Some("myrepo"));
    }

    #[test]
    fn load_none_missing_default_is_ok_none() {
        let dir = tempfile::tempdir().unwrap();
        let config = load_discovered(&HashMap::new(), Some(dir.path())).unwrap();
        assert!(config.is_none());
    }

    #[test]
    fn load_none_reads_default_home_path() {
        let dir = tempfile::tempdir().unwrap();
        let config_dir = dir.path().join(".config").join("shardline");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(config_dir.join(super::CONFIG_FILE_NAME), VALID_TOML).unwrap();
        let config = load_discovered(&HashMap::new(), Some(dir.path()))
            .unwrap()
            .unwrap();
        assert_eq!(config.repo(), Some("myrepo"));
    }

    #[test]
    fn credential_priority_env_beats_config() {
        let mut env = HashMap::new();
        env.insert(
            super::SHARDLINE_API_KEY_ENV.to_owned(),
            "env-key".to_owned(),
        );
        let config: SdxConfig =
            toml::from_str("[auth]\ntoken = \"config-token\"\napi_key = \"config-key\"\n").unwrap();
        let credential = resolve_credential_from_config(Some(&config), &env_lookup(&env))
            .unwrap()
            .unwrap();
        // Env (api_key) wins over the config token.
        assert_eq!(credential, Credential::ProviderKey("env-key".to_owned()));
    }

    #[test]
    fn credential_priority_token_over_api_key_over_token_file_within_config() {
        let env = HashMap::new();
        let config: SdxConfig = toml::from_str(
            "[auth]\ntoken = \"cfg-token\"\napi_key = \"cfg-key\"\ntoken_file = \"/nope\"\n",
        )
        .unwrap();
        let credential = resolve_credential_from_config(Some(&config), &env_lookup(&env))
            .unwrap()
            .unwrap();
        assert_eq!(credential, Credential::Bearer("cfg-token".to_owned()));

        let config: SdxConfig =
            toml::from_str("[auth]\napi_key = \"cfg-key\"\ntoken_file = \"/nope\"\n").unwrap();
        let credential = resolve_credential_from_config(Some(&config), &env_lookup(&env))
            .unwrap()
            .unwrap();
        assert_eq!(credential, Credential::ProviderKey("cfg-key".to_owned()));
    }

    #[test]
    fn credential_priority_config_token_file_read_and_trim() {
        let dir = tempfile::tempdir().unwrap();
        let token_file = dir.path().join("token");
        std::fs::write(&token_file, "  file-token \n").unwrap();
        let env = HashMap::new();
        let config: SdxConfig = toml::from_str(&format!(
            "[auth]\ntoken_file = \"{}\"\n",
            token_file.display()
        ))
        .unwrap();
        let credential = resolve_credential_from_config(Some(&config), &env_lookup(&env))
            .unwrap()
            .unwrap();
        assert_eq!(credential, Credential::Bearer("file-token".to_owned()));
    }

    #[test]
    fn credential_priority_no_source_returns_none() {
        let env = HashMap::new();
        let config = SdxConfig::default();
        let credential = resolve_credential_from_config(Some(&config), &env_lookup(&env)).unwrap();
        assert!(credential.is_none());
    }

    #[test]
    fn config_sections_are_comparable() {
        let a = DefaultSection {
            owner: Some("o".to_owned()),
            ..Default::default()
        };
        assert_eq!(a, a.clone());
        let auth = AuthSection {
            token: Some("t".to_owned()),
            ..Default::default()
        };
        assert_eq!(auth, auth.clone());
    }
}
