//! Environment-variable credential resolution and token-file loading for the
//! `sdx` token service (M1).
//!
//! Mirrors the credential priority order from `docs/SDX_PLAN.md` §5.2 and the
//! Authentication section of `docs/XET_NATIVE_CLI.md`:
//!
//! 1. `SHARDLINE_TOKEN` — opaque server bearer token
//! 2. `SHARDLINE_API_KEY` — provider bootstrap API key
//! 3. `SHARDLINE_TOKEN_FILE` — path to a file containing an opaque bearer token
//!
//! Config-file `[auth]` section parsing is M6 CLI scope and is intentionally
//! not handled here.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use thiserror::Error;

/// Environment variable carrying an opaque server bearer token.
pub const SHARDLINE_TOKEN_ENV: &str = "SHARDLINE_TOKEN";
/// Environment variable carrying a provider bootstrap API key.
pub const SHARDLINE_API_KEY_ENV: &str = "SHARDLINE_API_KEY";
/// Environment variable carrying a path to a token file.
pub const SHARDLINE_TOKEN_FILE_ENV: &str = "SHARDLINE_TOKEN_FILE";

/// A resolved credential for a Shardline Xet frontend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Credential {
    /// Opaque bearer token, sent as `Authorization: Bearer <token>`.
    Bearer(String),
    /// Provider bootstrap API key, sent as `X-Shardline-Provider-Key: <key>`.
    ProviderKey(String),
}

/// Failure to resolve a credential from the environment.
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

/// Reads an opaque bearer token from `path`, trimming surrounding whitespace.
///
/// # Errors
///
/// Returns an [`std::io::Error`] when the file cannot be read.
pub fn read_token_file(path: &Path) -> Result<String, io::Error> {
    let contents = fs::read_to_string(path)?;
    Ok(contents.trim().to_owned())
}

/// Resolves a credential from the environment in the §5.2 priority order:
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
}
