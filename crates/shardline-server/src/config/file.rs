use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

/// Top-level TOML configuration document.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardlineTomlConfig {
    #[serde(default)]
    pub server: Option<ServerSection>,
    #[serde(default)]
    pub storage: Option<StorageSection>,
    #[serde(default)]
    pub index: Option<IndexSection>,
    #[serde(default)]
    pub cache: Option<CacheSection>,
    #[serde(default)]
    pub auth: Option<AuthSection>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerSection {
    pub bind_addr: Option<String>,
    pub public_base_url: Option<String>,
    pub server_role: Option<String>,
    pub frontends: Option<Vec<String>>,
    pub root_dir: Option<String>,
    pub max_request_body_bytes: Option<u64>,
    pub chunk_size: Option<String>,
    pub chunk_size_bytes: Option<u64>,
    pub upload_max_in_flight_chunks: Option<u64>,
    pub transfer_max_in_flight_chunks: Option<u64>,
    /// Path to the dedicated read-only administration API bearer token.
    /// Secret values are intentionally not accepted inline in TOML.
    pub admin_read_token_path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageSection {
    pub adapter: Option<String>,
    pub s3: Option<S3Section>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Section {
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub allow_http: Option<bool>,
    pub virtual_hosted_style: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexSection {
    pub postgres_url: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheSection {
    pub redis_url: Option<String>,
    pub adapter: Option<String>,
    pub ttl_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ed25519Section {
    pub private_key_path: Option<String>,
    pub public_key_path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthSection {
    pub provider: Option<String>,
    pub token_signing_key_path: Option<String>,
    pub provider_api_key_path: Option<String>,
    pub provider_token_issuer: Option<String>,
    pub provider_token_ttl_seconds: Option<u64>,
    pub jwks: Option<JwksSection>,
    pub oidc: Option<OidcSection>,
    pub ed25519: Option<Ed25519Section>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JwksSection {
    pub url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct OidcSection {
    pub issuer_url: Option<String>,
    pub audience: Option<String>,
    pub jwks_host_allowlist: Option<Vec<String>>,
}

/// Standard paths checked for shardline.toml, in priority order (first found wins).
const CONFIG_FILE_CANDIDATES: &[&str] = &[
    "shardline.toml",
    "~/.config/shardline/shardline.toml",
    "/etc/shardline/shardline.toml",
];

/// Expands a leading `~/` to the user's home directory.
fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/")
        && let Ok(home) = std::env::var("HOME")
    {
        return PathBuf::from(home).join(rest);
    }
    PathBuf::from(path)
}

/// Resolves the active shardline.toml path.
/// Returns `None` when neither an explicit `--config` path nor any
/// auto-detected candidate exists.
/// Resolves the active shardline.toml content as a string.
/// Returns `None` when neither an explicit `--config` path nor any
/// auto-detected candidate exists.
pub(crate) fn resolve_config_content(explicit: Option<&Path>) -> Result<Option<String>, String> {
    if let Some(path) = explicit {
        return fs::read_to_string(path)
            .map(Some)
            .map_err(|error| format!("failed to read config file {}: {error}", path.display()));
    }
    for candidate in CONFIG_FILE_CANDIDATES {
        let expanded = expand_tilde(candidate);
        if expanded.exists() {
            return fs::read_to_string(&expanded).map(Some).map_err(|error| {
                format!("failed to read config file {}: {error}", expanded.display())
            });
        }
    }
    Ok(None)
}

/// Parses a shardline.toml file at the given path (or auto-detected) and
/// returns the deserialized config struct.
///
/// # Examples
///
/// ```
/// use shardline_server::load_toml_config;
///
/// let dir = tempfile::tempdir()?;
/// let path = dir.path().join("shardline.toml");
/// std::fs::write(
///     &path,
///     "[server]\nbind_addr = \"0.0.0.0:9999\"\npublic_base_url = \"http://localhost:9999\"\n",
/// )?;
///
/// let parsed = load_toml_config(Some(&path))?;
/// assert!(parsed.is_some(), "the provided config file was parsed");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// When `config_path` is `None`, the standard candidate paths
/// (`shardline.toml`, `~/.config/shardline/shardline.toml`, and
/// `/etc/shardline/shardline.toml`) are probed in order and the first existing
/// file is used; `Ok(None)` is returned when none of them exist.
///
/// # Errors
///
/// Returns an error message string when the file exists but cannot be
/// read or parsed as valid TOML.
pub fn load_toml_config(config_path: Option<&Path>) -> Result<Option<ShardlineTomlConfig>, String> {
    let Some(content) = resolve_config_content(config_path)? else {
        return Ok(None);
    };
    let config: ShardlineTomlConfig =
        toml::from_str(&content).map_err(|e| format!("TOML parse error: {e}"))?;
    Ok(Some(config))
}

/// Parses the server TOML schema without touching the filesystem.
///
/// This narrow hook keeps the production parser under continuous fuzzing
/// without exposing its internal representation as public API.
///
/// # Errors
///
/// Returns the TOML parser error when the document does not satisfy the strict
/// server configuration schema.
#[cfg(feature = "fuzzing")]
pub fn parse_toml_config_for_fuzzing(input: &str) -> Result<(), String> {
    toml::from_str::<ShardlineTomlConfig>(input)
        .map(|_config| ())
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    proptest! {
        #[test]
        fn arbitrary_toml_parsing_is_deterministic_and_never_panics(input in any::<String>()) {
            let first = toml::from_str::<ShardlineTomlConfig>(&input);
            let second = toml::from_str::<ShardlineTomlConfig>(&input);
            prop_assert_eq!(format!("{first:?}"), format!("{second:?}"));
        }
    }
    #[test]
    fn test_expand_tilde_no_tilde() {
        let result = expand_tilde("/etc/shardline/shardline.toml");
        assert_eq!(result, PathBuf::from("/etc/shardline/shardline.toml"));
    }

    #[test]
    fn test_expand_tilde_with_home() {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_owned());
        let result = expand_tilde("~/.config/shardline/shardline.toml");
        assert_eq!(
            result,
            PathBuf::from(home).join(".config/shardline/shardline.toml")
        );
    }

    #[test]
    fn test_resolve_config_content_explicit() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "[server]\nbind_addr = \"0.0.0.0:9999\"").unwrap();
        let content = resolve_config_content(Some(file.path())).unwrap();
        assert!(content.is_some());
        assert!(content.unwrap().contains("0.0.0.0:9999"));
    }

    #[test]
    fn test_resolve_config_content_nonexistent() {
        let content = resolve_config_content(Some(Path::new("/nonexistent/path/shardline.toml")));
        assert!(content.is_err());
    }

    #[test]
    fn test_resolve_config_content_auto_detect() {
        // When no explicit path is given, auto-detection tries candidates.
        // In a test environment none should exist, so returns None.
        let content = resolve_config_content(None).unwrap();
        assert!(content.is_none());
    }
}
