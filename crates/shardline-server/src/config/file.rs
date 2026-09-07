use std::{
    fs::{self, OpenOptions},
    os::unix::fs::OpenOptionsExt,
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
#[serde(deny_unknown_fields)]
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

/// Resolves a config file path to its canonical location, verifying the
/// resolved target is a regular file that stays inside `path`'s parent
/// directory.
///
/// Kubernetes projects ConfigMap and Secret volume mounts through a `..data`
/// indirection: `/etc/shardline/shardline.toml` is a symlink to
/// `../..data/shardline.toml`, and `..data` is itself a symlink to a
/// versioned directory. Those symlinks never escape the mount's parent
/// directory, so they are legitimate. A symlink whose canonical target lands
/// outside the parent (a config-manipulation / symlink-swap attack) is
/// rejected. The caller must still open with `O_NOFOLLOW` on the resolved
/// path to close the TOCTOU window between resolve and read.
fn resolve_within_parent(path: &Path) -> Result<PathBuf, String> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let canonical_parent = fs::canonicalize(parent)
        .map_err(|e| format!("failed to resolve config dir {}: {e}", parent.display()))?;
    let resolved = fs::canonicalize(path)
        .map_err(|e| format!("failed to resolve config file {}: {e}", path.display()))?;
    if !resolved.starts_with(&canonical_parent) {
        return Err(format!(
            "config file {} resolves outside its directory; refusing symlinked config",
            path.display()
        ));
    }
    Ok(resolved)
}

/// Resolves the active shardline.toml content as a string.
/// Returns `None` when neither an explicit `--config` path nor any
/// auto-detected candidate exists.
pub(crate) fn resolve_config_content(explicit: Option<&Path>) -> Result<Option<String>, String> {
    if let Some(path) = explicit {
        let resolved = resolve_within_parent(path)?;
        // Use O_NOFOLLOW to prevent TOCTOU symlink swap between resolve and read.
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&resolved)
            .map_err(|e| format!("failed to open config file {}: {e}", resolved.display()))?;
        let mut content = String::new();
        std::io::Read::read_to_string(&mut file, &mut content)
            .map_err(|e| format!("failed to read config file {}: {e}", resolved.display()))?;
        return Ok(Some(content));
    }
    for candidate in CONFIG_FILE_CANDIDATES {
        let expanded = expand_tilde(candidate);
        if expanded.exists() {
            let resolved = match resolve_within_parent(&expanded) {
                Ok(resolved) => resolved,
                Err(error) => {
                    tracing::warn!("skipping config candidate {}: {error}", expanded.display());
                    continue;
                }
            };
            // Use O_NOFOLLOW to prevent TOCTOU symlink swap.
            let mut file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&resolved)
                .map_err(|e| format!("failed to open config file {}: {e}", resolved.display()))?;
            let mut content = String::new();
            std::io::Read::read_to_string(&mut file, &mut content)
                .map_err(|e| format!("failed to read config file {}: {e}", resolved.display()))?;
            return Ok(Some(content));
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

    #[cfg(unix)]
    #[test]
    fn test_resolve_config_content_accepts_projected_symlink_within_directory() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("..data");
        std::fs::create_dir(&data_dir).unwrap();
        let target = data_dir.join("shardline.toml");
        std::fs::write(
            &target,
            "[server]\nbind_addr = \"0.0.0.0:8080\"\npublic_base_url = \"http://shardline\"\n",
        )
        .unwrap();
        // Kubernetes ConfigMap projection: /etc/shardline/shardline.toml ->
        // ../..data/shardline.toml (..data is a symlink to a versioned dir).
        let link = temp.path().join("shardline.toml");
        symlink(Path::new("..data").join("shardline.toml"), &link).unwrap();

        let content = resolve_config_content(Some(&link)).unwrap();
        assert!(content.is_some());
        assert!(content.unwrap().contains("0.0.0.0:8080"));
    }

    #[cfg(unix)]
    #[test]
    fn test_resolve_config_content_rejects_symlink_escaping_directory() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            outside.path(),
            "[server]\nbind_addr = \"0.0.0.0:9999\"\npublic_base_url = \"http://evil\"\n",
        )
        .unwrap();
        let link = temp.path().join("shardline.toml");
        symlink(outside.path(), &link).unwrap();

        let result = resolve_config_content(Some(&link));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("outside its directory"));
    }
}
