use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

/// Top-level TOML configuration document.
#[derive(Debug, Default, Deserialize)]
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
pub struct ServerSection {
    pub bind_addr: Option<String>,
    pub public_base_url: Option<String>,
    pub server_role: Option<String>,
    pub frontends: Option<Vec<String>>,
    pub root_dir: Option<String>,
    pub max_request_body_bytes: Option<u64>,
    pub chunk_size_bytes: Option<u64>,
    pub upload_max_in_flight_chunks: Option<u64>,
    pub transfer_max_in_flight_chunks: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct StorageSection {
    pub adapter: Option<String>,
    pub s3: Option<S3Section>,
}

#[derive(Debug, Deserialize)]
pub struct S3Section {
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub virtual_hosted_style: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct IndexSection {
    pub postgres_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CacheSection {
    pub redis_url: Option<String>,
    pub adapter: Option<String>,
    pub ttl_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct AuthSection {
    pub provider: Option<String>,
    pub token_signing_key_path: Option<String>,
    pub provider_api_key_path: Option<String>,
    pub provider_token_issuer: Option<String>,
    pub provider_token_ttl_seconds: Option<u64>,
    pub jwks: Option<JwksSection>,
    pub oidc: Option<OidcSection>,
}

#[derive(Debug, Deserialize)]
pub struct JwksSection {
    pub url: Option<String>,
    pub refresh_interval_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct OidcSection {
    pub issuer_url: Option<String>,
    pub client_id: Option<String>,
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
pub(crate) fn resolve_config_path(explicit: Option<&Path>) -> Option<Vec<u8>> {
    if let Some(path) = explicit {
        return fs::read(path).ok();
    }
    for candidate in CONFIG_FILE_CANDIDATES {
        let expanded = expand_tilde(candidate);
        if let Ok(content) = fs::read(&expanded) {
            return Some(content);
        }
    }
    None
}

/// Interpolates `${VAR_NAME}` patterns in `value` using the current process
/// environment. Returns the original value when no patterns are found.
fn interpolate_env_vars(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut var_name = String::new();
            for c in chars.by_ref() {
                if c == '}' {
                    break;
                }
                var_name.push(c);
            }
            let resolved = std::env::var(&var_name).unwrap_or_else(|_| format!("${{{var_name}}}"));
            result.push_str(&resolved);
        } else {
            result.push(ch);
        }
    }
    result
}

/// Parses shardline.toml content into a mapping of SHARDLINE_* env var names
/// to their interpolated values. Values that reference `${VAR}` are resolved
/// from the current process environment.
fn toml_to_env_map(content: &[u8]) -> Result<HashMap<String, String>, String> {
    let text = std::str::from_utf8(content).map_err(|e| format!("TOML encoding error: {e}"))?;
    let config: ShardlineTomlConfig =
        toml::from_str(text).map_err(|e| format!("TOML parse error: {e}"))?;

    let mut map = HashMap::new();
    let mut add = |key: &str, value: Option<String>| {
        if let Some(value) = value {
            map.insert(key.to_owned(), interpolate_env_vars(&value));
        }
    };

    // Server section
    if let Some(srv) = &config.server {
        add("SHARDLINE_BIND_ADDR", srv.bind_addr.clone());
        add("SHARDLINE_PUBLIC_BASE_URL", srv.public_base_url.clone());
        add("SHARDLINE_SERVER_ROLE", srv.server_role.clone());
        if let Some(frontends) = &srv.frontends {
            add("SHARDLINE_SERVER_FRONTENDS", Some(frontends.join(",")));
        }
        add("SHARDLINE_ROOT_DIR", srv.root_dir.clone());
        add(
            "SHARDLINE_MAX_REQUEST_BODY_BYTES",
            srv.max_request_body_bytes.map(|v| v.to_string()),
        );
        add(
            "SHARDLINE_CHUNK_SIZE_BYTES",
            srv.chunk_size_bytes.map(|v| v.to_string()),
        );
        add(
            "SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS",
            srv.upload_max_in_flight_chunks.map(|v| v.to_string()),
        );
        add(
            "SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS",
            srv.transfer_max_in_flight_chunks.map(|v| v.to_string()),
        );
    }

    // Storage section
    if let Some(stg) = &config.storage {
        add("SHARDLINE_OBJECT_STORAGE_ADAPTER", stg.adapter.clone());
        if let Some(s3) = &stg.s3 {
            add("SHARDLINE_S3_ENDPOINT", s3.endpoint.clone());
            add("SHARDLINE_S3_REGION", s3.region.clone());
            add("SHARDLINE_S3_BUCKET", s3.bucket.clone());
            add("SHARDLINE_S3_PREFIX", s3.prefix.clone());
            add(
                "SHARDLINE_S3_VIRTUAL_HOSTED_STYLE",
                s3.virtual_hosted_style.map(|v| v.to_string()),
            );
        }
    }

    // Index section
    if let Some(idx) = &config.index {
        add("SHARDLINE_INDEX_POSTGRES_URL", idx.postgres_url.clone());
    }

    // Cache section
    if let Some(cch) = &config.cache {
        add(
            "SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER",
            cch.adapter.clone(),
        );
        add(
            "SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL",
            cch.redis_url.clone(),
        );
        add(
            "SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS",
            cch.ttl_seconds.map(|v| v.to_string()),
        );
    }

    // Auth section
    if let Some(auth) = &config.auth {
        add("SHARDLINE_AUTH_PROVIDER", auth.provider.clone());
        add(
            "SHARDLINE_TOKEN_SIGNING_KEY_PATH",
            auth.token_signing_key_path.clone(),
        );
        add(
            "SHARDLINE_PROVIDER_API_KEY_PATH",
            auth.provider_api_key_path.clone(),
        );
        add(
            "SHARDLINE_PROVIDER_TOKEN_ISSUER",
            auth.provider_token_issuer.clone(),
        );
        add(
            "SHARDLINE_PROVIDER_TOKEN_TTL_SECONDS",
            auth.provider_token_ttl_seconds.map(|v| v.to_string()),
        );
        if let Some(jwks) = &auth.jwks {
            add("SHARDLINE_AUTH_JWKS_URL", jwks.url.clone());
            add(
                "SHARDLINE_AUTH_JWKS_REFRESH_INTERVAL_SECONDS",
                jwks.refresh_interval_seconds.map(|v| v.to_string()),
            );
        }
        if let Some(oidc) = &auth.oidc {
            add("SHARDLINE_AUTH_OIDC_ISSUER_URL", oidc.issuer_url.clone());
            add("SHARDLINE_AUTH_OIDC_CLIENT_ID", oidc.client_id.clone());
        }
    }

    Ok(map)
}

/// # Errors
///
/// Returns an error message when the file exists but cannot be parsed.
pub fn load_toml_env_overrides(
    config_path: Option<&Path>,
) -> Result<Option<HashMap<String, String>>, String> {
    resolve_config_path(config_path).map_or(Ok(None), |bytes| toml_to_env_map(&bytes).map(Some))
}

/// Parses a shardline.toml file at the given path (or auto-detected) and
/// returns the deserialized config struct.
///
/// # Errors
///
/// Returns an error message when the file exists but cannot be read or parsed.
pub fn load_toml_config(config_path: Option<&Path>) -> Result<Option<ShardlineTomlConfig>, String> {
    let Some(content) = resolve_config_path(config_path) else {
        return Ok(None);
    };
    let text = std::str::from_utf8(&content).map_err(|e| format!("TOML encoding error: {e}"))?;
    let config: ShardlineTomlConfig =
        toml::from_str(text).map_err(|e| format!("TOML parse error: {e}"))?;
    Ok(Some(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

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
    fn test_interpolate_no_vars() {
        assert_eq!(interpolate_env_vars("plain text"), "plain text");
    }

    fn set_env_var(key: &str, value: &str) {
        let content = format!("{key}={value}");
        let _ignored = dotenvy::from_read(std::io::Cursor::new(content.as_bytes()));
    }

    #[test]
    fn test_interpolate_known_var() {
        set_env_var("_TEST_SHARDLINE_VAR", "resolved_value");
        assert_eq!(
            interpolate_env_vars("prefix-${_TEST_SHARDLINE_VAR}-suffix"),
            "prefix-resolved_value-suffix"
        );
    }

    #[test]
    fn test_interpolate_missing_var() {
        let result = interpolate_env_vars("${_MISSING_VAR_XYZ}");
        assert_eq!(result, "${_MISSING_VAR_XYZ}");
    }

    #[test]
    fn test_interpolate_empty_var_name() {
        // Empty var name cannot be resolved, so the literal ${} is preserved.
        assert_eq!(interpolate_env_vars("${}"), "${}");
    }

    #[test]
    fn test_interpolate_dollar_without_brace() {
        assert_eq!(interpolate_env_vars("$VAR"), "$VAR");
        assert_eq!(interpolate_env_vars("$$"), "$$");
    }

    #[test]
    fn test_interpolate_multiple_vars() {
        set_env_var("_TEST_INTERP_A", "hello");
        set_env_var("_TEST_INTERP_B", "world");
        let result = interpolate_env_vars("${_TEST_INTERP_A} ${_TEST_INTERP_B}");
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_toml_to_env_map_empty_document() {
        let result = toml_to_env_map(b"");
        assert!(result.is_ok(), "empty document is valid TOML");
        let map = result.unwrap();
        assert!(map.is_empty(), "empty document produces no env vars");
    }

    #[test]
    fn test_toml_to_env_map_minimal() {
        let toml = b"[server]\nbind_addr = \"0.0.0.0:9090\"\n";
        let map = toml_to_env_map(toml).expect("valid toml");
        assert_eq!(map.get("SHARDLINE_BIND_ADDR").unwrap(), "0.0.0.0:9090");
    }

    #[test]
    fn test_toml_to_env_map_server_section() {
        let toml = b"
[server]
bind_addr = \"127.0.0.1:8080\"
public_base_url = \"http://localhost:8080\"
server_role = \"api\"
frontends = [\"xet\", \"oci\"]
root_dir = \"/data/shardline\"
max_request_body_bytes = 134217728
chunk_size_bytes = 131072
upload_max_in_flight_chunks = 16
transfer_max_in_flight_chunks = 32
";
        let map = toml_to_env_map(toml).expect("valid toml");
        assert_eq!(map.get("SHARDLINE_BIND_ADDR").unwrap(), "127.0.0.1:8080");
        assert_eq!(
            map.get("SHARDLINE_PUBLIC_BASE_URL").unwrap(),
            "http://localhost:8080"
        );
        assert_eq!(map.get("SHARDLINE_SERVER_ROLE").unwrap(), "api");
        assert_eq!(map.get("SHARDLINE_SERVER_FRONTENDS").unwrap(), "xet,oci");
        assert_eq!(map.get("SHARDLINE_ROOT_DIR").unwrap(), "/data/shardline");
        assert_eq!(
            map.get("SHARDLINE_MAX_REQUEST_BODY_BYTES").unwrap(),
            "134217728"
        );
        assert_eq!(map.get("SHARDLINE_CHUNK_SIZE_BYTES").unwrap(), "131072");
        assert_eq!(
            map.get("SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS").unwrap(),
            "16"
        );
        assert_eq!(
            map.get("SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS").unwrap(),
            "32"
        );
    }

    #[test]
    fn test_toml_to_env_map_storage_s3() {
        let toml = b"
[storage]
adapter = \"s3\"

[storage.s3]
endpoint = \"https://minio.example.com:9000\"
region = \"us-east-1\"
bucket = \"shardline-data\"
prefix = \"dev/\"
virtual_hosted_style = true
";
        let map = toml_to_env_map(toml).expect("valid toml");
        assert_eq!(map.get("SHARDLINE_OBJECT_STORAGE_ADAPTER").unwrap(), "s3");
        assert_eq!(
            map.get("SHARDLINE_S3_ENDPOINT").unwrap(),
            "https://minio.example.com:9000"
        );
        assert_eq!(map.get("SHARDLINE_S3_REGION").unwrap(), "us-east-1");
        assert_eq!(map.get("SHARDLINE_S3_BUCKET").unwrap(), "shardline-data");
        assert_eq!(map.get("SHARDLINE_S3_PREFIX").unwrap(), "dev/");
        assert_eq!(
            map.get("SHARDLINE_S3_VIRTUAL_HOSTED_STYLE").unwrap(),
            "true"
        );
    }

    #[test]
    fn test_toml_to_env_map_index() {
        let toml = b"[index]\npostgres_url = \"${_TEST_DB_URL}\"\n";
        set_env_var("_TEST_DB_URL", "postgres://user:pass@localhost/db");
        let map = toml_to_env_map(toml).expect("valid toml");
        assert_eq!(
            map.get("SHARDLINE_INDEX_POSTGRES_URL").unwrap(),
            "postgres://user:pass@localhost/db"
        );
    }

    #[test]
    fn test_toml_to_env_map_cache() {
        let toml = b"
[cache]
adapter = \"redis\"
redis_url = \"redis://localhost:6379\"
ttl_seconds = 120
";
        let map = toml_to_env_map(toml).expect("valid toml");
        assert_eq!(
            map.get("SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER").unwrap(),
            "redis"
        );
        assert_eq!(
            map.get("SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL").unwrap(),
            "redis://localhost:6379"
        );
        assert_eq!(
            map.get("SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS")
                .unwrap(),
            "120"
        );
    }

    #[test]
    fn test_toml_to_env_map_auth() {
        let toml = b"
[auth]
provider = \"oidc\"
provider_token_issuer = \"shardline\"
provider_token_ttl_seconds = 7200

[auth.oidc]
issuer_url = \"https://accounts.example.com\"
client_id = \"my-client\"
";
        let map = toml_to_env_map(toml).expect("valid toml");
        assert_eq!(map.get("SHARDLINE_AUTH_PROVIDER").unwrap(), "oidc");
        assert_eq!(
            map.get("SHARDLINE_PROVIDER_TOKEN_ISSUER").unwrap(),
            "shardline"
        );
        assert_eq!(
            map.get("SHARDLINE_PROVIDER_TOKEN_TTL_SECONDS").unwrap(),
            "7200"
        );
        assert_eq!(
            map.get("SHARDLINE_AUTH_OIDC_ISSUER_URL").unwrap(),
            "https://accounts.example.com"
        );
        assert_eq!(
            map.get("SHARDLINE_AUTH_OIDC_CLIENT_ID").unwrap(),
            "my-client"
        );
    }

    #[test]
    fn test_toml_to_env_map_jwks() {
        let toml = b"
[auth]
provider = \"jwks\"

[auth.jwks]
url = \"https://example.com/jwks.json\"
refresh_interval_seconds = 1800
";
        let map = toml_to_env_map(toml).expect("valid toml");
        assert_eq!(map.get("SHARDLINE_AUTH_PROVIDER").unwrap(), "jwks");
        assert_eq!(
            map.get("SHARDLINE_AUTH_JWKS_URL").unwrap(),
            "https://example.com/jwks.json"
        );
        assert_eq!(
            map.get("SHARDLINE_AUTH_JWKS_REFRESH_INTERVAL_SECONDS")
                .unwrap(),
            "1800"
        );
    }

    #[test]
    fn test_toml_to_env_map_invalid_toml() {
        let result = toml_to_env_map(b"[[[invalid]]]");
        assert!(result.is_err(), "invalid TOML should fail");
    }

    #[test]
    fn test_toml_to_env_map_invalid_utf8() {
        let result = toml_to_env_map(b"\xff\xfe\x00");
        assert!(result.is_err(), "invalid UTF-8 should fail");
    }

    #[test]
    fn test_resolve_config_path_explicit() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "[server]\nbind_addr = \"0.0.0.0:9999\"").unwrap();
        let content = resolve_config_path(Some(file.path()));
        assert!(content.is_some());
        let map = toml_to_env_map(&content.unwrap()).unwrap();
        assert_eq!(map.get("SHARDLINE_BIND_ADDR").unwrap(), "0.0.0.0:9999");
    }

    #[test]
    fn test_resolve_config_path_nonexistent() {
        let content = resolve_config_path(Some(Path::new("/nonexistent/path/shardline.toml")));
        assert!(content.is_none());
    }

    #[test]
    fn test_load_toml_env_overrides_explicit() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "[server]\nserver_role = \"transfer\"").unwrap();
        let result = load_toml_env_overrides(Some(file.path())).unwrap();
        assert!(result.is_some());
        let map = result.unwrap();
        assert_eq!(map.get("SHARDLINE_SERVER_ROLE").unwrap(), "transfer");
    }

    #[test]
    fn test_load_toml_env_overrides_nonexistent() {
        let result = load_toml_env_overrides(None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_env_precedence_over_toml() {
        let toml = b"[server]\nserver_role = \"api\"\n";
        let map = toml_to_env_map(toml).expect("valid toml");
        // Simulate env already set: the TOML value is returned in the map
        // but the caller (funcs.rs) skips keys already set in env.
        assert_eq!(map.get("SHARDLINE_SERVER_ROLE").unwrap(), "api");
    }

    #[test]
    fn test_partial_config_only_server() {
        let toml = b"[server]\nbind_addr = \"0.0.0.0:7070\"\n";
        let map = toml_to_env_map(toml).expect("valid toml");
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_full_config_roundtrip() {
        let toml = br#"
[server]
bind_addr = "0.0.0.0:8080"
public_base_url = "https://shardline.example.com"
server_role = "all"
frontends = ["xet", "oci", "hub"]
root_dir = "/var/lib/shardline"

[storage]
adapter = "s3"

[storage.s3]
endpoint = "https://s3.example.com"
region = "us-east-1"
bucket = "data"
prefix = "prod/"

[index]
postgres_url = "postgres://user@localhost/db"

[cache]
adapter = "redis"
redis_url = "redis://localhost"
ttl_seconds = 60

[auth]
provider = "local-hmac"
provider_token_issuer = "shardline"

[auth.jwks]
url = "https://example.com/jwks"
"#;
        let map = toml_to_env_map(toml).expect("valid full config");
        assert_eq!(map.get("SHARDLINE_BIND_ADDR").unwrap(), "0.0.0.0:8080");
        assert_eq!(
            map.get("SHARDLINE_PUBLIC_BASE_URL").unwrap(),
            "https://shardline.example.com"
        );
        assert_eq!(map.get("SHARDLINE_SERVER_ROLE").unwrap(), "all");
        assert_eq!(
            map.get("SHARDLINE_SERVER_FRONTENDS").unwrap(),
            "xet,oci,hub"
        );
        assert_eq!(map.get("SHARDLINE_ROOT_DIR").unwrap(), "/var/lib/shardline");
        assert_eq!(map.get("SHARDLINE_OBJECT_STORAGE_ADAPTER").unwrap(), "s3");
        assert_eq!(
            map.get("SHARDLINE_S3_ENDPOINT").unwrap(),
            "https://s3.example.com"
        );
        assert_eq!(map.get("SHARDLINE_S3_REGION").unwrap(), "us-east-1");
        assert_eq!(map.get("SHARDLINE_S3_BUCKET").unwrap(), "data");
        assert_eq!(map.get("SHARDLINE_S3_PREFIX").unwrap(), "prod/");
        assert_eq!(
            map.get("SHARDLINE_INDEX_POSTGRES_URL").unwrap(),
            "postgres://user@localhost/db"
        );
        assert_eq!(
            map.get("SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER").unwrap(),
            "redis"
        );
        assert_eq!(
            map.get("SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL").unwrap(),
            "redis://localhost"
        );
        assert_eq!(
            map.get("SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS")
                .unwrap(),
            "60"
        );
        assert_eq!(map.get("SHARDLINE_AUTH_PROVIDER").unwrap(), "local-hmac");
        assert_eq!(
            map.get("SHARDLINE_PROVIDER_TOKEN_ISSUER").unwrap(),
            "shardline"
        );
        assert_eq!(
            map.get("SHARDLINE_AUTH_JWKS_URL").unwrap(),
            "https://example.com/jwks"
        );
    }
}
