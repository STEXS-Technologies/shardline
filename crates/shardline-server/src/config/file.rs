use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

/// Top-level TOML configuration document.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct ShardlineTomlConfig {
    #[serde(default)]
    server: Option<ServerSection>,
    #[serde(default)]
    storage: Option<StorageSection>,
    #[serde(default)]
    index: Option<IndexSection>,
    #[serde(default)]
    cache: Option<CacheSection>,
    #[serde(default)]
    auth: Option<AuthSection>,
}

#[derive(Debug, Deserialize)]
struct ServerSection {
    bind_addr: Option<String>,
    public_base_url: Option<String>,
    server_role: Option<String>,
    frontends: Option<Vec<String>>,
    root_dir: Option<String>,
    max_request_body_bytes: Option<u64>,
    chunk_size_bytes: Option<u64>,
    upload_max_in_flight_chunks: Option<u64>,
    transfer_max_in_flight_chunks: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct StorageSection {
    adapter: Option<String>,
    s3: Option<S3Section>,
}

#[derive(Debug, Deserialize)]
struct S3Section {
    endpoint: Option<String>,
    region: Option<String>,
    bucket: Option<String>,
    prefix: Option<String>,
    virtual_hosted_style: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct IndexSection {
    postgres_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CacheSection {
    redis_url: Option<String>,
    adapter: Option<String>,
    ttl_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct AuthSection {
    provider: Option<String>,
    token_signing_key_path: Option<String>,
    provider_api_key_path: Option<String>,
    provider_token_issuer: Option<String>,
    provider_token_ttl_seconds: Option<u64>,
    jwks: Option<JwksSection>,
    oidc: Option<OidcSection>,
}

#[derive(Debug, Deserialize)]
struct JwksSection {
    url: Option<String>,
    refresh_interval_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct OidcSection {
    issuer_url: Option<String>,
    client_id: Option<String>,
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

/// Reads an optional shardline.toml (explicit path or auto-detected) and
/// returns a mapping of SHARDLINE_* env var names to their interpolated values.
/// Returns `Ok(None)` when no config file is found.
///
/// The caller is responsible for applying these values to the environment
/// before calling the env-based config loader.
///
/// # Errors
///
/// Returns an error message when the file exists but cannot be parsed.
pub fn load_toml_env_overrides(
    config_path: Option<&Path>,
) -> Result<Option<HashMap<String, String>>, String> {
    resolve_config_path(config_path).map_or(Ok(None), |bytes| toml_to_env_map(&bytes).map(Some))
}
