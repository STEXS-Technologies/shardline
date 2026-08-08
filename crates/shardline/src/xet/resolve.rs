//! Target-URL resolution, credential resolution, and client construction for
//! the `sdx` CLI lane.

use std::path::Path;

use sdx::config::read_token_file;
use sdx::{
    Auth, Credential, SdxConfig, XetClient, XetClientBuilder, XetUrl,
    resolve_credential_from_config,
};

use super::cli::GlobalArgs;
use super::error::XetError;

/// Loads the `shardline.toml` config file (explicit `--config` path or the
/// default discovery path).
///
/// # Errors
///
/// Returns an error when an explicit or discovered config file is unreadable
/// or invalid.
pub(crate) fn load_config(config_path: Option<&Path>) -> Result<Option<SdxConfig>, XetError> {
    SdxConfig::load(config_path).map_err(|error| XetError::message(error.to_string()))
}

/// Resolves a user-supplied remote operand to a full [`XetUrl`].
///
/// Accepts either a complete `xet://host/provider/owner/repo/revision/path`
/// URL, or a shorthand `owner/repo/revision/path` that fills `provider` and
/// the host from `[default]` in the config file.
///
/// # Errors
///
/// Returns an error when the URL is malformed or a shorthand lacks required
/// config defaults.
pub(crate) fn resolve_remote(input: &str, config: Option<&SdxConfig>) -> Result<XetUrl, XetError> {
    if input.starts_with("xet://") {
        XetUrl::parse(input).map_err(XetError::Sdx)
    } else {
        shorthand_to_url(input, config)
    }
}

/// Builds the `XetUrl` for a shorthand `owner/repo/revision[/path...]` operand.
fn shorthand_to_url(input: &str, config: Option<&SdxConfig>) -> Result<XetUrl, XetError> {
    let endpoint = config
        .and_then(|config| config.endpoint())
        .map(str::to_owned)
        .ok_or_else(|| {
            XetError::message(
                "a shorthand remote needs [default].endpoint in shardline.toml".to_owned(),
            )
        })?;
    let provider = config
        .and_then(|config| config.provider())
        .map(str::to_owned)
        .ok_or_else(|| {
            XetError::message(
                "a shorthand remote needs [default].provider in shardline.toml".to_owned(),
            )
        })?;

    let trimmed = input.trim_matches('/');
    let mut segments = trimmed.split('/');
    let owner = segments
        .next()
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| {
            XetError::message("shorthand remote needs owner/repo/revision".to_owned())
        })?;
    let repo = segments
        .next()
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| {
            XetError::message("shorthand remote needs owner/repo/revision".to_owned())
        })?;
    let revision = segments
        .next()
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| {
            XetError::message("shorthand remote needs owner/repo/revision".to_owned())
        })?;
    let path = segments.collect::<Vec<_>>().join("/");

    let authority = endpoint_to_authority(&endpoint)?;
    let mut url = format!("xet://{authority}/{provider}/{owner}/{repo}/{revision}");
    if !path.is_empty() {
        url.push('/');
        url.push_str(&path);
    }
    if input.ends_with('/') && !path.is_empty() {
        url.push('/');
    }
    XetUrl::parse(&url).map_err(XetError::Sdx)
}

/// Extracts a `host[:port]` authority from a config `[default].endpoint`.
fn endpoint_to_authority(endpoint: &str) -> Result<String, XetError> {
    for prefix in ["http://", "https://"] {
        if let Some(rest) = endpoint.strip_prefix(prefix) {
            return Ok(rest.trim_end_matches('/').to_owned());
        }
    }
    if let Some(rest) = endpoint.strip_prefix("xet://") {
        return Ok(rest.trim_end_matches('/').to_owned());
    }
    Err(XetError::message(format!(
        "unsupported [default].endpoint {endpoint:?}; expected http(s)://host[:port]"
    )))
}

/// Resolves an [`Auth`] for `url`, applying the §5.2 credential priority:
/// CLI flags > environment > config `[auth]`.
///
/// # Errors
///
/// Returns an error when a token file cannot be read or no credential is
/// configured from any source.
pub(crate) fn resolve_auth(
    global: &GlobalArgs,
    config: Option<&SdxConfig>,
    url: &XetUrl,
) -> Result<Auth, XetError> {
    let flag_credential = resolve_flag_credential(global)?;
    let config_credential =
        resolve_credential_from_config(config, &|name| std::env::var(name).ok())
            .map_err(|error| XetError::message(error.to_string()))?;
    let credential = flag_credential.or(config_credential).ok_or_else(|| {
        XetError::message(
            "no credential configured; set --token/--api-key/--token-file, SHARDLINE_TOKEN/\
             SHARDLINE_API_KEY/SHARDLINE_TOKEN_FILE, or [auth] in shardline.toml"
                .to_owned(),
        )
    })?;
    let auth = Auth::new(&url.api_base, url.repository_id()).map_err(XetError::Auth)?;
    let mut auth = match credential {
        Credential::Bearer(token) => auth.with_token(token),
        Credential::ProviderKey(key) => auth.with_api_key(key),
    };
    if let Some(subject) = &global.subject {
        auth = auth.with_subject(subject.clone());
    }
    Ok(auth)
}

/// Resolves a credential from explicit CLI flags only.
fn resolve_flag_credential(global: &GlobalArgs) -> Result<Option<Credential>, XetError> {
    if let Some(token) = global
        .token
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return Ok(Some(Credential::Bearer(token.to_owned())));
    }
    if let Some(key) = global
        .api_key
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return Ok(Some(Credential::ProviderKey(key.to_owned())));
    }
    if let Some(path) = &global.token_file {
        let token = read_token_file(path).map_err(|error| {
            XetError::message(format!(
                "failed to read token file {}: {error}",
                path.display()
            ))
        })?;
        return Ok(Some(Credential::Bearer(token)));
    }
    Ok(None)
}

/// Builds an [`XetClient`] for the resolved remote.
///
/// # Errors
///
/// Returns an error when the endpoint/auth combination is invalid or the
/// client cannot be built.
pub(crate) fn build_client(
    url: &XetUrl,
    auth: Auth,
    chunk_size: Option<usize>,
    buffer_cap: Option<u64>,
) -> Result<XetClient, XetError> {
    let mut builder = XetClientBuilder::new().from_url(url).auth(auth);
    if let Some(size) = chunk_size {
        builder = builder.with_upload_chunk_size(size);
    }
    if let Some(cap) = buffer_cap {
        builder = builder.with_buffer_semaphore(cap);
    }
    builder.build().map_err(XetError::Sdx)
}

/// Resolves a repository-level operand for `branch` operations.
///
/// A repository URL may omit the revision segment
/// (`xet://host/provider/owner/repo/`); the default revision is supplied from
/// `[default].revision` (or `main`) so a client can be built for token
/// issuance. Full URLs and shorthands fall through to [`resolve_remote`].
///
/// # Errors
///
/// Returns an error when the URL is malformed.
pub(crate) fn resolve_repo(input: &str, config: Option<&SdxConfig>) -> Result<XetUrl, XetError> {
    if input.starts_with("xet://") {
        let after = input.strip_prefix("xet://").unwrap_or(input);
        let total = after
            .split('/')
            .filter(|segment| !segment.is_empty())
            .count();
        if total.saturating_sub(1) == 3 {
            let default_revision = config
                .and_then(|config| config.revision())
                .unwrap_or("main");
            let base = input.trim_end_matches('/');
            let url = format!("{base}/{default_revision}");
            return XetUrl::parse(&url).map_err(XetError::Sdx);
        }
    }
    resolve_remote(input, config)
}

/// Builds a session for a repository-level remote (used by `branch`).
///
/// # Errors
///
/// Returns an error when the URL or credentials are invalid.
pub(crate) fn session_for_repo(
    remote: &str,
    global: &GlobalArgs,
    config: Option<&SdxConfig>,
) -> Result<Session, XetError> {
    let url = resolve_repo(remote, config)?;
    let auth = resolve_auth(global, config, &url)?;
    let client = build_client(&url, auth, None, None)?;
    Ok(Session { client, url })
}

/// Builds a session scoped to a specific revision (used by `branch --create`
/// and `branch --delete`, where the server enforces strict scope matching on
/// the revision).
///
/// # Errors
///
/// Returns an error when the URL or credentials are invalid.
pub(crate) fn session_for_revision(
    remote: &str,
    revision: &str,
    global: &GlobalArgs,
    config: Option<&SdxConfig>,
) -> Result<Session, XetError> {
    let url = resolve_repo(remote, config)?;
    let url = with_revision(&url, revision);
    let auth = resolve_auth(global, config, &url)?;
    let client = build_client(&url, auth, None, None)?;
    Ok(Session { client, url })
}

/// Returns a copy of `url` with `revision` substituted into the repository
/// identity.
pub(crate) fn with_revision(url: &XetUrl, revision: &str) -> XetUrl {
    let mut updated = url.clone();
    updated.revision = revision.to_owned();
    updated.raw = updated.display();
    updated
}

/// Builds a session (client + resolved remote URL) for a single remote operand.
///
/// # Errors
///
/// Returns an error when the URL or credentials are invalid.
pub(crate) fn session_for(
    remote: &str,
    global: &GlobalArgs,
    config: Option<&SdxConfig>,
    chunk_size: Option<usize>,
) -> Result<Session, XetError> {
    let url = resolve_remote(remote, config)?;
    let auth = resolve_auth(global, config, &url)?;
    let client = build_client(&url, auth, chunk_size, None)?;
    Ok(Session { client, url })
}

/// A resolved remote plus its built client.
pub(crate) struct Session {
    /// The connected client.
    pub(crate) client: XetClient,
    /// The resolved remote URL.
    pub(crate) url: XetUrl,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use sdx::{SdxConfig, auth::RepositoryId};

    use super::{endpoint_to_authority, resolve_flag_credential, resolve_remote, shorthand_to_url};
    use crate::xet::cli::GlobalArgs;

    #[test]
    fn endpoint_to_authority_strips_schemes() {
        assert_eq!(
            endpoint_to_authority("http://127.0.0.1:8080").unwrap(),
            "127.0.0.1:8080"
        );
        assert_eq!(
            endpoint_to_authority("https://xet.example.com/").unwrap(),
            "xet.example.com"
        );
        assert_eq!(
            endpoint_to_authority("xet://host:9000").unwrap(),
            "host:9000"
        );
        assert!(endpoint_to_authority("not-a-url").is_err());
    }

    #[test]
    fn shorthand_fills_provider_and_endpoint_from_default() {
        let config = SdxConfig {
            default: sdx::DefaultSection {
                endpoint: Some("http://127.0.0.1:8080".to_owned()),
                provider: Some("github".to_owned()),
                owner: None,
                repo: None,
                revision: None,
            },
            auth: Default::default(),
        };
        let url = shorthand_to_url("team/assets/main/dir/file.txt", Some(&config)).unwrap();
        assert_eq!(url.api_base, "http://127.0.0.1:8080");
        assert_eq!(url.provider, "github");
        assert_eq!(url.owner, "team");
        assert_eq!(url.repo, "assets");
        assert_eq!(url.revision, "main");
        assert_eq!(url.path, "dir/file.txt");
        assert_eq!(
            url.repository_id(),
            RepositoryId {
                provider: "github".to_owned(),
                owner: "team".to_owned(),
                repo: "assets".to_owned(),
                revision: "main".to_owned(),
            }
        );
    }

    #[test]
    fn shorthand_trailing_slash_marks_directory() {
        let config = SdxConfig {
            default: sdx::DefaultSection {
                endpoint: Some("http://h".to_owned()),
                provider: Some("github".to_owned()),
                owner: None,
                repo: None,
                revision: None,
            },
            auth: Default::default(),
        };
        let url = shorthand_to_url("team/assets/main/", Some(&config)).unwrap();
        assert_eq!(url.path, "");
    }

    #[test]
    fn full_xet_url_is_used_directly() {
        let url = resolve_remote("xet://h/github/team/assets/main/f", None).unwrap();
        assert_eq!(url.path, "f");
    }

    #[test]
    fn shorthand_without_endpoint_errors() {
        let config = SdxConfig::default();
        assert!(shorthand_to_url("a/b/c", Some(&config)).is_err());
    }

    #[test]
    fn flag_credential_priority_token_over_api_key() {
        let global = GlobalArgs {
            config: None,
            token: Some("token".to_owned()),
            api_key: Some("key".to_owned()),
            token_file: None,
            subject: None,
        };
        let credential = resolve_flag_credential(&global).unwrap().unwrap();
        assert_eq!(credential, sdx::Credential::Bearer("token".to_owned()));
    }

    #[test]
    fn flag_credential_api_key_fallback() {
        let global = GlobalArgs {
            config: None,
            token: None,
            api_key: Some("key".to_owned()),
            token_file: None,
            subject: None,
        };
        let credential = resolve_flag_credential(&global).unwrap().unwrap();
        assert_eq!(credential, sdx::Credential::ProviderKey("key".to_owned()));
    }

    #[test]
    fn flag_credential_none_when_unset() {
        let global = GlobalArgs {
            config: None,
            token: None,
            api_key: None,
            token_file: None,
            subject: None,
        };
        assert!(resolve_flag_credential(&global).unwrap().is_none());
    }

    #[test]
    fn flag_credential_reads_token_file() {
        let dir = std::env::temp_dir();
        let path = dir.join("sdx-test-token");
        std::fs::write(&path, b"file-token\n").unwrap();
        let global = GlobalArgs {
            config: None,
            token: None,
            api_key: None,
            token_file: Some(PathBuf::from(&path)),
            subject: None,
        };
        let credential = resolve_flag_credential(&global).unwrap().unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(credential, sdx::Credential::Bearer("file-token".to_owned()));
    }
}
