#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    env::var,
    fs::{self, File, OpenOptions},
    io::{self, Error as IoError, ErrorKind, Read},
    num::NonZeroU64,
    path::{Path, PathBuf},
};

use shardline_protocol::parse_bool;
use shardline_storage::S3ObjectStoreConfig;

use super::{
    MAX_PROVIDER_API_KEY_BYTES, MAX_S3_CREDENTIAL_BYTES, ServerConfig, ServerConfigError,
    run_before_secret_file_read_hook_for_tests,
};

pub(super) fn configure_provider_runtime_from_paths(
    mut config: ServerConfig,
    provider_config_path: Option<PathBuf>,
    provider_api_key_path: Option<PathBuf>,
    issuer_identity: String,
    provider_ttl_seconds: Result<NonZeroU64, ServerConfigError>,
) -> Result<ServerConfig, ServerConfigError> {
    match (provider_config_path, provider_api_key_path) {
        (Some(provider_config_path), Some(provider_api_key_path)) => {
            if config.token_signing_key().is_none() {
                return Err(ServerConfigError::ProviderTokensRequireSigningKey);
            }
            let ttl_seconds = provider_ttl_seconds?;
            let provider_api_key = read_secret_file_bytes(
                &provider_api_key_path,
                MAX_PROVIDER_API_KEY_BYTES,
                ServerConfigError::ProviderApiKey,
                |observed_bytes, maximum_bytes| ServerConfigError::ProviderApiKeyTooLarge {
                    observed_bytes,
                    maximum_bytes,
                },
                |expected_bytes, observed_bytes| ServerConfigError::ProviderApiKeyLengthMismatch {
                    expected_bytes,
                    observed_bytes,
                },
            )?;
            config = config.with_provider_runtime(
                provider_config_path,
                provider_api_key,
                issuer_identity,
                ttl_seconds,
            )?;
        }
        (None, None) => {}
        (Some(_), None) | (None, Some(_)) => {
            return Err(ServerConfigError::IncompleteProviderTokenConfig);
        }
    }

    Ok(config)
}

pub(super) fn load_s3_object_store_config_from_env()
-> Result<S3ObjectStoreConfig, ServerConfigError> {
    let bucket = var("SHARDLINE_S3_BUCKET").map_err(|_error| ServerConfigError::MissingS3Bucket);
    let inputs = PendingS3ObjectStoreConfig {
        region: var("SHARDLINE_S3_REGION").unwrap_or_else(|_error| "us-east-1".to_owned()),
        endpoint: var("SHARDLINE_S3_ENDPOINT").ok(),
        key_prefix: var("SHARDLINE_S3_KEY_PREFIX").ok(),
        allow_http: parse_env_bool("SHARDLINE_S3_ALLOW_HTTP")
            .map_err(|_error| ServerConfigError::InvalidS3AllowHttp),
        virtual_hosted_style_request: parse_env_bool("SHARDLINE_S3_VIRTUAL_HOSTED_STYLE_REQUEST")
            .map_err(|_error| ServerConfigError::InvalidS3VirtualHostedStyleRequest),
    };

    configure_s3_object_store_config(bucket, inputs, || {
        Ok((
            optional_s3_secret_env_or_file(
                "SHARDLINE_S3_ACCESS_KEY_ID",
                "SHARDLINE_S3_ACCESS_KEY_ID_FILE",
            )?,
            optional_s3_secret_env_or_file(
                "SHARDLINE_S3_SECRET_ACCESS_KEY",
                "SHARDLINE_S3_SECRET_ACCESS_KEY_FILE",
            )?,
            optional_s3_secret_env_or_file(
                "SHARDLINE_S3_SESSION_TOKEN",
                "SHARDLINE_S3_SESSION_TOKEN_FILE",
            )?,
        ))
    })
}

fn optional_s3_secret_env_or_file(
    env_name: &'static str,
    file_env_name: &'static str,
) -> Result<Option<String>, ServerConfigError> {
    optional_s3_secret_from_sources(
        env_name,
        var(env_name).ok(),
        file_env_name,
        var(file_env_name).ok(),
    )
}

pub(super) fn optional_s3_secret_from_sources(
    env_name: &'static str,
    direct: Option<String>,
    file_env_name: &'static str,
    file: Option<String>,
) -> Result<Option<String>, ServerConfigError> {
    match (direct, file) {
        (Some(_direct), Some(_file)) => Err(ServerConfigError::S3CredentialSourceConflict {
            env: env_name,
            file_env: file_env_name,
        }),
        (Some(value), None) => Ok(Some(value)),
        (None, Some(path)) => {
            let bytes = read_secret_file_bytes(
                Path::new(&path),
                MAX_S3_CREDENTIAL_BYTES,
                |source| ServerConfigError::S3CredentialFile {
                    name: file_env_name,
                    source,
                },
                |observed_bytes, maximum_bytes| ServerConfigError::S3CredentialTooLarge {
                    name: file_env_name,
                    observed_bytes,
                    maximum_bytes,
                },
                |expected_bytes, observed_bytes| ServerConfigError::S3CredentialLengthMismatch {
                    name: file_env_name,
                    expected_bytes,
                    observed_bytes,
                },
            )?;
            String::from_utf8(bytes).map(Some).map_err(|_error| {
                ServerConfigError::S3CredentialUtf8 {
                    name: file_env_name,
                }
            })
        }
        (None, None) => Ok(None),
    }
}

pub(super) struct PendingS3ObjectStoreConfig {
    pub(super) region: String,
    pub(super) endpoint: Option<String>,
    pub(super) key_prefix: Option<String>,
    pub(super) allow_http: Result<Option<bool>, ServerConfigError>,
    pub(super) virtual_hosted_style_request: Result<Option<bool>, ServerConfigError>,
}

pub(super) fn configure_s3_object_store_config<LoadCredentials>(
    bucket: Result<String, ServerConfigError>,
    inputs: PendingS3ObjectStoreConfig,
    load_credentials: LoadCredentials,
) -> Result<S3ObjectStoreConfig, ServerConfigError>
where
    LoadCredentials:
        FnOnce() -> Result<(Option<String>, Option<String>, Option<String>), ServerConfigError>,
{
    let bucket = bucket?;
    let PendingS3ObjectStoreConfig {
        region,
        endpoint,
        key_prefix,
        allow_http,
        virtual_hosted_style_request,
    } = inputs;
    let allow_http = allow_http?.unwrap_or(false);
    let virtual_hosted_style_request = virtual_hosted_style_request?.unwrap_or(false);
    let (access_key_id, secret_access_key, session_token) = load_credentials()?;

    Ok(S3ObjectStoreConfig::new(bucket, region)
        .with_endpoint(endpoint)
        .with_credentials(access_key_id, secret_access_key, session_token)
        .with_key_prefix(key_prefix.as_deref())
        .with_allow_http(allow_http)
        .with_virtual_hosted_style_request(virtual_hosted_style_request))
}

pub(super) fn read_secret_file_bytes(
    path: &Path,
    maximum_bytes: u64,
    read_error: impl Fn(IoError) -> ServerConfigError + Copy,
    error: impl Fn(u64, u64) -> ServerConfigError + Copy,
    _length_mismatch_error: impl Fn(u64, u64) -> ServerConfigError + Copy,
) -> Result<Vec<u8>, ServerConfigError> {
    let mut file = open_secret_file(path).map_err(read_error)?;

    run_before_secret_file_read_hook_for_tests(path);

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(read_error)?;
    ensure_secret_size_within_limit(bytes.len() as u64, maximum_bytes, error)?;

    Ok(bytes)
}

#[cfg(unix)]
fn open_secret_file(path: &Path) -> io::Result<File> {
    let resolved_path = resolve_secret_file_path(path)?;
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(resolved_path)
}

#[cfg(not(unix))]
fn open_secret_file(path: &Path) -> io::Result<File> {
    let resolved_path = resolve_secret_file_path(path)?;
    File::open(resolved_path)
}

fn resolve_secret_file_path(path: &Path) -> io::Result<PathBuf> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_file() {
        return Ok(path.to_path_buf());
    }

    if !metadata.file_type().is_symlink() {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            "secret file path must be a regular file and must not be a symlink",
        ));
    }

    let Some(parent) = path.parent() else {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            "secret file path must have a parent directory",
        ));
    };
    let parent = fs::canonicalize(parent)?;
    let resolved = fs::canonicalize(path)?;
    let resolved_metadata = fs::metadata(&resolved)?;

    if !resolved_metadata.is_file() || !resolved.starts_with(&parent) {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            "secret file path must be a regular file and must not be a symlink",
        ));
    }

    Ok(resolved)
}

pub(super) fn ensure_secret_size_within_limit(
    observed_bytes: u64,
    maximum_bytes: u64,
    error: impl Fn(u64, u64) -> ServerConfigError,
) -> Result<(), ServerConfigError> {
    if observed_bytes > maximum_bytes {
        return Err(error(observed_bytes, maximum_bytes));
    }

    Ok(())
}

fn parse_env_bool(name: &str) -> Result<Option<bool>, ()> {
    let Ok(value) = var(name) else {
        return Ok(None);
    };

    parse_bool(&value).map(Some).ok_or(())
}

#[cfg(test)]
mod tests {
    use super::{
        ensure_secret_size_within_limit, open_secret_file, read_secret_file_bytes,
    };
    use shardline_protocol::parse_bool;
    use std::io::Write;
    use std::path::Path;

    use super::super::ServerConfigError;

    // -----------------------------------------------------------------------
    // parse_bool (the underlying parser used by parse_env_bool)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_bool_true_literal() {
        assert_eq!(parse_bool("true"), Some(true));
    }

    #[test]
    fn parse_bool_false_literal() {
        assert_eq!(parse_bool("false"), Some(false));
    }

    #[test]
    fn parse_bool_one_is_true() {
        assert_eq!(parse_bool("1"), Some(true));
    }

    #[test]
    fn parse_bool_zero_is_false() {
        assert_eq!(parse_bool("0"), Some(false));
    }

    #[test]
    fn parse_bool_yes_is_true() {
        assert_eq!(parse_bool("yes"), Some(true));
    }

    #[test]
    fn parse_bool_no_is_false() {
        assert_eq!(parse_bool("no"), Some(false));
    }

    #[test]
    fn parse_bool_on_is_true() {
        assert_eq!(parse_bool("on"), Some(true));
    }

    #[test]
    fn parse_bool_off_is_false() {
        assert_eq!(parse_bool("off"), Some(false));
    }

    #[test]
    fn parse_bool_invalid_returns_none() {
        assert_eq!(parse_bool("invalid"), None);
    }

    #[test]
    fn parse_bool_empty_string_returns_none() {
        assert_eq!(parse_bool(""), None);
    }

    // -----------------------------------------------------------------------
    // ensure_secret_size_within_limit
    // -----------------------------------------------------------------------

    fn size_error(observed: u64, maximum: u64) -> ServerConfigError {
        ServerConfigError::ProviderApiKeyTooLarge {
            observed_bytes: observed,
            maximum_bytes: maximum,
        }
    }

    #[test]
    fn size_under_limit_is_ok() {
        assert!(ensure_secret_size_within_limit(100, 1024, size_error).is_ok());
    }

    #[test]
    fn size_at_limit_is_ok() {
        assert!(ensure_secret_size_within_limit(1024, 1024, size_error).is_ok());
    }

    #[test]
    fn size_at_zero_limit_is_ok() {
        assert!(ensure_secret_size_within_limit(0, 0, size_error).is_ok());
    }

    #[test]
    fn size_over_limit_is_err() {
        let result = ensure_secret_size_within_limit(1025, 1024, size_error);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("exceed") || err_msg.contains("TooLarge"));
    }

    #[test]
    fn size_one_byte_over_limit_is_err() {
        assert!(ensure_secret_size_within_limit(1, 0, size_error).is_err());
    }

    // -----------------------------------------------------------------------
    // read_secret_file_bytes
    // -----------------------------------------------------------------------

    fn read_error(e: std::io::Error) -> ServerConfigError {
        ServerConfigError::ProviderApiKey(e)
    }

    fn length_mismatch_error(expected: u64, observed: u64) -> ServerConfigError {
        ServerConfigError::ProviderApiKeyLengthMismatch {
            expected_bytes: expected,
            observed_bytes: observed,
        }
    }

    #[test]
    fn read_secret_file_roundtrip() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"hello world").unwrap();
        tmp.flush().unwrap();

        let result = read_secret_file_bytes(
            tmp.path(),
            4096,
            read_error,
            size_error,
            length_mismatch_error,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"hello world");
    }

    #[test]
    fn read_secret_file_empty_is_ok() {
        let tmp = tempfile::NamedTempFile::new().unwrap();

        let result = read_secret_file_bytes(
            tmp.path(),
            4096,
            read_error,
            size_error,
            length_mismatch_error,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"");
    }

    #[test]
    fn read_secret_file_exceeds_limit_is_err() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        let data = vec![0u8; 100];
        tmp.write_all(&data).unwrap();
        tmp.flush().unwrap();

        let result = read_secret_file_bytes(
            tmp.path(),
            50,
            read_error,
            size_error,
            length_mismatch_error,
        );
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("exceed") || err_msg.contains("TooLarge"));
    }

    // -----------------------------------------------------------------------
    // open_secret_file
    // -----------------------------------------------------------------------

    #[test]
    fn open_secret_file_valid_path() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let result = open_secret_file(tmp.path());
        assert!(result.is_ok());
    }

    #[test]
    fn open_secret_file_nonexistent_path() {
        let result = open_secret_file(Path::new("/nonexistent/path/secret_file_test"));
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn open_secret_file_symlink_resolved_is_ok() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let symlink_path = tmp.path().with_extension("link");
        symlink(tmp.path(), &symlink_path).unwrap();

        // Symlinks are resolved to their target and opened successfully.
        let result = open_secret_file(&symlink_path);
        assert!(result.is_ok());
    }
}
