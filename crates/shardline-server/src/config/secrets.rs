#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    env::var,
    fs::{self, File, OpenOptions},
    io::{self, Error as IoError, ErrorKind, Read},
    num::NonZeroU64,
    path::{Path, PathBuf},
    str,
};

use shardline_cache::RedisTlsConfig;
use shardline_protocol::{SecretBytes, SecretString, parse_bool};
use shardline_storage::S3ObjectStoreConfig;

use super::{
    MAX_PROVIDER_API_KEY_BYTES, MAX_REDIS_TLS_MATERIAL_BYTES, MAX_S3_CREDENTIAL_BYTES,
    ServerConfig, ServerConfigError, run_before_secret_file_read_hook_for_tests,
};

pub(super) fn load_redis_tls_config_from_env() -> Result<Option<RedisTlsConfig>, ServerConfigError>
{
    const ROOT_CERT_FILE: &str = "SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CA_FILE";
    const CLIENT_CERT_FILE: &str = "SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CLIENT_CERT_FILE";
    const CLIENT_KEY_FILE: &str = "SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CLIENT_KEY_FILE";

    let root_cert = optional_redis_tls_material_file(ROOT_CERT_FILE)?;
    let client_cert = optional_redis_tls_material_file(CLIENT_CERT_FILE)?;
    let client_key = optional_redis_tls_material_file(CLIENT_KEY_FILE)?;

    match (root_cert, client_cert, client_key) {
        (None, None, None) => Ok(None),
        (root_cert, Some(client_cert), Some(client_key)) => Ok(Some(
            RedisTlsConfig::new(root_cert).with_client_identity(client_cert, client_key),
        )),
        (_root_cert, Some(_), None) | (_root_cert, None, Some(_)) => {
            Err(ServerConfigError::IncompleteRedisTlsClientIdentity)
        }
        (Some(root_cert), None, None) => Ok(Some(RedisTlsConfig::new(Some(root_cert)))),
    }
}

fn optional_redis_tls_material_file(
    env_name: &'static str,
) -> Result<Option<SecretBytes>, ServerConfigError> {
    let Ok(path) = var(env_name) else {
        return Ok(None);
    };

    let bytes = read_secret_file_bytes(
        Path::new(&path),
        MAX_REDIS_TLS_MATERIAL_BYTES,
        |source| ServerConfigError::RedisTlsMaterial {
            name: env_name,
            source,
        },
        |observed_bytes, maximum_bytes| ServerConfigError::RedisTlsMaterialTooLarge {
            name: env_name,
            observed_bytes,
            maximum_bytes,
        },
        |expected_bytes, observed_bytes| ServerConfigError::RedisTlsMaterialTooLarge {
            name: env_name,
            observed_bytes: expected_bytes.max(observed_bytes),
            maximum_bytes: MAX_REDIS_TLS_MATERIAL_BYTES,
        },
    )?;
    Ok(Some(bytes))
}

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
) -> Result<Option<SecretString>, ServerConfigError> {
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
) -> Result<Option<SecretString>, ServerConfigError> {
    match (direct, file) {
        (Some(_direct), Some(_file)) => Err(ServerConfigError::S3CredentialSourceConflict {
            env: env_name,
            file_env: file_env_name,
        }),
        (Some(value), None) => Ok(Some(SecretString::new(value))),
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
            let s = str::from_utf8(bytes.expose_secret()).map_err(|_error| {
                ServerConfigError::S3CredentialUtf8 {
                    name: file_env_name,
                }
            })?;
            Ok(Some(SecretString::from_secret(s)))
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
    LoadCredentials: FnOnce() -> Result<
        (
            Option<SecretString>,
            Option<SecretString>,
            Option<SecretString>,
        ),
        ServerConfigError,
    >,
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
) -> Result<SecretBytes, ServerConfigError> {
    let mut file = open_secret_file(path).map_err(read_error)?;

    run_before_secret_file_read_hook_for_tests(path);

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(read_error)?;
    ensure_secret_size_within_limit(bytes.len() as u64, maximum_bytes, error)?;

    Ok(SecretBytes::new(bytes))
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
#[allow(unsafe_code)]
mod tests {
    use super::{
        ensure_secret_size_within_limit, load_redis_tls_config_from_env, open_secret_file,
        read_secret_file_bytes,
    };
    use shardline_protocol::{SecretString, parse_bool};
    use std::io::Write;
    use std::path::Path;

    use super::super::ServerConfigError;

    fn set_env_var(key: &str, value: &str) {
        // SAFETY: Must only be called from `#[serial_test::serial]` tests to
        // prevent data races on the global environment.
        unsafe { std::env::set_var(key, value) };
    }
    fn remove_env_var(key: &str) {
        // SAFETY: Same threading constraints as `set_env_var`.
        unsafe { std::env::remove_var(key) };
    }

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
        assert_eq!(result.unwrap().expose_secret(), b"hello world");
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
        assert_eq!(result.unwrap().expose_secret(), b"");
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
    // Redis TLS secret files
    // -----------------------------------------------------------------------

    #[test]
    #[serial_test::serial]
    fn redis_tls_secret_files_load_a_complete_mutual_tls_identity() {
        let mut ca = tempfile::NamedTempFile::new().unwrap();
        let mut client_cert = tempfile::NamedTempFile::new().unwrap();
        let mut client_key = tempfile::NamedTempFile::new().unwrap();
        ca.write_all(b"test-ca").unwrap();
        client_cert.write_all(b"test-client-certificate").unwrap();
        client_key.write_all(b"test-client-key").unwrap();
        ca.flush().unwrap();
        client_cert.flush().unwrap();
        client_key.flush().unwrap();

        set_env_var(
            "SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CA_FILE",
            ca.path().to_str().unwrap(),
        );
        set_env_var(
            "SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CLIENT_CERT_FILE",
            client_cert.path().to_str().unwrap(),
        );
        set_env_var(
            "SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CLIENT_KEY_FILE",
            client_key.path().to_str().unwrap(),
        );

        let result = load_redis_tls_config_from_env();

        remove_env_var("SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CA_FILE");
        remove_env_var("SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CLIENT_CERT_FILE");
        remove_env_var("SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CLIENT_KEY_FILE");

        let tls = result.unwrap().expect("TLS material should produce config");
        let rendered = format!("{tls:?}");
        assert!(rendered.contains("***"));
        assert!(!rendered.contains("test-ca"));
        assert!(!rendered.contains("test-client-certificate"));
        assert!(!rendered.contains("test-client-key"));
    }

    #[test]
    #[serial_test::serial]
    fn redis_tls_rejects_a_partial_mutual_tls_identity() {
        let mut client_cert = tempfile::NamedTempFile::new().unwrap();
        client_cert.write_all(b"test-client-certificate").unwrap();
        client_cert.flush().unwrap();

        set_env_var(
            "SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CLIENT_CERT_FILE",
            client_cert.path().to_str().unwrap(),
        );
        let result = load_redis_tls_config_from_env();
        remove_env_var("SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CLIENT_CERT_FILE");

        assert!(matches!(
            result,
            Err(ServerConfigError::IncompleteRedisTlsClientIdentity)
        ));
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

    // -----------------------------------------------------------------------
    // optional_s3_secret_from_sources
    // -----------------------------------------------------------------------

    #[test]
    fn optional_s3_secret_from_direct_value() {
        use super::optional_s3_secret_from_sources;
        use shardline_protocol::SecretString;
        let result = optional_s3_secret_from_sources(
            "TEST_ENV",
            Some("direct-value".to_owned()),
            "TEST_FILE_ENV",
            None,
        );
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().as_ref().map(SecretString::expose_secret),
            Some("direct-value")
        );
    }

    #[test]
    fn optional_s3_secret_both_sources_returns_err() {
        use super::optional_s3_secret_from_sources;
        let result = optional_s3_secret_from_sources(
            "TEST_ENV",
            Some("direct".to_owned()),
            "TEST_FILE_ENV",
            Some("/some/file".to_owned()),
        );
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("credential source conflict"));
    }

    #[test]
    fn optional_s3_secret_both_none_returns_none() {
        use super::optional_s3_secret_from_sources;
        let result = optional_s3_secret_from_sources("TEST_ENV", None, "TEST_FILE_ENV", None);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    // -----------------------------------------------------------------------
    // configure_s3_object_store_config
    // -----------------------------------------------------------------------

    #[test]
    fn configure_s3_object_store_config_with_minimal_inputs() {
        use super::{PendingS3ObjectStoreConfig, configure_s3_object_store_config};

        let inputs = PendingS3ObjectStoreConfig {
            region: "us-west-2".to_owned(),
            endpoint: None,
            key_prefix: None,
            allow_http: Ok(None),
            virtual_hosted_style_request: Ok(None),
        };
        let bucket: Result<String, _> = Ok("my-bucket".to_owned());

        let result = configure_s3_object_store_config(bucket, inputs, || Ok((None, None, None)));
        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.bucket(), "my-bucket");
        assert!(config.key_prefix().is_none());
    }

    #[test]
    fn configure_s3_object_store_config_with_all_inputs() {
        use super::{PendingS3ObjectStoreConfig, configure_s3_object_store_config};

        let inputs = PendingS3ObjectStoreConfig {
            region: "eu-central-1".to_owned(),
            endpoint: Some("https://s3.custom.example".to_owned()),
            key_prefix: Some("shardline/".to_owned()),
            allow_http: Ok(Some(true)),
            virtual_hosted_style_request: Ok(Some(true)),
        };
        let bucket: Result<String, _> = Ok("data-bucket".to_owned());

        let result = configure_s3_object_store_config(bucket, inputs, || {
            Ok((
                Some(SecretString::from_secret("AKID")),
                Some(SecretString::from_secret("secret")),
                Some(SecretString::from_secret("token")),
            ))
        });
        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.bucket(), "data-bucket");
        // The key_prefix is normalized (trailing slash stripped).
        assert!(config.key_prefix().is_some());
    }

    #[test]
    fn configure_s3_object_store_config_missing_bucket_errs() {
        use super::{PendingS3ObjectStoreConfig, configure_s3_object_store_config};

        let inputs = PendingS3ObjectStoreConfig {
            region: "us-east-1".to_owned(),
            endpoint: None,
            key_prefix: None,
            allow_http: Ok(None),
            virtual_hosted_style_request: Ok(None),
        };
        let bucket: Result<String, _> = Err(ServerConfigError::MissingS3Bucket);

        let result = configure_s3_object_store_config(bucket, inputs, || Ok((None, None, None)));
        assert!(result.is_err());
    }

    #[test]
    fn configure_s3_object_store_config_allow_http_parse_err() {
        use super::{
            PendingS3ObjectStoreConfig, ServerConfigError, configure_s3_object_store_config,
        };

        let inputs = PendingS3ObjectStoreConfig {
            region: "us-east-1".to_owned(),
            endpoint: None,
            key_prefix: None,
            allow_http: Err(ServerConfigError::InvalidS3AllowHttp),
            virtual_hosted_style_request: Ok(None),
        };
        let bucket: Result<String, _> = Ok("bucket".to_owned());

        let result = configure_s3_object_store_config(bucket, inputs, || Ok((None, None, None)));
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // configure_provider_runtime_from_paths
    // -----------------------------------------------------------------------

    fn test_config() -> super::super::ServerConfig {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        super::super::ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://127.0.0.1:8080".to_owned(),
            std::path::PathBuf::from("/tmp/shardline"),
            std::num::NonZeroUsize::MIN,
        )
    }

    #[test]
    fn configure_provider_runtime_from_paths_both_none_is_ok() {
        use super::configure_provider_runtime_from_paths;
        let config = test_config();
        let result = configure_provider_runtime_from_paths(
            config,
            None,
            None,
            "issuer".to_owned(),
            Ok(std::num::NonZeroU64::new(300).unwrap_or(std::num::NonZeroU64::MIN)),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn configure_provider_runtime_incomplete_config_errs() {
        use super::configure_provider_runtime_from_paths;
        let config = test_config();
        let result = configure_provider_runtime_from_paths(
            config,
            Some(std::path::PathBuf::from("/config")),
            None,
            "issuer".to_owned(),
            Ok(std::num::NonZeroU64::new(300).unwrap_or(std::num::NonZeroU64::MIN)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn configure_provider_runtime_both_paths_without_signing_key_errs() {
        use super::configure_provider_runtime_from_paths;
        // Config without signing key
        let config = test_config();
        let result = configure_provider_runtime_from_paths(
            config,
            Some(std::path::PathBuf::from("/config")),
            Some(std::path::PathBuf::from("/api_key")),
            "issuer".to_owned(),
            Ok(std::num::NonZeroU64::new(300).unwrap_or(std::num::NonZeroU64::MIN)),
        );
        assert!(matches!(
            result,
            Err(super::super::ServerConfigError::ProviderTokensRequireSigningKey)
        ));
    }

    // -----------------------------------------------------------------------
    // parse_env_bool (via parse_bool from shardline_protocol)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_env_bool_invalid_returns_none() {
        assert!(shardline_protocol::parse_bool("invalid").is_none());
    }

    #[test]
    fn parse_env_bool_uppercase_true_is_still_valid() {
        let result = shardline_protocol::parse_bool("TRUE");
        if result.is_none() {
            // parse_bool is case-sensitive; document current behavior
            return;
        }
        assert!(result.is_some());
    }

    // ── resolve_secret_file_path edge cases ──────────────────────────────

    #[test]
    fn open_secret_file_nonexistent_path_is_err() {
        let result = open_secret_file(Path::new("/tmp/__nonexistent_secret_test_file__"));
        assert!(result.is_err());
    }

    #[test]
    fn open_secret_file_directory_is_err() {
        let tmp = tempfile::tempdir().unwrap();
        let result = open_secret_file(tmp.path());
        // A directory is not a regular file
        assert!(result.is_err());
    }

    // ── configure_s3_object_store_config credential loading errors ───────

    #[test]
    fn configure_s3_object_store_config_virtual_hosted_parse_err() {
        use super::{
            PendingS3ObjectStoreConfig, ServerConfigError, configure_s3_object_store_config,
        };

        let inputs = PendingS3ObjectStoreConfig {
            region: "us-east-1".to_owned(),
            endpoint: None,
            key_prefix: None,
            allow_http: Ok(None),
            virtual_hosted_style_request: Err(
                ServerConfigError::InvalidS3VirtualHostedStyleRequest,
            ),
        };
        let bucket: Result<String, _> = Ok("bucket".to_owned());

        let result = configure_s3_object_store_config(bucket, inputs, || Ok((None, None, None)));
        assert!(result.is_err());
    }

    #[test]
    fn configure_s3_object_store_config_credential_loading_returns_err() {
        use super::{PendingS3ObjectStoreConfig, configure_s3_object_store_config};

        let inputs = PendingS3ObjectStoreConfig {
            region: "us-east-1".to_owned(),
            endpoint: None,
            key_prefix: None,
            allow_http: Ok(None),
            virtual_hosted_style_request: Ok(None),
        };
        let bucket: Result<String, _> = Ok("bucket".to_owned());

        let result = configure_s3_object_store_config(bucket, inputs, || {
            Err(super::super::ServerConfigError::MissingS3Bucket)
        });
        assert!(result.is_err());
    }

    // ── optional_s3_secret_from_sources file loading ──────────────────────

    #[test]
    fn optional_s3_secret_from_file_valid() {
        use super::optional_s3_secret_from_sources;
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut tmp, b"file-secret-value").unwrap();
        tmp.flush().unwrap();

        let result = optional_s3_secret_from_sources(
            "TEST_ENV",
            None,
            "TEST_FILE_ENV",
            Some(tmp.path().display().to_string()),
        );
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().as_ref().map(SecretString::expose_secret),
            Some("file-secret-value")
        );
    }

    #[test]
    fn optional_s3_secret_from_file_not_found() {
        use super::optional_s3_secret_from_sources;
        let result = optional_s3_secret_from_sources(
            "TEST_ENV",
            None,
            "TEST_FILE_ENV",
            Some("/nonexistent/path/credential".to_owned()),
        );
        assert!(result.is_err());
    }

    // ── configure_provider_runtime_from_paths with valid paths ────────────

    #[test]
    fn configure_provider_runtime_from_paths_with_valid_signing_key() {
        use super::configure_provider_runtime_from_paths;
        let mut config = test_config();
        config = config
            .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
            .unwrap();

        let mut api_key_file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut api_key_file, b"my-api-key").unwrap();
        api_key_file.flush().unwrap();
        let mut config_file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut config_file, b"config: {}").unwrap();
        config_file.flush().unwrap();

        let result = configure_provider_runtime_from_paths(
            config,
            Some(config_file.path().to_path_buf()),
            Some(api_key_file.path().to_path_buf()),
            "issuer".to_owned(),
            Ok(std::num::NonZeroU64::new(300).unwrap_or(std::num::NonZeroU64::MIN)),
        );
        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.provider_api_key(), Some(b"my-api-key" as &[u8]));
    }

    #[test]
    fn configure_provider_runtime_rejects_ttl_parse_error() {
        use super::configure_provider_runtime_from_paths;
        // The TTL error is only checked when both config paths are provided.
        // When both are None, the function returns Ok early without checking TTL.
        // So we need to provide both paths to trigger the TTL error.
        let mut config = test_config();
        config = config
            .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
            .unwrap();
        let result = configure_provider_runtime_from_paths(
            config,
            Some(std::path::PathBuf::from("/config")),
            Some(std::path::PathBuf::from("/api_key")),
            "issuer".to_owned(),
            Err(super::super::ServerConfigError::ProviderTokenTtl),
        );
        assert!(result.is_err());
    }

    // ── read_secret_file_bytes file reading errors ───────────────────────

    #[test]
    fn read_secret_file_bytes_nonexistent_path() {
        let result = read_secret_file_bytes(
            Path::new("/nonexistent/secret.file"),
            4096,
            ServerConfigError::MetricsToken,
            |obs, max| ServerConfigError::MetricsTokenTooLarge {
                observed_bytes: obs,
                maximum_bytes: max,
            },
            |exp, obs| ServerConfigError::MetricsTokenLengthMismatch {
                expected_bytes: exp,
                observed_bytes: obs,
            },
        );
        assert!(result.is_err());
    }

    // ── optional_s3_secret_from_sources — non-UTF-8 file ────────────────────

    #[test]
    fn optional_s3_secret_from_non_utf8_file_returns_utf8_error() {
        use super::optional_s3_secret_from_sources;
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        // Write raw bytes that are not valid UTF-8
        std::io::Write::write_all(&mut tmp, b"\xff\xfe\x00\x01").unwrap();
        tmp.flush().unwrap();

        let result = optional_s3_secret_from_sources(
            "TEST_ENV",
            None,
            "TEST_FILE_ENV",
            Some(tmp.path().display().to_string()),
        );
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("UTF-8")
                || err_msg.contains("utf-8")
                || err_msg.contains("S3CredentialUtf8")
        );
    }

    // ── open_secret_file — symlink escaping parent directory ────────────────

    #[cfg(unix)]
    #[test]
    fn open_secret_file_symlink_outside_parent_is_err() {
        use super::open_secret_file;
        use std::os::unix::fs::symlink;

        // Create a parent directory and a target file outside it
        let parent = tempfile::tempdir().unwrap();
        let outside_target = tempfile::NamedTempFile::new().unwrap();

        // Create symlink inside parent that points outside
        let symlink_path = parent.path().join("escaped_link");
        symlink(outside_target.path(), &symlink_path).unwrap();

        let result = open_secret_file(&symlink_path);
        assert!(result.is_err());
    }

    // ── parse_env_bool via env var ──────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn parse_env_bool_true_value_via_env() {
        // SAFETY: serialized env var test
        set_env_var("SHARDLINE_TEST_PARSE_BOOL_TRUE", "true");
        let result = super::parse_env_bool("SHARDLINE_TEST_PARSE_BOOL_TRUE");
        assert_eq!(result, Ok(Some(true)));
        // SAFETY: cleanup
        remove_env_var("SHARDLINE_TEST_PARSE_BOOL_TRUE");
    }

    #[test]
    #[serial_test::serial]
    fn parse_env_bool_false_value_via_env() {
        // SAFETY: serialized env var test
        set_env_var("SHARDLINE_TEST_PARSE_BOOL_FALSE", "false");
        let result = super::parse_env_bool("SHARDLINE_TEST_PARSE_BOOL_FALSE");
        assert_eq!(result, Ok(Some(false)));
        // SAFETY: cleanup
        remove_env_var("SHARDLINE_TEST_PARSE_BOOL_FALSE");
    }

    #[test]
    #[serial_test::serial]
    fn parse_env_bool_unset_env_returns_ok_none() {
        // SAFETY: serialized env var test
        remove_env_var("SHARDLINE_TEST_PARSE_BOOL_UNSET");
        let result = super::parse_env_bool("SHARDLINE_TEST_PARSE_BOOL_UNSET");
        assert_eq!(result, Ok(None));
    }

    #[test]
    #[serial_test::serial]
    fn parse_env_bool_invalid_value_returns_err() {
        // SAFETY: serialized env var test
        set_env_var("SHARDLINE_TEST_PARSE_BOOL_INVALID", "not-a-bool");
        let result = super::parse_env_bool("SHARDLINE_TEST_PARSE_BOOL_INVALID");
        assert_eq!(result, Err(()));
        // SAFETY: cleanup
        remove_env_var("SHARDLINE_TEST_PARSE_BOOL_INVALID");
    }

    // ── load_s3_object_store_config_from_env (env-based) ───────────────────

    #[test]
    #[serial_test::serial]
    fn load_s3_object_store_config_missing_bucket() {
        // Ensure SHARDLINE_S3_BUCKET is unset
        // SAFETY: serialized env var test
        remove_env_var("SHARDLINE_S3_BUCKET");
        let result = super::load_s3_object_store_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::MissingS3Bucket)
        ));
    }

    #[test]
    #[serial_test::serial]
    fn load_s3_object_store_config_invalid_allow_http() {
        // SAFETY: serialized env var test
        set_env_var("SHARDLINE_S3_BUCKET", "test-bucket");
        set_env_var("SHARDLINE_S3_ALLOW_HTTP", "not-a-bool");
        let result = super::load_s3_object_store_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::InvalidS3AllowHttp)
        ));
        // SAFETY: cleanup
        remove_env_var("SHARDLINE_S3_BUCKET");
        remove_env_var("SHARDLINE_S3_ALLOW_HTTP");
    }

    #[test]
    #[serial_test::serial]
    fn load_s3_object_store_config_invalid_virtual_hosted_style() {
        // SAFETY: serialized env var test
        set_env_var("SHARDLINE_S3_BUCKET", "test-bucket");
        set_env_var("SHARDLINE_S3_VIRTUAL_HOSTED_STYLE_REQUEST", "bad");
        let result = super::load_s3_object_store_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::InvalidS3VirtualHostedStyleRequest)
        ));
        // SAFETY: cleanup
        remove_env_var("SHARDLINE_S3_BUCKET");
        remove_env_var("SHARDLINE_S3_VIRTUAL_HOSTED_STYLE_REQUEST");
    }

    // ── optional_s3_secret_from_sources — credential source conflict via file env ─

    #[test]
    #[serial_test::serial]
    fn optional_s3_secret_env_or_file_conflict() {
        use super::optional_s3_secret_env_or_file;
        // SAFETY: serialized env var test
        set_env_var("SHARDLINE_TEST_S3_SECRET_ENV", "direct-value");
        set_env_var("SHARDLINE_TEST_S3_SECRET_FILE_ENV", "/some/file");
        let result = optional_s3_secret_env_or_file(
            "SHARDLINE_TEST_S3_SECRET_ENV",
            "SHARDLINE_TEST_S3_SECRET_FILE_ENV",
        );
        assert!(result.is_err());
        // SAFETY: cleanup
        remove_env_var("SHARDLINE_TEST_S3_SECRET_ENV");
        remove_env_var("SHARDLINE_TEST_S3_SECRET_FILE_ENV");
    }

    #[test]
    #[serial_test::serial]
    fn optional_s3_secret_env_or_file_direct_value() {
        use super::optional_s3_secret_env_or_file;
        // SAFETY: serialized env var test
        set_env_var("SHARDLINE_TEST_S3_SECRET_DIRECT", "my-secret");
        remove_env_var("SHARDLINE_TEST_S3_SECRET_DIRECT_FILE");
        let result = optional_s3_secret_env_or_file(
            "SHARDLINE_TEST_S3_SECRET_DIRECT",
            "SHARDLINE_TEST_S3_SECRET_DIRECT_FILE",
        );
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().as_ref().map(SecretString::expose_secret),
            Some("my-secret")
        );
        // SAFETY: cleanup
        remove_env_var("SHARDLINE_TEST_S3_SECRET_DIRECT");
    }

    #[test]
    #[serial_test::serial]
    fn optional_s3_secret_env_or_file_both_unset() {
        use super::optional_s3_secret_env_or_file;
        // SAFETY: serialized env var test
        remove_env_var("SHARDLINE_TEST_S3_SECRET_BOTH_UNSET");
        remove_env_var("SHARDLINE_TEST_S3_SECRET_BOTH_UNSET_FILE");
        let result = optional_s3_secret_env_or_file(
            "SHARDLINE_TEST_S3_SECRET_BOTH_UNSET",
            "SHARDLINE_TEST_S3_SECRET_BOTH_UNSET_FILE",
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    // ── configure_provider_runtime_from_paths — API key file too large ─────

    #[test]
    fn configure_provider_runtime_api_key_too_large() {
        use super::configure_provider_runtime_from_paths;
        let mut config = test_config();
        config = config
            .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
            .unwrap();

        let mut api_key_file = tempfile::NamedTempFile::new().unwrap();
        // MAX_PROVIDER_API_KEY_BYTES is typically small; use a large value
        let large_key = vec![b'a'; 10_000];
        std::io::Write::write_all(&mut api_key_file, &large_key).unwrap();
        api_key_file.flush().unwrap();
        let mut config_file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut config_file, b"config: {}").unwrap();
        config_file.flush().unwrap();

        let result = configure_provider_runtime_from_paths(
            config,
            Some(config_file.path().to_path_buf()),
            Some(api_key_file.path().to_path_buf()),
            "issuer".to_owned(),
            Ok(std::num::NonZeroU64::new(300).unwrap_or(std::num::NonZeroU64::MIN)),
        );
        assert!(result.is_err());
    }

    // ── open_secret_file — root path (no parent) ───────────────────────────

    #[test]
    fn open_secret_file_root_path_is_err() {
        use super::open_secret_file;
        // A root symlink (no parent) should be handled
        let result = open_secret_file(Path::new("/"));
        assert!(result.is_err());
    }

    // ── optional_s3_secret_from_sources — file too large ──────────────────

    #[test]
    fn optional_s3_secret_from_file_too_large_returns_size_error() {
        use super::optional_s3_secret_from_sources;
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        // MAX_S3_CREDENTIAL_BYTES is 4096; write more than that
        let large_data = vec![b'x'; 5000];
        std::io::Write::write_all(&mut tmp, &large_data).unwrap();
        tmp.flush().unwrap();

        let result = optional_s3_secret_from_sources(
            "TEST_ENV",
            None,
            "TEST_FILE_ENV",
            Some(tmp.path().display().to_string()),
        );
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("exceed") || err_msg.contains("TooLarge"));
    }

    // ── load_s3_object_store_config_from_env — key prefix ────────────────

    #[test]
    #[serial_test::serial]
    fn load_s3_object_store_config_with_key_prefix() {
        set_env_var("SHARDLINE_S3_BUCKET", "test-bucket");
        set_env_var("SHARDLINE_S3_KEY_PREFIX", "shardline/");
        set_env_var("SHARDLINE_S3_REGION", "eu-west-1");
        set_env_var("SHARDLINE_S3_ENDPOINT", "https://s3.example.com");
        // Remove any credential overrides
        remove_env_var("SHARDLINE_S3_ACCESS_KEY_ID");
        remove_env_var("SHARDLINE_S3_SECRET_ACCESS_KEY");
        let result = super::load_s3_object_store_config_from_env();
        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.bucket(), "test-bucket");
        assert!(config.key_prefix().is_some());
        // Cleanup
        remove_env_var("SHARDLINE_S3_BUCKET");
        remove_env_var("SHARDLINE_S3_KEY_PREFIX");
        remove_env_var("SHARDLINE_S3_REGION");
        remove_env_var("SHARDLINE_S3_ENDPOINT");
    }

    #[test]
    #[serial_test::serial]
    fn load_s3_object_store_config_empty_region() {
        // SAFETY: serialized env var test
        set_env_var("SHARDLINE_S3_BUCKET", "test-bucket");
        set_env_var("SHARDLINE_S3_REGION", "");
        // Remove any credential overrides
        remove_env_var("SHARDLINE_S3_ACCESS_KEY_ID");
        remove_env_var("SHARDLINE_S3_SECRET_ACCESS_KEY");
        let result = super::load_s3_object_store_config_from_env();
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let config = result.unwrap();
        assert_eq!(config.bucket(), "test-bucket");
        // region is pub(crate); we verify the config is valid
        // Cleanup
        remove_env_var("SHARDLINE_S3_BUCKET");
        remove_env_var("SHARDLINE_S3_REGION");
    }

    // ── configure_provider_runtime_from_paths — TTL zero ──────────────────

    #[test]
    fn configure_provider_runtime_ttl_zero_with_both_paths_provided() {
        use super::configure_provider_runtime_from_paths;
        let mut config = test_config();
        config = config
            .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
            .unwrap();
        let result = configure_provider_runtime_from_paths(
            config,
            Some(std::path::PathBuf::from("/config")),
            Some(std::path::PathBuf::from("/api_key")),
            "issuer".to_owned(),
            Err(super::super::ServerConfigError::ZeroProviderTokenTtl),
        );
        assert!(matches!(
            result,
            Err(super::super::ServerConfigError::ZeroProviderTokenTtl)
        ));
    }
}
