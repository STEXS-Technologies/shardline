#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{Error as IoError, ErrorKind, Read},
    path::{Path, PathBuf},
};

use shardline_protocol::parse_bool;
use shardline_server::{
    ObjectStorageAdapter, ServerConfigError, ServerError, StorageMigrationEndpoint,
    StorageMigrationOptions, StorageMigrationReport,
    run_storage_migration as run_server_storage_migration,
};
use shardline_storage::S3ObjectStoreConfig;
use thiserror::Error;

use crate::{effective_root, local_path::ensure_directory_path_components_are_not_symlinked};

const FROM_S3_ENV_PREFIX: &str = "SHARDLINE_MIGRATE_FROM_S3";
const TO_S3_ENV_PREFIX: &str = "SHARDLINE_MIGRATE_TO_S3";
const MAX_S3_CREDENTIAL_BYTES: u64 = 4096;

#[cfg(test)]
type S3CredentialReadHook = Box<dyn FnOnce() + Send>;

#[cfg(test)]
struct S3CredentialReadHookRegistration {
    path: PathBuf,
    hook: S3CredentialReadHook,
}

#[cfg(test)]
type S3CredentialReadHookSlot = Vec<S3CredentialReadHookRegistration>;

#[cfg(test)]
static BEFORE_S3_CREDENTIAL_READ_HOOK: LazyLock<Mutex<S3CredentialReadHookSlot>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

/// Storage-migration runtime failure.
#[derive(Debug, Error)]
pub enum StorageMigrationRuntimeError {
    /// Configuration loading failed.
    #[error(transparent)]
    Config(#[from] ServerConfigError),
    /// Server-side migration failed.
    #[error(transparent)]
    Server(#[from] ServerError),
    /// A local destination requires an explicit state root.
    #[error("{flag} is required when {side} uses local storage")]
    MissingLocalRoot {
        /// Missing flag name.
        flag: &'static str,
        /// Endpoint side.
        side: &'static str,
    },
    /// Required S3 environment variable was missing.
    #[error("missing environment variable: {0}")]
    MissingS3Env(String),
    /// S3 boolean environment variable was invalid.
    #[error("invalid boolean environment variable {name}: {value}")]
    InvalidS3Bool {
        /// Environment variable name.
        name: String,
        /// Raw environment value.
        value: String,
    },
    /// An S3 credential was provided through both direct env and file indirection.
    #[error("s3 credential source conflict: both {env} and {file_env} are set")]
    S3CredentialSourceConflict {
        /// Direct environment variable name.
        env: String,
        /// File-indirection environment variable name.
        file_env: String,
    },
    /// An S3 credential file could not be read.
    #[error("s3 credential file {name} could not be read")]
    S3CredentialFile {
        /// Credential file-indirection environment variable name.
        name: String,
        /// Underlying filesystem failure.
        #[source]
        source: IoError,
    },
    /// An S3 credential file exceeded the bounded parser ceiling.
    #[error("s3 credential file {name} exceeded the bounded parser ceiling")]
    S3CredentialTooLarge {
        /// Credential file-indirection environment variable name.
        name: String,
        /// Observed secret file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted secret file length in bytes.
        maximum_bytes: u64,
    },
    /// An S3 credential file changed after validation and was rejected.
    #[error("s3 credential file {name} changed during bounded read")]
    S3CredentialLengthMismatch {
        /// Credential file-indirection environment variable name.
        name: String,
        /// Validated secret file length in bytes.
        expected_bytes: u64,
        /// Observed secret file length in bytes after bounded read.
        observed_bytes: u64,
    },
    /// An S3 credential file was not valid UTF-8.
    #[error("s3 credential file {name} was not valid utf-8")]
    S3CredentialUtf8 {
        /// Credential file-indirection environment variable name.
        name: String,
    },
    /// One local state root contained an invalid filesystem component.
    #[error("invalid {side} local state root: {path}")]
    InvalidLocalRoot {
        /// Endpoint side.
        side: &'static str,
        /// Rejected local root path.
        path: PathBuf,
        /// Underlying filesystem failure.
        #[source]
        source: IoError,
    },
}

/// Runs an object-storage migration.
///
/// # Errors
///
/// Returns [`StorageMigrationRuntimeError`] when endpoint configuration or migration
/// execution fails.
pub fn run_storage_migration(
    from: ObjectStorageAdapter,
    from_root: Option<&Path>,
    to: ObjectStorageAdapter,
    to_root: Option<&Path>,
    prefix: String,
    dry_run: bool,
) -> Result<StorageMigrationReport, StorageMigrationRuntimeError> {
    let source = endpoint(from, from_root, "source")?;
    let destination = endpoint(to, to_root, "destination")?;
    let options = StorageMigrationOptions::new(source, destination)
        .with_prefix(prefix)
        .with_dry_run(dry_run);

    Ok(run_server_storage_migration(&options)?)
}

fn endpoint(
    adapter: ObjectStorageAdapter,
    root: Option<&Path>,
    side: &'static str,
) -> Result<StorageMigrationEndpoint, StorageMigrationRuntimeError> {
    match adapter {
        ObjectStorageAdapter::Local => local_endpoint(root, side),
        ObjectStorageAdapter::S3 => {
            let prefix = if side == "source" {
                FROM_S3_ENV_PREFIX
            } else {
                TO_S3_ENV_PREFIX
            };
            Ok(StorageMigrationEndpoint::S3(load_s3_config(prefix)?))
        }
    }
}

fn local_endpoint(
    root: Option<&Path>,
    side: &'static str,
) -> Result<StorageMigrationEndpoint, StorageMigrationRuntimeError> {
    if let Some(root) = root {
        ensure_directory_path_components_are_not_symlinked(root).map_err(|source| {
            StorageMigrationRuntimeError::InvalidLocalRoot {
                side,
                path: root.to_path_buf(),
                source,
            }
        })?;
        return Ok(StorageMigrationEndpoint::LocalStateRoot(root.to_path_buf()));
    }

    if side == "source" {
        let resolved_root = effective_root(None)?;
        ensure_directory_path_components_are_not_symlinked(&resolved_root).map_err(|source| {
            StorageMigrationRuntimeError::InvalidLocalRoot {
                side,
                path: resolved_root.clone(),
                source,
            }
        })?;
        return Ok(StorageMigrationEndpoint::LocalStateRoot(resolved_root));
    }

    Err(StorageMigrationRuntimeError::MissingLocalRoot {
        flag: "--to-root",
        side,
    })
}

fn load_s3_config(prefix: &str) -> Result<S3ObjectStoreConfig, StorageMigrationRuntimeError> {
    let bucket = required_env(prefix, "BUCKET");
    let inputs = PendingS3Config {
        region: env_value(prefix, "REGION").unwrap_or_else(|| "us-east-1".to_owned()),
        endpoint: env_value(prefix, "ENDPOINT"),
        key_prefix: env_value(prefix, "KEY_PREFIX"),
        allow_http: optional_bool_env(prefix, "ALLOW_HTTP"),
        virtual_hosted_style_request: optional_bool_env(prefix, "VIRTUAL_HOSTED_STYLE_REQUEST"),
    };

    build_s3_config(bucket, inputs, || {
        Ok((
            optional_s3_secret_env_or_file(prefix, "ACCESS_KEY_ID")?,
            optional_s3_secret_env_or_file(prefix, "SECRET_ACCESS_KEY")?,
            optional_s3_secret_env_or_file(prefix, "SESSION_TOKEN")?,
        ))
    })
}

struct PendingS3Config {
    region: String,
    endpoint: Option<String>,
    key_prefix: Option<String>,
    allow_http: Result<Option<bool>, StorageMigrationRuntimeError>,
    virtual_hosted_style_request: Result<Option<bool>, StorageMigrationRuntimeError>,
}

fn build_s3_config<LoadCredentials>(
    bucket: Result<String, StorageMigrationRuntimeError>,
    inputs: PendingS3Config,
    load_credentials: LoadCredentials,
) -> Result<S3ObjectStoreConfig, StorageMigrationRuntimeError>
where
    LoadCredentials: FnOnce() -> Result<
        (Option<String>, Option<String>, Option<String>),
        StorageMigrationRuntimeError,
    >,
{
    let bucket = bucket?;
    let PendingS3Config {
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

fn required_env(prefix: &str, name: &str) -> Result<String, StorageMigrationRuntimeError> {
    let key = env_key(prefix, name);
    env::var(&key).map_err(|_error| StorageMigrationRuntimeError::MissingS3Env(key))
}

fn env_value(prefix: &str, name: &str) -> Option<String> {
    env::var(env_key(prefix, name)).ok()
}

fn optional_s3_secret_env_or_file(
    prefix: &str,
    name: &str,
) -> Result<Option<String>, StorageMigrationRuntimeError> {
    let env_name = env_key(prefix, name);
    let file_name = format!("{name}_FILE");
    let file_env_name = env_key(prefix, &file_name);

    optional_s3_secret_from_sources(
        env_name.clone(),
        env::var(&env_name).ok(),
        file_env_name.clone(),
        env::var(&file_env_name).ok(),
    )
}

fn optional_s3_secret_from_sources(
    env_name: String,
    direct: Option<String>,
    file_env_name: String,
    file: Option<String>,
) -> Result<Option<String>, StorageMigrationRuntimeError> {
    match (direct, file) {
        (Some(_direct), Some(_file)) => {
            Err(StorageMigrationRuntimeError::S3CredentialSourceConflict {
                env: env_name,
                file_env: file_env_name,
            })
        }
        (Some(value), None) => Ok(Some(value)),
        (None, Some(path)) => read_s3_credential_file(Path::new(&path), file_env_name).map(Some),
        (None, None) => Ok(None),
    }
}

fn read_s3_credential_file(
    path: &Path,
    name: String,
) -> Result<String, StorageMigrationRuntimeError> {
    let mut file = open_s3_credential_file(path).map_err(|source| {
        StorageMigrationRuntimeError::S3CredentialFile {
            name: name.clone(),
            source,
        }
    })?;
    let metadata =
        file.metadata()
            .map_err(|source| StorageMigrationRuntimeError::S3CredentialFile {
                name: name.clone(),
                source,
            })?;
    ensure_s3_credential_size_within_limit(name.clone(), metadata.len())?;

    run_before_s3_credential_read_hook_for_tests(path);

    let bytes = read_bounded_s3_credential_file(&name, &mut file, metadata.len())?;
    ensure_s3_credential_size_within_limit(
        name.clone(),
        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
    )?;

    String::from_utf8(bytes)
        .map_err(|_error| StorageMigrationRuntimeError::S3CredentialUtf8 { name })
}

#[cfg(unix)]
fn open_s3_credential_file(path: &Path) -> Result<File, IoError> {
    ensure_s3_credential_path_is_regular(path)?;
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

#[cfg(not(unix))]
fn open_s3_credential_file(path: &Path) -> Result<File, IoError> {
    ensure_s3_credential_path_is_regular(path)?;
    File::open(path)
}

fn ensure_s3_credential_path_is_regular(path: &Path) -> Result<(), IoError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            "s3 credential path must be a regular file and must not be a symlink",
        ));
    }

    Ok(())
}

fn read_bounded_s3_credential_file(
    name: &str,
    file: &mut File,
    expected_length: u64,
) -> Result<Vec<u8>, StorageMigrationRuntimeError> {
    let capacity = usize::try_from(expected_length).unwrap_or(usize::MAX);
    let mut bytes = Vec::with_capacity(capacity);
    let mut limited = file.by_ref().take(expected_length);

    limited.read_to_end(&mut bytes).map_err(|source| {
        StorageMigrationRuntimeError::S3CredentialFile {
            name: name.to_owned(),
            source,
        }
    })?;

    let read_length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if read_length != expected_length {
        return Err(StorageMigrationRuntimeError::S3CredentialLengthMismatch {
            name: name.to_owned(),
            expected_bytes: expected_length,
            observed_bytes: read_length,
        });
    }

    let mut trailing = [0_u8; 1];
    match file.read(&mut trailing) {
        Ok(0) => {}
        Ok(_read) => {
            return Err(StorageMigrationRuntimeError::S3CredentialLengthMismatch {
                name: name.to_owned(),
                expected_bytes: expected_length,
                observed_bytes: expected_length.saturating_add(1),
            });
        }
        Err(source) => {
            return Err(StorageMigrationRuntimeError::S3CredentialFile {
                name: name.to_owned(),
                source,
            });
        }
    }

    let observed_length = file
        .metadata()
        .map_err(|source| StorageMigrationRuntimeError::S3CredentialFile {
            name: name.to_owned(),
            source,
        })?
        .len();
    if observed_length != expected_length {
        return Err(StorageMigrationRuntimeError::S3CredentialLengthMismatch {
            name: name.to_owned(),
            expected_bytes: expected_length,
            observed_bytes: observed_length,
        });
    }

    Ok(bytes)
}

fn ensure_s3_credential_size_within_limit(
    name: String,
    observed_bytes: u64,
) -> Result<(), StorageMigrationRuntimeError> {
    if observed_bytes > MAX_S3_CREDENTIAL_BYTES {
        return Err(StorageMigrationRuntimeError::S3CredentialTooLarge {
            name,
            observed_bytes,
            maximum_bytes: MAX_S3_CREDENTIAL_BYTES,
        });
    }

    Ok(())
}

#[cfg(test)]
fn run_before_s3_credential_read_hook_for_tests(path: &Path) {
    let hook = match BEFORE_S3_CREDENTIAL_READ_HOOK.lock() {
        Ok(mut guard) => take_s3_credential_read_hook_for_path(&mut guard, path),
        Err(poisoned) => take_s3_credential_read_hook_for_path(&mut poisoned.into_inner(), path),
    };

    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn take_s3_credential_read_hook_for_path(
    slot: &mut S3CredentialReadHookSlot,
    path: &Path,
) -> Option<S3CredentialReadHook> {
    let index = slot
        .iter()
        .position(|registration| registration.path == path)?;
    Some(slot.remove(index).hook)
}

#[cfg(not(test))]
const fn run_before_s3_credential_read_hook_for_tests(_path: &Path) {}

fn optional_bool_env(
    prefix: &str,
    name: &str,
) -> Result<Option<bool>, StorageMigrationRuntimeError> {
    let key = env_key(prefix, name);
    let Some(value) = env::var(&key).ok() else {
        return Ok(None);
    };

    parse_bool(&value)
        .map(Some)
        .ok_or_else(|| StorageMigrationRuntimeError::InvalidS3Bool { name: key, value })
}

fn env_key(prefix: &str, name: &str) -> String {
    format!("{prefix}_{name}")
}
#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use std::{
        fs::{self, OpenOptions},
        io::Write,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use shardline_server::StorageMigrationEndpoint;

    use super::{
        PendingS3Config, StorageMigrationRuntimeError, build_s3_config, local_endpoint,
        optional_s3_secret_from_sources,
    };
    use shardline_protocol::parse_bool;

    fn set_before_s3_credential_read_hook_for_tests(
        path: PathBuf,
        hook: impl FnOnce() + Send + 'static,
    ) {
        let mut slot = match super::BEFORE_S3_CREDENTIAL_READ_HOOK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(index) = slot
            .iter()
            .position(|registration| registration.path == path)
        {
            drop(slot.remove(index));
        }
        slot.push(super::S3CredentialReadHookRegistration {
            path,
            hook: Box::new(hook),
        });
    }

    #[test]
    fn storage_migration_bool_parser_accepts_operator_values() {
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("1"), Some(true));
        assert_eq!(parse_bool("on"), Some(true));
        assert_eq!(parse_bool("false"), Some(false));
        assert_eq!(parse_bool("0"), Some(false));
        assert_eq!(parse_bool("off"), Some(false));
        assert_eq!(parse_bool("maybe"), None);
    }

    #[cfg(unix)]
    #[test]
    fn local_endpoint_rejects_symlinked_root() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let target = sandbox.path().join("target-root");
        let create_target = fs::create_dir_all(&target);
        assert!(create_target.is_ok());
        let root = sandbox.path().join("root-link");
        let linked = symlink(&target, &root);
        assert!(linked.is_ok());

        let endpoint = local_endpoint(Some(&root), "source");

        assert!(matches!(
            endpoint,
            Err(StorageMigrationRuntimeError::InvalidLocalRoot { side: "source", .. })
        ));
    }

    #[test]
    fn local_endpoint_accepts_real_root() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };

        let endpoint = local_endpoint(Some(sandbox.path()), "source");

        assert!(matches!(
            endpoint,
            Ok(StorageMigrationEndpoint::LocalStateRoot(_))
        ));
    }

    #[test]
    fn build_s3_config_rejects_missing_bucket_before_loading_credentials() {
        let credentials_loaded = Arc::new(AtomicBool::new(false));
        let credentials_loaded_for_hook = Arc::clone(&credentials_loaded);

        let configured = build_s3_config(
            Err(StorageMigrationRuntimeError::MissingS3Env(
                "SHARDLINE_MIGRATE_FROM_S3_BUCKET".to_owned(),
            )),
            PendingS3Config {
                region: "us-east-1".to_owned(),
                endpoint: None,
                key_prefix: None,
                allow_http: Ok(None),
                virtual_hosted_style_request: Ok(None),
            },
            move || {
                credentials_loaded_for_hook.store(true, Ordering::SeqCst);
                Ok((Some("access".to_owned()), Some("secret".to_owned()), None))
            },
        );

        assert!(matches!(
            configured,
            Err(StorageMigrationRuntimeError::MissingS3Env(_))
        ));
        assert!(!credentials_loaded.load(Ordering::SeqCst));
    }

    #[test]
    fn build_s3_config_rejects_invalid_bool_before_loading_credentials() {
        let credentials_loaded = Arc::new(AtomicBool::new(false));
        let credentials_loaded_for_hook = Arc::clone(&credentials_loaded);

        let configured = build_s3_config(
            Ok("assets".to_owned()),
            PendingS3Config {
                region: "us-east-1".to_owned(),
                endpoint: None,
                key_prefix: None,
                allow_http: Err(StorageMigrationRuntimeError::InvalidS3Bool {
                    name: "SHARDLINE_MIGRATE_FROM_S3_ALLOW_HTTP".to_owned(),
                    value: "maybe".to_owned(),
                }),
                virtual_hosted_style_request: Ok(None),
            },
            move || {
                credentials_loaded_for_hook.store(true, Ordering::SeqCst);
                Ok((Some("access".to_owned()), Some("secret".to_owned()), None))
            },
        );

        assert!(matches!(
            configured,
            Err(StorageMigrationRuntimeError::InvalidS3Bool { .. })
        ));
        assert!(!credentials_loaded.load(Ordering::SeqCst));
    }

    #[test]
    fn build_s3_config_accepts_file_backed_credentials_without_debug_leak() {
        let access_key_file = tempfile::NamedTempFile::new();
        assert!(access_key_file.is_ok());
        let Ok(access_key_file) = access_key_file else {
            return;
        };
        let write = fs::write(access_key_file.path(), b"file-access-key");
        assert!(write.is_ok());

        let configured = build_s3_config(
            Ok("assets".to_owned()),
            PendingS3Config {
                region: "us-east-1".to_owned(),
                endpoint: None,
                key_prefix: None,
                allow_http: Ok(None),
                virtual_hosted_style_request: Ok(None),
            },
            || {
                Ok((
                    optional_s3_secret_from_sources(
                        "SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID".to_owned(),
                        None,
                        "SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID_FILE".to_owned(),
                        Some(access_key_file.path().display().to_string()),
                    )?,
                    None,
                    None,
                ))
            },
        );

        assert!(configured.is_ok());
        let debug = format!("{configured:?}");
        assert!(!debug.contains("file-access-key"));
    }

    #[test]
    fn build_s3_config_rejects_direct_and_file_credential_conflict() {
        let configured = build_s3_config(
            Ok("assets".to_owned()),
            PendingS3Config {
                region: "us-east-1".to_owned(),
                endpoint: None,
                key_prefix: None,
                allow_http: Ok(None),
                virtual_hosted_style_request: Ok(None),
            },
            || {
                Ok((
                    optional_s3_secret_from_sources(
                        "SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID".to_owned(),
                        Some("direct-access-key".to_owned()),
                        "SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID_FILE".to_owned(),
                        Some("/run/secrets/access-key".to_owned()),
                    )?,
                    None,
                    None,
                ))
            },
        );

        assert!(matches!(
            configured,
            Err(StorageMigrationRuntimeError::S3CredentialSourceConflict {
                env,
                file_env,
            }) if env == "SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID"
                && file_env == "SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID_FILE"
        ));
    }

    #[test]
    fn build_s3_config_rejects_invalid_utf8_file_backed_credential() {
        let access_key_file = tempfile::NamedTempFile::new();
        assert!(access_key_file.is_ok());
        let Ok(access_key_file) = access_key_file else {
            return;
        };
        let write = fs::write(access_key_file.path(), [0xff, 0xfe]);
        assert!(write.is_ok());

        let credential = optional_s3_secret_from_sources(
            "SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID".to_owned(),
            None,
            "SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID_FILE".to_owned(),
            Some(access_key_file.path().display().to_string()),
        );

        assert!(matches!(
            credential,
            Err(StorageMigrationRuntimeError::S3CredentialUtf8 { name })
                if name == "SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID_FILE"
        ));
    }

    #[test]
    fn build_s3_config_rejects_credential_file_growth_after_validation() {
        let access_key_file = tempfile::NamedTempFile::new();
        assert!(access_key_file.is_ok());
        let Ok(access_key_file) = access_key_file else {
            return;
        };
        let write = fs::write(access_key_file.path(), b"access");
        assert!(write.is_ok());
        let path = access_key_file.path().to_path_buf();
        let path_for_hook = path.clone();
        set_before_s3_credential_read_hook_for_tests(path.clone(), move || {
            let opened = OpenOptions::new().append(true).open(&path_for_hook);
            assert!(opened.is_ok());
            let Ok(mut opened) = opened else {
                return;
            };
            let append = opened.write_all(b"-appended");
            assert!(append.is_ok());
        });

        let credential = optional_s3_secret_from_sources(
            "SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID".to_owned(),
            None,
            "SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID_FILE".to_owned(),
            Some(path.display().to_string()),
        );

        assert!(matches!(
            credential,
            Err(StorageMigrationRuntimeError::S3CredentialLengthMismatch {
                name,
                expected_bytes: 6,
                ..
            }) if name == "SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID_FILE"
        ));
    }
}
