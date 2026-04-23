#[cfg(test)]
use std::path::PathBuf;
#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::{
    env::var,
    fs::{self, File, OpenOptions},
    io::{Error as IoError, ErrorKind, Read},
    path::{Path, PathBuf as StdPathBuf},
};

use shardline_protocol::{
    RepositoryScope, TokenClaims, TokenClaimsError, TokenCodecError, TokenScope, TokenSigner,
    unix_now_seconds_lossy,
};
use thiserror::Error;

const MAX_TOKEN_SIGNING_KEY_BYTES: u64 = 1_048_576;

#[cfg(test)]
type SigningKeyReadHook = Box<dyn FnOnce() + Send>;

#[cfg(test)]
struct SigningKeyReadHookRegistration {
    path: PathBuf,
    hook: SigningKeyReadHook,
}

#[cfg(test)]
type SigningKeyReadHookSlot = Option<SigningKeyReadHookRegistration>;

#[cfg(test)]
static BEFORE_SIGNING_KEY_READ_HOOK: LazyLock<Mutex<SigningKeyReadHookSlot>> =
    LazyLock::new(|| Mutex::new(None));

/// Runtime failure while minting a local admin token.
#[derive(Debug, Error)]
pub enum AdminTokenError {
    /// The signing key file could not be read.
    #[error("token signing key file could not be read")]
    Io(#[from] IoError),
    /// The signing key file exceeded the bounded parser ceiling.
    #[error("token signing key file exceeded the bounded parser ceiling")]
    SigningKeyTooLarge {
        /// Observed secret file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted secret file length in bytes.
        maximum_bytes: u64,
    },
    /// The signing key file changed after validation and was rejected.
    #[error("token signing key file changed during bounded read")]
    SigningKeyLengthMismatch {
        /// Validated secret file length in bytes.
        expected_bytes: u64,
        /// Observed secret file length in bytes after bounded read.
        observed_bytes: u64,
    },
    /// The token could not be created.
    #[error("token operation failed")]
    Token(#[from] TokenCodecError),
    /// The token claims were invalid.
    #[error("token claims were invalid")]
    Claims(#[from] TokenClaimsError),
    /// No signing key source was provided.
    #[error("provide --key-file or --key-env")]
    MissingSigningKeySource,
    /// More than one signing key source was provided.
    #[error("provide either --key-file or --key-env, not both")]
    SigningKeySourceConflict,
    /// The requested signing-key environment variable was not present.
    #[error("token signing key environment variable {name} was not set")]
    MissingSigningKeyEnv {
        /// Environment variable name.
        name: String,
    },
    /// The requested signing-key environment variable was present but empty.
    #[error("token signing key environment variable {name} must not be empty")]
    EmptySigningKeyEnv {
        /// Environment variable name.
        name: String,
    },
    /// The requested token lifetime overflowed the supported timestamp range.
    #[error("token ttl exceeded supported bounds")]
    TtlOverflow,
}

/// Mints a local bearer token for testing and self-hosted operation.
///
/// # Errors
///
/// Returns [`AdminTokenError`] when the key file cannot be read or the token cannot be
/// signed.
pub fn mint_admin_token(
    key_file: &Path,
    issuer: &str,
    subject: &str,
    scope: TokenScope,
    repository: RepositoryScope,
    ttl_seconds: u64,
) -> Result<String, AdminTokenError> {
    let signing_key = read_signing_key_bytes(key_file)?;
    let signer = TokenSigner::new(&signing_key)?;
    let expires_at_unix_seconds = unix_now_seconds_lossy()
        .checked_add(ttl_seconds)
        .ok_or(AdminTokenError::TtlOverflow)?;
    let claims = TokenClaims::new(issuer, subject, scope, repository, expires_at_unix_seconds)?;
    Ok(signer.sign(&claims)?)
}

/// Mints a local bearer token from either a signing-key file or an environment variable.
///
/// # Errors
///
/// Returns [`AdminTokenError`] when the selected key source cannot be read or the token
/// cannot be signed.
pub fn mint_admin_token_from_sources(
    key_file: Option<&Path>,
    key_env: Option<&str>,
    issuer: &str,
    subject: &str,
    scope: TokenScope,
    repository: RepositoryScope,
    ttl_seconds: u64,
) -> Result<String, AdminTokenError> {
    let signing_key = match (key_file, key_env) {
        (Some(path), None) => read_signing_key_bytes(path)?,
        (None, Some(name)) => read_signing_key_bytes_from_env(name)?,
        (None, None) => return Err(AdminTokenError::MissingSigningKeySource),
        (Some(_path), Some(_name)) => return Err(AdminTokenError::SigningKeySourceConflict),
    };
    let signer = TokenSigner::new(&signing_key)?;
    let expires_at_unix_seconds = unix_now_seconds_lossy()
        .checked_add(ttl_seconds)
        .ok_or(AdminTokenError::TtlOverflow)?;
    let claims = TokenClaims::new(issuer, subject, scope, repository, expires_at_unix_seconds)?;
    Ok(signer.sign(&claims)?)
}

pub(crate) fn read_signing_key_bytes(path: &Path) -> Result<Vec<u8>, AdminTokenError> {
    let mut file = open_signing_key_file(path)?;
    let metadata = file.metadata()?;
    ensure_signing_key_size_within_limit(metadata.len())?;

    run_before_signing_key_read_hook_for_tests(path);

    let bytes = read_bounded_signing_key(&mut file, metadata.len())?;
    ensure_signing_key_size_within_limit(metadata.len())?;

    Ok(bytes)
}

pub(crate) fn read_signing_key_bytes_from_env(name: &str) -> Result<Vec<u8>, AdminTokenError> {
    let value = var(name).map_err(|_error| AdminTokenError::MissingSigningKeyEnv {
        name: name.to_owned(),
    })?;
    let bytes = value.into_bytes();
    ensure_signing_key_size_within_limit(u64::try_from(bytes.len()).unwrap_or(u64::MAX))?;
    if bytes.is_empty() {
        return Err(AdminTokenError::EmptySigningKeyEnv {
            name: name.to_owned(),
        });
    }
    Ok(bytes)
}

#[cfg(unix)]
fn open_signing_key_file(path: &Path) -> Result<File, AdminTokenError> {
    use std::os::unix::fs::OpenOptionsExt;

    let resolved_path = resolve_signing_key_path(path)?;
    Ok(OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(resolved_path)?)
}

#[cfg(not(unix))]
fn open_signing_key_file(path: &Path) -> Result<File, AdminTokenError> {
    let resolved_path = resolve_signing_key_path(path)?;
    Ok(File::open(resolved_path)?)
}

fn resolve_signing_key_path(path: &Path) -> Result<StdPathBuf, AdminTokenError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_file() {
        return Ok(path.to_path_buf());
    }

    if !metadata.file_type().is_symlink() {
        return Err(AdminTokenError::Io(IoError::new(
            ErrorKind::InvalidInput,
            "token signing key path must be a regular file and must not be a symlink",
        )));
    }

    let Some(parent) = path.parent() else {
        return Err(AdminTokenError::Io(IoError::new(
            ErrorKind::InvalidInput,
            "token signing key path must have a parent directory",
        )));
    };
    let parent = fs::canonicalize(parent)?;
    let resolved = fs::canonicalize(path)?;
    let resolved_metadata = fs::metadata(&resolved)?;

    if !resolved_metadata.is_file() || !resolved.starts_with(&parent) {
        return Err(AdminTokenError::Io(IoError::new(
            ErrorKind::InvalidInput,
            "token signing key path must be a regular file and must not be a symlink",
        )));
    }

    Ok(resolved)
}

fn read_bounded_signing_key(
    file: &mut File,
    expected_length: u64,
) -> Result<Vec<u8>, AdminTokenError> {
    let capacity = usize::try_from(expected_length).unwrap_or(usize::MAX);
    let mut bytes = Vec::with_capacity(capacity);
    let mut limited = file.by_ref().take(expected_length);
    limited.read_to_end(&mut bytes)?;

    let read_length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if read_length != expected_length {
        return Err(AdminTokenError::SigningKeyLengthMismatch {
            expected_bytes: expected_length,
            observed_bytes: read_length,
        });
    }

    let mut trailing = [0_u8; 1];
    if file.read(&mut trailing)? != 0 {
        return Err(AdminTokenError::SigningKeyLengthMismatch {
            expected_bytes: expected_length,
            observed_bytes: expected_length.saturating_add(1),
        });
    }

    let observed_length = file.metadata()?.len();
    if observed_length != expected_length {
        return Err(AdminTokenError::SigningKeyLengthMismatch {
            expected_bytes: expected_length,
            observed_bytes: observed_length,
        });
    }

    Ok(bytes)
}

const fn ensure_signing_key_size_within_limit(observed_bytes: u64) -> Result<(), AdminTokenError> {
    if observed_bytes > MAX_TOKEN_SIGNING_KEY_BYTES {
        return Err(AdminTokenError::SigningKeyTooLarge {
            observed_bytes,
            maximum_bytes: MAX_TOKEN_SIGNING_KEY_BYTES,
        });
    }

    Ok(())
}

#[cfg(test)]
fn run_before_signing_key_read_hook_for_tests(path: &Path) {
    let hook = match BEFORE_SIGNING_KEY_READ_HOOK.lock() {
        Ok(mut guard) => take_signing_key_read_hook_for_path(&mut guard, path),
        Err(poisoned) => take_signing_key_read_hook_for_path(&mut poisoned.into_inner(), path),
    };

    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn take_signing_key_read_hook_for_path(
    slot: &mut SigningKeyReadHookSlot,
    path: &Path,
) -> Option<SigningKeyReadHook> {
    if !matches!(slot, Some(registration) if registration.path == path) {
        return None;
    }
    slot.take().map(|registration| registration.hook)
}

#[cfg(test)]
fn set_before_signing_key_read_hook_for_tests(path: PathBuf, hook: impl FnOnce() + Send + 'static) {
    let mut slot = match BEFORE_SIGNING_KEY_READ_HOOK.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *slot = Some(SigningKeyReadHookRegistration {
        path,
        hook: Box::new(hook),
    });
}

#[cfg(not(test))]
const fn run_before_signing_key_read_hook_for_tests(_path: &Path) {}

#[cfg(test)]
mod tests {
    use std::{
        fs::{OpenOptions, write},
        io::{ErrorKind, Write},
        path::Path,
    };

    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenScope};

    use super::{mint_admin_token, mint_admin_token_from_sources};

    #[test]
    fn mint_admin_token_rejects_oversized_signing_key_file() {
        let temp = tempfile::NamedTempFile::new();
        assert!(temp.is_ok());
        let Ok(mut temp) = temp else {
            return;
        };
        let write_result = temp.as_file_mut().write_all(&vec![0_u8; 1_048_577]);
        assert!(write_result.is_ok());
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };

        let minted = mint_admin_token(
            temp.path(),
            "issuer",
            "subject",
            TokenScope::Read,
            repository,
            60,
        );

        assert!(matches!(
            minted,
            Err(super::AdminTokenError::SigningKeyTooLarge {
                maximum_bytes: 1_048_576,
                ..
            })
        ));
    }

    #[test]
    fn mint_admin_token_reads_key_file_and_signs_token() {
        let temp = tempfile::NamedTempFile::new();
        assert!(temp.is_ok());
        let Ok(temp) = temp else {
            return;
        };
        let wrote = write(temp.path(), b"signing-key");
        assert!(wrote.is_ok());
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };

        let token = mint_admin_token(
            temp.path(),
            "local",
            "operator-1",
            TokenScope::Write,
            repository,
            60,
        );

        assert!(token.is_ok());
        let Ok(token) = token else {
            return;
        };
        assert!(token.contains('.'));
    }

    #[test]
    fn mint_admin_token_rejects_signing_key_growth_after_validation_without_retaining_appended_bytes()
     {
        let temp = tempfile::NamedTempFile::new();
        assert!(temp.is_ok());
        let Ok(temp) = temp else {
            return;
        };
        let initial = b"signing-key";
        let wrote = write(temp.path(), initial);
        assert!(wrote.is_ok());
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };

        let path = temp.path().to_path_buf();
        super::set_before_signing_key_read_hook_for_tests(path.clone(), move || {
            let opened = OpenOptions::new().append(true).open(&path);
            assert!(opened.is_ok());
            let Ok(mut file) = opened else {
                return;
            };
            let appended = file.write_all(b"-rotated");
            assert!(appended.is_ok());
        });

        let minted = mint_admin_token(
            temp.path(),
            "issuer",
            "subject",
            TokenScope::Read,
            repository,
            60,
        );

        assert!(matches!(
            minted,
            Err(super::AdminTokenError::SigningKeyLengthMismatch {
                expected_bytes,
                observed_bytes,
            }) if expected_bytes == u64::try_from(initial.len()).unwrap_or(u64::MAX)
                && observed_bytes > expected_bytes
        ));
    }

    #[cfg(unix)]
    #[test]
    fn mint_admin_token_accepts_projected_secret_symlinked_signing_key_file() {
        use std::os::unix::fs::symlink;

        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let data_dir = sandbox.path().join("..data");
        let created = std::fs::create_dir(&data_dir);
        assert!(created.is_ok());
        let target = data_dir.join("real.key");
        let link = sandbox.path().join("linked.key");
        let wrote = write(&target, b"signing-key");
        assert!(wrote.is_ok());
        let linked = symlink(std::path::Path::new("..data").join("real.key"), &link);
        assert!(linked.is_ok());
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };

        let minted = mint_admin_token(&link, "issuer", "subject", TokenScope::Read, repository, 60);

        assert!(minted.is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn mint_admin_token_rejects_symlinked_signing_key_file_outside_directory() {
        use std::os::unix::fs::symlink;

        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let outside = tempfile::NamedTempFile::new();
        assert!(outside.is_ok());
        let Ok(outside) = outside else {
            return;
        };
        let wrote = write(outside.path(), b"signing-key");
        assert!(wrote.is_ok());
        let link = sandbox.path().join("linked.key");
        let linked = symlink(outside.path(), &link);
        assert!(linked.is_ok());
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };

        let minted = mint_admin_token(&link, "issuer", "subject", TokenScope::Read, repository, 60);

        assert!(matches!(
            minted,
            Err(super::AdminTokenError::Io(error)) if error.kind() == ErrorKind::InvalidInput
        ));
    }

    #[test]
    fn mint_admin_token_rejects_missing_signing_key_source() {
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };

        let token = mint_admin_token_from_sources(
            None,
            None,
            "local",
            "operator-1",
            TokenScope::Write,
            repository,
            60,
        );

        assert!(matches!(
            token,
            Err(super::AdminTokenError::MissingSigningKeySource)
        ));
    }

    #[test]
    fn mint_admin_token_rejects_multiple_signing_key_sources() {
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };

        let token = mint_admin_token_from_sources(
            Some(Path::new("/tmp/key")),
            Some("SHARDLINE_TOKEN_SIGNING_KEY"),
            "local",
            "operator-1",
            TokenScope::Write,
            repository,
            60,
        );

        assert!(matches!(
            token,
            Err(super::AdminTokenError::SigningKeySourceConflict)
        ));
    }
}
