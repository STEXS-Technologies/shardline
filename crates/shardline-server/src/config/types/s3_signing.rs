use shardline_protocol::SecretString;

use super::{ServerConfig, ServerConfigError};
use crate::config::secrets::read_secret_file_bytes;

/// An S3 signing credential bound to an existing Shardline authorization token.
/// Secret fields are redacted from Debug output and zeroized on drop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3SigningIdentity {
    pub(crate) access_key: String,
    pub(crate) secret_key: SecretString,
    pub(crate) token: SecretString,
    pub(crate) region: String,
    pub(crate) session_token: Option<SecretString>,
}

impl S3SigningIdentity {
    /// Creates signing material. The bound token is still verified on every
    /// request, including its expiry, repository and permission checks.
    ///
    /// # Errors
    /// Rejects empty, oversized or malformed credential configuration.
    pub fn new(
        access_key: String,
        secret_key: SecretString,
        token: SecretString,
        region: String,
    ) -> Result<Self, ServerConfigError> {
        if access_key.is_empty()
            || access_key.len() > 128
            || !access_key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
            || secret_key.expose_secret().len() < 16
            || secret_key.expose_secret().len() > 4096
            || token.expose_secret().is_empty()
            || token.expose_secret().len() > 65536
            || region.is_empty()
            || region.len() > 64
            || !region
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(ServerConfigError::ConfigFileError(
                "invalid S3 signing identity".to_owned(),
            ));
        }
        Ok(Self {
            access_key,
            secret_key,
            token,
            region,
            session_token: None,
        })
    }

    /// Requires a signed session token as well as the access key and secret.
    /// Its lifetime is bounded by the bound Shardline authorization token.
    ///
    /// # Errors
    /// Rejects empty or oversized session tokens.
    pub fn with_session_token(
        mut self,
        session_token: SecretString,
    ) -> Result<Self, ServerConfigError> {
        if session_token.expose_secret().is_empty() || session_token.expose_secret().len() > 65536 {
            return Err(ServerConfigError::ConfigFileError(
                "invalid S3 signing session token".to_owned(),
            ));
        }
        self.session_token = Some(session_token);
        Ok(self)
    }
}

impl ServerConfig {
    /// Loads a bounded JSON array of signing identities from a secret file.
    ///
    /// # Errors
    /// Rejects unreadable, oversized, changing, malformed or duplicate input.
    pub fn with_s3_signing_identities_file(
        mut self,
        path: &std::path::Path,
    ) -> Result<Self, ServerConfigError> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Entry {
            access_key: String,
            secret_key: SecretString,
            token: SecretString,
            region: String,
            #[serde(default)]
            session_token: Option<SecretString>,
        }
        let invalid =
            || ServerConfigError::ConfigFileError("invalid S3 signing identity file".to_owned());
        let bytes = read_secret_file_bytes(
            path,
            1024 * 1024,
            false,
            |_error| {
                ServerConfigError::ConfigFileError(
                    "S3 signing identity file could not be read".to_owned(),
                )
            },
            |_observed, _maximum| {
                ServerConfigError::ConfigFileError(
                    "S3 signing identity file exceeds 1 MiB".to_owned(),
                )
            },
            |_expected, _observed| {
                ServerConfigError::ConfigFileError(
                    "S3 signing identity file changed during read".to_owned(),
                )
            },
        )?;
        let entries: Vec<Entry> =
            serde_json::from_slice(bytes.expose_secret()).map_err(|_invalid_json| invalid())?;
        for entry in entries {
            let mut identity = S3SigningIdentity::new(
                entry.access_key,
                entry.secret_key,
                entry.token,
                entry.region,
            )?;
            if let Some(session_token) = entry.session_token {
                identity = identity.with_session_token(session_token)?;
            }
            self = self.with_s3_signing_identity(identity)?;
        }
        Ok(self)
    }

    /// Adds a signing identity without changing the existing bearer/token
    /// bridge. A registered key always requires a valid signature.
    ///
    /// # Errors
    /// Rejects duplicate keys and excessive identity counts.
    pub fn with_s3_signing_identity(
        mut self,
        identity: S3SigningIdentity,
    ) -> Result<Self, ServerConfigError> {
        if self.s3_signing_identities.len() >= 1024
            || self
                .s3_signing_identities
                .iter()
                .any(|current| current.access_key == identity.access_key)
        {
            return Err(ServerConfigError::ConfigFileError(
                "duplicate or excessive S3 signing identities".to_owned(),
            ));
        }
        self.s3_signing_identities.push(identity);
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn identity() -> S3SigningIdentity {
        S3SigningIdentity::new(
            "KEY".to_owned(),
            SecretString::new("private-signing-secret".to_owned()),
            SecretString::new("private-bound-token".to_owned()),
            "us-east-1".to_owned(),
        )
        .unwrap()
    }

    fn config() -> ServerConfig {
        ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://localhost".to_owned(),
            std::path::PathBuf::from("."),
            std::num::NonZeroUsize::new(65536).unwrap(),
        )
    }

    #[test]
    fn signing_identity_file_is_bounded_validated_and_redacted() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("identities.json");
        let entry = r#"{"access_key":"KEY","secret_key":"private-signing-secret","token":"private-bound-token","region":"us-east-1"}"#;
        std::fs::write(&path, format!("[{entry}]")).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let loaded = config().with_s3_signing_identities_file(&path).unwrap();
        assert_eq!(loaded.s3_signing_identities.len(), 1);
        let session_entry = entry.replace(
            "\"region\"",
            "\"session_token\":\"private-session-token\",\"region\"",
        );
        std::fs::write(&path, format!("[{session_entry}]")).unwrap();
        let loaded = config().with_s3_signing_identities_file(&path).unwrap();
        assert!(
            loaded
                .s3_signing_identities
                .first()
                .unwrap()
                .session_token
                .is_some()
        );
        for invalid in [
            format!("[{entry},{entry}]"),
            "[{\"secret_key\":\"private-signing-secret\"}]".to_owned(),
            "not json".to_owned(),
        ] {
            std::fs::write(&path, invalid).unwrap();
            let error = config()
                .with_s3_signing_identities_file(&path)
                .unwrap_err()
                .to_string();
            assert!(!error.contains("private-signing-secret"));
        }
        std::fs::File::create(&path)
            .unwrap()
            .set_len(1024 * 1024 + 1)
            .unwrap();
        assert!(config().with_s3_signing_identities_file(&path).is_err());
    }

    #[test]
    fn signing_identity_debug_redacts_secrets() {
        let debug = format!(
            "{:?}",
            identity()
                .with_session_token(SecretString::new("private-session-token".to_owned()))
                .unwrap()
        );
        assert!(debug.contains("KEY"));
        assert!(!debug.contains("private-signing-secret"));
        assert!(!debug.contains("private-bound-token"));
        assert!(!debug.contains("private-session-token"));
    }

    #[test]
    fn signing_config_rejects_duplicates_and_missing_authentication() {
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://localhost".to_owned(),
            std::path::PathBuf::from("."),
            std::num::NonZeroUsize::new(65536).unwrap(),
        )
        .with_s3_signing_identity(identity())
        .unwrap();
        assert!(config.validate_runtime_requirements().is_err());
        assert!(config.with_s3_signing_identity(identity()).is_err());
    }

    #[test]
    fn signing_identity_rejects_malformed_keys_and_short_secrets() {
        assert!(
            identity()
                .with_session_token(SecretString::new(String::new()))
                .is_err()
        );
        for key in ["", "bad/key", "white space"] {
            assert!(
                S3SigningIdentity::new(
                    key.to_owned(),
                    identity().secret_key,
                    identity().token,
                    "us-east-1".to_owned()
                )
                .is_err()
            );
        }
        assert!(
            S3SigningIdentity::new(
                "KEY".to_owned(),
                SecretString::new("short".to_owned()),
                identity().token,
                "us-east-1".to_owned()
            )
            .is_err()
        );
    }
}
