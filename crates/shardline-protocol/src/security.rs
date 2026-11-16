use std::fmt;

use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Zeroizing byte-oriented secret material.
///
/// Constant-time equality is used for [`PartialEq`] to avoid timing side-channels.
///
/// # Examples
///
/// ```
/// use shardline_protocol::SecretBytes;
///
/// let secret = SecretBytes::from_slice(b"my-secret-key");
/// assert_eq!(secret.expose_secret(), b"my-secret-key");
/// assert!(!secret.is_empty());
/// ```
#[derive(Clone, Eq, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct SecretBytes(Vec<u8>);

impl PartialEq for SecretBytes {
    fn eq(&self, other: &Self) -> bool {
        self.0.ct_eq(&other.0).into()
    }
}

impl SecretBytes {
    /// Wraps owned secret bytes.
    #[must_use]
    pub const fn new(secret: Vec<u8>) -> Self {
        Self(secret)
    }

    /// Copies borrowed secret bytes into zeroizing storage.
    #[must_use]
    pub fn from_slice(secret: &[u8]) -> Self {
        Self(secret.to_vec())
    }

    /// Returns the secret bytes.
    #[must_use]
    pub fn expose_secret(&self) -> &[u8] {
        &self.0
    }

    /// Returns the secret length in bytes.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the secret is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<[u8]> for SecretBytes {
    fn as_ref(&self) -> &[u8] {
        self.expose_secret()
    }
}

impl From<Vec<u8>> for SecretBytes {
    fn from(secret: Vec<u8>) -> Self {
        Self(secret)
    }
}

impl fmt::Debug for SecretBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("***")
    }
}

/// Zeroizing UTF-8 secret material.
///
/// Constant-time equality is used for [`PartialEq`] to avoid timing side-channels.
///
/// # Examples
///
/// ```
/// use shardline_protocol::SecretString;
///
/// let secret = SecretString::from_secret("bootstrap-token");
/// assert_eq!(secret.expose_secret(), "bootstrap-token");
/// ```
#[derive(Clone, Eq, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct SecretString(String);

impl PartialEq for SecretString {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_bytes().ct_eq(other.0.as_bytes()).into()
    }
}

impl SecretString {
    /// Wraps owned secret text.
    #[must_use]
    pub const fn new(secret: String) -> Self {
        Self(secret)
    }

    /// Copies borrowed secret text into zeroizing storage.
    ///
    /// Note: the input `&str` memory is not zeroed; callers should independently
    /// clear their source buffer if it contains secret data.
    #[must_use]
    pub fn from_secret(secret: &str) -> Self {
        Self(secret.to_owned())
    }

    /// Returns the secret text.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }

    /// Returns whether the secret text is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<str> for SecretString {
    fn as_ref(&self) -> &str {
        self.expose_secret()
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("***")
    }
}

#[cfg(test)]
mod tests {
    use super::{SecretBytes, SecretString};
    use zeroize::Zeroize;

    #[test]
    fn byte_secret_constructors_accessors_and_redaction() {
        let cases: &[&[u8]] = &[
            b"",
            b"test-signing-key-32-bytes-long!!",
            &[1, 2, 3, 4],
            b"abc",
            b"hello",
            b"secret-data",
            b"same",
            b"different",
            b"a",
            &[0; 100],
            b"x",
            b"hunter2-password-12345",
        ];
        for value in cases {
            for secret in [
                SecretBytes::from_slice(value),
                SecretBytes::new(value.to_vec()),
                SecretBytes::from(value.to_vec()),
            ] {
                assert_eq!(secret.expose_secret(), *value);
                assert_eq!(secret.as_ref(), *value);
                assert_eq!(secret.len(), value.len());
                assert_eq!(secret.is_empty(), value.is_empty());
                assert_eq!(secret, secret.clone());
                assert_eq!(format!("{secret:?}"), "***");
            }
        }
        let same = SecretBytes::from_slice(b"same");
        assert_ne!(same, SecretBytes::from_slice(b"different"));
        assert_ne!(same, SecretBytes::from_slice(b"diff"));
    }

    #[test]
    fn text_secret_constructors_accessors_and_redaction() {
        for value in [
            "",
            "bootstrap-token",
            "owned",
            "data",
            "text",
            "secret-text",
            "same",
            "different",
            "my-secret-api-token",
        ] {
            for secret in [
                SecretString::from_secret(value),
                SecretString::new(value.to_owned()),
            ] {
                assert_eq!(secret.expose_secret(), value);
                assert_eq!(secret.as_ref(), value);
                assert_eq!(secret.is_empty(), value.is_empty());
                assert_eq!(secret, secret.clone());
                assert_eq!(format!("{secret:?}"), "***");
            }
        }
        let same = SecretString::from_secret("same");
        assert_ne!(same, SecretString::from_secret("different"));
        assert_ne!(same, SecretString::from_secret("diff"));
    }

    #[test]
    fn explicit_zeroization_clears_byte_and_text_secrets() {
        let mut bytes = SecretBytes::from_slice(b"hello-world");
        assert_eq!(bytes.len(), 11);
        assert!(!bytes.is_empty());
        bytes.zeroize();
        assert!(bytes.is_empty());
        assert_eq!(bytes.len(), 0);
        assert!(bytes.expose_secret().is_empty());

        let mut text = SecretString::from_secret("hello-world");
        text.zeroize();
        assert!(text.is_empty());
        assert!(text.expose_secret().is_empty());
    }
}
