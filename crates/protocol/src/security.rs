use std::fmt;

use serde::Deserialize;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Zeroizing byte-oriented secret material.
#[derive(Clone, PartialEq, Eq, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct SecretBytes(Vec<u8>);

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

impl fmt::Debug for SecretBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("***")
    }
}

/// Zeroizing UTF-8 secret material.
#[derive(Clone, PartialEq, Eq, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct SecretString(String);

impl SecretString {
    /// Wraps owned secret text.
    #[must_use]
    pub const fn new(secret: String) -> Self {
        Self(secret)
    }

    /// Copies borrowed secret text into zeroizing storage.
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

    #[test]
    fn secret_bytes_debug_redacts_contents() {
        let secret = SecretBytes::from_slice(b"signing-key");

        assert_eq!(format!("{secret:?}"), "***");
    }

    #[test]
    fn secret_bytes_exposes_underlying_bytes() {
        let secret = SecretBytes::from_slice(b"signing-key");

        assert_eq!(secret.expose_secret(), b"signing-key");
    }

    #[test]
    fn secret_string_debug_redacts_contents() {
        let secret = SecretString::from_secret("bootstrap-token");

        assert_eq!(format!("{secret:?}"), "***");
    }

    #[test]
    fn secret_string_exposes_underlying_text() {
        let secret = SecretString::from_secret("bootstrap-token");

        assert_eq!(secret.expose_secret(), "bootstrap-token");
    }
}
