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
        let secret = SecretBytes::from_slice(b"test-signing-key-32-bytes-long!!");

        assert_eq!(format!("{secret:?}"), "***");
    }

    #[test]
    fn secret_bytes_exposes_underlying_bytes() {
        let secret = SecretBytes::from_slice(b"test-signing-key-32-bytes-long!!");

        assert_eq!(secret.expose_secret(), b"test-signing-key-32-bytes-long!!");
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

    #[test]
    fn secret_bytes_new_wraps_owned() {
        let data = vec![1, 2, 3, 4];
        let secret = SecretBytes::new(data);
        assert_eq!(secret.expose_secret(), &[1, 2, 3, 4]);
    }

    #[test]
    fn secret_bytes_len_and_is_empty() {
        let empty = SecretBytes::new(Vec::new());
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);

        let non_empty = SecretBytes::from_slice(b"abc");
        assert!(!non_empty.is_empty());
        assert_eq!(non_empty.len(), 3);
    }

    #[test]
    fn secret_bytes_as_ref() {
        use std::convert::AsRef;
        let secret = SecretBytes::from_slice(b"hello");
        let bytes: &[u8] = secret.as_ref();
        assert_eq!(bytes, b"hello");
    }

    #[test]
    fn secret_string_new_wraps_owned() {
        let secret = SecretString::new("owned".to_owned());
        assert_eq!(secret.expose_secret(), "owned");
    }

    #[test]
    fn secret_string_is_empty() {
        let empty = SecretString::new(String::new());
        assert!(empty.is_empty());

        let non_empty = SecretString::from_secret("data");
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn secret_string_as_ref() {
        use std::convert::AsRef;
        let secret = SecretString::from_secret("text");
        let s: &str = secret.as_ref();
        assert_eq!(s, "text");
    }
}
