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

    #[test]
    fn secret_bytes_clone_produces_equal_data() {
        let a = SecretBytes::from_slice(b"secret-data");
        let b = a.clone();
        assert_eq!(a.expose_secret(), b.expose_secret());
    }

    #[test]
    fn secret_string_clone_produces_equal_data() {
        let a = SecretString::from_secret("secret-text");
        let b = a.clone();
        assert_eq!(a.expose_secret(), b.expose_secret());
    }

    #[test]
    fn secret_bytes_partial_eq_compares_content() {
        let a = SecretBytes::from_slice(b"same");
        let b = SecretBytes::from_slice(b"same");
        let c = SecretBytes::from_slice(b"different");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn secret_string_partial_eq_compares_content() {
        let a = SecretString::from_secret("same");
        let b = SecretString::from_secret("same");
        let c = SecretString::from_secret("different");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn secret_bytes_len_various_sizes() {
        assert_eq!(SecretBytes::new(vec![]).len(), 0);
        assert_eq!(SecretBytes::from_slice(b"a").len(), 1);
        assert_eq!(SecretBytes::from_slice(b"hello").len(), 5);
        assert_eq!(SecretBytes::from_slice(&[0; 100]).len(), 100);
    }

    #[test]
    fn secret_string_empty_vs_non_empty() {
        assert!(SecretString::new(String::new()).is_empty());
        assert!(!SecretString::from_secret("data").is_empty());
    }

    #[test]
    fn secret_bytes_is_empty_various() {
        assert!(SecretBytes::new(vec![]).is_empty());
        assert!(!SecretBytes::from_slice(b"x").is_empty());
    }

    #[test]
    fn secret_bytes_debug_no_content_leak() {
        let secret = SecretBytes::from_slice(b"hunter2-password-12345");
        let debug = format!("{secret:?}");
        assert_eq!(debug, "***");
        assert!(!debug.contains("hunter2"));
    }

    #[test]
    fn secret_string_debug_no_content_leak() {
        let secret = SecretString::from_secret("my-secret-api-token");
        let debug = format!("{secret:?}");
        assert_eq!(debug, "***");
        assert!(!debug.contains("api-token"));
    }

    #[test]
    fn secret_bytes_zeroize_clears_memory() {
        use zeroize::Zeroize;
        let mut secret = SecretBytes::from_slice(b"hello-world");
        assert!(!secret.is_empty());
        assert_eq!(secret.len(), 11);
        // Explicitly zeroize the buffer and verify the underlying Vec is cleared.
        secret.zeroize();
        assert!(secret.is_empty());
        assert_eq!(secret.len(), 0);
        assert!(secret.expose_secret().is_empty());
    }
}
