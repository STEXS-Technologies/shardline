use thiserror::Error;

/// Maximum byte length for a validated file identifier.
const MAX_IDENTIFIER_BYTES: usize = 1024;

/// Validates that a content hash is exactly 64 lowercase hex characters.
///
/// # Errors
///
/// Returns an error with the given `error_fn` when the hash is malformed.
pub fn validate_content_hash_with<E>(value: &str, error_fn: fn() -> E) -> Result<(), E> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(error_fn());
    }
    Ok(())
}

/// Validates that a file identifier is safe for use as a single path component.
///
/// Rejects empty or `.` values, absolute paths, `..` traversal, backslashes,
/// control characters, and identifiers longer than 1024 bytes.
///
/// # Examples
///
/// ```
/// use shardline_validation::validate_identifier;
///
/// assert!(validate_identifier("models.bin").is_ok());
/// assert!(validate_identifier("nested/model.bin").is_err(), "no path separators");
/// assert!(validate_identifier("../secret").is_err(), "no traversal");
/// assert!(validate_identifier("/etc/passwd").is_err(), "no absolute paths");
/// ```
///
/// # Errors
///
/// Returns [`ValidateIdentifierError`] if the identifier is empty, contains
/// path separators, traversal sequences, control characters, or exceeds the
/// maximum byte length.
pub fn validate_identifier(value: &str) -> Result<(), ValidateIdentifierError> {
    if value.trim().is_empty()
        || value == "."
        || value.len() > MAX_IDENTIFIER_BYTES
        || value.starts_with('/')
        || value.contains("..")
        || value.contains('\\')
        || value.contains('/')
        || value.chars().any(char::is_control)
    {
        return Err(ValidateIdentifierError);
    }

    Ok(())
}

/// File identifier validation failure.
#[derive(Debug, Clone, Copy, Error)]
#[error("file identifier must be relative and must not contain traversal or control characters")]
pub struct ValidateIdentifierError;

/// Validates that a content hash is exactly 64 lowercase hex characters.
///
/// # Examples
///
/// ```
/// use shardline_validation::validate_content_hash;
///
/// let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
/// assert!(validate_content_hash(hash).is_ok());
/// assert!(validate_content_hash("short").is_err());
/// ```
///
/// # Errors
///
/// Returns [`ValidateContentHashError`] if the hash is malformed.
pub fn validate_content_hash(value: &str) -> Result<(), ValidateContentHashError> {
    validate_content_hash_with(value, || ValidateContentHashError)
}

/// Content hash validation failure.
#[derive(Debug, Clone, Copy, Error)]
#[error("content hash must be 64 hexadecimal characters")]
pub struct ValidateContentHashError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_validation_cases_and_byte_boundaries() {
        for (value, valid) in [
            ("hello.txt".to_owned(), true),
            ("file.name.txt".to_owned(), true),
            (String::new(), false),
            ("   ".to_owned(), false),
            (".".to_owned(), false),
            ("/etc/passwd".to_owned(), false),
            ("foo/../bar".to_owned(), false),
            ("foo\\bar".to_owned(), false),
            ("foo\tbar".to_owned(), false),
            ("foo\0bar".to_owned(), false),
            ("\0".to_owned(), false),
            ("null\0end".to_owned(), false),
            ("a".repeat(1024), true),
            ("a".repeat(1025), false),
            ("é".repeat(512), true),
            ("é".repeat(513), false),
        ] {
            assert_eq!(validate_identifier(&value).is_ok(), valid, "{value:?}");
        }
    }

    #[test]
    fn content_hash_validators_share_cases_and_preserve_custom_errors() {
        for (value, valid) in [
            ("a".repeat(64), true),
            ("0123456789abcdef".repeat(4), true),
            ("abc123".to_owned(), false),
            ("a".repeat(65), false),
            ("A".repeat(64), false),
            (format!("{}g", "a".repeat(63)), false),
            (format!("{}ga", "a".repeat(62)), false),
            (String::new(), false),
            ("a".repeat(63), false),
            ("é".repeat(32), false),
        ] {
            assert_eq!(validate_content_hash(&value).is_ok(), valid, "{value:?}");
            assert_eq!(
                validate_content_hash_with(&value, || "invalid hash"),
                if valid { Ok(()) } else { Err("invalid hash") },
                "{value:?}",
            );
        }
        use std::sync::atomic::{AtomicUsize, Ordering};
        static ERROR_CALLS: AtomicUsize = AtomicUsize::new(0);
        let error_factory = || ERROR_CALLS.fetch_add(1, Ordering::Relaxed);
        assert_eq!(
            validate_content_hash_with(&"a".repeat(64), error_factory),
            Ok(())
        );
        assert_eq!(ERROR_CALLS.load(Ordering::Relaxed), 0);
        assert_eq!(validate_content_hash_with("invalid", error_factory), Err(0));
        assert_eq!(ERROR_CALLS.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn validate_identifier_error_display() {
        let msg = ValidateIdentifierError.to_string();
        assert!(!msg.is_empty());
        assert!(msg.contains("identifier"));
    }

    #[test]
    fn validate_content_hash_error_display() {
        let msg = ValidateContentHashError.to_string();
        assert!(!msg.is_empty());
        assert!(msg.contains("hexadecimal"));
    }
}
