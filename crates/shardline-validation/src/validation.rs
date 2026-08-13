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
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(ValidateContentHashError);
    }

    Ok(())
}

/// Content hash validation failure.
#[derive(Debug, Clone, Copy, Error)]
#[error("content hash must be 64 hexadecimal characters")]
pub struct ValidateContentHashError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_identifier_accepts_simple_name() {
        assert!(validate_identifier("hello.txt").is_ok());
    }

    #[test]
    fn validate_identifier_accepts_dotted_name() {
        assert!(validate_identifier("file.name.txt").is_ok());
    }

    #[test]
    fn validate_identifier_rejects_empty() {
        assert!(validate_identifier("").is_err());
    }

    #[test]
    fn validate_identifier_rejects_whitespace_only() {
        assert!(validate_identifier("   ").is_err());
    }

    #[test]
    fn validate_identifier_rejects_dot() {
        assert!(validate_identifier(".").is_err());
    }

    #[test]
    fn validate_identifier_rejects_leading_slash() {
        assert!(validate_identifier("/etc/passwd").is_err());
    }

    #[test]
    fn validate_identifier_rejects_traversal() {
        assert!(validate_identifier("foo/../bar").is_err());
    }

    #[test]
    fn validate_identifier_rejects_backslash() {
        assert!(validate_identifier("foo\\bar").is_err());
    }

    #[test]
    fn validate_identifier_rejects_control_char() {
        assert!(validate_identifier("foo\tbar").is_err());
    }

    #[test]
    fn validate_identifier_rejects_null_byte() {
        assert!(validate_identifier("foo\0bar").is_err());
        assert!(validate_identifier("\0").is_err());
        assert!(validate_identifier("null\0end").is_err());
    }

    #[test]
    fn validate_content_hash_accepts_valid_hash() {
        let hash = "a".repeat(64);
        assert!(validate_content_hash(&hash).is_ok());
    }

    #[test]
    fn validate_content_hash_rejects_too_short() {
        assert!(validate_content_hash("abc123").is_err());
    }

    #[test]
    fn validate_content_hash_rejects_too_long() {
        let hash = "a".repeat(65);
        assert!(validate_content_hash(&hash).is_err());
    }

    #[test]
    fn validate_content_hash_rejects_uppercase() {
        let hash = "A".repeat(64);
        assert!(validate_content_hash(&hash).is_err());
    }

    #[test]
    fn validate_content_hash_rejects_non_hex() {
        let mut hash = "a".repeat(64);
        hash.push('g');
        hash.remove(0);
        assert!(validate_content_hash(&hash).is_err());
    }

    #[test]
    fn validate_content_hash_with_valid_64_hex() {
        let hash = "0123456789abcdef".repeat(4);
        assert!(validate_content_hash_with(&hash, || ()).is_ok());
    }

    #[test]
    fn validate_content_hash_with_too_short() {
        assert!(validate_content_hash_with("abc123", || ()).is_err());
    }

    #[test]
    fn validate_content_hash_with_uppercase_rejected() {
        let hash = "A".repeat(64);
        assert!(validate_content_hash_with(&hash, || ()).is_err());
    }

    #[test]
    fn validate_content_hash_with_non_hex_rejected() {
        let hash = format!("{}g{}", "a".repeat(62), "a");
        assert!(validate_content_hash_with(&hash, || ()).is_err());
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
