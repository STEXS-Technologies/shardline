use shardline_index::parse_xet_hash_hex;

use crate::error::XetAdapterError;

pub const XORB_TRANSFER_NAMESPACE: &str = "default";
pub const XORB_TRANSFER_ROUTE: &str = "/transfer/xorb/{prefix}/{hash}";
pub const XET_READ_TOKEN_ROUTE: &str = "/api/{provider}/{owner}/{repo}/xet-read-token/{rev}";
pub const XET_WRITE_TOKEN_ROUTE: &str = "/api/{provider}/{owner}/{repo}/xet-write-token/{rev}";
pub const XET_TREE_ROUTE: &str = "/api/{provider}/{owner}/{repo}/tree/{rev}";
pub const XET_PATH_ROUTE: &str = "/api/{provider}/{owner}/{repo}/path/{rev}/{*path}";
pub const XET_REVISIONS_ROUTE: &str = "/api/{provider}/{owner}/{repo}/revisions";
pub const XET_REVISION_ROUTE: &str = "/api/{provider}/{owner}/{repo}/revisions/{rev}";

/// Validates that a hash path is a 64-character lowercase hex digest.
///
/// Hash paths appear in xorb transfer URLs and object keys; this rejects
/// malformed values before they reach storage.
///
/// # Examples
///
/// ```
/// use shardline_xet_adapter::validate_hash_path;
///
/// let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
/// assert!(validate_hash_path(hash).is_ok());
/// assert!(validate_hash_path("short").is_err());
/// ```
///
/// # Errors
///
/// Returns an error when the hash is not valid hex.
pub fn validate_hash_path(value: &str) -> Result<(), XetAdapterError> {
    parse_xet_hash_hex(value)?;
    Ok(())
}

/// Validates an optional content hash, passing when the value is `None`.
///
/// Useful for request bodies where a content hash may be omitted.
///
/// # Examples
///
/// ```
/// use shardline_xet_adapter::validate_optional_content_hash;
///
/// let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
/// assert!(validate_optional_content_hash(Some(hash)).is_ok());
/// assert!(validate_optional_content_hash(None).is_ok());
/// assert!(validate_optional_content_hash(Some("bad")).is_err());
/// ```
///
/// # Errors
///
/// Returns an error when the provided hash is not valid hex.
pub fn validate_optional_content_hash(content_hash: Option<&str>) -> Result<(), XetAdapterError> {
    if let Some(content_hash) = content_hash {
        validate_hash_path(content_hash)?;
    }

    Ok(())
}

/// Validates the transfer namespace prefix for xorb transfer routes.
///
/// # Examples
///
/// ```
/// use shardline_xet_adapter::validate_xorb_transfer_namespace;
///
/// assert!(validate_xorb_transfer_namespace("default").is_ok());
/// assert!(validate_xorb_transfer_namespace("custom").is_err());
/// ```
///
/// # Errors
///
/// Returns an error when the prefix does not match the expected transfer namespace.
pub fn validate_xorb_transfer_namespace(prefix: &str) -> Result<(), XetAdapterError> {
    if prefix != XORB_TRANSFER_NAMESPACE {
        return Err(XetAdapterError::InvalidXorbPrefix);
    }

    Ok(())
}

/// Builds the xorb transfer URL for a hash on the given server base URL.
///
/// # Examples
///
/// ```
/// use shardline_xet_adapter::build_xorb_transfer_url;
///
/// let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
/// let url = build_xorb_transfer_url("http://localhost:8080/", hash);
/// assert_eq!(
///     url,
///     "http://localhost:8080/transfer/xorb/default/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
/// );
/// ```
#[must_use]
pub fn build_xorb_transfer_url(public_base_url: &str, hash_hex: &str) -> String {
    let trimmed_base_url = public_base_url.trim_end_matches('/');
    let mut url = String::with_capacity(
        trimmed_base_url
            .len()
            .saturating_add("/transfer/xorb/default/".len())
            .saturating_add(hash_hex.len()),
    );
    url.push_str(trimmed_base_url);
    url.push_str("/transfer/xorb/");
    url.push_str(XORB_TRANSFER_NAMESPACE);
    url.push('/');
    url.push_str(hash_hex);
    url
}

#[cfg(test)]
mod tests {
    use super::{
        XORB_TRANSFER_NAMESPACE, build_xorb_transfer_url, validate_hash_path,
        validate_optional_content_hash, validate_xorb_transfer_namespace,
    };
    use crate::error::XetAdapterError;

    #[test]
    fn validate_hash_path_accepts_valid_64_char_hex_hash() {
        let hash = "abcdef0123456789".repeat(4);
        assert_eq!(hash.len(), 64);
        assert!(validate_hash_path(&hash).is_ok());
    }

    #[test]
    fn validate_hash_path_rejects_uppercase_characters() {
        let hash = "AB".repeat(32);
        assert!(validate_hash_path(&hash).is_err());
    }

    #[test]
    fn validate_hash_path_rejects_wrong_length() {
        // 63 chars instead of 64
        let hash = "a".repeat(63);
        assert!(validate_hash_path(&hash).is_err());
        // 65 chars instead of 64
        let hash = "a".repeat(65);
        assert!(validate_hash_path(&hash).is_err());
    }

    #[test]
    fn validate_optional_content_hash_validates_present_hash() {
        assert!(validate_optional_content_hash(Some(&"a".repeat(64))).is_ok());
        assert!(validate_optional_content_hash(Some("not-a-hash")).is_err());
    }

    #[test]
    fn build_xorb_transfer_url_uses_default_namespace() {
        let url = build_xorb_transfer_url("http://127.0.0.1:8080/", &"a".repeat(64));
        assert_eq!(
            url,
            "http://127.0.0.1:8080/transfer/xorb/default/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[test]
    fn validate_xorb_transfer_namespace_rejects_non_default_namespace() {
        assert!(matches!(
            validate_xorb_transfer_namespace("other"),
            Err(XetAdapterError::InvalidXorbPrefix)
        ));
        assert!(validate_xorb_transfer_namespace(XORB_TRANSFER_NAMESPACE).is_ok());
    }

    #[test]
    fn validate_optional_content_hash_accepts_absent_hash() {
        assert!(validate_optional_content_hash(None).is_ok());
    }

    #[test]
    fn validate_hash_path_rejects_invalid_hash() {
        assert!(validate_hash_path("not-a-hash").is_err());
    }

    #[test]
    fn build_xorb_transfer_url_without_trailing_slash_on_base() {
        let url = build_xorb_transfer_url("http://example.com", &"a".repeat(64));
        assert!(url.starts_with("http://example.com/transfer/xorb/default/"));
        assert_eq!(
            url.len(),
            "http://example.com/transfer/xorb/default/".len() + 64
        );
    }

    #[test]
    fn build_xorb_transfer_url_empty_base_url() {
        let url = build_xorb_transfer_url("", &"b".repeat(64));
        assert!(url.starts_with("/transfer/xorb/default/"));
        assert_eq!(url.len(), "/transfer/xorb/default/".len() + 64);
    }
}
