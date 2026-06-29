use shardline_index::parse_xet_hash_hex;

use crate::error::XetAdapterError;

pub const XORB_TRANSFER_NAMESPACE: &str = "default";
pub const XORB_TRANSFER_ROUTE: &str = "/transfer/xorb/{prefix}/{hash}";
pub const XET_READ_TOKEN_ROUTE: &str = "/api/{provider}/{owner}/{repo}/xet-read-token/{rev}";
pub const XET_WRITE_TOKEN_ROUTE: &str = "/api/{provider}/{owner}/{repo}/xet-write-token/{rev}";

pub fn validate_hash_path(value: &str) -> Result<(), XetAdapterError> {
    parse_xet_hash_hex(value)?;
    Ok(())
}

pub fn validate_optional_content_hash(content_hash: Option<&str>) -> Result<(), XetAdapterError> {
    if let Some(content_hash) = content_hash {
        validate_hash_path(content_hash)?;
    }

    Ok(())
}

pub fn validate_xorb_transfer_namespace(prefix: &str) -> Result<(), XetAdapterError> {
    if prefix != XORB_TRANSFER_NAMESPACE {
        return Err(XetAdapterError::InvalidXorbPrefix);
    }

    Ok(())
}

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
}
