use sha2::{Digest, Sha256};
use shardline_server_core::protocol_support as core_ps;
use shardline_storage::ObjectKey;

use crate::OciAdapterError;

impl core_ps::ProtocolValidation for OciAdapterError {
    fn invalid_digest() -> Self {
        Self::InvalidDigest
    }
    fn invalid_content_hash() -> Self {
        Self::InvalidContentHash
    }
    fn invalid_repository_name() -> Self {
        Self::InvalidRepositoryName
    }
    fn not_found() -> Self {
        Self::NotFound
    }
    fn invalid_manifest_reference() -> Self {
        Self::InvalidManifestReference
    }
    fn invalid_upload_session() -> Self {
        Self::InvalidUploadSession
    }
}

pub(crate) fn parse_sha256_digest(value: &str) -> Result<String, OciAdapterError> {
    core_ps::parse_sha256_digest(value)
}

pub(crate) fn scope_namespace(
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
) -> String {
    core_ps::scope_namespace(repository_scope)
}

pub(crate) fn stable_hex_id(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

pub(crate) fn object_key(value: &str) -> Result<ObjectKey, OciAdapterError> {
    core_ps::object_key(value)
}

pub(crate) fn shared_sha256_object_key(digest_hex: &str) -> Result<ObjectKey, OciAdapterError> {
    core_ps::shared_sha256_object_key(digest_hex)
}

pub(crate) fn validate_oci_repository_name(value: &str) -> Result<(), OciAdapterError> {
    core_ps::validate_oci_repository_name(value)
}

pub(crate) fn validate_oci_repository_scope(
    value: &str,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
) -> Result<(), OciAdapterError> {
    core_ps::validate_oci_repository_scope(value, repository_scope)
}

pub(crate) fn validate_oci_tag(value: &str) -> Result<(), OciAdapterError> {
    core_ps::validate_oci_tag(value)
}

pub(crate) fn validate_upload_session_id(value: &str) -> Result<(), OciAdapterError> {
    core_ps::validate_upload_session_id(value)
}

#[cfg(test)]
mod tests {
    use super::{
        parse_sha256_digest, shared_sha256_object_key, validate_oci_repository_name,
        validate_oci_repository_scope, validate_oci_tag, validate_upload_session_id,
    };
    use crate::OciAdapterError;
    use shardline_protocol::{RepositoryProvider, RepositoryScope};
    use shardline_server_core::protocol_support::ProtocolValidation;

    #[test]
    fn sha256_digest_parser_requires_prefixed_lowercase_hex() {
        let digest = parse_sha256_digest(
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        );
        assert!(digest.is_ok());
        assert_eq!(
            digest.unwrap_or_default(),
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
        assert!(matches!(
            parse_sha256_digest("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
            Err(OciAdapterError::InvalidDigest)
        ));
        assert!(matches!(
            parse_sha256_digest(
                "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg"
            ),
            Err(OciAdapterError::InvalidDigest)
        ));
    }

    #[test]
    fn oci_repository_validator_rejects_traversal_and_uppercase() {
        assert!(validate_oci_repository_name("team/assets").is_ok());
        assert!(matches!(
            validate_oci_repository_name("../assets"),
            Err(OciAdapterError::InvalidRepositoryName)
        ));
        assert!(matches!(
            validate_oci_repository_name("Team/assets"),
            Err(OciAdapterError::InvalidRepositoryName)
        ));
        assert!(matches!(
            validate_oci_repository_name("team//assets"),
            Err(OciAdapterError::InvalidRepositoryName)
        ));
    }

    #[test]
    fn oci_tag_validator_enforces_allowed_characters() {
        assert!(validate_oci_tag("v1").is_ok());
        assert!(validate_oci_tag("_debug.2026-04-23").is_ok());
        assert!(matches!(
            validate_oci_tag("bad/tag"),
            Err(OciAdapterError::InvalidManifestReference)
        ));
        assert!(matches!(
            validate_oci_tag("-bad"),
            Err(OciAdapterError::InvalidManifestReference)
        ));
    }

    #[test]
    fn upload_session_validator_accepts_hex_and_hyphen_only() {
        assert!(validate_upload_session_id("0000000000000001").is_ok());
        assert!(validate_upload_session_id("dead-beef").is_ok());
        assert!(matches!(
            validate_upload_session_id("session_1"),
            Err(OciAdapterError::InvalidUploadSession)
        ));
        assert!(matches!(
            validate_upload_session_id(&"a".repeat(65)),
            Err(OciAdapterError::InvalidUploadSession)
        ));
    }

    #[test]
    fn oci_repository_scope_validator_accepts_bound_roots_and_nested_namespaces() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None);
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };

        assert!(validate_oci_repository_scope("team/assets", Some(&scope)).is_ok());
        assert!(validate_oci_repository_scope("team/assets/cache", Some(&scope)).is_ok());
        assert!(matches!(
            validate_oci_repository_scope("team/other", Some(&scope)),
            Err(OciAdapterError::NotFound)
        ));
        assert!(matches!(
            validate_oci_repository_scope("other/assets", Some(&scope)),
            Err(OciAdapterError::NotFound)
        ));
    }

    #[test]
    fn shared_sha256_key_uses_stable_shared_namespace() {
        let key = shared_sha256_object_key(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        );
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        assert_eq!(
            key.as_str(),
            "protocols/shared/sha256/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
    }

    #[test]
    fn stable_hex_id_produces_64_char_hex() {
        let id = super::stable_hex_id("hello");
        assert_eq!(id.len(), 64);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn stable_hex_id_is_deterministic() {
        let a = super::stable_hex_id("test-value");
        let b = super::stable_hex_id("test-value");
        assert_eq!(a, b);
    }

    #[test]
    fn stable_hex_id_differs_for_different_inputs() {
        let a = super::stable_hex_id("input-a");
        let b = super::stable_hex_id("input-b");
        assert_ne!(a, b);
    }

    #[test]
    fn scope_namespace_none_returns_global() {
        assert_eq!(super::scope_namespace(None), "global");
    }

    #[test]
    fn scope_namespace_with_scope_returns_64_char_hex() {
        let scope = shardline_protocol::RepositoryScope::new(
            shardline_protocol::RepositoryProvider::GitHub,
            "org",
            "repo",
            None,
        )
        .unwrap();
        let ns = super::scope_namespace(Some(&scope));
        assert_eq!(ns.len(), 64);
        assert!(ns.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn object_key_valid() {
        let key = super::object_key("valid/path").unwrap();
        assert_eq!(key.as_str(), "valid/path");
    }

    #[test]
    fn object_key_invalid_empty() {
        assert!(super::object_key("").is_err());
    }

    #[test]
    fn object_key_invalid_unsafe_path() {
        assert!(super::object_key("../unsafe").is_err());
    }

    #[test]
    fn protocol_validation_invalid_digest() {
        let err = OciAdapterError::invalid_digest();
        assert!(matches!(err, OciAdapterError::InvalidDigest));
    }

    #[test]
    fn protocol_validation_invalid_content_hash() {
        let err = OciAdapterError::invalid_content_hash();
        assert!(matches!(err, OciAdapterError::InvalidContentHash));
    }

    #[test]
    fn protocol_validation_invalid_repository_name() {
        let err = OciAdapterError::invalid_repository_name();
        assert!(matches!(err, OciAdapterError::InvalidRepositoryName));
    }

    #[test]
    fn protocol_validation_not_found() {
        let err = OciAdapterError::not_found();
        assert!(matches!(err, OciAdapterError::NotFound));
    }

    #[test]
    fn protocol_validation_invalid_manifest_reference() {
        let err = OciAdapterError::invalid_manifest_reference();
        assert!(matches!(err, OciAdapterError::InvalidManifestReference));
    }

    #[test]
    fn protocol_validation_invalid_upload_session() {
        let err = OciAdapterError::invalid_upload_session();
        assert!(matches!(err, OciAdapterError::InvalidUploadSession));
    }

    #[test]
    fn shared_sha256_object_key_invalid_digest_errors() {
        assert!(shared_sha256_object_key("not-64-chars").is_err());
    }

    #[test]
    fn validate_oci_repository_scope_none_scope_ok() {
        assert!(validate_oci_repository_scope("any/repo", None).is_ok());
    }

    #[test]
    fn validate_upload_session_id_too_long_errors() {
        let long = "a".repeat(65);
        assert!(matches!(
            validate_upload_session_id(&long),
            Err(OciAdapterError::InvalidUploadSession)
        ));
    }

    #[test]
    fn validate_oci_tag_valid_tags() {
        assert!(validate_oci_tag("v1.0.0").is_ok());
        assert!(validate_oci_tag("latest").is_ok());
        assert!(validate_oci_tag("build-2026-04-23").is_ok());
    }

    #[test]
    fn validate_oci_tag_invalid_tags() {
        assert!(matches!(
            validate_oci_tag("contains spaces"),
            Err(OciAdapterError::InvalidManifestReference)
        ));
        assert!(matches!(
            validate_oci_tag("tag/with/slashes"),
            Err(OciAdapterError::InvalidManifestReference)
        ));
        assert!(matches!(
            validate_oci_tag("-starts-with-hyphen"),
            Err(OciAdapterError::InvalidManifestReference)
        ));
        assert!(matches!(
            validate_oci_tag(".starts-with-dot"),
            Err(OciAdapterError::InvalidManifestReference)
        ));
    }
}
