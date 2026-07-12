use crate::{ServerError, oci_adapter::validate_repository, protocol_support::parse_sha256_digest};

#[cfg_attr(test, derive(Debug))]
pub(crate) enum OciPath {
    Blob {
        repository: String,
        digest_hex: String,
    },
    BlobUploads {
        repository: String,
    },
    BlobUploadSession {
        repository: String,
        session_id: String,
    },
    Manifest {
        repository: String,
        reference: String,
    },
    TagsList {
        repository: String,
    },
}

pub(crate) fn parse_oci_path(path: &str) -> Result<OciPath, ServerError> {
    let path = path.trim_end_matches('/');
    if let Some(repository) = path.strip_suffix("/blobs/uploads") {
        validate_repository(repository)?;
        return Ok(OciPath::BlobUploads {
            repository: repository.to_owned(),
        });
    }
    if let Some((repository, session_id)) = path.split_once("/blobs/uploads/") {
        validate_repository(repository)?;
        return Ok(OciPath::BlobUploadSession {
            repository: repository.to_owned(),
            session_id: session_id.to_owned(),
        });
    }
    if let Some((repository, digest)) = path.split_once("/blobs/") {
        validate_repository(repository)?;
        return Ok(OciPath::Blob {
            repository: repository.to_owned(),
            digest_hex: parse_sha256_digest(digest)?,
        });
    }
    if let Some((repository, reference)) = path.split_once("/manifests/") {
        validate_repository(repository)?;
        return Ok(OciPath::Manifest {
            repository: repository.to_owned(),
            reference: reference.to_owned(),
        });
    }
    if let Some(repository) = path.strip_suffix("/tags/list") {
        validate_repository(repository)?;
        return Ok(OciPath::TagsList {
            repository: repository.to_owned(),
        });
    }

    Err(ServerError::NotFound)
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_DIGEST: &str =
        "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    // ── Blob ────────────────────────────────────────────────────────────

    #[test]
    fn blob_path_parses_valid_sha256_digest() {
        let path = format!("team/assets/blobs/{VALID_DIGEST}");
        let result = parse_oci_path(&path);
        assert!(matches!(
            result,
            Ok(OciPath::Blob {
                repository,
                digest_hex
            }) if repository == "team/assets"
                && digest_hex == "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
    }

    #[test]
    fn blob_path_rejects_invalid_digest() {
        assert!(matches!(
            parse_oci_path("team/assets/blobs/sha256:not-a-hex"),
            Err(ServerError::InvalidDigest)
        ));
    }

    // ── BlobUploads ─────────────────────────────────────────────────────

    #[test]
    fn blob_uploads_path_parses() {
        let result = parse_oci_path("team/assets/blobs/uploads");
        assert!(matches!(
            result,
            Ok(OciPath::BlobUploads { repository }) if repository == "team/assets"
        ));
    }

    #[test]
    fn blob_uploads_path_strips_trailing_slash() {
        let result = parse_oci_path("team/assets/blobs/uploads/");
        assert!(matches!(
            result,
            Ok(OciPath::BlobUploads { repository }) if repository == "team/assets"
        ));
    }

    // ── BlobUploadSession ───────────────────────────────────────────────

    #[test]
    fn blob_upload_session_path_parses() {
        let result = parse_oci_path("team/assets/blobs/uploads/0000000000000001");
        assert!(matches!(
            result,
            Ok(OciPath::BlobUploadSession {
                repository,
                session_id
            }) if repository == "team/assets" && session_id == "0000000000000001"
        ));
    }

    // ── Manifest ────────────────────────────────────────────────────────

    #[test]
    fn manifest_tag_reference_parses() {
        let result = parse_oci_path("team/assets/manifests/v1");
        assert!(matches!(
            result,
            Ok(OciPath::Manifest {
                repository,
                reference
            }) if repository == "team/assets" && reference == "v1"
        ));
    }

    #[test]
    fn manifest_digest_reference_parses() {
        let path = format!("team/assets/manifests/{VALID_DIGEST}");
        let result = parse_oci_path(&path);
        assert!(matches!(
            result,
            Ok(OciPath::Manifest {
                repository,
                reference
            }) if repository == "team/assets" && reference == VALID_DIGEST
        ));
    }

    // ── TagsList ────────────────────────────────────────────────────────

    #[test]
    fn tags_list_path_parses() {
        let result = parse_oci_path("team/assets/tags/list");
        assert!(matches!(
            result,
            Ok(OciPath::TagsList { repository }) if repository == "team/assets"
        ));
    }

    #[test]
    fn tags_list_path_strips_trailing_slash() {
        let result = parse_oci_path("team/assets/tags/list/");
        assert!(matches!(
            result,
            Ok(OciPath::TagsList { repository }) if repository == "team/assets"
        ));
    }

    // ── Unknown / invalid paths ─────────────────────────────────────────

    #[test]
    fn unknown_path_returns_not_found() {
        assert!(matches!(
            parse_oci_path("team/assets/unknown"),
            Err(ServerError::NotFound)
        ));
    }

    #[test]
    fn empty_path_returns_not_found() {
        assert!(matches!(parse_oci_path(""), Err(ServerError::NotFound)));
    }

    // ── Repository validation ───────────────────────────────────────────

    #[test]
    fn uppercase_repository_rejected() {
        assert!(matches!(
            parse_oci_path("Team/assets/tags/list"),
            Err(ServerError::InvalidRepositoryName)
        ));
    }

    #[test]
    fn repository_with_spaces_rejected() {
        assert!(matches!(
            parse_oci_path("team/my assets/tags/list"),
            Err(ServerError::InvalidRepositoryName)
        ));
    }

    // ── Deeply nested repositories ──────────────────────────────────────

    #[test]
    fn deeply_nested_repository_parses() {
        let result = parse_oci_path("a/b/c/d/tags/list");
        assert!(matches!(
            result,
            Ok(OciPath::TagsList { repository }) if repository == "a/b/c/d"
        ));
    }

    // ── Path ordering (blobs/uploads must be checked before blobs) ──────

    #[test]
    fn blob_uploads_session_not_confused_with_blob() {
        let result = parse_oci_path("team/assets/blobs/uploads/abc123");
        assert!(matches!(result, Ok(OciPath::BlobUploadSession { .. })));
    }
}
