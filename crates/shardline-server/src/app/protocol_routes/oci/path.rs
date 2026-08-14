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

impl OciPath {
    /// Returns the requested repository name carried by this parsed OCI path.
    #[must_use]
    pub(crate) fn repository(&self) -> &str {
        match self {
            OciPath::Blob { repository, .. }
            | OciPath::BlobUploads { repository }
            | OciPath::BlobUploadSession { repository, .. }
            | OciPath::Manifest { repository, .. }
            | OciPath::TagsList { repository } => repository,
        }
    }
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

    // ── Additional edge cases ────────────────────────────────────────────

    #[test]
    fn blob_path_with_trailing_slash_after_blobs_rejects() {
        // Trimmed to "team/assets/blobs" — no "/blobs/" match (no trailing /),
        // so it falls through to NotFound.
        let path = "team/assets/blobs/";
        let result = parse_oci_path(path);
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[test]
    fn blob_path_with_missing_sha256_prefix_rejects() {
        // Digest that doesn't start with "sha256:" is invalid
        let path = "team/assets/blobs/not-sha256-prefix";
        let result = parse_oci_path(path);
        assert!(matches!(result, Err(ServerError::InvalidDigest)));
    }

    #[test]
    fn path_with_too_many_segments_still_parses_if_matches_pattern() {
        // Deep nesting in the repository portion is fine (matches blob pattern)
        let path = format!("a/b/c/d/e/f/blobs/{VALID_DIGEST}");
        let result = parse_oci_path(&path);
        assert!(matches!(
            result,
            Ok(OciPath::Blob { repository, .. }) if repository == "a/b/c/d/e/f"
        ));
    }

    #[test]
    fn manifest_without_reference_returns_not_found() {
        // "/manifests" with nothing after (trailing slash trimmed)
        let result = parse_oci_path("team/assets/manifests/");
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[test]
    fn manifest_reference_with_slash_is_valid() {
        // Slashes in the reference part are accepted as-is.
        let result = parse_oci_path("team/assets/manifests/v1/something");
        assert!(matches!(
            result,
            Ok(OciPath::Manifest { reference, .. }) if reference == "v1/something"
        ));
    }

    #[test]
    fn blob_session_with_extra_trailing_slash() {
        // Trailing slash after session ID
        let result = parse_oci_path("team/assets/blobs/uploads/abc123/");
        assert!(matches!(
            result,
            Ok(OciPath::BlobUploadSession { session_id, .. }) if session_id == "abc123"
        ));
    }

    #[test]
    fn tags_list_with_repository_having_hyphens_and_dots() {
        // Repository names commonly contain hyphens and dots.
        let result = parse_oci_path("my-team/my-repo.v2/tags/list");
        assert!(matches!(
            result,
            Ok(OciPath::TagsList { repository }) if repository == "my-team/my-repo.v2"
        ));
    }

    #[test]
    fn repository_with_underscores_accepted() {
        let result = parse_oci_path("team/my_repo/tags/list");
        assert!(matches!(
            result,
            Ok(OciPath::TagsList { repository }) if repository == "team/my_repo"
        ));
    }

    #[test]
    fn repository_with_numbers_accepted() {
        let result = parse_oci_path("team2/project3/blobs/uploads");
        assert!(matches!(
            result,
            Ok(OciPath::BlobUploads { repository }) if repository == "team2/project3"
        ));
    }

    #[test]
    fn path_dot_segment_rejected() {
        // Directories like "." or ".." are not valid in repository names
        let result = parse_oci_path("team/./assets/tags/list");
        assert!(matches!(result, Err(ServerError::InvalidRepositoryName)));
    }

    // ── Null byte rejection ──────────────────────────────────────────────

    #[test]
    fn oci_repository_name_with_null_byte_rejected() {
        let path = "team/asset\0s/tags/list";
        let result = parse_oci_path(path);
        assert!(matches!(result, Err(ServerError::InvalidRepositoryName)));
    }

    #[test]
    fn oci_blob_path_with_null_byte_rejected() {
        let path = "team/assets/blobs/sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abc\0ef";
        let result = parse_oci_path(path);
        assert!(matches!(result, Err(ServerError::InvalidDigest)));
    }

    #[test]
    fn oci_manifest_path_with_null_byte_in_reference() {
        let path = "team/assets/manifests/v1\0tag";
        let result = parse_oci_path(path);
        // parse_oci_path validates the repository portion but passes the
        // reference through without validation.  The null byte is part of
        // the reference string.  Downstream manifest handlers should reject
        // the null-containing reference.
        assert!(matches!(
            result,
            Ok(OciPath::Manifest { reference, .. }) if reference == "v1\0tag"
        ));
    }
}
