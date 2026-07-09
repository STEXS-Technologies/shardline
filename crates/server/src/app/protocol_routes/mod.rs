mod bazel;
mod helpers;
mod lfs;
pub(super) mod oci;

pub(crate) use bazel::{
    bazel_get, bazel_get_ac, bazel_get_cas, bazel_head, bazel_head_ac, bazel_head_cas,
    bazel_put, bazel_put_ac, bazel_put_cas,
};
pub(crate) use helpers::{direct_object_response, ensure_upload_growth_within_limit, parse_query_map, parse_query_values, parse_upload_content_range};
pub(crate) use lfs::{lfs_batch, lfs_delete_object, lfs_get_object, lfs_head_object, lfs_patch_object, lfs_put_object, lfs_verify_object};
pub(crate) use oci::parse_oci_path;
pub(crate) use oci::{
    oci_api_dispatch, oci_dispatch, oci_registry_token, oci_transfer_dispatch, oci_v2_root,
};

use super::MAX_PROTOCOL_QUERY_BYTES;

// Re-export parent items needed by sibling sub-modules (bazel.rs, lfs.rs, oci/).
use super::{
    AppState, MAX_LFS_BATCH_OBJECTS, MAX_OCI_MANIFEST_TAGS, MAX_OCI_TAG_LIST_PAGE_SIZE,
    authorize, scope_from_auth,
};
use super::reconstruction_helpers::{byte_range_stream_response, full_byte_stream_response};

#[cfg(test)]
mod tests {
    use super::parse_oci_path;
    use crate::ServerError;
    use crate::app::protocol_routes::oci::path::OciPath;
    use axum::http::Uri;

    #[test]
    fn oci_path_parser_accepts_supported_routes() {
        assert!(matches!(
            parse_oci_path("team/assets/blobs/sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
            Ok(OciPath::Blob { repository, digest_hex })
                if repository == "team/assets"
                    && digest_hex
                        == "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
        assert!(matches!(
            parse_oci_path("team/assets/blobs/uploads"),
            Ok(OciPath::BlobUploads { repository }) if repository == "team/assets"
        ));
        assert!(matches!(
            parse_oci_path("team/assets/blobs/uploads/0000000000000001"),
            Ok(OciPath::BlobUploadSession { repository, session_id })
                if repository == "team/assets" && session_id == "0000000000000001"
        ));
        assert!(matches!(
            parse_oci_path("team/assets/manifests/v1"),
            Ok(OciPath::Manifest { repository, reference })
                if repository == "team/assets" && reference == "v1"
        ));
        assert!(matches!(
            parse_oci_path("team/assets/tags/list"),
            Ok(OciPath::TagsList { repository }) if repository == "team/assets"
        ));
    }

    #[test]
    fn oci_path_parser_rejects_invalid_repository_and_digest_inputs() {
        assert!(matches!(
            parse_oci_path("Team/assets/tags/list"),
            Err(ServerError::InvalidRepositoryName)
        ));
        assert!(matches!(
            parse_oci_path("team/assets/blobs/sha256:not-a-digest"),
            Err(ServerError::InvalidDigest)
        ));
        assert!(matches!(
            parse_oci_path("team/assets/unknown"),
            Err(ServerError::NotFound)
        ));
    }

    #[test]
    fn upload_content_range_parser_accepts_common_client_formats() {
        assert_eq!(
            super::parse_upload_content_range("0-9")
                .ok()
                .map(|range| (range.start(), range.end_inclusive())),
            Some((0, 9))
        );
        assert_eq!(
            super::parse_upload_content_range("bytes 10-19/20")
                .ok()
                .map(|range| (range.start(), range.end_inclusive())),
            Some((10, 19))
        );
        assert_eq!(
            super::parse_upload_content_range("bytes 20-29/*")
                .ok()
                .map(|range| (range.start(), range.end_inclusive())),
            Some((20, 29))
        );
    }

    #[test]
    fn protocol_query_parser_rejects_oversized_inputs() {
        let uri = Uri::builder()
            .path_and_query(format!(
                "/v2/team/assets/blobs/uploads?mount={}",
                "a".repeat(super::MAX_PROTOCOL_QUERY_BYTES + 1)
            ))
            .build();
        assert!(uri.is_ok());
        let Ok(uri) = uri else {
            return;
        };

        assert!(matches!(
            super::parse_query_map(&uri),
            Err(ServerError::RequestQueryTooLarge)
        ));
        assert!(matches!(
            super::parse_query_values(&uri, "mount"),
            Err(ServerError::RequestQueryTooLarge)
        ));
    }
}
