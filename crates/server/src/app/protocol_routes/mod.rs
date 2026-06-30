mod bazel;
mod lfs;
pub(super) mod oci;

pub(crate) use bazel::{bazel_get_ac, bazel_put_ac, bazel_get_cas, bazel_put_cas};
pub(crate) use lfs::{lfs_batch, lfs_get_object, lfs_head_object, lfs_put_object};
pub(crate) use oci::{
    oci_api_dispatch, oci_dispatch, oci_registry_token, oci_transfer_dispatch, oci_v2_root,
};
pub(crate) use oci::parse_oci_path;

use std::{
    collections::BTreeMap,
    sync::Arc,
};

use axum::{
    http::{
        HeaderMap, HeaderValue,
        header::{CONTENT_TYPE, RANGE},
    },
    response::Response,
};
use shardline_protocol::{ByteRange, parse_http_byte_range};

use crate::ServerError;

use super::{
    AppState, MAX_LFS_BATCH_OBJECTS, MAX_OCI_MANIFEST_TAGS, MAX_OCI_TAG_LIST_PAGE_SIZE,
    MAX_PROTOCOL_QUERY_BYTES, authorize,
    reconstruction_helpers::{byte_range_stream_response, full_byte_stream_response},
    scope_from_auth,
};

pub(super) async fn direct_object_response(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    object_key: &shardline_storage::ObjectKey,
    content_type: &str,
    content_digest: Option<String>,
) -> Result<Response, ServerError> {
    let total_length = state.backend.object_length(object_key).await?;
    let range = parse_optional_range(headers, total_length)?;
    let byte_stream = state
        .backend
        .read_object_stream(object_key, total_length, range)
        .await?;
    let mut response = if let Some(range) = range {
        let transfer_length = range.len().ok_or(ServerError::Overflow)?;
        byte_range_stream_response(
            byte_stream,
            state.transfer_limiter.clone(),
            range,
            total_length,
            transfer_length,
        )
    } else {
        full_byte_stream_response(byte_stream, state.transfer_limiter.clone(), total_length)
    };
    let content_type_value = HeaderValue::from_str(content_type)
        .map_err(|_error| ServerError::InvalidManifestReference)?;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, content_type_value);
    if let Some(content_digest) = content_digest {
        let digest_value =
            HeaderValue::from_str(&content_digest).map_err(|_error| ServerError::InvalidDigest)?;
        response
            .headers_mut()
            .insert("Docker-Content-Digest", digest_value);
    }
    Ok(response)
}

fn parse_optional_range(
    headers: &HeaderMap,
    total_length: u64,
) -> Result<Option<ByteRange>, ServerError> {
    let Some(range) = headers.get(RANGE) else {
        return Ok(None);
    };
    let range = range
        .to_str()
        .map_err(|_error| ServerError::InvalidRangeHeader)?;
    let range = parse_http_byte_range(range, total_length).map_err(ServerError::from)?;
    Ok(Some(range))
}

pub(crate) fn parse_upload_content_range(value: &str) -> Result<ByteRange, ServerError> {
    let value = value.trim();
    let value = value.strip_prefix("bytes ").unwrap_or(value);
    let value = value.split_once('/').map_or(value, |(range, _rest)| range);
    let Some((start, end)) = value.split_once('-') else {
        return Err(ServerError::InvalidRangeHeader);
    };
    let start = start
        .parse::<u64>()
        .map_err(|_error| ServerError::InvalidRangeHeader)?;
    let end = end
        .parse::<u64>()
        .map_err(|_error| ServerError::InvalidRangeHeader)?;
    ByteRange::new(start, end).map_err(|_error| ServerError::InvalidRangeHeader)
}

fn ensure_upload_growth_within_limit(
    state: &Arc<AppState>,
    current_length: u64,
    additional_bytes: usize,
) -> Result<(), ServerError> {
    let additional_bytes = u64::try_from(additional_bytes)?;
    let next_length = current_length
        .checked_add(additional_bytes)
        .ok_or(ServerError::Overflow)?;
    let max_bytes = u64::try_from(state.config.max_request_body_bytes().get())?;
    if next_length > max_bytes {
        return Err(ServerError::RequestBodyTooLarge);
    }

    Ok(())
}

fn parse_query_map(uri: &axum::http::Uri) -> Result<BTreeMap<String, String>, ServerError> {
    let Some(query) = uri.query() else {
        return Ok(BTreeMap::new());
    };
    if query.len() > MAX_PROTOCOL_QUERY_BYTES {
        return Err(ServerError::RequestQueryTooLarge);
    }

    Ok(url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect())
}

fn parse_query_values(uri: &axum::http::Uri, key: &str) -> Result<Vec<String>, ServerError> {
    let Some(query) = uri.query() else {
        return Ok(Vec::new());
    };
    if query.len() > MAX_PROTOCOL_QUERY_BYTES {
        return Err(ServerError::RequestQueryTooLarge);
    }

    Ok(url::form_urlencoded::parse(query.as_bytes())
        .filter_map(|(candidate_key, value)| (candidate_key == key).then(|| value.into_owned()))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::parse_oci_path;
    use crate::app::protocol_routes::oci::OciPath;
    use crate::ServerError;
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
