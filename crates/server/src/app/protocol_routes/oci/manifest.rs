use std::sync::Arc;

use axum::{
    body::Body,
    http::{
        HeaderMap, StatusCode, Uri,
        header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE},
    },
    response::Response,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use shardline_protocol::TokenScope;
use shardline_storage::DeleteOutcome;

use crate::{
    ServerError,
    oci_adapter::{
        oci_blob_key, oci_manifest_key, oci_manifest_location, oci_manifest_media_type_key,
        oci_tag_key, parse_reference,
    },
    protocol_support::parse_sha256_digest,
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

use super::super::{AppState, direct_object_response, parse_query_values, scope_from_auth};
use super::tags::{delete_oci_tags_pointing_to_digest, update_oci_tags};
use super::token::oci_authorize;

pub(super) const OCI_IMAGE_MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";
pub(super) const OCI_IMAGE_INDEX_MEDIA_TYPE: &str = "application/vnd.oci.image.index.v1+json";
pub(super) const DOCKER_SCHEMA2_MANIFEST_MEDIA_TYPE: &str =
    "application/vnd.docker.distribution.manifest.v2+json";
pub(super) const DOCKER_SCHEMA2_MANIFEST_LIST_MEDIA_TYPE: &str =
    "application/vnd.docker.distribution.manifest.list.v2+json";

#[tracing::instrument(skip(state, headers), fields(repository, reference))]
pub(crate) async fn oci_get_manifest(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    repository: &str,
    reference: &str,
    head_only: bool,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, headers, Some(repository), TokenScope::Read)?;
    let scope = auth.as_ref().map(scope_from_auth);
    let digest_hex = resolve_manifest_digest(state, repository, reference, scope).await?;
    let manifest_key = oci_manifest_key(repository, &digest_hex, scope)?;
    let media_type_key = oci_manifest_media_type_key(repository, &digest_hex, scope)?;
    let total_length = state.backend.object_length(&manifest_key).await?;
    let media_type = String::from_utf8(state.backend.read_object(&media_type_key).await?)
        .map_err(|_error| ServerError::InvalidManifestReference)?;
    ensure_manifest_representation_is_acceptable(headers, &media_type)?;
    if head_only {
        return Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_LENGTH, total_length.to_string())
            .header(CONTENT_TYPE, media_type)
            .header("Docker-Content-Digest", format!("sha256:{digest_hex}"))
            .body(Body::empty())
            .map_err(|_error| ServerError::Overflow);
    }

    direct_object_response(
        state,
        headers,
        &manifest_key,
        &media_type,
        Some(format!("sha256:{digest_hex}")),
        "oci",
    )
    .await
}

#[tracing::instrument(skip(state, headers, uri, body), fields(repository, reference))]
pub(crate) async fn oci_put_manifest(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    uri: &Uri,
    repository: &str,
    reference: &str,
    body: Body,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, headers, Some(repository), TokenScope::Write)?;
    let scope = auth.as_ref().map(scope_from_auth);
    let mut body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let bytes = read_body_to_bytes(&mut body).await?;
    let digest_hex = hex::encode(Sha256::digest(&bytes));
    let reference = parse_reference(reference)?;
    if let crate::oci_adapter::OciReference::Digest(reference_digest) = &reference
        && reference_digest != &digest_hex
    {
        return Err(ServerError::ExpectedBodyHashMismatch);
    }
    let media_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or(OCI_IMAGE_MANIFEST_MEDIA_TYPE)
        .to_owned();
    validate_oci_manifest_document(state, repository, scope, &media_type, &bytes).await?;
    let manifest_key = oci_manifest_key(repository, &digest_hex, scope)?;
    let media_type_key = oci_manifest_media_type_key(repository, &digest_hex, scope)?;
    let _stored_manifest = state.backend.put_sha256_addressed_object_bytes_if_absent(
        &manifest_key,
        &digest_hex,
        bytes,
    )?;
    let _stored_media_type = state
        .backend
        .put_object_bytes_if_absent(&media_type_key, media_type.clone().into_bytes())?;
    let mut accepted_tags = match reference {
        crate::oci_adapter::OciReference::Tag(tag) => vec![tag],
        crate::oci_adapter::OciReference::Digest(_) => Vec::new(),
    };
    accepted_tags.extend(parse_query_values(uri, "tag")?);
    if accepted_tags.len() > super::super::MAX_OCI_MANIFEST_TAGS {
        return Err(ServerError::InvalidManifestReference);
    }
    accepted_tags.sort();
    accepted_tags.dedup();
    if !accepted_tags.is_empty() {
        update_oci_tags(state, repository, scope, &accepted_tags, &digest_hex).await?;
    }

    let mut builder = Response::builder()
        .status(StatusCode::CREATED)
        .header(
            axum::http::header::LOCATION,
            oci_manifest_location(repository, &digest_hex),
        )
        .header("Docker-Content-Digest", format!("sha256:{digest_hex}"));
    if !accepted_tags.is_empty() {
        let joined = accepted_tags.join(", ");
        builder = builder.header("OCI-Tag", joined);
    }
    builder
        .body(Body::empty())
        .map_err(|_error| ServerError::Overflow)
}

#[tracing::instrument(skip(state, headers), fields(repository, reference))]
pub(crate) async fn oci_delete_manifest(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    repository: &str,
    reference: &str,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, headers, Some(repository), TokenScope::Write)?;
    let scope = auth.as_ref().map(scope_from_auth);
    let digest_hex = resolve_manifest_digest(state, repository, reference, scope).await?;
    let manifest_key = oci_manifest_key(repository, &digest_hex, scope)?;
    match state
        .backend
        .delete_object_if_present(&manifest_key)
        .await?
    {
        DeleteOutcome::Deleted => {}
        DeleteOutcome::NotFound => return Err(ServerError::NotFound),
    }

    let media_type_key = oci_manifest_media_type_key(repository, &digest_hex, scope)?;
    let _deleted = state
        .backend
        .delete_object_if_present(&media_type_key)
        .await?;
    delete_oci_tags_pointing_to_digest(state, repository, scope, &digest_hex).await?;

    Response::builder()
        .status(StatusCode::ACCEPTED)
        .body(Body::empty())
        .map_err(|_error| ServerError::Overflow)
}

async fn resolve_manifest_digest(
    state: &Arc<AppState>,
    repository: &str,
    reference: &str,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
) -> Result<String, ServerError> {
    match parse_reference(reference)? {
        crate::oci_adapter::OciReference::Digest(digest_hex) => Ok(digest_hex),
        crate::oci_adapter::OciReference::Tag(tag) => {
            load_oci_tag_digest(state, repository, repository_scope, &tag).await
        }
    }
}

async fn load_oci_tag_digest(
    state: &Arc<AppState>,
    repository: &str,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
    tag: &str,
) -> Result<String, ServerError> {
    let tag_key = oci_tag_key(repository, tag, repository_scope)?;
    let bytes = state.backend.read_object(&tag_key).await?;
    let digest_hex = String::from_utf8(bytes).map_err(|_error| ServerError::InvalidDigest)?;
    parse_sha256_digest(&format!("sha256:{digest_hex}"))?;
    Ok(digest_hex)
}

async fn validate_oci_manifest_document(
    state: &Arc<AppState>,
    repository: &str,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
    media_type: &str,
    bytes: &[u8],
) -> Result<(), ServerError> {
    let document: Value =
        serde_json::from_slice(bytes).map_err(|_error| ServerError::InvalidManifestReference)?;
    validate_oci_schema_version(&document)?;
    let normalized_media_type = normalize_media_type(media_type);
    if let Some(document_media_type) = document.get("mediaType").and_then(Value::as_str)
        && normalize_media_type(document_media_type) != normalized_media_type
    {
        return Err(ServerError::InvalidManifestReference);
    }
    if let Some(subject) = document.get("subject") {
        // Per the OCI Distribution spec, a registry MUST accept a manifest
        // with a subject field that references a manifest that does not exist.
        // We validate the descriptor format but do not check existence.
        validate_oci_descriptor(subject)?;
    }

    match normalized_media_type {
        OCI_IMAGE_MANIFEST_MEDIA_TYPE | DOCKER_SCHEMA2_MANIFEST_MEDIA_TYPE => {
            validate_oci_image_manifest_document(state, repository, repository_scope, &document)
                .await
        }
        OCI_IMAGE_INDEX_MEDIA_TYPE | DOCKER_SCHEMA2_MANIFEST_LIST_MEDIA_TYPE => {
            validate_oci_image_index_document(state, repository, repository_scope, &document).await
        }
        _ => Err(ServerError::InvalidManifestReference),
    }
}

fn validate_oci_schema_version(document: &Value) -> Result<(), ServerError> {
    if document.get("schemaVersion").and_then(Value::as_u64) != Some(2) {
        return Err(ServerError::InvalidManifestReference);
    }
    Ok(())
}

async fn validate_oci_image_manifest_document(
    state: &Arc<AppState>,
    repository: &str,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
    document: &Value,
) -> Result<(), ServerError> {
    let config = document
        .get("config")
        .ok_or(ServerError::InvalidManifestReference)?;
    let config_digest_hex = validate_oci_descriptor(config)?;
    ensure_oci_blob_exists(state, repository, repository_scope, &config_digest_hex).await?;

    let layers = document
        .get("layers")
        .and_then(Value::as_array)
        .ok_or(ServerError::InvalidManifestReference)?;
    for layer in layers {
        let digest_hex = validate_oci_descriptor(layer)?;
        ensure_oci_blob_exists(state, repository, repository_scope, &digest_hex).await?;
    }

    Ok(())
}

async fn validate_oci_image_index_document(
    state: &Arc<AppState>,
    repository: &str,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
    document: &Value,
) -> Result<(), ServerError> {
    let manifests = document
        .get("manifests")
        .and_then(Value::as_array)
        .ok_or(ServerError::InvalidManifestReference)?;
    for manifest in manifests {
        let digest_hex = validate_oci_descriptor(manifest)?;
        ensure_oci_manifest_exists(state, repository, repository_scope, &digest_hex).await?;
    }

    Ok(())
}

fn validate_oci_descriptor(descriptor: &Value) -> Result<String, ServerError> {
    let descriptor = descriptor
        .as_object()
        .ok_or(ServerError::InvalidManifestReference)?;
    let digest = descriptor
        .get("digest")
        .and_then(Value::as_str)
        .ok_or(ServerError::InvalidManifestReference)?;
    let digest_hex = parse_sha256_digest(digest)?;
    let _size = descriptor
        .get("size")
        .and_then(Value::as_u64)
        .ok_or(ServerError::InvalidManifestReference)?;
    if descriptor
        .get("mediaType")
        .is_some_and(|media_type| !media_type.is_string())
    {
        return Err(ServerError::InvalidManifestReference);
    }
    Ok(digest_hex)
}

async fn ensure_oci_blob_exists(
    state: &Arc<AppState>,
    repository: &str,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
    digest_hex: &str,
) -> Result<(), ServerError> {
    let object_key = oci_blob_key(repository, digest_hex, repository_scope)?;
    match state.backend.object_length(&object_key).await {
        Ok(_length) => Ok(()),
        Err(ServerError::NotFound) => Err(ServerError::InvalidManifestReference),
        Err(error) => Err(error),
    }
}

async fn ensure_oci_manifest_exists(
    state: &Arc<AppState>,
    repository: &str,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
    digest_hex: &str,
) -> Result<(), ServerError> {
    let object_key = oci_manifest_key(repository, digest_hex, repository_scope)?;
    match state.backend.object_length(&object_key).await {
        Ok(_length) => Ok(()),
        Err(ServerError::NotFound) => Err(ServerError::InvalidManifestReference),
        Err(error) => Err(error),
    }
}

fn normalize_media_type(value: &str) -> &str {
    value.split(';').next().map_or(value, str::trim)
}

fn ensure_manifest_representation_is_acceptable(
    headers: &HeaderMap,
    media_type: &str,
) -> Result<(), ServerError> {
    let normalized_media_type = normalize_media_type(media_type);
    let Some((stored_type, stored_subtype)) = normalized_media_type.split_once('/') else {
        return Err(ServerError::InvalidManifestReference);
    };
    let accepted = headers.get_all(ACCEPT);
    let mut accepted_iter = accepted.iter().peekable();
    if accepted_iter.peek().is_none() {
        return Ok(());
    }

    for value in accepted_iter {
        let value = value
            .to_str()
            .map_err(|_error| ServerError::NotAcceptable)?;
        for candidate in value.split(',') {
            let candidate = normalize_media_type(candidate);
            if candidate.is_empty() {
                continue;
            }
            if candidate == "*/*" || candidate == normalized_media_type {
                return Ok(());
            }
            let Some((accepted_type, accepted_subtype)) = candidate.split_once('/') else {
                continue;
            };
            if (accepted_type == "*" || accepted_type == stored_type)
                && (accepted_subtype == "*" || accepted_subtype == stored_subtype)
            {
                return Ok(());
            }
        }
    }

    Err(ServerError::NotAcceptable)
}

#[cfg(test)]
mod tests {
    use axum::body::Bytes;
    use axum::http::{HeaderMap, HeaderValue};
    use serde_json::json;

    use super::*;

    // ── normalize_media_type ──

    #[test]
    fn normalize_media_type_passthrough_plain() {
        assert_eq!(normalize_media_type("application/json"), "application/json");
    }

    #[test]
    fn normalize_media_type_strips_parameters() {
        assert_eq!(
            normalize_media_type("application/json; charset=utf-8"),
            "application/json"
        );
    }

    #[test]
    fn normalize_media_type_strips_multiple_parameters() {
        assert_eq!(
            normalize_media_type("application/json;param=val;other=val"),
            "application/json"
        );
    }

    #[test]
    fn normalize_media_type_trims_whitespace() {
        assert_eq!(
            normalize_media_type("  application/json  ; charset=utf-8"),
            "application/json"
        );
    }

    // ── validate_oci_schema_version ──

    #[test]
    fn schema_version_valid() {
        let doc = json!({"schemaVersion": 2});
        assert!(validate_oci_schema_version(&doc).is_ok());
    }

    #[test]
    fn schema_version_wrong_number() {
        let doc = json!({"schemaVersion": 1});
        assert!(matches!(
            validate_oci_schema_version(&doc),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn schema_version_missing() {
        let doc = json!({});
        assert!(matches!(
            validate_oci_schema_version(&doc),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn schema_version_not_a_number() {
        let doc = json!({"schemaVersion": "2"});
        assert!(matches!(
            validate_oci_schema_version(&doc),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    // ── validate_oci_descriptor ──

    #[test]
    fn descriptor_valid() {
        let desc = json!({
            "digest": "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
            "size": 1234
        });
        let result = validate_oci_descriptor(&desc);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        );
    }

    #[test]
    fn descriptor_valid_without_mediatype() {
        let desc = json!({
            "digest": "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
            "size": 100
        });
        assert!(validate_oci_descriptor(&desc).is_ok());
    }

    #[test]
    fn descriptor_valid_with_string_mediatype() {
        let desc = json!({
            "digest": "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
            "size": 100,
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip"
        });
        assert!(validate_oci_descriptor(&desc).is_ok());
    }

    #[test]
    fn descriptor_missing_digest() {
        let desc = json!({"size": 100});
        assert!(matches!(
            validate_oci_descriptor(&desc),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn descriptor_missing_size() {
        let desc = json!({
            "digest": "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        });
        assert!(matches!(
            validate_oci_descriptor(&desc),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn descriptor_invalid_digest_format() {
        let desc = json!({
            "digest": "not-a-digest",
            "size": 100
        });
        assert!(matches!(
            validate_oci_descriptor(&desc),
            Err(ServerError::InvalidDigest)
        ));
    }

    #[test]
    fn descriptor_non_object() {
        let desc = json!("just a string");
        assert!(matches!(
            validate_oci_descriptor(&desc),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn descriptor_non_string_mediatype() {
        let desc = json!({
            "digest": "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
            "size": 100,
            "mediaType": 123
        });
        assert!(matches!(
            validate_oci_descriptor(&desc),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn descriptor_array_digest() {
        let desc = json!({
            "digest": ["sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"],
            "size": 100
        });
        assert!(matches!(
            validate_oci_descriptor(&desc),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    // ── ensure_manifest_representation_is_acceptable ──

    #[test]
    fn accept_header_absent_allows_any() {
        let headers = HeaderMap::new();
        assert!(
            ensure_manifest_representation_is_acceptable(
                &headers,
                "application/vnd.oci.image.manifest.v1+json"
            )
            .is_ok()
        );
    }

    #[test]
    fn accept_wildcard_accepts_any() {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
        assert!(
            ensure_manifest_representation_is_acceptable(
                &headers,
                "application/vnd.oci.image.manifest.v1+json"
            )
            .is_ok()
        );
    }

    #[test]
    fn accept_exact_match() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.oci.image.manifest.v1+json"),
        );
        assert!(
            ensure_manifest_representation_is_acceptable(
                &headers,
                "application/vnd.oci.image.manifest.v1+json"
            )
            .is_ok()
        );
    }

    #[test]
    fn accept_type_wildcard() {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/*"));
        assert!(
            ensure_manifest_representation_is_acceptable(
                &headers,
                "application/vnd.oci.image.manifest.v1+json"
            )
            .is_ok()
        );
    }

    #[test]
    fn accept_subtype_wildcard() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.oci.image.manifest.v1+json"),
        );
        assert!(
            ensure_manifest_representation_is_acceptable(
                &headers,
                "application/vnd.oci.image.manifest.v1+json"
            )
            .is_ok()
        );
    }

    #[test]
    fn accept_mismatch_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.docker.distribution.manifest.v2+json"),
        );
        assert!(matches!(
            ensure_manifest_representation_is_acceptable(
                &headers,
                "application/vnd.oci.image.manifest.v1+json"
            ),
            Err(ServerError::NotAcceptable)
        ));
    }

    #[test]
    fn accept_comma_separated_first_match_wins() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static(
                "application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json",
            ),
        );
        assert!(
            ensure_manifest_representation_is_acceptable(
                &headers,
                "application/vnd.oci.image.manifest.v1+json"
            )
            .is_ok()
        );
    }

    #[test]
    fn accept_with_parameters_strips_before_comparison() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.oci.image.manifest.v1+json; q=0.9"),
        );
        assert!(
            ensure_manifest_representation_is_acceptable(
                &headers,
                "application/vnd.oci.image.manifest.v1+json"
            )
            .is_ok()
        );
    }

    #[test]
    fn accept_with_parameters_type_wildcard() {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/*; q=0.5"));
        assert!(
            ensure_manifest_representation_is_acceptable(
                &headers,
                "application/vnd.oci.image.manifest.v1+json"
            )
            .is_ok()
        );
    }

    #[test]
    fn accept_empty_candidate_entry_skipped() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static(", application/vnd.oci.image.manifest.v1+json"),
        );
        assert!(
            ensure_manifest_representation_is_acceptable(
                &headers,
                "application/vnd.oci.image.manifest.v1+json"
            )
            .is_ok()
        );
    }

    #[test]
    fn accept_entry_without_slash_skipped() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("bogus, application/vnd.oci.image.manifest.v1+json"),
        );
        assert!(
            ensure_manifest_representation_is_acceptable(
                &headers,
                "application/vnd.oci.image.manifest.v1+json"
            )
            .is_ok()
        );
    }

    #[test]
    fn media_type_with_parameters_is_normalized() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.oci.image.manifest.v1+json"),
        );
        // The media_type has a parameter; normalize_media_type should strip it before comparison
        assert!(
            ensure_manifest_representation_is_acceptable(
                &headers,
                "application/vnd.oci.image.manifest.v1+json; charset=utf-8"
            )
            .is_ok()
        );
    }

    // ── validate_oci_schema_version ────────────────────────────────────

    // Already covered above. Add missing schema version edges.

    #[test]
    fn schema_version_negative_number() {
        let doc = json!({"schemaVersion": -2});
        assert!(matches!(
            validate_oci_schema_version(&doc),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    // ── validate_oci_descriptor additional edges ────────────────────────

    #[test]
    fn descriptor_with_non_string_digest() {
        let desc = json!({
            "digest": null,
            "size": 100
        });
        assert!(matches!(
            validate_oci_descriptor(&desc),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    // ── normalize_media_type without '/' (line 345) ────────────────────

    #[test]
    fn media_type_without_slash_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
        assert!(matches!(
            ensure_manifest_representation_is_acceptable(
                &headers,
                "applicationjson"
            ),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    // ── ensure_manifest_representation_is_acceptable: header value split ──

    #[test]
    fn accept_header_value_without_slash_skipped() {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("bogus, application/json"));
        assert!(
            ensure_manifest_representation_is_acceptable(&headers, "application/json").is_ok()
        );
    }

    // ── validate_oci_descriptor: subject field (line 221) ──────────────

    #[test]
    fn manifest_with_invalid_subject_descriptor_rejected() {
        let doc = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "subject": { "size": 100 },  // missing digest
            "config": { "digest": "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", "size": 100 },
            "layers": []
        });
        // validate_oci_descriptor is called on subject at line 221.
        let subject = doc.get("subject").unwrap();
        assert!(matches!(
            validate_oci_descriptor(subject),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn manifest_with_valid_subject_descriptor_accepted() {
        let doc = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "subject": { "digest": "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", "size": 100 },
            "config": { "digest": "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", "size": 100 },
            "layers": []
        });
        let subject = doc.get("subject").unwrap();
        assert!(validate_oci_descriptor(subject).is_ok());
    }

    // ── ensure_manifest_representation_is_acceptable additional edges ──

    #[test]
    fn accept_header_valid_utf8_rejects_invalid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_maybe_shared(Bytes::from_static(b"\xff\xfe\xfd"))
                .unwrap(),
        );
        assert!(matches!(
            ensure_manifest_representation_is_acceptable(
                &headers,
                "application/vnd.oci.image.manifest.v1+json"
            ),
            Err(ServerError::NotAcceptable)
        ));
    }
}
