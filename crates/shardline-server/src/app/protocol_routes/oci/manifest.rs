use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Body,
    http::{
        HeaderMap, StatusCode, Uri,
        header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE, VARY},
    },
    response::{IntoResponse, Response},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use shardline_server_core::AuthorizedRepository;

use crate::{
    ServerError,
    oci_adapter::{OciReference, oci_manifest_location, parse_reference},
    protocol_support::{parse_sha256_digest, scope_namespace, validate_oci_tag},
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

use super::super::{AppState, direct_object_response_from_snapshot, parse_query_values};
use super::helpers::{
    OciRepository, oci_blob_index_key, oci_blob_key, oci_manifest_index_key, oci_manifest_key,
    oci_manifest_media_type_key,
};
use super::tags::{resolve_oci_tag_digest, update_oci_tags};

pub(super) const OCI_IMAGE_MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";
pub(super) const OCI_IMAGE_INDEX_MEDIA_TYPE: &str = "application/vnd.oci.image.index.v1+json";
pub(super) const DOCKER_SCHEMA2_MANIFEST_MEDIA_TYPE: &str =
    "application/vnd.docker.distribution.manifest.v2+json";
pub(super) const DOCKER_SCHEMA2_MANIFEST_LIST_MEDIA_TYPE: &str =
    "application/vnd.docker.distribution.manifest.list.v2+json";

#[tracing::instrument(skip(state, headers, repo), fields(repository = %repo.repository(), reference))]
pub(crate) async fn oci_get_manifest(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    repo: &OciRepository,
    reference: &str,
    head_only: bool,
) -> Result<Response, ServerError> {
    let repository = repo.repository();
    let auth = repo.capability();
    let digest_hex = resolve_manifest_digest(state, repository, reference, auth).await?;
    ensure_oci_object_visible(
        state,
        &oci_manifest_index_key(repository, &digest_hex, auth),
    )
    .await?;
    let manifest_key = oci_manifest_key(repository, &digest_hex, auth)?;
    let media_type_key = oci_manifest_media_type_key(repository, &digest_hex, auth)?;
    let snapshot = state.backend.object_read_snapshot(&manifest_key).await?;
    let total_length = snapshot.total_length;
    let media_type =
        String::from_utf8(state.backend.read_object(&media_type_key).await?).map_err(|e| {
            tracing::warn!(error = %e, "invalid media type utf-8");
            ServerError::InvalidManifestReference
        })?;
    if let Err(error) = ensure_manifest_representation_is_acceptable(headers, &media_type) {
        if matches!(error, ServerError::NotAcceptable) {
            let mut response = error.into_response();
            response
                .headers_mut()
                .append(VARY, axum::http::HeaderValue::from_static("Accept"));
            return Ok(response);
        }
        return Err(error);
    }
    if head_only {
        return Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_LENGTH, total_length.to_string())
            .header(CONTENT_TYPE, media_type)
            .header(VARY, "Accept")
            .header("Docker-Content-Digest", format!("sha256:{digest_hex}"))
            .body(Body::empty())
            .map_err(|e| {
                tracing::warn!(error = %e, "failed to build head manifest response");
                ServerError::Overflow
            });
    }

    let mut response = direct_object_response_from_snapshot(
        state,
        headers,
        &manifest_key,
        &media_type,
        Some(format!("sha256:{digest_hex}")),
        "oci",
        snapshot,
    )
    .await?;
    response
        .headers_mut()
        .append(VARY, axum::http::HeaderValue::from_static("Accept"));
    Ok(response)
}

#[tracing::instrument(skip(state, headers, uri, body, repo), fields(repository = %repo.repository(), reference))]
pub(crate) async fn oci_put_manifest(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    uri: &Uri,
    repo: &OciRepository,
    reference: &str,
    body: Body,
) -> Result<Response, ServerError> {
    let repository = repo.repository();
    let auth = repo.capability();
    // Reject all request references before immutable bytes become visible.
    let reference = parse_reference(reference)?;
    let mut accepted_tags = match &reference {
        OciReference::Tag(tag) => vec![tag.clone()],
        OciReference::Digest(_) => Vec::new(),
    };
    accepted_tags.extend(parse_query_values(uri, "tag")?);
    if accepted_tags.len() > super::super::MAX_OCI_MANIFEST_TAGS {
        return Err(ServerError::InvalidManifestReference);
    }
    for tag in &accepted_tags {
        validate_oci_tag(tag)?;
    }
    accepted_tags.sort();
    accepted_tags.dedup();
    let mut body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let bytes = read_body_to_bytes(&mut body).await?;
    let digest_hex = hex::encode(Sha256::digest(&bytes));
    if let OciReference::Digest(reference_digest) = &reference
        && reference_digest != &digest_hex
    {
        return Err(ServerError::ExpectedBodyHashMismatch);
    }
    let media_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or(OCI_IMAGE_MANIFEST_MEDIA_TYPE)
        .to_owned();
    let lock_key = shardline_index::ResourceLockKey::oci_repository(
        &scope_namespace(auth.namespace()),
        repository,
    );
    let mut repository_guard = state
        .backend
        .acquire_resource_write_lock(state.config.root_dir(), &lock_key)
        .await?;
    validate_oci_manifest_document(state, repository, auth, &media_type, &bytes).await?;
    let manifest_key = oci_manifest_key(repository, &digest_hex, auth)?;
    let media_type_key = oci_manifest_media_type_key(repository, &digest_hex, auth)?;
    let _stored_manifest = state
        .backend
        // Manifests are authoritative, repository-enumerable metadata. Keep
        // them in the manifest namespace so reference checks can inventory
        // every digest; large blob payloads use chunk-backed file records.
        .put_object_bytes_if_absent(&manifest_key, bytes)
        .await?;
    let _stored_media_type = state
        .backend
        .put_object_bytes_if_absent(&media_type_key, media_type.clone().into_bytes())
        .await?;
    update_oci_tags(
        state,
        &mut repository_guard,
        repository,
        auth,
        &accepted_tags,
        &digest_hex,
    )
    .await?;
    repository_guard.assert_current().await?;

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
    builder.body(Body::empty()).map_err(|e| {
        tracing::warn!(error = %e, "failed to build put manifest response body");
        ServerError::Overflow
    })
}

#[tracing::instrument(skip(state, _headers, repo), fields(repository = %repo.repository(), reference))]
pub(crate) async fn oci_delete_manifest(
    state: &Arc<AppState>,
    _headers: &HeaderMap,
    repo: &OciRepository,
    reference: &str,
) -> Result<Response, ServerError> {
    let repository = repo.repository();
    let auth = repo.capability();
    let lock_key = shardline_index::ResourceLockKey::oci_repository(
        &scope_namespace(auth.namespace()),
        repository,
    );
    let mut repository_guard = state
        .backend
        .acquire_resource_write_lock(state.config.root_dir(), &lock_key)
        .await?;
    let digest_hex = resolve_manifest_digest(state, repository, reference, auth).await?;
    let manifest_key = oci_manifest_key(repository, &digest_hex, auth)?;
    let index_key = oci_manifest_index_key(repository, &digest_hex, auth);
    ensure_oci_object_visible(state, &index_key).await?;
    // Confirm that legacy deployments really contain the immutable bytes
    // before creating a tombstone. The request never physically deletes them.
    let _length = state.backend.object_length(&manifest_key).await?;
    repository_guard.assert_current().await?;
    state
        .backend
        .delete_oci_object_locked(&mut repository_guard, &index_key)
        .await?;
    repository_guard.assert_current().await?;

    Response::builder()
        .status(StatusCode::ACCEPTED)
        .body(Body::empty())
        .map_err(|e| {
            tracing::warn!(error = %e, "failed to build delete manifest response body");
            ServerError::Overflow
        })
}

async fn resolve_manifest_digest(
    state: &Arc<AppState>,
    repository: &str,
    reference: &str,
    auth: &AuthorizedRepository,
) -> Result<String, ServerError> {
    match parse_reference(reference)? {
        OciReference::Digest(digest_hex) => Ok(digest_hex),
        OciReference::Tag(tag) => resolve_oci_tag_digest(state, repository, auth, &tag).await,
    }
}

async fn validate_oci_manifest_document(
    state: &Arc<AppState>,
    repository: &str,
    auth: &AuthorizedRepository,
    media_type: &str,
    bytes: &[u8],
) -> Result<(), ServerError> {
    let document: Value = serde_json::from_slice(bytes).map_err(|e| {
        tracing::warn!(error = %e, "invalid manifest json");
        ServerError::InvalidManifestReference
    })?;
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
            validate_oci_image_manifest_document(state, repository, auth, &document).await
        }
        OCI_IMAGE_INDEX_MEDIA_TYPE | DOCKER_SCHEMA2_MANIFEST_LIST_MEDIA_TYPE => {
            validate_oci_image_index_document(state, repository, auth, &document).await
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
    auth: &AuthorizedRepository,
    document: &Value,
) -> Result<(), ServerError> {
    let config = document
        .get("config")
        .ok_or(ServerError::InvalidManifestReference)?;
    let (config_digest_hex, config_size) = validate_oci_descriptor(config)?;
    ensure_oci_blob_exists(state, repository, auth, &config_digest_hex, config_size).await?;
    let mut verified_sizes = HashMap::from([(config_digest_hex, config_size)]);

    let layers = document
        .get("layers")
        .and_then(Value::as_array)
        .ok_or(ServerError::InvalidManifestReference)?;
    for layer in layers {
        let (digest_hex, size) = validate_oci_descriptor(layer)?;
        if let Some(verified_size) = verified_sizes.get(&digest_hex) {
            if *verified_size != size {
                return Err(ServerError::InvalidManifestReference);
            }
        } else {
            ensure_oci_blob_exists(state, repository, auth, &digest_hex, size).await?;
            verified_sizes.insert(digest_hex, size);
        }
    }

    Ok(())
}

async fn validate_oci_image_index_document(
    state: &Arc<AppState>,
    repository: &str,
    auth: &AuthorizedRepository,
    document: &Value,
) -> Result<(), ServerError> {
    let manifests = document
        .get("manifests")
        .and_then(Value::as_array)
        .ok_or(ServerError::InvalidManifestReference)?;
    let mut verified_sizes = HashMap::new();
    for manifest in manifests {
        let (digest_hex, size) = validate_oci_descriptor(manifest)?;
        if let Some(verified_size) = verified_sizes.get(&digest_hex) {
            if *verified_size != size {
                return Err(ServerError::InvalidManifestReference);
            }
        } else {
            ensure_oci_manifest_exists(state, repository, auth, &digest_hex, size).await?;
            verified_sizes.insert(digest_hex, size);
        }
    }

    Ok(())
}

fn validate_oci_descriptor(descriptor: &Value) -> Result<(String, u64), ServerError> {
    let descriptor = descriptor
        .as_object()
        .ok_or(ServerError::InvalidManifestReference)?;
    let digest = descriptor
        .get("digest")
        .and_then(Value::as_str)
        .ok_or(ServerError::InvalidManifestReference)?;
    let digest_hex = parse_sha256_digest(digest)?;
    let size = descriptor
        .get("size")
        .and_then(Value::as_u64)
        .ok_or(ServerError::InvalidManifestReference)?;
    if descriptor
        .get("mediaType")
        .is_some_and(|media_type| !media_type.is_string())
    {
        return Err(ServerError::InvalidManifestReference);
    }
    Ok((digest_hex, size))
}

async fn ensure_oci_blob_exists(
    state: &Arc<AppState>,
    repository: &str,
    auth: &AuthorizedRepository,
    digest_hex: &str,
    expected_size: u64,
) -> Result<(), ServerError> {
    if state
        .backend
        .oci_object_is_deleted(&oci_blob_index_key(repository, digest_hex, auth))
        .await?
    {
        return Err(ServerError::InvalidManifestReference);
    }
    let object_key = oci_blob_key(repository, digest_hex, auth)?;
    match state.backend.object_length(&object_key).await {
        Ok(length) if length == expected_size => Ok(()),
        Ok(_) => Err(ServerError::InvalidManifestReference),
        Err(ServerError::NotFound) => Err(ServerError::InvalidManifestReference),
        Err(error) => Err(error),
    }
}

async fn ensure_oci_manifest_exists(
    state: &Arc<AppState>,
    repository: &str,
    auth: &AuthorizedRepository,
    digest_hex: &str,
    expected_size: u64,
) -> Result<(), ServerError> {
    if state
        .backend
        .oci_object_is_deleted(&oci_manifest_index_key(repository, digest_hex, auth))
        .await?
    {
        return Err(ServerError::InvalidManifestReference);
    }
    let object_key = oci_manifest_key(repository, digest_hex, auth)?;
    match state.backend.object_length(&object_key).await {
        Ok(length) if length == expected_size => Ok(()),
        Ok(_) => Err(ServerError::InvalidManifestReference),
        Err(ServerError::NotFound) => Err(ServerError::InvalidManifestReference),
        Err(error) => Err(error),
    }
}

pub(super) async fn ensure_oci_object_visible(
    state: &Arc<AppState>,
    key: &shardline_index::OciObjectKey,
) -> Result<(), ServerError> {
    if state.backend.oci_object_is_deleted(key).await? {
        Err(ServerError::NotFound)
    } else {
        Ok(())
    }
}

fn normalize_media_type(value: &str) -> &str {
    value.split(';').next().map_or(value, str::trim)
}

// Delimiters inside quoted parameter values are data, not list separators.
fn split_media_field(value: &str, delimiter: char) -> Vec<&str> {
    let mut quoted = false;
    let mut escaped = false;
    let mut start = 0;
    let mut parts = Vec::new();
    for (index, ch) in value.char_indices() {
        if escaped {
            escaped = false;
        } else if quoted && ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            quoted = !quoted;
        } else if !quoted && ch == delimiter {
            parts.push(value.get(start..index).unwrap_or_default().trim());
            start = index.saturating_add(ch.len_utf8());
        }
    }
    parts.push(value.get(start..).unwrap_or_default().trim());
    parts
}

fn media_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

fn media_parameter_value(value: &str) -> Option<String> {
    if media_token(value) {
        return Some(value.to_owned());
    }
    let inner = value.strip_prefix('"')?.strip_suffix('"')?;
    let mut decoded = String::new();
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        let ch = if ch == '\\' {
            chars.next()?
        } else {
            if ch == '"' {
                return None;
            }
            ch
        };
        if ch.is_control() && ch != '\t' {
            return None;
        }
        decoded.push(ch);
    }
    Some(decoded)
}

struct ManifestMediaRange<'input> {
    kind: &'input str,
    subtype: &'input str,
    parameters: Vec<(String, String)>,
    quality: u16,
}

fn parse_manifest_media_range(field: &str, accept: bool) -> Option<ManifestMediaRange<'_>> {
    let parts = split_media_field(field, ';');
    let (kind, subtype) = parts.first()?.split_once('/')?;
    if !media_token(kind)
        || !media_token(subtype)
        || (kind == "*" && subtype != "*")
        || (!accept && (kind == "*" || subtype == "*"))
    {
        return None;
    }
    let mut range = ManifestMediaRange {
        kind,
        subtype,
        parameters: Vec::new(),
        quality: 1000,
    };
    let mut quality_seen = false;
    for part in parts.iter().skip(1).filter(|part| !part.is_empty()) {
        let (name, value) = part.split_once('=')?;
        let name = name.trim();
        let value = value.trim();
        if !media_token(name) {
            return None;
        }
        if accept && name.eq_ignore_ascii_case("q") {
            if quality_seen {
                return None;
            }
            quality_seen = true;
            let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
            if fraction.len() > 3 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            range.quality = match whole {
                "0" => fraction
                    .parse::<u16>()
                    .unwrap_or(0)
                    .checked_mul(match fraction.len() {
                        0 => 1000,
                        1 => 100,
                        2 => 10,
                        _ => 1,
                    })?,
                "1" if fraction.bytes().all(|b| b == b'0') => 1000,
                _ => return None,
            };
        } else {
            // RFC 9110 permits tolerating q before other media parameters.
            let name = name.to_ascii_lowercase();
            if range
                .parameters
                .iter()
                .any(|(existing, _)| *existing == name)
            {
                return None;
            }
            range.parameters.push((name, media_parameter_value(value)?));
        }
    }
    Some(range)
}

fn ensure_manifest_representation_is_acceptable(
    headers: &HeaderMap,
    media_type: &str,
) -> Result<(), ServerError> {
    let stored = parse_manifest_media_range(media_type, false)
        .ok_or(ServerError::InvalidManifestReference)?;
    let accepted = headers.get_all(ACCEPT);
    if accepted.iter().next().is_none() {
        return Ok(());
    }
    let mut best: Option<((u8, usize), u16)> = None;
    for header_value in accepted {
        let header_text = header_value
            .to_str()
            .map_err(|_error| ServerError::NotAcceptable)?;
        for candidate in split_media_field(header_text, ',') {
            let Some(range) = parse_manifest_media_range(candidate, true) else {
                continue;
            };
            if !(range.kind == "*" || range.kind.eq_ignore_ascii_case(stored.kind))
                || !(range.subtype == "*" || range.subtype.eq_ignore_ascii_case(stored.subtype))
                || !range.parameters.iter().all(|(name, value)| {
                    stored.parameters.iter().any(|(stored_name, stored_value)| {
                        name == stored_name
                            && (value == stored_value
                                || (name == "charset" && value.eq_ignore_ascii_case(stored_value)))
                    })
                })
            {
                continue;
            }
            let specificity = (
                if range.kind == "*" {
                    0
                } else if range.subtype == "*" {
                    1
                } else {
                    2
                },
                range.parameters.len(),
            );
            // Most specific matching range determines quality, independent of order.
            // For duplicate equally specific entries, prefer the highest quality.
            if best.is_none_or(|(previous, quality)| {
                specificity > previous || (specificity == previous && range.quality > quality)
            }) {
                best = Some((specificity, range.quality));
            }
        }
    }
    if best.is_some_and(|(_, quality)| quality > 0) {
        Ok(())
    } else {
        Err(ServerError::NotAcceptable)
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Bytes;
    use axum::http::{HeaderMap, HeaderValue};
    use serde_json::json;

    use super::*;

    #[test]
    fn accept_quality_specificity_and_parameters() {
        let cases = [
            ("application/json;q=0", "application/json", false),
            ("*/*;q=0", "application/json", false),
            ("application/json;q=0, */*;q=1", "application/json", false),
            ("*/*;q=1, application/json;q=0", "application/json", false),
            ("application/*;q=0, */*", "application/json", false),
            (
                "application/json;q=0.001, application/*;q=0",
                "application/json",
                true,
            ),
            ("APPLICATION/JSON;Q=0.5", "application/json", true),
            ("*/json", "application/json", false),
            ("application/json;profile=v2", "application/json", false),
            (
                "application/json;profile=v2",
                "application/json;profile=v1",
                false,
            ),
            (
                "application/json;profile=v2;q=0, application/json",
                "application/json;profile=v2",
                false,
            ),
            (
                "application/json;profile=v2;q=0, application/json",
                "application/json;profile=v1",
                true,
            ),
            (
                "application/json;PROFILE=\"v2\"",
                "application/json;profile=v2",
                true,
            ),
            (
                "application/json;profile=V2",
                "application/json;profile=v2",
                false,
            ),
            (
                "application/json;charset=UTF-8",
                "application/json;charset=utf-8",
                true,
            ),
            (
                "application/json;profile=\"a,b;c\"",
                "application/json;profile=\"a,b;c\"",
                true,
            ),
            (
                "application/json;profile=\"a,b;c\"",
                "application/json;profile=a",
                false,
            ),
            ("application/json;q=0.", "application/json", false),
            ("application/json;q=0.000", "application/json", false),
            ("application/json;q=1.000", "application/json", true),
            (
                "application/json;q=0.5;profile=v2",
                "application/json;profile=v2",
                true,
            ),
            (
                r#"application/json;profile="a\"b""#,
                r#"application/json;profile="a\"b""#,
                true,
            ),
            (
                r#"application/json;profile="unterminated"#,
                "application/json",
                false,
            ),
            ("application/json/foo", "application/json", false),
            ("application/json;q=1.001", "application/json", false),
            ("application/json;q=0.0001", "application/json", false),
            ("application/json;q=NaN", "application/json", false),
            ("application/json;q=\"0.5\"", "application/json", false),
            ("application/json;q=0;q=1", "application/json", false),
        ];
        for (accept, stored, expected) in cases {
            let mut headers = HeaderMap::new();
            headers.insert(ACCEPT, HeaderValue::from_str(accept).unwrap());
            assert_eq!(
                ensure_manifest_representation_is_acceptable(&headers, stored).is_ok(),
                expected,
                "Accept: {accept}; stored: {stored}"
            );
        }
        let mut headers = HeaderMap::new();
        headers.append(ACCEPT, HeaderValue::from_static("*/*"));
        headers.append(ACCEPT, HeaderValue::from_static("application/json;q=0"));
        assert!(matches!(
            ensure_manifest_representation_is_acceptable(&headers, "application/json"),
            Err(ServerError::NotAcceptable)
        ));
    }

    #[tokio::test]
    async fn manifest_get_and_head_honor_accept_exclusions() {
        use super::super::test_helpers::{build_oci_test_state, oci_test_router};
        use axum::http::{Method, Request};
        use tower::ServiceExt;
        let context = build_oci_test_state().await;
        let app = oci_test_router(&context.state);
        let manifest =
            json!({"schemaVersion": 2, "mediaType": OCI_IMAGE_INDEX_MEDIA_TYPE, "manifests": []})
                .to_string();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/v2/accept/test/manifests/latest")
                    .header(CONTENT_TYPE, OCI_IMAGE_INDEX_MEDIA_TYPE)
                    .body(Body::from(manifest))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        for method in [Method::GET, Method::HEAD] {
            for (accept, expected) in [
                (
                    format!("{OCI_IMAGE_INDEX_MEDIA_TYPE};q=0, */*"),
                    StatusCode::NOT_ACCEPTABLE,
                ),
                (
                    format!("*/*, {OCI_IMAGE_INDEX_MEDIA_TYPE};q=0"),
                    StatusCode::NOT_ACCEPTABLE,
                ),
                (
                    format!("{OCI_IMAGE_INDEX_MEDIA_TYPE};profile=unsupported"),
                    StatusCode::NOT_ACCEPTABLE,
                ),
                (
                    format!(
                        "{};Q=0.001",
                        OCI_IMAGE_INDEX_MEDIA_TYPE.to_ascii_uppercase()
                    ),
                    StatusCode::OK,
                ),
            ] {
                let response = app
                    .clone()
                    .oneshot(
                        Request::builder()
                            .method(method.clone())
                            .uri("/v2/accept/test/manifests/latest")
                            .header(ACCEPT, accept)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), expected, "{method}");
                assert!(
                    response
                        .headers()
                        .get_all(VARY)
                        .iter()
                        .any(|value| value == "Accept")
                );
            }
        }
    }

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
            result.unwrap().0,
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
            ensure_manifest_representation_is_acceptable(&headers, "applicationjson"),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    // ── ensure_manifest_representation_is_acceptable: header value split ──

    #[test]
    fn accept_header_value_without_slash_skipped() {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("bogus, application/json"));
        assert!(ensure_manifest_representation_is_acceptable(&headers, "application/json").is_ok());
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
            HeaderValue::from_maybe_shared(Bytes::from_static(b"\xff\xfe\xfd")).unwrap(),
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
