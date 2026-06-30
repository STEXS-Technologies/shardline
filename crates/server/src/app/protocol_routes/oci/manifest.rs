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

use super::super::{
    AppState,
    direct_object_response,
    parse_query_values,
    scope_from_auth,
};
use super::token::oci_authorize;
use super::tags::{update_oci_tags, delete_oci_tags_pointing_to_digest};

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
        .header(axum::http::header::LOCATION, oci_manifest_location(repository, &digest_hex))
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
    let crate::oci_adapter::OciReference::Digest(digest_hex) = parse_reference(reference)? else {
        return Err(ServerError::InvalidManifestReference);
    };
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
        let _subject_digest = validate_oci_descriptor(subject)?;
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
