use std::sync::Arc;

use axum::{
    body::Body,
    http::{
        StatusCode, Uri,
        header::{CONTENT_LENGTH, CONTENT_TYPE, LINK},
    },
    response::Response,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::super::parse_query_map;
use super::{
    helpers::{
        OciRepository, oci_manifest_index_key, oci_manifest_key, oci_manifest_media_type_key,
    },
    manifest::OCI_IMAGE_INDEX_MEDIA_TYPE,
};
use crate::{ServerError, app::AppState, protocol_support::parse_sha256_digest};

const DEFAULT_PAGE_SIZE: usize = 100;
const MAX_PAGE_SIZE: usize = 1000;
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;
mod subject_index;

pub(super) async fn index_manifest(
    state: &Arc<AppState>,
    repository: &str,
    auth: &shardline_server_core::AuthorizedRepository,
    digest: &str,
    document: &Value,
) -> Result<(), ServerError> {
    if matches!(
        subject_index::lookup(state).await?,
        subject_index::Lookup::ManifestInventory
    ) {
        return Ok(());
    }
    subject_index::retain(
        state,
        subject_index::mapping(repository, auth, digest, document)?,
    )
    .await
}

/// Derive referrers from authoritative, immutable manifests, including documents
/// uploaded before referrer discovery was available. Tombstones remain the
/// publication boundary; a staged or logically deleted manifest is never listed.
pub(super) async fn oci_referrers(
    state: &Arc<AppState>,
    uri: &Uri,
    repo: &OciRepository,
    digest_hex: &str,
) -> Result<Response, ServerError> {
    let query = parse_query_map(uri)?;
    let limit = query.get("n").map_or(Ok(DEFAULT_PAGE_SIZE), |value| {
        value
            .parse::<usize>()
            .map_err(|_invalid_limit| ServerError::InvalidManifestReference)
    })?;
    if limit == 0 || limit > MAX_PAGE_SIZE {
        return Err(ServerError::InvalidManifestReference);
    }
    let filter = query.get("artifactType");
    let mut after = query
        .get("last")
        .map(|last| parse_sha256_digest(&format!("sha256:{last}")))
        .transpose()?;
    let lookup = subject_index::lookup(state).await?;
    if matches!(lookup, subject_index::Lookup::Indexed) {
        subject_index::reconcile(state, repo.repository(), repo.capability()).await?;
    }
    let subject = format!("sha256:{digest_hex}");
    let mut descriptors = Vec::new();
    let mut descriptor_bytes = 0usize;
    let mut next_cursor = None;
    let mut last_returned = None;
    'scan: loop {
        let page = subject_index::candidates(
            state,
            repo.repository(),
            repo.capability(),
            digest_hex,
            after.as_deref(),
            lookup,
        )
        .await?;
        if page.is_empty() {
            break;
        }
        for entry in page {
            let candidate = entry.digest_hex.as_str();
            after = Some(entry.digest_hex.clone());
            let key = oci_manifest_key(repo.repository(), candidate, repo.capability())?;
            if state
                .backend
                .oci_object_is_deleted(&oci_manifest_index_key(
                    repo.repository(),
                    candidate,
                    repo.capability(),
                ))
                .await?
            {
                continue;
            }
            let bytes = match state.backend.read_object(&key).await {
                Ok(bytes) => bytes,
                // A derived row may outlive physical reclamation. It cannot
                // resurrect bytes absent from the immutable inventory. Other
                // read failures, including corruption, still fail closed.
                Err(ServerError::NotFound) => continue,
                Err(error) => return Err(error),
            };
            if hex::encode(Sha256::digest(&bytes)) != candidate {
                return Err(ServerError::InvalidManifestReference);
            }
            let mut document: Value = serde_json::from_slice(&bytes)?;
            if let Some(expected) = &entry.mapping
                && subject_index::mapping(
                    repo.repository(),
                    repo.capability(),
                    candidate,
                    &document,
                )? != *expected
            {
                return Err(ServerError::InvalidManifestReference);
            }
            if document
                .get("subject")
                .and_then(|descriptor| descriptor.get("digest"))
                .and_then(Value::as_str)
                == Some(subject.as_str())
                && document.get("mediaType").is_none()
            {
                let media_type_key =
                    oci_manifest_media_type_key(repo.repository(), candidate, repo.capability())?;
                let media_type = state.backend.read_object(&media_type_key).await?;
                let media_type = json!(
                    std::str::from_utf8(&media_type)
                        .map_err(|_invalid_utf8| ServerError::InvalidManifestReference)?
                );
                document
                    .as_object_mut()
                    .ok_or(ServerError::InvalidManifestReference)?
                    .insert("mediaType".to_owned(), media_type);
            }
            let Some(descriptor) = referrer_descriptor(&document, &subject, candidate, bytes.len())
            else {
                continue;
            };
            if filter.is_some_and(|expected| {
                descriptor.get("artifactType").and_then(Value::as_str) != Some(expected.as_str())
            }) {
                continue;
            }
            let encoded_bytes = serde_json::to_vec(&descriptor)?.len();
            if descriptors.len() >= limit
                || (!descriptors.is_empty()
                    && descriptor_bytes.saturating_add(encoded_bytes) > MAX_PAGE_BYTES)
            {
                next_cursor = last_returned;
                break 'scan;
            }
            descriptor_bytes = descriptor_bytes.saturating_add(encoded_bytes);
            descriptors.push(descriptor);
            last_returned = Some(candidate.to_owned());
        }
    }
    let body = serde_json::to_vec(
        &json!({"schemaVersion": 2, "mediaType": OCI_IMAGE_INDEX_MEDIA_TYPE, "manifests": descriptors}),
    )?;
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, OCI_IMAGE_INDEX_MEDIA_TYPE)
        .header(CONTENT_LENGTH, body.len().to_string());
    if filter.is_some() {
        response = response.header("OCI-Filters-Applied", "artifactType");
    }
    if let Some(cursor) = next_cursor {
        let mut parameters = url::form_urlencoded::Serializer::new(String::new());
        parameters
            .append_pair("n", &limit.to_string())
            .append_pair("last", &cursor);
        if let Some(artifact_type) = filter {
            parameters.append_pair("artifactType", artifact_type);
        }
        response = response.header(
            LINK,
            format!(
                "</v2/{}/referrers/{}?{}>; rel=\"next\"",
                repo.repository(),
                subject,
                parameters.finish()
            ),
        );
    }
    response
        .body(Body::from(body))
        .map_err(|_invalid_response| ServerError::Overflow)
}

fn referrer_descriptor(
    document: &Value,
    subject: &str,
    digest: &str,
    size: usize,
) -> Option<Value> {
    if document.get("subject")?.get("digest")?.as_str()? != subject {
        return None;
    }
    let media_type = document.get("mediaType")?.as_str()?;
    let mut descriptor =
        json!({"mediaType": media_type, "digest": format!("sha256:{digest}"), "size": size});
    let artifact_type = document
        .get("artifactType")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .or_else(|| document.get("config")?.get("mediaType")?.as_str());
    if let Some(artifact_type) = artifact_type {
        descriptor
            .as_object_mut()?
            .insert("artifactType".to_owned(), json!(artifact_type));
    }
    if let Some(annotations) = document.get("annotations") {
        descriptor
            .as_object_mut()?
            .insert("annotations".to_owned(), annotations.clone());
    }
    Some(descriptor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_preserves_artifact_type_and_annotations() {
        let document = json!({"subject":{"digest":"sha256:subject"}, "mediaType":"application/vnd.oci.image.manifest.v1+json", "artifactType":"application/example.signature", "config":{"mediaType":"application/example.config"}, "annotations":{"org.example.name":"signature"}});
        let descriptor = referrer_descriptor(&document, "sha256:subject", "hash", 123).unwrap();
        assert_eq!(descriptor["artifactType"], "application/example.signature");
        assert_eq!(descriptor["annotations"], document["annotations"]);
        assert_eq!(descriptor["size"], 123);
        assert!(referrer_descriptor(&document, "sha256:other", "hash", 123).is_none());
    }

    #[test]
    fn descriptor_uses_config_type_when_artifact_type_is_absent() {
        let document = json!({"subject":{"digest":"sha256:subject"}, "mediaType":"application/vnd.oci.image.manifest.v1+json", "config":{"mediaType":"application/example.config"}});
        assert_eq!(
            referrer_descriptor(&document, "sha256:subject", "hash", 1).unwrap()["artifactType"],
            "application/example.config"
        );
    }
}
