use std::sync::Arc;

use axum::{
    body::Body,
    http::{
        StatusCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE, LINK},
    },
    response::Response,
};
use shardline_index::OciTagEntry;
use shardline_server_core::AuthorizedRepository;

use crate::{
    ServerError,
    maintenance_barrier::ResourceWriteGuard,
    protocol_support::{parse_sha256_digest, scope_namespace, validate_oci_tag},
};

use super::super::{AppState, parse_query_map};
use super::helpers::{OciRepository, oci_manifest_index_key, oci_tag_key, oci_tag_prefix};

pub(crate) async fn oci_tags_list(
    state: &Arc<AppState>,
    uri: &axum::http::Uri,
    repo: &OciRepository,
) -> Result<Response, ServerError> {
    let repository = repo.repository();
    let query = parse_query_map(uri)?;
    let page_size = parse_oci_tag_list_page_size(query.get("n").map(String::as_str))?;
    let last = query.get("last").map(String::as_str);
    if let Some(last) = last {
        validate_oci_tag(last)?;
    }

    // Per the OCI Distribution spec, when n is zero the endpoint MUST return
    // an empty list and MUST NOT include a Link header.
    let mut tags = if page_size == 0 {
        Vec::new()
    } else {
        backfill_legacy_oci_tags(state, repository, repo.capability()).await?;
        state
            .backend
            .list_oci_tags(
                &scope_namespace(repo.capability().namespace()),
                repository,
                last,
                page_size.saturating_add(1),
            )
            .await?
            .into_iter()
            .map(|entry| entry.tag)
            .collect::<Vec<_>>()
    };
    let has_more = tags.len() > page_size;
    tags.truncate(page_size);
    let body = serde_json::to_vec(&serde_json::json!({
        "name": repository,
        "tags": tags,
    }))?;
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .header(CONTENT_LENGTH, body.len().to_string());
    if has_more && let Some(last_tag) = tags.last() {
        builder = builder.header(
            LINK,
            oci_tags_list_next_link(repository, page_size, last_tag)?,
        );
    }
    builder
        .body(Body::from(body))
        .map_err(|_error| ServerError::Overflow)
}

pub(crate) async fn update_oci_tags(
    state: &Arc<AppState>,
    resource_guard: &mut ResourceWriteGuard,
    repository: &str,
    auth: &AuthorizedRepository,
    tags: &[String],
    digest_hex: &str,
    references: &[shardline_index::OciObjectKey],
) -> Result<(), ServerError> {
    let scope_namespace = scope_namespace(auth.namespace());
    let mut entries = Vec::with_capacity(tags.len());
    for tag in tags {
        validate_oci_tag(tag)?;
        entries.push(OciTagEntry {
            scope_namespace: scope_namespace.clone(),
            repository: repository.to_owned(),
            tag: tag.clone(),
            digest_hex: digest_hex.to_owned(),
        });
    }
    state
        .backend
        .publish_oci_object_with_references_locked(
            resource_guard,
            &oci_manifest_index_key(repository, digest_hex, auth),
            &entries,
            references,
        )
        .await
}

pub(crate) async fn resolve_oci_tag_digest(
    state: &Arc<AppState>,
    repository: &str,
    auth: &AuthorizedRepository,
    tag: &str,
) -> Result<String, ServerError> {
    let scope_namespace = scope_namespace(auth.namespace());
    if let Some(entry) = state
        .backend
        .oci_tag(&scope_namespace, repository, tag)
        .await?
    {
        return Ok(entry.digest_hex);
    }

    let tag_key = oci_tag_key(repository, tag, auth)?;
    let bytes = state.backend.read_object(&tag_key).await?;
    let digest_hex = String::from_utf8(bytes).map_err(|_error| ServerError::InvalidDigest)?;
    parse_sha256_digest(&format!("sha256:{digest_hex}"))?;
    if state
        .backend
        .oci_object_is_deleted(&oci_manifest_index_key(repository, &digest_hex, auth))
        .await?
    {
        return Err(ServerError::NotFound);
    }
    let legacy = OciTagEntry {
        scope_namespace: scope_namespace.clone(),
        repository: repository.to_owned(),
        tag: tag.to_owned(),
        digest_hex,
    };
    let _inserted = state.backend.insert_oci_tag_if_absent(&legacy).await?;
    state
        .backend
        .oci_tag(&scope_namespace, repository, tag)
        .await?
        .map(|entry| entry.digest_hex)
        .ok_or(ServerError::NotFound)
}

async fn backfill_legacy_oci_tags(
    state: &Arc<AppState>,
    repository: &str,
    auth: &AuthorizedRepository,
) -> Result<(), ServerError> {
    let prefix = oci_tag_prefix(repository, auth)?;
    let mut tags = Vec::new();
    state.backend.visit_object_prefix(&prefix, |object| {
        if let Some(tag) = object.key().as_str().rsplit('/').next() {
            tags.push(tag.to_owned());
        }
        Ok(())
    })?;
    for tag in tags {
        validate_oci_tag(&tag)?;
        let result = resolve_oci_tag_digest(state, repository, auth, &tag).await;
        if result
            .as_ref()
            .is_err_and(|error| matches!(error, ServerError::NotFound))
        {
            continue;
        }
        let _digest = result?;
    }
    Ok(())
}

fn parse_oci_tag_list_page_size(value: Option<&str>) -> Result<usize, ServerError> {
    let Some(value) = value else {
        return Ok(super::super::MAX_OCI_TAG_LIST_PAGE_SIZE);
    };
    let page_size = value
        .parse::<usize>()
        .map_err(|_error| ServerError::InvalidManifestReference)?;
    if page_size > super::super::MAX_OCI_TAG_LIST_PAGE_SIZE {
        return Err(ServerError::InvalidManifestReference);
    }
    Ok(page_size)
}

fn oci_tags_list_next_link(
    repository: &str,
    page_size: usize,
    last_tag: &str,
) -> Result<axum::http::HeaderValue, ServerError> {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("n", &page_size.to_string())
        .append_pair("last", last_tag)
        .finish();
    axum::http::HeaderValue::from_str(&format!(
        "</v2/{repository}/tags/list?{query}>; rel=\"next\""
    ))
    .map_err(|_error| ServerError::InvalidManifestReference)
}

pub(crate) fn oci_created_response(
    location: &str,
    digest_hex: Option<&str>,
) -> Result<Response, ServerError> {
    let mut builder = Response::builder()
        .status(StatusCode::CREATED)
        .header(axum::http::header::LOCATION, location);
    if let Some(digest_hex) = digest_hex {
        builder = builder.header("Docker-Content-Digest", format!("sha256:{digest_hex}"));
    }
    builder
        .body(Body::empty())
        .map_err(|_error| ServerError::Overflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_oci_tag_list_page_size ────────────────────────────────────

    #[test]
    fn page_size_accepts_default_and_protocol_bounds() {
        let max = super::super::super::MAX_OCI_TAG_LIST_PAGE_SIZE;
        let max_value = max.to_string();
        for (value, expected) in [
            (None, max),
            (Some("0"), 0),
            (Some("10"), 10),
            (Some(max_value.as_str()), max),
        ] {
            assert_eq!(
                parse_oci_tag_list_page_size(value).unwrap(),
                expected,
                "{value:?}"
            );
        }
    }

    #[test]
    fn page_size_rejects_invalid_values() {
        let oversized = (super::super::super::MAX_OCI_TAG_LIST_PAGE_SIZE + 1).to_string();
        for value in [oversized.as_str(), "abc", "", "-1", " 10", "10 "] {
            assert!(
                matches!(
                    parse_oci_tag_list_page_size(Some(value)),
                    Err(ServerError::InvalidManifestReference)
                ),
                "{value:?}"
            );
        }
    }

    // ── oci_tags_list_next_link ─────────────────────────────────────────

    #[test]
    fn next_link_contains_repository_and_pagination() {
        let header = oci_tags_list_next_link("team/assets", 10, "v1.0").unwrap();
        let value = header.to_str().unwrap();
        assert!(value.contains("team/assets"));
        assert!(value.contains("n=10"));
        assert!(value.contains("last=v1.0"));
        assert!(value.contains("rel=\"next\""));
    }

    // ── oci_created_response ────────────────────────────────────────────

    #[test]
    fn created_response_preserves_location_and_optional_digest() {
        let digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        for (location, digest) in [
            ("/v2/team/assets/blobs/uploads/abc", Some(digest)),
            ("/v2/team/assets/blobs/uploads/abc", None),
            ("/v2/test/blob/uploads/123", None),
        ] {
            let response = oci_created_response(location, digest).unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
            assert_eq!(response.headers()[axum::http::header::LOCATION], location);
            let expected = digest.map(|digest| format!("sha256:{digest}"));
            let actual = response
                .headers()
                .get("Docker-Content-Digest")
                .map(|value| value.to_str().unwrap());
            assert_eq!(actual, expected.as_deref());
        }
    }
}
