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

struct OciTagListPage {
    tags: Vec<String>,
    has_more: bool,
}

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
    if page_size == 0 {
        let body = serde_json::to_vec(&serde_json::json!({
            "name": repository,
            "tags": [],
        }))?;
        return Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, body.len().to_string())
            .body(Body::from(body))
            .map_err(|_error| ServerError::Overflow);
    }

    let tag_page = list_oci_tags(state, repository, repo.capability(), page_size, last).await?;
    let tags = tag_page.tags;
    let has_more = tag_page.has_more;
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

async fn list_oci_tags(
    state: &Arc<AppState>,
    repository: &str,
    auth: &AuthorizedRepository,
    page_size: usize,
    last: Option<&str>,
) -> Result<OciTagListPage, ServerError> {
    backfill_legacy_oci_tags(state, repository, auth).await?;
    let entries = state
        .backend
        .list_oci_tags(
            &scope_namespace(auth.namespace()),
            repository,
            last,
            page_size.saturating_add(1),
        )
        .await?;
    let mut tags: Vec<String> = entries.into_iter().map(|entry| entry.tag).collect();
    let has_more = tags.len() > page_size;
    if has_more {
        let _removed = tags.pop();
    }
    Ok(OciTagListPage { tags, has_more })
}

pub(crate) async fn update_oci_tags(
    state: &Arc<AppState>,
    resource_guard: &mut ResourceWriteGuard,
    repository: &str,
    auth: &AuthorizedRepository,
    tags: &[String],
    digest_hex: &str,
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
        .publish_oci_object_locked(
            resource_guard,
            &oci_manifest_index_key(repository, digest_hex, auth),
            &entries,
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
    if page_size == 0 {
        return Ok(0);
    }
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
    fn default_page_size_when_none() {
        assert_eq!(
            parse_oci_tag_list_page_size(None).unwrap(),
            super::super::super::MAX_OCI_TAG_LIST_PAGE_SIZE
        );
    }

    #[test]
    fn zero_page_size_returns_zero() {
        assert_eq!(parse_oci_tag_list_page_size(Some("0")).unwrap(), 0);
    }

    #[test]
    fn valid_page_size_returned() {
        assert_eq!(parse_oci_tag_list_page_size(Some("10")).unwrap(), 10);
    }

    #[test]
    fn max_page_size_accepted() {
        let max = super::super::super::MAX_OCI_TAG_LIST_PAGE_SIZE;
        assert_eq!(
            parse_oci_tag_list_page_size(Some(&max.to_string())).unwrap(),
            max
        );
    }

    #[test]
    fn oversized_page_size_rejected() {
        let oversized = super::super::super::MAX_OCI_TAG_LIST_PAGE_SIZE + 1;
        assert!(matches!(
            parse_oci_tag_list_page_size(Some(&oversized.to_string())),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn non_numeric_page_size_rejected() {
        assert!(matches!(
            parse_oci_tag_list_page_size(Some("abc")),
            Err(ServerError::InvalidManifestReference)
        ));
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
    fn created_response_with_digest() {
        let digest_hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let response =
            oci_created_response("/v2/team/assets/blobs/uploads/abc", Some(digest_hex)).unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert!(response.headers().get("Docker-Content-Digest").is_some());
        assert_eq!(
            response
                .headers()
                .get("Docker-Content-Digest")
                .unwrap()
                .to_str()
                .unwrap(),
            format!("sha256:{digest_hex}")
        );
    }

    #[test]
    fn created_response_without_digest() {
        let response = oci_created_response("/v2/team/assets/blobs/uploads/abc", None).unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert!(response.headers().get("Docker-Content-Digest").is_none());
    }

    // ── validate_oci_tag in tags list (line 37) ──────────────────────────

    #[test]
    fn validate_oci_tag_accepts_valid_rejects_invalid() {
        assert!(validate_oci_tag("valid-tag").is_ok());
        assert!(validate_oci_tag("v1.0.0").is_ok());
        assert!(validate_oci_tag("").is_err());
        assert!(validate_oci_tag("tag with spaces").is_err());
        assert!(validate_oci_tag("tag\nnewline").is_err());
    }

    // ── oci_tags_list_next_link ─────────────────────────────────────────

    #[test]
    fn next_link_encodes_last_parameter() {
        let header = oci_tags_list_next_link("team/assets", 10, "v1.0").unwrap();
        let value = header.to_str().unwrap();
        assert!(value.contains("last=v1.0"));
    }

    // ── oci_created_response edge cases ──────────────────────────────────

    #[test]
    fn created_response_includes_location_header() {
        let response = oci_created_response("/v2/test/blob/uploads/123", None).unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::LOCATION)
                .unwrap(),
            "/v2/test/blob/uploads/123"
        );
    }
}
