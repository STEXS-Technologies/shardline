use std::{
    collections::BTreeSet,
    sync::Arc,
};

use axum::{
    body::Body,
    http::{
        HeaderMap, StatusCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE, LINK},
    },
    response::Response,
};
use shardline_protocol::TokenScope;

use crate::{
    ServerError,
    oci_adapter::{
        oci_tag_key, oci_tag_prefix, oci_tag_target_key, oci_tag_target_prefix,
    },
};

use super::super::{
    AppState,
    parse_query_map,
    scope_from_auth,
};
use super::token::oci_authorize;

struct OciTagListPage {
    tags: Vec<String>,
    has_more: bool,
}

pub(crate) async fn oci_tags_list(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    uri: &axum::http::Uri,
    repository: &str,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, headers, Some(repository), TokenScope::Read)?;
    let query = parse_query_map(uri)?;
    let page_size = parse_oci_tag_list_page_size(query.get("n").map(String::as_str))?;
    let last = query.get("last").map(String::as_str);
    if let Some(last) = last {
        crate::protocol_support::validate_oci_tag(last)?;
    }
    let tag_page = list_oci_tags(
        state,
        repository,
        auth.as_ref().map(scope_from_auth),
        page_size,
        last,
    )?;
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

fn list_oci_tags(
    state: &Arc<AppState>,
    repository: &str,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
    page_size: usize,
    last: Option<&str>,
) -> Result<OciTagListPage, ServerError> {
    let prefix = oci_tag_prefix(repository, repository_scope)?;
    let start_after = last
        .map(|tag| oci_tag_key(repository, tag, repository_scope))
        .transpose()?;
    let objects = state.backend.list_object_flat_namespace_page(
        &prefix,
        start_after.as_ref(),
        page_size.saturating_add(1),
    )?;
    let mut tags = BTreeSet::new();
    for object in objects {
        let Some(tag) = object.key().as_str().rsplit('/').next() else {
            continue;
        };
        crate::protocol_support::validate_oci_tag(tag)?;
        let _inserted = tags.insert(tag.to_owned());
    }
    let has_more = tags.len() > page_size;
    if has_more {
        let _removed = tags.pop_last();
    }
    Ok(OciTagListPage {
        tags: tags.into_iter().collect(),
        has_more,
    })
}

pub(crate) async fn update_oci_tags(
    state: &Arc<AppState>,
    repository: &str,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
    tags: &[String],
    digest_hex: &str,
) -> Result<(), ServerError> {
    let digest_bytes = digest_hex.as_bytes().to_vec();
    for tag in tags {
        crate::protocol_support::validate_oci_tag(tag)?;
        let tag_key = oci_tag_key(repository, tag, repository_scope)?;
        let previous_digest = match state.backend.read_object(&tag_key).await {
            Ok(bytes) => {
                Some(String::from_utf8(bytes).map_err(|_error| ServerError::InvalidDigest)?)
            }
            Err(ServerError::NotFound) => None,
            Err(error) => return Err(error),
        };
        let target_key = oci_tag_target_key(repository, digest_hex, tag, repository_scope)?;
        state
            .backend
            .put_object_bytes_overwrite(&target_key, Vec::new())?;
        state
            .backend
            .put_object_bytes_overwrite(&tag_key, digest_bytes.clone())?;
        if let Some(previous_digest) = previous_digest
            && previous_digest != digest_hex
        {
            let previous_target =
                oci_tag_target_key(repository, &previous_digest, tag, repository_scope)?;
            let _deleted = state
                .backend
                .delete_object_if_present(&previous_target)
                .await?;
        }
    }
    Ok(())
}

pub(crate) async fn delete_oci_tags_pointing_to_digest(
    state: &Arc<AppState>,
    repository: &str,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
    digest_hex: &str,
) -> Result<(), ServerError> {
    let prefix = oci_tag_target_prefix(repository, digest_hex, repository_scope)?;
    let mut target_keys = Vec::new();
    state.backend.visit_object_prefix(&prefix, |object| {
        target_keys.push(object.key().clone());
        Ok(())
    })?;

    for target_key in target_keys {
        let Some(tag) = target_key.as_str().rsplit('/').next() else {
            continue;
        };
        let tag_key = oci_tag_key(repository, tag, repository_scope)?;
        match state.backend.read_object(&tag_key).await {
            Ok(bytes) => {
                let stored_digest =
                    String::from_utf8(bytes).map_err(|_error| ServerError::InvalidDigest)?;
                if stored_digest == digest_hex {
                    let _deleted = state.backend.delete_object_if_present(&tag_key).await?;
                }
            }
            Err(ServerError::NotFound) => {}
            Err(error) => return Err(error),
        }
        let _deleted = state.backend.delete_object_if_present(&target_key).await?;
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
    if page_size == 0 || page_size > super::super::MAX_OCI_TAG_LIST_PAGE_SIZE {
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

pub(crate) fn oci_created_response(location: &str, digest_hex: Option<&str>) -> Result<Response, ServerError> {
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
