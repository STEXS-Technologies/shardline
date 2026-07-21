use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use shardline_metrics::metrics;
use shardline_protocol::TokenScope;
use shardline_storage::{DeleteOutcome, ObjectKey};

use crate::{
    ServerError,
    error::OciError,
    oci_adapter::{oci_blob_key, oci_manifest_prefix},
};

use super::super::{AppState, direct_object_response, scope_from_auth};
use super::blob_upload::{
    oci_delete_blob_upload, oci_get_blob_upload, oci_patch_blob_upload, oci_post_blob_upload,
    oci_put_blob_upload,
};
use super::helpers::{oci_route_served_by_api, oci_route_served_by_transfer};
use super::manifest::{oci_delete_manifest, oci_get_manifest, oci_put_manifest};
use super::path::{OciPath, parse_oci_path};
use super::tags::oci_tags_list;
use super::token::oci_authorize;

pub(crate) async fn oci_v2_root(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, OciError> {
    let _auth = oci_authorize(&state, &headers, None, TokenScope::Read)?;
    Ok((
        StatusCode::OK,
        [("Docker-Distribution-API-Version", "registry/2.0")],
    ))
}

pub(crate) async fn oci_dispatch(
    method: Method,
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    body: Body,
) -> Result<Response, OciError> {
    let parsed = parse_oci_path(&path)?;
    Ok(oci_dispatch_parsed(&state, method, headers, uri, body, parsed).await?)
}

pub(crate) async fn oci_api_dispatch(
    method: Method,
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    body: Body,
) -> Result<Response, OciError> {
    let parsed = parse_oci_path(&path)?;
    if !oci_route_served_by_api(&method, &parsed) {
        return Err(ServerError::NotFound.into());
    }
    Ok(oci_dispatch_parsed(&state, method, headers, uri, body, parsed).await?)
}

pub(crate) async fn oci_transfer_dispatch(
    method: Method,
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    body: Body,
) -> Result<Response, OciError> {
    let parsed = parse_oci_path(&path)?;
    if !oci_route_served_by_transfer(&method, &parsed) {
        return Err(ServerError::NotFound.into());
    }
    Ok(oci_dispatch_parsed(&state, method, headers, uri, body, parsed).await?)
}

async fn oci_dispatch_parsed(
    state: &Arc<AppState>,
    method: Method,
    headers: HeaderMap,
    uri: Uri,
    body: Body,
    parsed: OciPath,
) -> Result<Response, ServerError> {
    match (method, parsed) {
        (
            Method::GET,
            OciPath::Blob {
                repository,
                digest_hex,
            },
        ) => {
            let auth = oci_authorize(state, &headers, Some(&repository), TokenScope::Read)?;
            let object_key = oci_blob_key(
                &repository,
                &digest_hex,
                auth.as_ref().map(scope_from_auth),
            )?;
            metrics().protocol.record_oci_download();
            direct_object_response(
                state,
                &headers,
                &object_key,
                "application/octet-stream",
                Some(format!("sha256:{digest_hex}")),
                "oci",
            )
            .await
        }
        (
            Method::HEAD,
            OciPath::Blob {
                repository,
                digest_hex,
            },
        ) => {
            let auth = oci_authorize(state, &headers, Some(&repository), TokenScope::Read)?;
            let object_key = oci_blob_key(
                &repository,
                &digest_hex,
                auth.as_ref().map(scope_from_auth),
            )?;
            let total_length = state.backend.object_length(&object_key).await?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(axum::http::header::CONTENT_LENGTH, total_length.to_string())
                .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
                .header("Docker-Content-Digest", format!("sha256:{digest_hex}"))
                .body(Body::empty())
                .map_err(|e| {
                    tracing::warn!(error = %e, "failed to build head blob response");
                    ServerError::Overflow
                })?)
        }
        (
            Method::GET,
            OciPath::Manifest {
                repository,
                reference,
            },
        ) => oci_get_manifest(state, &headers, &repository, &reference, false).await,
        (
            Method::HEAD,
            OciPath::Manifest {
                repository,
                reference,
            },
        ) => oci_get_manifest(state, &headers, &repository, &reference, true).await,
        (Method::GET, OciPath::TagsList { repository }) => {
            oci_tags_list(state, &headers, &uri, &repository).await
        }
        (Method::POST, OciPath::BlobUploads { repository }) => {
            oci_post_blob_upload(state, &headers, &uri, &repository, body).await
        }
        (
            Method::PATCH,
            OciPath::BlobUploadSession {
                repository,
                session_id,
            },
        ) => oci_patch_blob_upload(state, &headers, &headers, &repository, &session_id, body).await,
        (
            Method::PUT,
            OciPath::BlobUploadSession {
                repository,
                session_id,
            },
        ) => oci_put_blob_upload(state, &headers, &uri, &repository, &session_id, body).await,
        (
            Method::GET,
            OciPath::BlobUploadSession {
                repository,
                session_id,
            },
        ) => oci_get_blob_upload(state, &headers, &repository, &session_id).await,
        (
            Method::DELETE,
            OciPath::BlobUploadSession {
                repository,
                session_id,
            },
        ) => oci_delete_blob_upload(state, &headers, &repository, &session_id).await,
        (
            Method::PUT,
            OciPath::Manifest {
                repository,
                reference,
            },
        ) => oci_put_manifest(state, &headers, &uri, &repository, &reference, body).await,
        (
            Method::DELETE,
            OciPath::Manifest {
                repository,
                reference,
            },
        ) => oci_delete_manifest(state, &headers, &repository, &reference).await,
        (
            Method::DELETE,
            OciPath::Blob {
                repository,
                digest_hex,
            },
        ) => oci_delete_blob(state, &headers, &repository, &digest_hex).await,
        _ => Err(ServerError::NotFound),
    }
}

async fn oci_delete_blob(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    repository: &str,
    digest_hex: &str,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, headers, Some(repository), TokenScope::Write)?;
    let scope = auth.as_ref().map(scope_from_auth);
    let object_key = oci_blob_key(repository, digest_hex, scope)?;

    // Check if any manifest references this blob by walking every page of
    // manifest listings and parsing the JSON document for digest references.
    let manifest_prefix = oci_manifest_prefix(repository, scope)?;
    let target_digest = format!("sha256:{digest_hex}");
    let mut start_after: Option<ObjectKey> = None;
    loop {
        let page: Vec<ObjectKey> = state
            .backend
            .list_object_flat_namespace_page(&manifest_prefix, start_after.as_ref(), 1000)?
            .into_iter()
            .map(|meta| meta.key().clone())
            .collect();
        if page.is_empty() {
            break;
        }
        for manifest_key in &page {
            let body = state.backend.read_object(manifest_key).await?;
            if manifest_references_digest(&body, &target_digest) {
                return Err(ServerError::InvalidManifestReference);
            }
        }
        start_after = page.last().cloned();
    }

    match state.backend.delete_object_if_present(&object_key).await? {
        DeleteOutcome::Deleted => {}
        DeleteOutcome::NotFound => return Err(ServerError::NotFound),
    }
    Response::builder()
        .status(StatusCode::ACCEPTED)
        .body(Body::empty())
        .map_err(|e| {
            tracing::warn!(error = %e, "failed to build delete blob response");
            ServerError::Overflow
        })
}

/// Returns `true` if the JSON document (OCI manifest or index) contains a
/// reference to `target_digest` anywhere in its descriptor tree.
fn manifest_references_digest(body: &[u8], target_digest: &str) -> bool {
    let Ok(doc) = serde_json::from_slice::<serde_json::Value>(body) else {
        return false;
    };
    let mut refs = Vec::new();
    collect_digest_refs(&doc, &mut refs);
    refs.contains(&target_digest)
}

fn collect_digest_refs<'value>(value: &'value serde_json::Value, out: &mut Vec<&'value str>) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(digest)) = map.get("digest") {
                out.push(digest.as_str());
                return;
            }
            for v in map.values() {
                collect_digest_refs(v, out);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                collect_digest_refs(v, out);
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST_A: &str =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const DIGEST_B: &str =
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    #[test]
    fn manifest_references_digest_returns_true_when_digest_present() {
        let doc = serde_json::json!({
            "config": { "digest": DIGEST_A, "size": 123, "mediaType": "application/json" },
            "layers": [
                { "digest": DIGEST_B, "size": 456, "mediaType": "application/octet-stream" }
            ]
        });
        let body = serde_json::to_vec(&doc).unwrap();
        assert!(manifest_references_digest(&body, DIGEST_A));
        assert!(manifest_references_digest(&body, DIGEST_B));
    }

    #[test]
    fn manifest_references_digest_returns_false_when_digest_absent() {
        let doc = serde_json::json!({
            "config": { "digest": DIGEST_A, "size": 123, "mediaType": "application/json" },
            "layers": []
        });
        let body = serde_json::to_vec(&doc).unwrap();
        assert!(!manifest_references_digest(&body, DIGEST_B));
    }

    #[test]
    fn manifest_references_digest_returns_false_for_invalid_json() {
        assert!(!manifest_references_digest(b"not json at all", DIGEST_A));
    }

    #[test]
    fn manifest_references_digest_returns_false_for_empty_body() {
        assert!(!manifest_references_digest(b"", DIGEST_A));
    }

    #[test]
    fn manifest_references_digest_handles_nested_manifest_index() {
        let doc = serde_json::json!({
            "schemaVersion": 2,
            "manifests": [
                { "digest": DIGEST_A, "size": 100, "platform": { "architecture": "amd64" } }
            ]
        });
        let body = serde_json::to_vec(&doc).unwrap();
        assert!(manifest_references_digest(&body, DIGEST_A));
        assert!(!manifest_references_digest(&body, DIGEST_B));
    }

    #[test]
    fn collect_digest_refs_finds_all_digests() {
        let doc = serde_json::json!({
            "config": { "digest": DIGEST_A, "size": 1 },
            "layers": [
                { "digest": DIGEST_B, "size": 2 }
            ]
        });
        let mut refs = Vec::new();
        collect_digest_refs(&doc, &mut refs);
        assert_eq!(refs.len(), 2);
        assert!(refs.contains(&DIGEST_A));
        assert!(refs.contains(&DIGEST_B));
    }

    #[test]
    fn collect_digest_refs_skips_non_digest_objects() {
        let doc = serde_json::json!({
            "mediaType": "application/json",
            "config": { "size": 123 },
            "annotations": { "key": "value" }
        });
        let mut refs = Vec::new();
        collect_digest_refs(&doc, &mut refs);
        assert!(refs.is_empty());
    }

    #[test]
    fn collect_digest_refs_handles_arrays() {
        let doc = serde_json::json!([
            { "digest": DIGEST_A, "size": 1 },
            { "digest": DIGEST_B, "size": 2 }
        ]);
        let mut refs = Vec::new();
        collect_digest_refs(&doc, &mut refs);
        assert_eq!(refs.len(), 2);
    }

    #[test]
    fn collect_digest_refs_does_not_follow_non_string_digest() {
        let doc = serde_json::json!({
            "digest": 42
        });
        let mut refs = Vec::new();
        collect_digest_refs(&doc, &mut refs);
        assert!(refs.is_empty());
    }

    #[test]
    fn collect_digest_refs_handles_primitive_values() {
        // Line 280: Null, Bool, Number, String values should be no-ops.
        let doc = serde_json::json!([
            null,
            true,
            false,
            42,
            3.5,
            "just a string",
            [{"digest": DIGEST_A, "size": 1}]
        ]);
        let mut refs = Vec::new();
        collect_digest_refs(&doc, &mut refs);
        // Only the nested object with a "digest" field should be collected.
        assert_eq!(refs.len(), 1);
        assert!(refs.contains(&DIGEST_A));
    }

    #[test]
    fn collect_digest_refs_skips_object_with_digest_returns_early() {
        // Line 269: when an object has a "digest" field, collect it and return
        // early without recursing into other fields.
        let doc = serde_json::json!({
            "digest": DIGEST_A,
            "annotations": { "digest": DIGEST_B }
        });
        let mut refs = Vec::new();
        collect_digest_refs(&doc, &mut refs);
        // Only DIGEST_A should be found because the function returns early
        // after the first "digest" key in an object.
        assert_eq!(refs.len(), 1);
        assert!(refs.contains(&DIGEST_A));
    }
}
