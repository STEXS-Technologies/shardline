mod blob_upload;
mod manifest;
pub(crate) mod path;
mod tags;
mod token;

pub(crate) use path::{OciPath, parse_oci_path};
pub(crate) use token::oci_registry_token;

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use shardline_protocol::TokenScope;

use crate::ServerError;

use super::{AppState, direct_object_response, scope_from_auth};
use blob_upload::{
    oci_delete_blob_upload, oci_get_blob_upload, oci_patch_blob_upload, oci_post_blob_upload,
    oci_put_blob_upload,
};
use manifest::{oci_delete_manifest, oci_get_manifest, oci_put_manifest};
use tags::oci_tags_list;
use token::oci_authorize;

pub(crate) async fn oci_v2_root(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
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
) -> Result<Response, ServerError> {
    let parsed = parse_oci_path(&path)?;
    oci_dispatch_parsed(&state, method, headers, uri, body, parsed).await
}

pub(crate) async fn oci_api_dispatch(
    method: Method,
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    body: Body,
) -> Result<Response, ServerError> {
    let parsed = parse_oci_path(&path)?;
    if !oci_route_served_by_api(&method, &parsed) {
        return Err(ServerError::NotFound);
    }
    oci_dispatch_parsed(&state, method, headers, uri, body, parsed).await
}

pub(crate) async fn oci_transfer_dispatch(
    method: Method,
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    body: Body,
) -> Result<Response, ServerError> {
    let parsed = parse_oci_path(&path)?;
    if !oci_route_served_by_transfer(&method, &parsed) {
        return Err(ServerError::NotFound);
    }
    oci_dispatch_parsed(&state, method, headers, uri, body, parsed).await
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
            let object_key = crate::oci_adapter::oci_blob_key(
                &repository,
                &digest_hex,
                auth.as_ref().map(scope_from_auth),
            )?;
            direct_object_response(
                state,
                &headers,
                &object_key,
                "application/octet-stream",
                Some(format!("sha256:{digest_hex}")),
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
            let object_key = crate::oci_adapter::oci_blob_key(
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
                .map_err(|_error| ServerError::Overflow)?)
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
        _ => Err(ServerError::NotFound),
    }
}

#[allow(clippy::missing_const_for_fn)]
fn oci_route_served_by_api(method: &Method, path: &OciPath) -> bool {
    matches!(
        (method, path),
        (
            &Method::GET,
            OciPath::Manifest { .. } | OciPath::TagsList { .. }
        ) | (&Method::HEAD, OciPath::Manifest { .. })
            | (&Method::PUT, OciPath::Manifest { .. })
            | (&Method::DELETE, OciPath::Manifest { .. })
    )
}

#[allow(clippy::missing_const_for_fn)]
fn oci_route_served_by_transfer(method: &Method, path: &OciPath) -> bool {
    matches!(
        (method, path),
        (
            &Method::GET,
            OciPath::Blob { .. } | OciPath::BlobUploadSession { .. }
        ) | (&Method::HEAD, OciPath::Blob { .. })
            | (&Method::POST, OciPath::BlobUploads { .. })
            | (&Method::PATCH, OciPath::BlobUploadSession { .. })
            | (&Method::PUT, OciPath::BlobUploadSession { .. })
            | (&Method::DELETE, OciPath::BlobUploadSession { .. })
    )
}
