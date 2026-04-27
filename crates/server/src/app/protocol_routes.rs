use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
    sync::Arc,
};

use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::{
        HeaderMap, HeaderValue, Method, StatusCode, Uri,
        header::{
            ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, LINK, LOCATION,
            RANGE,
        },
    },
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use shardline_protocol::{ByteRange, TokenClaims, TokenScope, TokenSigner, parse_http_byte_range};
use shardline_storage::DeleteOutcome;

use crate::{
    ServerError,
    bazel_http_adapter::{BazelCacheKind, bazel_cache_object_key},
    lfs_adapter::{
        LFS_CONTENT_TYPE, LfsBatchRequest, LfsBatchResponse, LfsObjectError, LfsObjectResponse,
        lfs_object_key,
    },
    oci_adapter::{
        OciReference, abort_s3_multipart_upload_session, append_s3_multipart_upload_bytes,
        append_upload_bytes, create_upload_session, delete_upload_session,
        finalize_s3_multipart_upload_session, lock_upload_sessions, oci_blob_key,
        oci_blob_location, oci_manifest_key, oci_manifest_location, oci_manifest_media_type_key,
        oci_tag_key, oci_tag_prefix, oci_tag_target_key, oci_tag_target_prefix, parse_reference,
        read_upload_session, touch_upload_session, upload_body_integrity,
        upload_body_path_for_session, upload_length, upload_session_length,
        upload_session_location, validate_repository,
    },
    protocol_support::{parse_sha256_digest, scope_namespace, validate_oci_repository_scope},
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

use super::{
    AppState, MAX_LFS_BATCH_OBJECTS, MAX_OCI_MANIFEST_TAGS, MAX_OCI_TAG_LIST_PAGE_SIZE,
    MAX_PROTOCOL_QUERY_BYTES, authorize,
    reconstruction_helpers::{byte_range_stream_response, full_byte_stream_response},
    scope_from_auth,
};

const OCI_IMAGE_MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";
const OCI_IMAGE_INDEX_MEDIA_TYPE: &str = "application/vnd.oci.image.index.v1+json";
const DOCKER_SCHEMA2_MANIFEST_MEDIA_TYPE: &str =
    "application/vnd.docker.distribution.manifest.v2+json";
const DOCKER_SCHEMA2_MANIFEST_LIST_MEDIA_TYPE: &str =
    "application/vnd.docker.distribution.manifest.list.v2+json";
const OCI_REGISTRY_SERVICE: &str = "shardline";
const MAX_OCI_TOKEN_BASIC_AUTH_BYTES: usize = 8192;
const MAX_OCI_TOKEN_QUERY_SERVICE_BYTES: usize = 128;
const MAX_OCI_TOKEN_QUERY_SCOPE_BYTES: usize = 1024;
const MAX_OCI_TOKEN_QUERY_ACCOUNT_BYTES: usize = 512;
const MAX_OCI_TOKEN_QUERY_SCOPES: usize = 16;

pub(super) async fn bazel_get_ac(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    direct_object_response(
        &state,
        &headers,
        &object_key,
        "application/octet-stream",
        None,
    )
    .await
}

pub(super) async fn bazel_put_ac(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Write)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let mut body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let bytes = read_body_to_bytes(&mut body).await?;
    let _stored = state
        .backend
        .put_object_bytes_if_absent(&object_key, bytes)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn bazel_get_cas(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    direct_object_response(
        &state,
        &headers,
        &object_key,
        "application/octet-stream",
        None,
    )
    .await
}

pub(super) async fn bazel_put_cas(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Write)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let _stored = state
        .backend
        .put_sha256_addressed_object_stream_if_absent(&object_key, &hash, body)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn lfs_batch(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<LfsBatchRequest>,
) -> Result<Response, ServerError> {
    let requested_scope = match request.operation.as_str() {
        "download" => TokenScope::Read,
        "upload" => TokenScope::Write,
        _ => return Err(ServerError::InvalidManifestReference),
    };
    let auth = authorize(&state, &headers, requested_scope)?;
    if request.objects.len() > MAX_LFS_BATCH_OBJECTS {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
            Json(json!({ "message": "too many objects in batch request" })),
        )
            .into_response());
    }
    if let Some(hash_algo) = request.hash_algo.as_deref()
        && hash_algo != "sha256"
    {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
            Json(json!({ "message": "unsupported hash algorithm" })),
        )
            .into_response());
    }

    let scope = auth.as_ref().map(scope_from_auth);
    let transfer = if request.transfers.is_empty()
        || request.transfers.iter().any(|transfer| transfer == "basic")
    {
        "basic".to_owned()
    } else {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
            Json(json!({ "message": "unsupported transfer adapter" })),
        )
            .into_response());
    };
    let mut objects = Vec::with_capacity(request.objects.len());
    for object in request.objects {
        let object_key = lfs_object_key(&object.oid, scope)?;
        let object_length = state.backend.object_length(&object_key).await;
        match request.operation.as_str() {
            "download" => match object_length {
                Ok(length) => {
                    let actions = json!({
                        "download": {
                            "href": format!(
                                "{}/v1/lfs/objects/{}",
                                state.config.public_base_url().trim_end_matches('/'),
                                object.oid
                            )
                        }
                    });
                    objects.push(LfsObjectResponse {
                        oid: object.oid,
                        size: length,
                        authenticated: Some(auth.is_some()),
                        actions: Some(actions),
                        error: None,
                    });
                }
                Err(ServerError::NotFound) => objects.push(LfsObjectResponse {
                    oid: object.oid,
                    size: object.size,
                    authenticated: None,
                    actions: None,
                    error: Some(LfsObjectError {
                        code: 404,
                        message: "Object does not exist".to_owned(),
                    }),
                }),
                Err(error) => return Err(error),
            },
            "upload" => {
                let (size, actions) = match object_length {
                    Ok(length) => (length, None),
                    Err(ServerError::NotFound) => (
                        object.size,
                        Some(json!({
                            "upload": {
                                "href": format!(
                                    "{}/v1/lfs/objects/{}",
                                    state.config.public_base_url().trim_end_matches('/'),
                                    object.oid
                                )
                            }
                        })),
                    ),
                    Err(error) => return Err(error),
                };
                objects.push(LfsObjectResponse {
                    oid: object.oid,
                    size,
                    authenticated: Some(auth.is_some()),
                    actions,
                    error: None,
                });
            }
            _ => return Err(ServerError::InvalidManifestReference),
        }
    }

    Ok((
        StatusCode::OK,
        [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
        Json(LfsBatchResponse {
            transfer,
            objects,
            hash_algo: "sha256",
        }),
    )
        .into_response())
}

pub(super) async fn lfs_get_object(
    State(state): State<Arc<AppState>>,
    Path(oid): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = lfs_object_key(&oid, auth.as_ref().map(scope_from_auth))?;
    direct_object_response(
        &state,
        &headers,
        &object_key,
        "application/octet-stream",
        Some(format!("sha256:{oid}")),
    )
    .await
}

pub(super) async fn lfs_head_object(
    State(state): State<Arc<AppState>>,
    Path(oid): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = lfs_object_key(&oid, auth.as_ref().map(scope_from_auth))?;
    let total_length = state.backend.object_length(&object_key).await?;
    Ok((StatusCode::OK, [(CONTENT_LENGTH, total_length.to_string())]))
}

pub(super) async fn lfs_put_object(
    State(state): State<Arc<AppState>>,
    Path(oid): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Write)?;
    let object_key = lfs_object_key(&oid, auth.as_ref().map(scope_from_auth))?;
    let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let _stored = state
        .backend
        .put_sha256_addressed_object_stream_if_absent(&object_key, &oid, body)
        .await?;
    Ok(StatusCode::OK)
}

pub(super) async fn oci_registry_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Json<crate::model::OciRegistryTokenResponse>, ServerError> {
    state
        .protocol_metrics
        .increment_oci_registry_token_requests();
    let _permit = state
        .oci_registry_token_limiter
        .clone()
        .try_acquire_owned()
        .map_err(|_error| {
            state
                .protocol_metrics
                .increment_oci_registry_token_rate_limited();
            ServerError::TooManyRegistryTokenRequests
        })?;
    let _active_request = state.protocol_metrics.begin_oci_registry_token_request();
    let signer = TokenSigner::new(
        state
            .config
            .token_signing_key()
            .ok_or(ServerError::MissingAuthorization)?,
    )?;
    let query = parse_oci_registry_token_query(&uri)?;
    if let Some(service) = query.service.as_deref()
        && service != OCI_REGISTRY_SERVICE
    {
        return Err(ServerError::InvalidManifestReference);
    }

    let bootstrap_claims =
        verify_oci_registry_bootstrap_credentials(&headers, &signer).map_err(|error| {
            if matches!(
                error,
                ServerError::MissingAuthorization
                    | ServerError::InvalidAuthorizationHeader
                    | ServerError::InvalidToken(_)
            ) {
                ServerError::UnauthorizedChallenge(oci_bearer_challenge(
                    state.config.public_base_url(),
                    None,
                    TokenScope::Read,
                ))
            } else {
                error
            }
        })?;
    let (requested_scope, requested_repository) = parse_oci_registry_token_scopes(&query.scopes)?;
    if let Some(repository) = requested_repository.as_deref() {
        validate_oci_repository_scope(repository, Some(bootstrap_claims.repository()))?;
    }
    if !scope_allows_oci_exchange(bootstrap_claims.scope(), requested_scope) {
        return Err(ServerError::InsufficientScope);
    }

    let now = crate::clock::unix_now_seconds_checked()?;
    let expires_at_unix_seconds = bootstrap_claims
        .expires_at_unix_seconds()
        .min(now.saturating_add(state.config.oci_registry_token_ttl_seconds().get()));
    let issued_claims = TokenClaims::new(
        bootstrap_claims.issuer(),
        bootstrap_claims.subject(),
        requested_scope.unwrap_or_else(|| bootstrap_claims.scope()),
        bootstrap_claims.repository().clone(),
        expires_at_unix_seconds,
    )
    .map_err(|_error| ServerError::InvalidProviderTokenRequest)?;
    let token = signer.sign(&issued_claims)?;
    Ok(Json(crate::model::OciRegistryTokenResponse {
        access_token: token.clone(),
        token,
        expires_in: issued_claims
            .expires_at_unix_seconds()
            .saturating_sub(now)
            .min(i32::MAX as u64),
    }))
}

pub(super) async fn oci_v2_root(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let _auth = oci_authorize(&state, &headers, None, TokenScope::Read)?;
    Ok((
        StatusCode::OK,
        [("Docker-Distribution-API-Version", "registry/2.0")],
    ))
}

pub(super) async fn oci_dispatch(
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

pub(super) async fn oci_api_dispatch(
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

pub(super) async fn oci_transfer_dispatch(
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
            let object_key =
                oci_blob_key(&repository, &digest_hex, auth.as_ref().map(scope_from_auth))?;
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
            let object_key =
                oci_blob_key(&repository, &digest_hex, auth.as_ref().map(scope_from_auth))?;
            let total_length = state.backend.object_length(&object_key).await?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_LENGTH, total_length.to_string())
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

async fn oci_get_manifest(
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

async fn oci_tags_list(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    uri: &Uri,
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
    let body = serde_json::to_vec(&json!({
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

async fn oci_post_blob_upload(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    uri: &Uri,
    repository: &str,
    body: Body,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, headers, Some(repository), TokenScope::Write)?;
    let scope = auth.as_ref().map(scope_from_auth);
    validate_repository(repository)?;
    let query = parse_query_map(uri)?;
    if let Some(mount_digest) = query.get("mount") {
        let digest_hex = parse_sha256_digest(mount_digest)?;
        let from = query.get("from").map(String::as_str).unwrap_or(repository);
        let source_key = oci_blob_key(from, &digest_hex, scope)?;
        let target_key = oci_blob_key(repository, &digest_hex, scope)?;
        match state
            .backend
            .copy_object_if_absent(&source_key, &target_key)
        {
            Ok(_stored) => {
                return oci_created_response(
                    &oci_blob_location(repository, &digest_hex),
                    Some(&digest_hex),
                );
            }
            Err(ServerError::NotFound) => {}
            Err(error) => return Err(error),
        }
    }

    if let Some(digest) = query.get("digest") {
        let digest_hex = parse_sha256_digest(digest)?;
        let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
        if body.expected_total_bytes() != Some(0) {
            let object_key = oci_blob_key(repository, &digest_hex, scope)?;
            let _stored = state
                .backend
                .put_sha256_addressed_object_stream_if_absent(&object_key, &digest_hex, body)
                .await?;
            return oci_created_response(
                &oci_blob_location(repository, &digest_hex),
                Some(&digest_hex),
            );
        }
    }

    let session_id = create_upload_session(
        state.config.root_dir(),
        repository,
        scope,
        state.config.oci_upload_session_ttl_seconds(),
        state.config.oci_upload_max_active_sessions(),
        state.backend.uses_s3_object_store(),
    )
    .await?;
    Response::builder()
        .status(StatusCode::ACCEPTED)
        .header(LOCATION, upload_session_location(repository, &session_id))
        .header(RANGE, "0-0")
        .body(Body::empty())
        .map_err(|_error| ServerError::Overflow)
}

async fn oci_patch_blob_upload(
    state: &Arc<AppState>,
    auth_headers: &HeaderMap,
    headers: &HeaderMap,
    repository: &str,
    session_id: &str,
    body: Body,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, auth_headers, Some(repository), TokenScope::Write)?;
    let scope = auth.as_ref().map(scope_from_auth);
    validate_oci_repository_scope(repository, scope)?;
    let mut body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let bytes = read_body_to_bytes(&mut body).await?;
    let _lock = lock_upload_sessions(state.config.root_dir()).await?;
    let session = read_upload_session(
        state.config.root_dir(),
        session_id,
        state.config.oci_upload_session_ttl_seconds(),
    )
    .await?;
    if session.repository != repository || session.scope_namespace != scope_namespace(scope) {
        return Err(ServerError::NotFound);
    }
    let current_length = if let Some(length) = upload_session_length(&session) {
        length
    } else {
        upload_length(state.config.root_dir(), session_id).await?
    };
    if let Some(content_range) = headers.get(CONTENT_RANGE) {
        let content_range = content_range
            .to_str()
            .map_err(|_error| ServerError::InvalidRangeHeader)?;
        let expected_range = parse_upload_content_range(content_range)?;
        if expected_range.start() != current_length {
            return Err(ServerError::RangeNotSatisfiable);
        }
        let observed_end = expected_range
            .start()
            .checked_add(u64::try_from(bytes.len())?)
            .and_then(|value| value.checked_sub(1))
            .ok_or(ServerError::Overflow)?;
        if observed_end != expected_range.end_inclusive() {
            return Err(ServerError::RangeNotSatisfiable);
        }
    }
    ensure_upload_growth_within_limit(state, current_length, bytes.len())?;
    let new_length = if session.use_s3_multipart {
        let (_session, new_length) = append_s3_multipart_upload_bytes(
            state.config.root_dir(),
            &state.backend,
            session_id,
            session,
            &bytes,
        )
        .await?;
        new_length
    } else {
        let new_length = append_upload_bytes(state.config.root_dir(), session_id, &bytes).await?;
        touch_upload_session(state.config.root_dir(), session_id, session).await?;
        new_length
    };
    let last = new_length.saturating_sub(1);
    Response::builder()
        .status(StatusCode::ACCEPTED)
        .header(LOCATION, upload_session_location(repository, session_id))
        .header(RANGE, format!("0-{last}"))
        .body(Body::empty())
        .map_err(|_error| ServerError::Overflow)
}

async fn oci_put_blob_upload(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    uri: &Uri,
    repository: &str,
    session_id: &str,
    body: Body,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, headers, Some(repository), TokenScope::Write)?;
    let scope = auth.as_ref().map(scope_from_auth);
    validate_oci_repository_scope(repository, scope)?;
    let query = parse_query_map(uri)?;
    let digest = query.get("digest").ok_or(ServerError::InvalidDigest)?;
    let digest_hex = parse_sha256_digest(digest)?;
    let mut body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let final_bytes = read_body_to_bytes(&mut body).await?;
    let _lock = lock_upload_sessions(state.config.root_dir()).await?;
    let session = read_upload_session(
        state.config.root_dir(),
        session_id,
        state.config.oci_upload_session_ttl_seconds(),
    )
    .await?;
    if session.repository != repository || session.scope_namespace != scope_namespace(scope) {
        return Err(ServerError::NotFound);
    }
    let current_length = if let Some(length) = upload_session_length(&session) {
        length
    } else {
        upload_length(state.config.root_dir(), session_id).await?
    };
    if let Some(content_range) = headers.get(CONTENT_RANGE) {
        let content_range = content_range
            .to_str()
            .map_err(|_error| ServerError::InvalidRangeHeader)?;
        let expected_range = parse_upload_content_range(content_range)?;
        if expected_range.start() != current_length {
            return Err(ServerError::RangeNotSatisfiable);
        }
        let observed_end = expected_range
            .start()
            .checked_add(u64::try_from(final_bytes.len())?)
            .and_then(|value| value.checked_sub(1))
            .ok_or(ServerError::Overflow)?;
        if observed_end != expected_range.end_inclusive() {
            return Err(ServerError::RangeNotSatisfiable);
        }
    }
    ensure_upload_growth_within_limit(state, current_length, final_bytes.len())?;
    let object_key = oci_blob_key(repository, &digest_hex, scope)?;
    if session.use_s3_multipart {
        let _stored = finalize_s3_multipart_upload_session(
            state.config.root_dir(),
            &state.backend,
            session_id,
            session,
            &object_key,
            &digest_hex,
            &final_bytes,
        )
        .await?;
        delete_upload_session(state.config.root_dir(), session_id).await?;
        return oci_created_response(
            &oci_blob_location(repository, &digest_hex),
            Some(&digest_hex),
        );
    }
    if !final_bytes.is_empty() {
        let _new_length =
            append_upload_bytes(state.config.root_dir(), session_id, &final_bytes).await?;
    }
    let (observed, integrity) = upload_body_integrity(state.config.root_dir(), session_id).await?;
    if observed != digest_hex {
        return Err(ServerError::ExpectedBodyHashMismatch);
    }
    let upload_path = upload_body_path_for_session(state.config.root_dir(), session_id)?;
    let _stored = state.backend.put_sha256_addressed_object_file(
        &object_key,
        &digest_hex,
        &upload_path,
        &integrity,
    )?;
    delete_upload_session(state.config.root_dir(), session_id).await?;
    oci_created_response(
        &oci_blob_location(repository, &digest_hex),
        Some(&digest_hex),
    )
}

async fn oci_get_blob_upload(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    repository: &str,
    session_id: &str,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, headers, Some(repository), TokenScope::Write)?;
    let scope = auth.as_ref().map(scope_from_auth);
    validate_oci_repository_scope(repository, scope)?;
    let _lock = lock_upload_sessions(state.config.root_dir()).await?;
    let session = read_upload_session(
        state.config.root_dir(),
        session_id,
        state.config.oci_upload_session_ttl_seconds(),
    )
    .await?;
    if session.repository != repository || session.scope_namespace != scope_namespace(scope) {
        return Err(ServerError::NotFound);
    }
    let length = if let Some(length) = upload_session_length(&session) {
        length
    } else {
        upload_length(state.config.root_dir(), session_id).await?
    };
    touch_upload_session(state.config.root_dir(), session_id, session).await?;
    let last = length.saturating_sub(1);
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(LOCATION, upload_session_location(repository, session_id))
        .header(RANGE, format!("0-{last}"))
        .body(Body::empty())
        .map_err(|_error| ServerError::Overflow)
}

async fn oci_delete_blob_upload(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    repository: &str,
    session_id: &str,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, headers, Some(repository), TokenScope::Write)?;
    let scope = auth.as_ref().map(scope_from_auth);
    validate_oci_repository_scope(repository, scope)?;
    let _lock = lock_upload_sessions(state.config.root_dir()).await?;
    let session = read_upload_session(
        state.config.root_dir(),
        session_id,
        state.config.oci_upload_session_ttl_seconds(),
    )
    .await?;
    if session.repository != repository || session.scope_namespace != scope_namespace(scope) {
        return Err(ServerError::NotFound);
    }
    if session.use_s3_multipart {
        abort_s3_multipart_upload_session(&state.backend, &session).await?;
    }
    delete_upload_session(state.config.root_dir(), session_id).await?;
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .map_err(|_error| ServerError::Overflow)
}

async fn oci_put_manifest(
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
        OciReference::Tag(tag) => vec![tag],
        OciReference::Digest(_) => Vec::new(),
    };
    accepted_tags.extend(parse_query_values(uri, "tag")?);
    if accepted_tags.len() > MAX_OCI_MANIFEST_TAGS {
        return Err(ServerError::InvalidManifestReference);
    }
    accepted_tags.sort();
    accepted_tags.dedup();
    if !accepted_tags.is_empty() {
        update_oci_tags(state, repository, scope, &accepted_tags, &digest_hex).await?;
    }

    let mut builder = Response::builder()
        .status(StatusCode::CREATED)
        .header(LOCATION, oci_manifest_location(repository, &digest_hex))
        .header("Docker-Content-Digest", format!("sha256:{digest_hex}"));
    if !accepted_tags.is_empty() {
        let joined = accepted_tags.join(", ");
        builder = builder.header("OCI-Tag", joined);
    }
    builder
        .body(Body::empty())
        .map_err(|_error| ServerError::Overflow)
}

async fn oci_delete_manifest(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    repository: &str,
    reference: &str,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, headers, Some(repository), TokenScope::Write)?;
    let scope = auth.as_ref().map(scope_from_auth);
    let OciReference::Digest(digest_hex) = parse_reference(reference)? else {
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
        OciReference::Digest(digest_hex) => Ok(digest_hex),
        OciReference::Tag(tag) => {
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

struct OciTagListPage {
    tags: Vec<String>,
    has_more: bool,
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

async fn update_oci_tags(
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

async fn delete_oci_tags_pointing_to_digest(
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

async fn direct_object_response(
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

fn oci_authorize(
    state: &AppState,
    headers: &HeaderMap,
    repository: Option<&str>,
    required_scope: TokenScope,
) -> Result<Option<super::AuthContext>, ServerError> {
    match authorize(state, headers, required_scope) {
        Ok(auth) => Ok(auth),
        Err(ServerError::MissingAuthorization)
        | Err(ServerError::InvalidAuthorizationHeader)
        | Err(ServerError::InvalidToken(_)) => Err(ServerError::UnauthorizedChallenge(
            oci_bearer_challenge(state.config.public_base_url(), repository, required_scope),
        )),
        Err(error) => Err(error),
    }
}

fn oci_bearer_challenge(
    public_base_url: &str,
    repository: Option<&str>,
    required_scope: TokenScope,
) -> String {
    let realm = format!("{}/v2/token", public_base_url.trim_end_matches('/'));
    let mut challenge = format!("Bearer realm=\"{realm}\",service=\"{OCI_REGISTRY_SERVICE}\"");
    if let Some(repository) = repository {
        let actions = match required_scope {
            TokenScope::Read => "pull",
            TokenScope::Write => "pull,push",
        };
        let _ignored = write!(challenge, ",scope=\"repository:{repository}:{actions}\"");
    }
    challenge
}

fn verify_oci_registry_bootstrap_credentials(
    headers: &HeaderMap,
    signer: &TokenSigner,
) -> Result<TokenClaims, ServerError> {
    let header = headers
        .get(AUTHORIZATION)
        .ok_or(ServerError::MissingAuthorization)?
        .to_str()
        .map_err(|_error| ServerError::InvalidAuthorizationHeader)?;
    if let Some(token) = header.strip_prefix("Bearer ") {
        return signer.verify_now(token).map_err(ServerError::from);
    }
    let Some(encoded) = header.strip_prefix("Basic ") else {
        return Err(ServerError::InvalidAuthorizationHeader);
    };
    if encoded.len() > MAX_OCI_TOKEN_BASIC_AUTH_BYTES {
        return Err(ServerError::InvalidAuthorizationHeader);
    }
    let decoded = BASE64_STANDARD
        .decode(encoded)
        .map_err(|_error| ServerError::InvalidAuthorizationHeader)?;
    let decoded =
        std::str::from_utf8(&decoded).map_err(|_error| ServerError::InvalidAuthorizationHeader)?;
    let Some((_username, password)) = decoded.split_once(':') else {
        return Err(ServerError::InvalidAuthorizationHeader);
    };
    if password.trim().is_empty() {
        return Err(ServerError::InvalidAuthorizationHeader);
    }
    signer.verify_now(password).map_err(ServerError::from)
}

fn parse_oci_registry_token_scope(
    scope: Option<&str>,
) -> Result<(Option<TokenScope>, Option<String>), ServerError> {
    let Some(scope) = scope.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok((None, None));
    };
    let Some((resource_type, repository, actions)) =
        scope
            .split_once(':')
            .and_then(|(resource_type, remainder)| {
                remainder
                    .rsplit_once(':')
                    .map(|(repository, actions)| (resource_type, repository, actions))
            })
    else {
        return Err(ServerError::InvalidManifestReference);
    };
    if resource_type != "repository" {
        return Err(ServerError::InvalidManifestReference);
    }
    validate_repository(repository)?;
    let requested_scope = parse_oci_registry_actions(actions)?;
    Ok((Some(requested_scope), Some(repository.to_owned())))
}

fn parse_oci_registry_token_scopes(
    scopes: &[String],
) -> Result<(Option<TokenScope>, Option<String>), ServerError> {
    let mut requested_scope = None;
    let mut requested_repository: Option<String> = None;
    for scope in scopes {
        let (scope_value, repository) = parse_oci_registry_token_scope(Some(scope))?;
        let Some(scope_value) = scope_value else {
            continue;
        };
        let Some(repository) = repository else {
            continue;
        };
        if let Some(existing_repository) = requested_repository.as_deref() {
            if existing_repository != repository {
                return Err(ServerError::InvalidManifestReference);
            }
        } else {
            requested_repository = Some(repository);
        }
        requested_scope = Some(match requested_scope {
            Some(TokenScope::Write) => TokenScope::Write,
            Some(TokenScope::Read) if scope_value == TokenScope::Write => TokenScope::Write,
            Some(existing_scope) => existing_scope,
            None => scope_value,
        });
    }
    Ok((requested_scope, requested_repository))
}

fn parse_oci_registry_actions(actions: &str) -> Result<TokenScope, ServerError> {
    let mut saw_pull = false;
    let mut saw_push = false;
    for action in actions
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match action {
            "pull" => saw_pull = true,
            "push" => saw_push = true,
            _ => return Err(ServerError::InvalidManifestReference),
        }
    }
    if saw_push {
        return Ok(TokenScope::Write);
    }
    if saw_pull {
        return Ok(TokenScope::Read);
    }
    Err(ServerError::InvalidManifestReference)
}

struct OciRegistryTokenQuery {
    service: Option<String>,
    scopes: Vec<String>,
    _account: Option<String>,
}

fn parse_oci_registry_token_query(uri: &Uri) -> Result<OciRegistryTokenQuery, ServerError> {
    Ok(OciRegistryTokenQuery {
        service: single_bounded_query_value(uri, "service", MAX_OCI_TOKEN_QUERY_SERVICE_BYTES)?,
        scopes: bounded_query_values(
            uri,
            "scope",
            MAX_OCI_TOKEN_QUERY_SCOPE_BYTES,
            MAX_OCI_TOKEN_QUERY_SCOPES,
        )?,
        _account: single_bounded_query_value(uri, "account", MAX_OCI_TOKEN_QUERY_ACCOUNT_BYTES)?,
    })
}

fn single_bounded_query_value(
    uri: &Uri,
    key: &str,
    max_bytes: usize,
) -> Result<Option<String>, ServerError> {
    let values = parse_query_values(uri, key)?;
    if values.len() > 1 {
        return Err(ServerError::InvalidManifestReference);
    }
    let Some(value) = values.into_iter().next() else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > max_bytes {
        return Err(ServerError::RequestQueryTooLarge);
    }
    Ok(Some(value.to_owned()))
}

fn bounded_query_values(
    uri: &Uri,
    key: &str,
    max_bytes: usize,
    max_values: usize,
) -> Result<Vec<String>, ServerError> {
    let values = parse_query_values(uri, key)?;
    if values.len() > max_values {
        return Err(ServerError::InvalidManifestReference);
    }
    let mut bounded = Vec::with_capacity(values.len());
    for value in values {
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        if value.len() > max_bytes {
            return Err(ServerError::RequestQueryTooLarge);
        }
        bounded.push(value.to_owned());
    }
    Ok(bounded)
}

fn scope_allows_oci_exchange(
    actual_scope: TokenScope,
    requested_scope: Option<TokenScope>,
) -> bool {
    match requested_scope.unwrap_or(actual_scope) {
        TokenScope::Read => actual_scope.allows_read(),
        TokenScope::Write => actual_scope.allows_write(),
    }
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

fn parse_query_map(uri: &Uri) -> Result<BTreeMap<String, String>, ServerError> {
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

fn parse_query_values(uri: &Uri, key: &str) -> Result<Vec<String>, ServerError> {
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

fn parse_oci_tag_list_page_size(value: Option<&str>) -> Result<usize, ServerError> {
    let Some(value) = value else {
        return Ok(MAX_OCI_TAG_LIST_PAGE_SIZE);
    };
    let page_size = value
        .parse::<usize>()
        .map_err(|_error| ServerError::InvalidManifestReference)?;
    if page_size == 0 || page_size > MAX_OCI_TAG_LIST_PAGE_SIZE {
        return Err(ServerError::InvalidManifestReference);
    }
    Ok(page_size)
}

fn oci_tags_list_next_link(
    repository: &str,
    page_size: usize,
    last_tag: &str,
) -> Result<HeaderValue, ServerError> {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("n", &page_size.to_string())
        .append_pair("last", last_tag)
        .finish();
    HeaderValue::from_str(&format!(
        "</v2/{repository}/tags/list?{query}>; rel=\"next\""
    ))
    .map_err(|_error| ServerError::InvalidManifestReference)
}

fn oci_created_response(location: &str, digest_hex: Option<&str>) -> Result<Response, ServerError> {
    let mut builder = Response::builder()
        .status(StatusCode::CREATED)
        .header(LOCATION, location);
    if let Some(digest_hex) = digest_hex {
        builder = builder.header("Docker-Content-Digest", format!("sha256:{digest_hex}"));
    }
    builder
        .body(Body::empty())
        .map_err(|_error| ServerError::Overflow)
}

pub(crate) enum OciPath {
    Blob {
        repository: String,
        digest_hex: String,
    },
    BlobUploads {
        repository: String,
    },
    BlobUploadSession {
        repository: String,
        session_id: String,
    },
    Manifest {
        repository: String,
        reference: String,
    },
    TagsList {
        repository: String,
    },
}

pub(crate) fn parse_oci_path(path: &str) -> Result<OciPath, ServerError> {
    let path = path.trim_end_matches('/');
    if let Some(repository) = path.strip_suffix("/blobs/uploads") {
        validate_repository(repository)?;
        return Ok(OciPath::BlobUploads {
            repository: repository.to_owned(),
        });
    }
    if let Some((repository, session_id)) = path.split_once("/blobs/uploads/") {
        validate_repository(repository)?;
        return Ok(OciPath::BlobUploadSession {
            repository: repository.to_owned(),
            session_id: session_id.to_owned(),
        });
    }
    if let Some((repository, digest)) = path.split_once("/blobs/") {
        validate_repository(repository)?;
        return Ok(OciPath::Blob {
            repository: repository.to_owned(),
            digest_hex: parse_sha256_digest(digest)?,
        });
    }
    if let Some((repository, reference)) = path.split_once("/manifests/") {
        validate_repository(repository)?;
        return Ok(OciPath::Manifest {
            repository: repository.to_owned(),
            reference: reference.to_owned(),
        });
    }
    if let Some(repository) = path.strip_suffix("/tags/list") {
        validate_repository(repository)?;
        return Ok(OciPath::TagsList {
            repository: repository.to_owned(),
        });
    }

    Err(ServerError::NotFound)
}

#[cfg(test)]
mod tests {
    use super::{OciPath, parse_oci_path};
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
