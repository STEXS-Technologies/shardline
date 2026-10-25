use std::{
    collections::BTreeMap,
    path::{Path as FsPath, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header::AUTHORIZATION},
    response::IntoResponse,
    response::Response,
};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use shardline_protocol::{ByteRange, ShardlineHash, TokenClaims, TokenScope};
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
use tempfile::NamedTempFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::{commit, error::HubApiError, models::*};

use super::{
    HubRepository, HubState, lfs_object_key, object_io::stream_object_range, stream_object,
};

/// Maximum number of objects allowed in a single batch request.
const MAX_LFS_BATCH_OBJECTS: usize = 1024;
/// Maximum body size for a single LFS upload (64 MiB, matches server-side default).
const MAX_LFS_UPLOAD_BYTES: usize = 64 * 1024 * 1024;
/// Avoid a scratch-file write for small LFS uploads while keeping aggregate
/// request-body memory bounded per server process.
const LFS_UPLOAD_MEMORY_THRESHOLD: usize = 256 * 1024;
const LFS_UPLOAD_MEMORY_SLOTS: usize = 64;
static LFS_UPLOAD_MEMORY_ADMISSION: Semaphore = Semaphore::const_new(LFS_UPLOAD_MEMORY_SLOTS);

#[cfg(test)]
std::thread_local! {
    static LFS_TEMPFILE_ATTEMPTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn test_lfs_tempfile_attempts() -> u64 {
    LFS_TEMPFILE_ATTEMPTS.with(std::cell::Cell::get)
}

enum LfsUploadStagedContent {
    Memory {
        bytes: Vec<u8>,
        _permit: SemaphorePermit<'static>,
    },
    File(NamedTempFile),
}

struct LfsUploadStaging {
    memory: Option<Vec<u8>>,
    permit: Option<SemaphorePermit<'static>>,
    temporary: Option<NamedTempFile>,
    output: Option<tokio::fs::File>,
    local_root: Option<PathBuf>,
}

impl LfsUploadStaging {
    async fn new(local_root: Option<&FsPath>) -> Result<Self, HubApiError> {
        let permit = LFS_UPLOAD_MEMORY_ADMISSION.try_acquire().ok();
        Self::with_permit(permit, local_root).await
    }

    async fn with_permit(
        permit: Option<SemaphorePermit<'static>>,
        local_root: Option<&FsPath>,
    ) -> Result<Self, HubApiError> {
        if permit.is_some() {
            return Ok(Self {
                memory: Some(Vec::new()),
                permit,
                temporary: None,
                output: None,
                local_root: local_root.map(FsPath::to_path_buf),
            });
        }

        Self::file_backed(local_root).await
    }

    async fn file_backed(local_root: Option<&FsPath>) -> Result<Self, HubApiError> {
        let temporary = new_lfs_temporary_file(local_root)
            .map_err(|error| HubApiError::CasError(error.to_string()))?;
        let output = staging_output(&temporary)?;
        Ok(Self {
            memory: None,
            permit: None,
            temporary: Some(temporary),
            output: Some(output),
            local_root: local_root.map(FsPath::to_path_buf),
        })
    }

    async fn write_chunk(&mut self, bytes: &[u8]) -> Result<(), HubApiError> {
        if let Some(memory) = &mut self.memory {
            let next_length = memory
                .len()
                .checked_add(bytes.len())
                .ok_or_else(|| HubApiError::BadRequest("upload too large".to_owned()))?;
            if next_length <= LFS_UPLOAD_MEMORY_THRESHOLD {
                memory.try_reserve_exact(bytes.len()).map_err(|error| {
                    HubApiError::CasError(format!("unable to stage small LFS upload: {error}"))
                })?;
                memory.extend_from_slice(bytes);
                return Ok(());
            }

            let temporary = new_lfs_temporary_file(self.local_root.as_deref())
                .map_err(|error| HubApiError::CasError(error.to_string()))?;
            let mut output = staging_output(&temporary)?;
            output
                .write_all(memory)
                .await
                .map_err(|error| HubApiError::CasError(error.to_string()))?;
            output
                .write_all(bytes)
                .await
                .map_err(|error| HubApiError::CasError(error.to_string()))?;
            self.memory = None;
            self.permit = None;
            self.temporary = Some(temporary);
            self.output = Some(output);
            return Ok(());
        }

        self.output
            .as_mut()
            .ok_or_else(|| HubApiError::CasError("LFS staging sink is unavailable".to_owned()))?
            .write_all(bytes)
            .await
            .map_err(|error| HubApiError::CasError(error.to_string()))
    }

    async fn finish(mut self) -> Result<LfsUploadStagedContent, HubApiError> {
        if let Some(output) = &mut self.output {
            output
                .flush()
                .await
                .map_err(|error| HubApiError::CasError(error.to_string()))?;
        }
        drop(self.output.take());
        if let (Some(bytes), Some(permit)) = (self.memory.take(), self.permit.take()) {
            return Ok(LfsUploadStagedContent::Memory {
                bytes,
                _permit: permit,
            });
        }
        self.temporary
            .take()
            .map(LfsUploadStagedContent::File)
            .ok_or_else(|| HubApiError::CasError("LFS staging sink is unavailable".to_owned()))
    }
}

fn staging_output(temporary: &NamedTempFile) -> Result<tokio::fs::File, HubApiError> {
    // Use the already-open temporary inode. An asynchronous path-based create
    // can run after cancellation unlinks the guard, recreating an orphan file.
    let file = temporary
        .as_file()
        .try_clone()
        .map_err(|error| HubApiError::CasError(error.to_string()))?;
    Ok(tokio::fs::File::from_std(file))
}

/// Place local-store staging files on the store's filesystem when possible.
/// They live beside (not inside) the object root, so walkers and GC cannot
/// mistake an in-progress upload for a published object. This lets local
/// publication use its verified atomic hard-link path instead of copying a
/// large upload across filesystems from the system temp directory.
fn new_lfs_temporary_file(local_root: Option<&FsPath>) -> std::io::Result<NamedTempFile> {
    #[cfg(test)]
    LFS_TEMPFILE_ATTEMPTS.with(|attempts| attempts.set(attempts.get().saturating_add(1)));

    if let Some(local_root) = local_root
        && let Some(parent) = local_root.parent()
        && same_filesystem(parent, local_root)
        && let Ok(temporary) = NamedTempFile::new_in(parent)
    {
        return Ok(temporary);
    }
    NamedTempFile::new()
}

#[cfg(unix)]
fn same_filesystem(left: &FsPath, right: &FsPath) -> bool {
    use std::os::unix::fs::MetadataExt;

    left.metadata()
        .ok()
        .zip(right.metadata().ok())
        .is_some_and(|(left, right)| left.dev() == right.dev())
}

#[cfg(not(unix))]
fn same_filesystem(_left: &FsPath, _right: &FsPath) -> bool {
    false
}

// ---- LFS batch (requires Read) ----

pub(crate) async fn lfs_batch(
    State(state): State<HubState>,
    repo: HubRepository,
    Json(request): Json<LfsBatchRequest>,
) -> Result<Json<LfsBatchResponse>, HubApiError> {
    lfs_batch_inner(&state, repo.capability(), request).await
}

/// Git LFS discovery endpoint used by Hugging Face's standard `hf upload`
/// client: `/{owner}/{repo}.git/info/lfs/objects/batch`.
pub(crate) async fn lfs_batch_git(
    State(state): State<HubState>,
    repo: HubRepository<false, false>,
    Path((ns, repo_name)): Path<(String, String)>,
    Json(request): Json<LfsBatchRequest>,
) -> Result<Json<LfsBatchResponse>, HubApiError> {
    let repo_name = crate::git::smart_http::repository::canonical_name_for_claims(
        &state,
        repo.capability().claims(),
        &ns,
        &repo_name,
    )
    .await?;
    repo.require_binding(&ns, &repo_name)?;
    lfs_batch_inner(&state, repo.capability(), request).await
}

/// Dataset and Space clients include the repo type as a URL prefix, unlike
/// model clients. Hugging Face builds their LFS batch URLs as
/// `/datasets/{owner}/{repo}.git/...` and `/spaces/{owner}/{repo}.git/...`.
pub(crate) async fn lfs_batch_git_typed(
    State(state): State<HubState>,
    repo: HubRepository<false, false>,
    Path((repo_type, ns, repo_name)): Path<(String, String, String)>,
    Json(request): Json<LfsBatchRequest>,
) -> Result<Json<LfsBatchResponse>, HubApiError> {
    if !matches!(repo_type.as_str(), "models" | "datasets" | "spaces") {
        return Err(HubApiError::NotFound);
    }
    let repo_name = crate::git::smart_http::repository::canonical_name_for_claims(
        &state,
        repo.capability().claims(),
        &ns,
        &repo_name,
    )
    .await?;
    repo.require_binding(&ns, &repo_name)?;
    lfs_batch_inner(&state, repo.capability(), request).await
}

async fn lfs_batch_inner(
    state: &HubState,
    capability: &shardline_server_core::AuthorizedRepository,
    request: LfsBatchRequest,
) -> Result<Json<LfsBatchResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("lfs_batch", "POST", 200);
    if request.operation != LfsBatchOperation::Download
        && capability
            .claims()
            .is_some_and(|claims| !claims.scope().allows_write())
    {
        return Err(HubApiError::Forbidden);
    }
    // LFS object keys are namespaced by the authenticated repository claim.
    // Validate batch size.
    if request.objects.len() > MAX_LFS_BATCH_OBJECTS {
        return Err(HubApiError::BadRequest(
            "too many objects in batch request".to_owned(),
        ));
    }

    // The official Hugging Face JavaScript client spells SHA-256 `sha_256`.
    // Accept that wire alias without changing object verification or identity.
    if let Some(hash_algo) = request.hash_algo.as_deref()
        && !matches!(hash_algo, "sha256" | "sha_256")
    {
        return Err(HubApiError::BadRequest(
            "unsupported hash algorithm".to_owned(),
        ));
    }

    super::lfs_revision::authorize(state, capability, &request).await?;

    // Xet uploads register both the native reconstruction and the file's LFS
    // SHA-256 in the uploaded shard. Hub commits still contain the ordinary
    // LFS pointer, and the resolve/download routes use that SHA-256 to find
    // the registered reconstruction. Only negotiate Xet when a write token
    // can be exchanged for a repository-scoped CAS token.
    let use_xet = request.operation == LfsBatchOperation::Upload
        && request.transfers.iter().any(|transfer| transfer == "xet")
        && capability.claims().is_some()
        && state.auth.is_some();
    let use_basic = request.transfers.is_empty()
        || request.transfers.iter().any(|transfer| transfer == "basic");
    if !use_xet && !use_basic {
        return Err(HubApiError::BadRequest(
            "none of the requested LFS transfer adapters are supported".to_owned(),
        ));
    }
    let transfer = if use_xet { "xet" } else { "basic" };
    if use_xet {
        for object in &request.objects {
            commit::validate_lfs_oid(&object.oid)
                .map_err(|error| HubApiError::BadRequest(error.to_string()))?;
        }
        let claims = capability.claims().ok_or(HubApiError::Unauthorized)?;
        let auth = state.auth.as_ref().ok_or(HubApiError::Unauthorized)?;
        let token = auth
            .provider()
            .mint_token(claims)
            .map_err(|_error| HubApiError::InvalidToken)?;
        let cas_url = state.public_base_url.trim_end_matches('/').to_owned();
        // Xet clients use the action href to renew credentials, rather than
        // uploading a body there. Keep the CAS base in its dedicated header.
        let scope = claims.repository();
        let name = format!("{}/{}", scope.owner(), scope.name());
        let stored_repo = state
            .store
            .get_repo(&name)
            .map_err(|error| HubApiError::CasError(error.to_string()))?;
        let repo_type = stored_repo.map_or("models", |repo| super::repo_type_path(repo.repo_type));
        let mut refresh = url::Url::parse(&state.public_base_url)
            .map_err(|error| HubApiError::CasError(error.to_string()))?;
        refresh
            .path_segments_mut()
            .map_err(|()| HubApiError::CasError("invalid public URL".to_owned()))?
            .pop_if_empty()
            .extend([
                "api",
                repo_type,
                scope.owner(),
                scope.name(),
                "xet-write-token",
                if request.ref_.name.is_empty() {
                    scope.revision().unwrap_or("main")
                } else {
                    &request.ref_.name
                },
            ]);
        let headers = BTreeMap::from([
            ("X-Xet-Cas-Url".to_owned(), cas_url.clone()),
            ("X-Xet-Access-Token".to_owned(), token),
            (
                "X-Xet-Token-Expiration".to_owned(),
                claims.expires_at_unix_seconds().to_string(),
            ),
        ]);
        let mut objects = Vec::with_capacity(request.objects.len());
        for object in request.objects {
            let registered = crate::git::smart_http::xet_file_hash(
                state.clone(),
                capability.clone(),
                object.oid.clone(),
                object.size,
            )
            .await?
            .is_some();
            // An absent action means already uploaded to stock Git LFS.
            // Only a verified repository-scoped native identity justifies it.
            let actions = (!registered).then(|| LfsObjectActions {
                upload: Some(LfsObjectAction {
                    href: refresh.to_string(),
                    header: Some(headers.clone()),
                    ssh: None,
                }),
                download: None,
                verify: None,
            });
            objects.push(LfsObjectResponse {
                oid: object.oid,
                size: object.size,
                actions,
                error: None,
            });
        }
        return Ok(Json(LfsBatchResponse {
            transfer: transfer.to_owned(),
            objects,
            hash_algo: Some("sha256".to_owned()),
        }));
    }

    let action_header = lfs_action_auth_header(state, capability)?;
    let mut published = None;
    let mut objects = Vec::with_capacity(request.objects.len());
    for obj in &request.objects {
        let key = match crate::object_locations::file_key(state, &obj.oid, capability) {
            Ok(k) => k,
            Err(e) => {
                objects.push(LfsObjectResponse {
                    oid: obj.oid.clone(),
                    size: obj.size,
                    actions: None,
                    error: Some(LfsObjectError {
                        code: 400,
                        message: format!("invalid oid: {e}"),
                    }),
                });
                continue;
            }
        };
        let mut exists = state
            .object_store
            .contains(&key)
            .map_err(|error| HubApiError::CasError(error.to_string()))?;
        if !exists && request.operation == LfsBatchOperation::Download {
            exists = crate::git::smart_http::xet_file_hash(
                state.clone(),
                capability.clone(),
                obj.oid.clone(),
                obj.size,
            )
            .await?
            .is_some();
        }
        if !exists && request.operation == LfsBatchOperation::Download {
            if published.is_none() {
                published =
                    Some(crate::git::smart_http::scoped_published_files(state, capability).await?);
            }
            if let Some(Some(published)) = &published
                && let Some(published_key) = published.key(state, &obj.oid, capability)?
            {
                exists = state
                    .object_store
                    .contains(&published_key)
                    .map_err(|error| HubApiError::CasError(error.to_string()))?;
            }
        }
        let actions = match request.operation {
            LfsBatchOperation::Download => {
                if exists {
                    Some(LfsObjectActions {
                        download: Some(LfsObjectAction {
                            href: format!(
                                "{}/lfs/objects/{}",
                                state.public_base_url.trim_end_matches('/'),
                                obj.oid
                            ),
                            header: action_header.clone(),
                            ssh: None,
                        }),
                        upload: None,
                        verify: None,
                    })
                } else {
                    None
                }
            }
            LfsBatchOperation::Upload => {
                if exists {
                    // Standard clients treat an object without actions as
                    // already uploaded and skip it. A verify-only action
                    // is invalid for their upload flow because it still
                    // requires an upload action.
                    None
                } else {
                    Some(LfsObjectActions {
                        download: None,
                        upload: Some(LfsObjectAction {
                            href: lfs_upload_action_url(state, capability, &obj.oid)?,
                            // huggingface_hub's single-part LFS uploader
                            // does not forward action headers. The short-
                            // lived, object-scoped upload URL carries its
                            // own capability instead.
                            header: None,
                            ssh: None,
                        }),
                        verify: Some(LfsObjectAction {
                            href: format!(
                                "{}/lfs/objects/{}/verify",
                                state.public_base_url.trim_end_matches('/'),
                                obj.oid
                            ),
                            header: action_header.clone(),
                            ssh: None,
                        }),
                    })
                }
            }
            LfsBatchOperation::Verify => {
                if exists {
                    Some(LfsObjectActions {
                        download: None,
                        upload: None,
                        verify: Some(LfsObjectAction {
                            href: format!(
                                "{}/lfs/objects/{}/verify",
                                state.public_base_url.trim_end_matches('/'),
                                obj.oid
                            ),
                            header: action_header.clone(),
                            ssh: None,
                        }),
                    })
                } else {
                    None
                }
            }
        };

        let error = if !exists
            && (request.operation == LfsBatchOperation::Download
                || request.operation == LfsBatchOperation::Verify)
        {
            Some(LfsObjectError {
                code: 404,
                message: "Object not found".to_owned(),
            })
        } else {
            None
        };

        objects.push(LfsObjectResponse {
            oid: obj.oid.clone(),
            size: obj.size,
            actions,
            error,
        });
    }

    Ok(Json(LfsBatchResponse {
        transfer: transfer.to_owned(),
        objects,
        hash_algo: Some("sha256".to_owned()),
    }))
}

fn lfs_upload_action_url(
    state: &HubState,
    capability: &shardline_server_core::AuthorizedRepository,
    oid: &str,
) -> Result<String, HubApiError> {
    let base_url = state.public_base_url.trim_end_matches('/');
    let Some(claims) = capability.claims() else {
        return Ok(format!("{base_url}/lfs/objects/{oid}"));
    };
    let auth = state.auth.as_ref().ok_or(HubApiError::Unauthorized)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| HubApiError::InvalidToken)?
        .as_secs();
    let expires_at = now
        .checked_add(300)
        .ok_or(HubApiError::InvalidToken)?
        .min(claims.expires_at_unix_seconds());
    let subject = format!("shardline-lfs-upload:{oid}");
    let upload_claims = TokenClaims::new(
        claims.issuer(),
        &subject,
        TokenScope::Write,
        claims.repository().clone(),
        expires_at,
    )
    .map_err(|_error| HubApiError::InvalidToken)?;
    let token = auth
        .provider()
        .mint_token(&upload_claims)
        .map_err(|_error| HubApiError::InvalidToken)?;
    Ok(format!("{base_url}/lfs/objects/{oid}?upload_token={token}"))
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct LfsUploadQuery {
    pub(crate) upload_token: Option<String>,
}

fn lfs_action_auth_header(
    state: &HubState,
    capability: &shardline_server_core::AuthorizedRepository,
) -> Result<Option<BTreeMap<String, String>>, HubApiError> {
    let Some(claims) = capability.claims() else {
        return Ok(None);
    };
    let auth = state.auth.as_ref().ok_or(HubApiError::Unauthorized)?;
    let token = auth.provider().mint_token(claims).map_err(|error| {
        tracing::debug!("failed to mint LFS action token: {error}");
        HubApiError::InvalidToken
    })?;
    Ok(Some(BTreeMap::from([(
        "Authorization".to_owned(),
        format!("Bearer {token}"),
    )])))
}

/// Git LFS combines action headers with `http.extraHeader`. Only byte-identical
/// Authorization repetitions have a single unambiguous credential. Other Hub
/// routes retain their strict rejection of repeated Authorization fields.
pub(crate) async fn normalize_lfs_authorization(
    mut request: axum::http::Request<Body>,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, HubApiError> {
    let values: Vec<_> = request.headers().get_all(AUTHORIZATION).iter().collect();
    if values.len() > 1 {
        let first = values.first().ok_or(HubApiError::InvalidToken)?;
        if values.iter().any(|value| value != first) {
            return Err(HubApiError::InvalidToken);
        }
        let credential = (*first).clone();
        request.headers_mut().remove(AUTHORIZATION);
        request.headers_mut().insert(AUTHORIZATION, credential);
    }
    Ok(next.run(request).await)
}

/// Convert Hub-level route errors into the Git LFS API error shape. LFS
/// clients expect a `message` field and the LFS media type even when
/// authorization, JSON extraction, or object storage fails before an action
/// response can be built.
pub(crate) async fn lfs_error_response(
    request: axum::http::Request<Body>,
    next: axum::middleware::Next,
) -> Response {
    const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
    const LFS_JSON: &str = "application/vnd.git-lfs+json";

    let response = next.run(request).await;
    if response.status().is_success() {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, MAX_ERROR_BODY_BYTES)
        .await
        .unwrap_or_default();
    let message = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|value| {
            value
                .get("message")
                .or_else(|| value.get("error"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "LFS request failed".to_owned());

    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    parts.headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(LFS_JSON),
    );
    let lfs_body = Body::from(serde_json::json!({ "message": message }).to_string());
    Response::from_parts(parts, lfs_body)
}

// ---- LFS upload (requires Write) ----

pub(crate) async fn lfs_upload(
    State(state): State<HubState>,
    headers: HeaderMap,
    Query(query): Query<LfsUploadQuery>,
    Path(oid): Path<String>,
    body: Body,
) -> Result<StatusCode, HubApiError> {
    let repo = authorize_lfs_upload(&state, &headers, query.upload_token.as_deref(), &oid)?;
    shardline_metrics::record_hub_api_request("lfs_upload", "PUT", 200);
    shardline_metrics::record_hub_api_file_upload();
    commit::validate_lfs_oid(&oid)?;

    let key = lfs_object_key(&oid, &repo)?;
    #[cfg(unix)]
    if let Some(existing) = state
        .object_store
        .open_local_object_file_if_present(&key)
        .map_err(|error| HubApiError::CasError(error.to_string()))?
    {
        verify_duplicate_local_lfs_upload(&state, &key, &oid, body, existing).await?;
        crate::object_locations::remember(
            &state,
            &repo,
            shardline_index::hub::HubObjectKind::File,
            &oid,
        )?;
        return Ok(StatusCode::OK);
    }

    let mut staging = LfsUploadStaging::new(state.object_store.local_root()).await?;
    let mut length = 0_usize;
    let mut hasher = blake3::Hasher::new();
    let mut sha256 = Sha256::new();
    let mut stream = body.into_data_stream();
    while let Some(frame) = stream.next().await {
        let bytes = frame.map_err(|e| HubApiError::CasError(e.to_string()))?;
        length = length
            .checked_add(bytes.len())
            .ok_or_else(|| HubApiError::BadRequest("upload too large".to_owned()))?;
        if length > MAX_LFS_UPLOAD_BYTES {
            return Err(HubApiError::BadRequest(format!(
                "upload body exceeds maximum size of {} bytes",
                MAX_LFS_UPLOAD_BYTES
            )));
        }
        hasher.update(&bytes);
        sha256.update(&bytes);
        staging.write_chunk(&bytes).await?;
    }
    if hex::encode(sha256.finalize()) != oid {
        return Err(HubApiError::BadRequest(
            "uploaded LFS content does not match its SHA-256 object id".to_owned(),
        ));
    }
    let staged_content = staging.finish().await?;
    let integrity = ObjectIntegrity::new(
        ShardlineHash::from_bytes(*hasher.finalize().as_bytes()),
        length as u64,
    );
    match staged_content {
        LfsUploadStagedContent::Memory { bytes, _permit } => state
            .object_store
            .put_if_absent(&key, ObjectBody::from_vec(bytes), &integrity)
            .map_err(|error| HubApiError::CasError(error.to_string()))?,
        LfsUploadStagedContent::File(temporary) => state
            .object_store
            .put_content_addressed_file(&key, temporary.path(), &integrity)
            .map_err(|error: shardline_server_core::ServerObjectStoreError| {
                HubApiError::CasError(error.to_string())
            })?,
    };
    crate::object_locations::remember(
        &state,
        &repo,
        shardline_index::hub::HubObjectKind::File,
        &oid,
    )?;
    Ok(StatusCode::OK)
}

/// Checks a retry against a pinned local object without writing the request
/// body to scratch storage. The final inode and parent directory are synced
/// again before acknowledging, so a concurrent GC unlink cannot turn this
/// duplicate fast path into a false durable success.
#[cfg(unix)]
async fn verify_duplicate_local_lfs_upload(
    state: &HubState,
    key: &ObjectKey,
    oid: &str,
    body: Body,
    existing: std::fs::File,
) -> Result<(), HubApiError> {
    const COMPARE_BUFFER_BYTES: usize = 64 * 1024;

    let stored_length = existing
        .metadata()
        .map_err(|error| HubApiError::CasError(error.to_string()))?
        .len();
    let mut stored = tokio::fs::File::from_std(
        existing
            .try_clone()
            .map_err(|error| HubApiError::CasError(error.to_string()))?,
    );
    let mut compare_buffer = vec![0_u8; COMPARE_BUFFER_BYTES];
    let mut stream = body.into_data_stream();
    let mut length = 0_usize;
    let mut sha256 = Sha256::new();
    let mut matches_existing = true;
    let mut stored_eof = false;

    while let Some(frame) = stream.next().await {
        let bytes = frame.map_err(|error| HubApiError::CasError(error.to_string()))?;
        length = length
            .checked_add(bytes.len())
            .ok_or_else(|| HubApiError::BadRequest("upload too large".to_owned()))?;
        if length > MAX_LFS_UPLOAD_BYTES {
            return Err(HubApiError::BadRequest(format!(
                "upload body exceeds maximum size of {} bytes",
                MAX_LFS_UPLOAD_BYTES
            )));
        }
        sha256.update(&bytes);

        let mut compared = 0;
        while compared < bytes.len() && !stored_eof {
            let read_limit = bytes
                .len()
                .saturating_sub(compared)
                .min(compare_buffer.len());
            let buffer = compare_buffer.get_mut(..read_limit).ok_or_else(|| {
                HubApiError::CasError("invalid LFS comparison buffer length".to_owned())
            })?;
            let read = stored
                .read(buffer)
                .await
                .map_err(|error| HubApiError::CasError(error.to_string()))?;
            if read == 0 {
                stored_eof = true;
                matches_existing = false;
                break;
            }
            let compared_end = compared.checked_add(read).ok_or_else(|| {
                HubApiError::CasError("LFS comparison offset overflow".to_owned())
            })?;
            if bytes.get(compared..compared_end) != compare_buffer.get(..read) {
                matches_existing = false;
            }
            compared = compared_end;
        }
    }

    if hex::encode(sha256.finalize()) != oid {
        return Err(HubApiError::BadRequest(
            "uploaded LFS content does not match its SHA-256 object id".to_owned(),
        ));
    }
    if !matches_existing || u64::try_from(length).ok() != Some(stored_length) {
        return Err(HubApiError::CasError(
            "existing local LFS object differs from uploaded content".to_owned(),
        ));
    }
    let mut trailing = [0_u8; 1];
    if stored
        .read(&mut trailing)
        .await
        .map_err(|error| HubApiError::CasError(error.to_string()))?
        != 0
    {
        return Err(HubApiError::CasError(
            "existing local LFS object changed while checking duplicate upload".to_owned(),
        ));
    }

    state
        .object_store
        .verify_local_object_file_is_durable(key, &existing)
        .map_err(|error| HubApiError::CasError(error.to_string()))
}

/// Verifies a completed LFS upload against its SHA-256 object id and declared
/// size. Hugging Face clients may call this action after the PUT succeeds.
pub(crate) async fn lfs_verify(
    State(state): State<HubState>,
    repo: HubRepository<true>,
    Path(path_oid): Path<String>,
    Json(request): Json<LfsObjectRequest>,
) -> Result<StatusCode, HubApiError> {
    commit::validate_lfs_oid(&path_oid)?;
    if request.oid != path_oid {
        return Err(HubApiError::BadRequest(
            "LFS verify body oid does not match request path".to_owned(),
        ));
    }
    if request.size > MAX_LFS_UPLOAD_BYTES as u64 {
        return Err(HubApiError::BadRequest(
            "LFS object exceeds verification size limit".to_owned(),
        ));
    }
    let key = crate::object_locations::file_key(&state, &path_oid, repo.capability())?;
    let metadata = state
        .object_store
        .metadata(&key)
        .map_err(|error| HubApiError::CasError(error.to_string()))?
        .ok_or(HubApiError::NotFound)?;
    if metadata.length() != request.size {
        return Err(HubApiError::BadRequest(
            "LFS object size does not match verify request".to_owned(),
        ));
    }
    let content = if request.size == 0 {
        Vec::new()
    } else {
        state
            .object_store
            .read_range(
                &key,
                ByteRange::new(
                    0,
                    request
                        .size
                        .checked_sub(1)
                        .ok_or_else(|| HubApiError::BadRequest("invalid LFS size".to_owned()))?,
                )
                .map_err(|error| HubApiError::BadRequest(error.to_string()))?,
            )
            .map_err(|error| HubApiError::CasError(error.to_string()))?
    };
    if hex::encode(Sha256::digest(&content)) != path_oid {
        return Err(HubApiError::BadRequest(
            "stored LFS content does not match its SHA-256 object id".to_owned(),
        ));
    }
    shardline_metrics::record_hub_api_request("lfs_verify", "POST", 200);
    Ok(StatusCode::OK)
}

fn authorize_lfs_upload(
    state: &HubState,
    request_headers: &HeaderMap,
    upload_token: Option<&str>,
    oid: &str,
) -> Result<shardline_server_core::AuthorizedRepository, HubApiError> {
    let Some(auth) = state.auth.as_ref() else {
        if upload_token.is_some() {
            return Err(HubApiError::InvalidToken);
        }
        return Ok(shardline_server_core::AuthorizedRepository::anonymous_full_access());
    };

    let mut headers = request_headers.clone();
    if let Some(token) = upload_token {
        // Native Git LFS also forwards its configured Authorization header.
        // Verify both credentials and require the same authority/repository;
        // the query capability still restricts this upload to its exact OID.
        let caller = if headers.contains_key(AUTHORIZATION) {
            Some(auth.authorize(&headers, TokenScope::Write)?)
        } else {
            None
        };
        let value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_error| HubApiError::InvalidToken)?;
        headers.remove(AUTHORIZATION);
        headers.insert(AUTHORIZATION, value);
        let upload = auth.authorize(&headers, TokenScope::Write)?;
        if caller.as_ref().is_some_and(|caller| {
            caller.claims().issuer() != upload.claims().issuer()
                || caller.claims().repository() != upload.claims().repository()
        }) {
            return Err(HubApiError::Forbidden);
        }
    }
    let context = auth.authorize(&headers, TokenScope::Write)?;
    if upload_token.is_some() && context.claims().subject() != format!("shardline-lfs-upload:{oid}")
    {
        return Err(HubApiError::Forbidden);
    }
    shardline_server_core::AuthorizedRepository::from_verified_context(context, TokenScope::Write)
        .map_err(Into::into)
}

// ---- LFS download (requires Read) ----

pub(crate) async fn lfs_download(
    State(state): State<HubState>,
    repo: HubRepository,
    Path(oid): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, HubApiError> {
    shardline_metrics::record_hub_api_request("lfs_download", "GET", 200);
    shardline_metrics::record_hub_api_file_download();
    let mut key = crate::object_locations::file_key(&state, &oid, repo.capability())?;
    let mut meta = state
        .object_store
        .metadata(&key)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    if meta.is_none()
        && let Some(published) =
            crate::git::smart_http::scoped_published_files(&state, repo.capability()).await?
        && let Some(published_key) = published.key(&state, &oid, repo.capability())?
    {
        key = published_key;
        meta = state
            .object_store
            .metadata(&key)
            .map_err(|error| HubApiError::CasError(error.to_string()))?;
    }
    let Some(meta) = meta else {
        return stream_hf_xet_object(&state, repo.capability(), &oid, &method, &headers).await;
    };
    let length = meta.length();
    let etag = format!("\"{oid}\"");
    if let Some(response) = super::download_conditions::precondition_response(&headers, &etag)? {
        return Ok(response);
    }
    let selection = if method == Method::HEAD {
        LfsRangeSelection::Full
    } else {
        parse_lfs_range(&headers, length, &etag)
    };
    let mut response = match selection {
        LfsRangeSelection::Full => {
            let mut response = if method == Method::HEAD {
                Response::new(Body::empty())
            } else {
                stream_object(&state.object_store, key, length).into_response()
            };
            response
                .headers_mut()
                .insert(axum::http::header::CONTENT_LENGTH, length.into());
            response
        }
        LfsRangeSelection::Partial(range) => {
            let range_length = range
                .end_inclusive()
                .checked_sub(range.start())
                .and_then(|distance| distance.checked_add(1))
                .ok_or_else(|| HubApiError::CasError("invalid LFS range".to_owned()))?;
            let mut response = stream_object_range(
                &state.object_store,
                key,
                length,
                range.start(),
                range_length,
            )
            .map_err(|error| HubApiError::CasError(error.to_string()))?
            .into_response();
            *response.status_mut() = StatusCode::PARTIAL_CONTENT;
            response.headers_mut().insert(
                axum::http::header::CONTENT_RANGE,
                format!("bytes {}-{}/{length}", range.start(), range.end_inclusive())
                    .parse()
                    .map_err(|_error| HubApiError::BadRequest("invalid LFS range".to_owned()))?,
            );
            response
                .headers_mut()
                .insert(axum::http::header::CONTENT_LENGTH, range_length.into());
            response
        }
        LfsRangeSelection::Unsatisfiable => {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
            response.headers_mut().insert(
                axum::http::header::CONTENT_RANGE,
                format!("bytes */{length}")
                    .parse()
                    .map_err(|_error| HubApiError::NotFound)?,
            );
            response
        }
    };
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        "application/octet-stream"
            .parse()
            .map_err(|_error| HubApiError::NotFound)?,
    );
    response.headers_mut().insert(
        axum::http::header::ACCEPT_RANGES,
        "bytes".parse().map_err(|_error| HubApiError::NotFound)?,
    );
    response.headers_mut().insert(
        axum::http::header::ETAG,
        etag.parse().map_err(|_error| HubApiError::NotFound)?,
    );
    Ok(response)
}

pub(crate) enum LfsRangeSelection {
    Full,
    Partial(ByteRange),
    Unsatisfiable,
}

pub(crate) fn parse_lfs_range(headers: &HeaderMap, length: u64, etag: &str) -> LfsRangeSelection {
    let Some(range_header) = headers.get(axum::http::header::RANGE) else {
        return LfsRangeSelection::Full;
    };
    if headers
        .get(axum::http::header::IF_RANGE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|if_range| if_range != etag)
    {
        return LfsRangeSelection::Full;
    }
    let Ok(range) = range_header.to_str() else {
        return LfsRangeSelection::Full;
    };
    let Some(specification) = range.strip_prefix("bytes=") else {
        return LfsRangeSelection::Full;
    };
    if specification.contains(',') {
        // Multi-range responses are not implemented. RFC 9110 permits ignoring
        // a Range field, so clients receive the complete representation.
        return LfsRangeSelection::Full;
    }
    let Some((start, end)) = specification.split_once('-') else {
        return LfsRangeSelection::Full;
    };
    if length == 0 {
        return LfsRangeSelection::Unsatisfiable;
    }

    let (start, end) = if start.is_empty() {
        let Ok(suffix_length) = end.parse::<u64>() else {
            return LfsRangeSelection::Full;
        };
        if suffix_length == 0 {
            return LfsRangeSelection::Unsatisfiable;
        }
        let selected_length = suffix_length.min(length);
        (
            length.saturating_sub(selected_length),
            length.saturating_sub(1),
        )
    } else {
        let Ok(start) = start.parse::<u64>() else {
            return LfsRangeSelection::Full;
        };
        if start >= length {
            return LfsRangeSelection::Unsatisfiable;
        }
        let end = if end.is_empty() {
            length.saturating_sub(1)
        } else {
            let Ok(end) = end.parse::<u64>() else {
                return LfsRangeSelection::Full;
            };
            if end < start {
                return LfsRangeSelection::Unsatisfiable;
            }
            end.min(length.saturating_sub(1))
        };
        (start, end)
    };

    ByteRange::new(start, end)
        .map(LfsRangeSelection::Partial)
        .unwrap_or(LfsRangeSelection::Unsatisfiable)
}

async fn stream_hf_xet_object(
    state: &HubState,
    capability: &shardline_server_core::AuthorizedRepository,
    oid: &str,
    method: &Method,
    request_headers: &HeaderMap,
) -> Result<Response, HubApiError> {
    let auth = state.auth.as_ref().ok_or(HubApiError::NotFound)?;
    let claims = capability.claims().ok_or(HubApiError::NotFound)?;
    let read_claims = TokenClaims::new(
        claims.issuer(),
        claims.subject(),
        TokenScope::Read,
        claims.repository().clone(),
        claims.expires_at_unix_seconds(),
    )
    .map_err(|_error| HubApiError::InvalidToken)?;
    let read_token = auth
        .provider()
        .mint_token(&read_claims)
        .map_err(|_error| HubApiError::InvalidToken)?;
    let client = super::native_reader_http::client()?;
    let url = format!(
        "{}/v1/hub/xet-files/{oid}",
        state.public_base_url.trim_end_matches('/')
    );
    let mut request = client.request(method.clone(), url).bearer_auth(read_token);
    for name in [
        axum::http::header::RANGE,
        axum::http::header::IF_RANGE,
        axum::http::header::IF_MATCH,
        axum::http::header::IF_NONE_MATCH,
    ] {
        for value in request_headers.get_all(&name) {
            request = request.header(name.clone(), value);
        }
    }
    let upstream = request.send().await.map_err(|error| {
        // reqwest's display includes the request URL. Keep deployment
        // details out of the public error while retaining a useful category.
        let category = if error.is_timeout() {
            "timeout"
        } else if error.is_connect() {
            "connection failure"
        } else {
            "transport failure"
        };
        HubApiError::CasError(format!("internal Xet file reader: {category}"))
    })?;
    if upstream.status() == StatusCode::NOT_FOUND {
        return Err(HubApiError::NotFound);
    }
    let status = upstream.status();
    if let Some(response) = retryable_xet_read_response(status) {
        return Ok(response);
    }
    if !status.is_success()
        && ![
            StatusCode::NOT_MODIFIED,
            StatusCode::PRECONDITION_FAILED,
            StatusCode::RANGE_NOT_SATISFIABLE,
        ]
        .contains(&status)
    {
        return Err(HubApiError::CasError(format!(
            "internal Xet file read returned {}",
            upstream.status()
        )));
    }
    let response_headers = upstream.headers().clone();
    let stream = upstream
        .bytes_stream()
        .map(|chunk| chunk.map_err(std::io::Error::other));
    let mut response = Response::new(Body::from_stream(stream));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        "application/octet-stream"
            .parse()
            .map_err(|_error| HubApiError::NotFound)?,
    );
    for name in [
        axum::http::header::CONTENT_LENGTH,
        axum::http::header::CONTENT_RANGE,
        axum::http::header::ACCEPT_RANGES,
        axum::http::header::ETAG,
    ] {
        if let Some(value) = response_headers.get(&name) {
            response.headers_mut().insert(name, value.clone());
        }
    }
    *response.status_mut() = status;
    Ok(response)
}

// Preserve overload semantics across the Hub/native-reader boundary. Do not
// forward an upstream error body, which may contain deployment details.
fn retryable_xet_read_response(status: StatusCode) -> Option<Response> {
    if !matches!(
        status,
        StatusCode::SERVICE_UNAVAILABLE | StatusCode::TOO_MANY_REQUESTS
    ) {
        return None;
    }
    let mut response = (
        status,
        Json(
            serde_json::json!({"error": "file reader temporarily unavailable; retry the request"}),
        ),
    )
        .into_response();
    response.headers_mut().insert(
        axum::http::header::RETRY_AFTER,
        HeaderValue::from_static("1"),
    );
    Some(response)
}

#[cfg(test)]
mod upload_staging_tests {
    #![allow(clippy::unwrap_used)]

    use super::{LFS_UPLOAD_MEMORY_THRESHOLD, LfsUploadStaging};
    use tokio::io::AsyncWriteExt;

    #[test]
    fn native_read_overload_preserves_retryable_status() {
        for status in [
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::http::StatusCode::TOO_MANY_REQUESTS,
        ] {
            let response = super::retryable_xet_read_response(status).unwrap();
            assert_eq!(response.status(), status);
            assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
        }
        for status in [
            axum::http::StatusCode::OK,
            axum::http::StatusCode::FORBIDDEN,
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert!(super::retryable_xet_read_response(status).is_none());
        }
    }

    #[tokio::test]
    async fn small_upload_stays_in_memory() {
        let mut staging = LfsUploadStaging::new(None).await.unwrap();
        staging.write_chunk(b"small content").await.unwrap();
        assert_eq!(staging.memory.as_deref(), Some(b"small content".as_slice()));
        assert!(staging.temporary.is_none());
    }

    #[tokio::test]
    async fn crossing_the_limit_spills_all_bytes_and_releases_memory_admission() {
        let mut staging = LfsUploadStaging::new(None).await.unwrap();
        let prefix = vec![b'a'; LFS_UPLOAD_MEMORY_THRESHOLD];
        staging.write_chunk(&prefix).await.unwrap();
        staging.write_chunk(b"b").await.unwrap();
        assert!(staging.memory.is_none());
        assert!(staging.permit.is_none());

        let temporary = staging.temporary.as_ref().unwrap();
        staging.output.as_mut().unwrap().flush().await.unwrap();
        let stored = std::fs::read(temporary.path()).unwrap();
        assert_eq!(stored.len(), LFS_UPLOAD_MEMORY_THRESHOLD + 1);
        assert!(
            stored[..LFS_UPLOAD_MEMORY_THRESHOLD]
                .iter()
                .all(|byte| *byte == b'a')
        );
        assert_eq!(stored[LFS_UPLOAD_MEMORY_THRESHOLD], b'b');
    }

    #[tokio::test]
    async fn unavailable_memory_admission_uses_tempfile_staging() {
        let staging = LfsUploadStaging::with_permit(None, None).await.unwrap();
        assert!(staging.memory.is_none());
        assert!(staging.temporary.as_ref().unwrap().path().exists());
    }

    #[tokio::test]
    async fn dropping_cancelled_file_backed_upload_removes_partial_staging_file() {
        let mut staging = LfsUploadStaging::with_permit(None, None).await.unwrap();
        staging.write_chunk(b"partial upload").await.unwrap();
        let temporary_path = staging.temporary.as_ref().unwrap().path().to_owned();
        assert!(temporary_path.exists());

        // The request future owns this staging guard. Dropping it after client
        // cancellation must discard the incomplete body rather than leave a
        // scratch file that could be mistaken for a published object.
        drop(staging);
        assert!(!temporary_path.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_staging_uses_store_parent_without_entering_object_root() {
        let parent = tempfile::tempdir().unwrap();
        let local_root = parent.path().join("objects");
        std::fs::create_dir(&local_root).unwrap();
        let mut staging = LfsUploadStaging::with_permit(None, Some(&local_root))
            .await
            .unwrap();
        staging.write_chunk(b"local LFS payload").await.unwrap();

        let temporary_path = staging.temporary.as_ref().unwrap().path();
        assert_eq!(temporary_path.parent(), Some(parent.path()));
        assert_ne!(temporary_path.parent(), Some(local_root.as_path()));
    }
}
