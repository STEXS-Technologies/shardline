use super::{LfsRepository, LfsWriteRepository};
use crate::{
    LFS_CONTENT_TYPE, ServerError,
    app::{AppState, protocol_routes::parse_query_map},
    protocol_support::scope_namespace,
};
use axum::{
    Json,
    extract::{Path, State, rejection::JsonRejection},
    http::{StatusCode, Uri, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};
use shardline_index::{CreateLfsLockOutcome, LfsLockRecord, UnlockLfsOutcome};
use shardline_protocol::RepositoryScope;
use shardline_server_core::AuthorizedRepository;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct AnonymousLockNamespace(pub(super) String);

const DEFAULT_LIMIT: u32 = 100;
const MAX_LIMIT: u32 = 1000;

#[derive(Deserialize)]
pub(crate) struct LockRef {
    name: String,
}

#[derive(Deserialize)]
pub(crate) struct CreateRequest {
    path: String,
    #[serde(rename = "ref")]
    reference: Option<LockRef>,
}

#[derive(Default, Deserialize)]
pub(crate) struct VerifyRequest {
    cursor: Option<String>,
    limit: Option<u32>,
    #[serde(rename = "ref")]
    reference: Option<LockRef>,
}

#[derive(Default, Deserialize)]
pub(crate) struct UnlockRequest {
    #[serde(default)]
    force: bool,
    #[serde(rename = "ref")]
    reference: Option<LockRef>,
}

fn response(status: StatusCode, body: Value) -> Response {
    (status, [(CONTENT_TYPE, LFS_CONTENT_TYPE)], Json(body)).into_response()
}

fn lock_request<T>(request: Result<Json<T>, JsonRejection>) -> Result<T, Box<Response>> {
    request.map(|Json(value)| value).map_err(|rejection| {
        Box::new(response(
            rejection.status(),
            json!({"message":"invalid lock request body"}),
        ))
    })
}

fn identity(auth: &AuthorizedRepository) -> Result<(String, String), ServerError> {
    auth.claims().map_or_else(
        || Ok(("anonymous".to_owned(), "anonymous".to_owned())),
        |claims| {
            Ok((
                serde_json::to_string(&(claims.issuer(), claims.subject()))?,
                claims.subject().to_owned(),
            ))
        },
    )
}

fn lock_namespace(
    auth: &AuthorizedRepository,
    anonymous: Option<&axum::Extension<AnonymousLockNamespace>>,
) -> Result<String, ServerError> {
    if auth.claims().is_none()
        && let Some(namespace) = anonymous
    {
        return Ok(namespace.0.0.clone());
    }
    let repository = auth
        .repository()
        .map(|scope| RepositoryScope::new(scope.provider(), scope.owner(), scope.name(), None))
        .transpose()
        .map_err(|_invalid_scope| ServerError::InsufficientScope)?;
    Ok(scope_namespace(repository.as_ref()))
}

fn check_reference(auth: &AuthorizedRepository, reference: Option<&str>) -> bool {
    auth.repository()
        .and_then(|repo| repo.revision())
        .is_none_or(|expected| {
            // Git LFS makes ref optional. Without one, the verified token's
            // revision is the authorization context; explicit refs must agree.
            reference.is_none_or(|actual| {
                actual.strip_prefix("refs/heads/").unwrap_or(actual)
                    == expected.strip_prefix("refs/heads/").unwrap_or(expected)
            })
        })
}

fn lock_json(lock: &LfsLockRecord) -> Value {
    json!({"id":lock.id,"path":lock.path,"locked_at":lock.locked_at,"owner":{"name":lock.owner_name}})
}

fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 4096
        && !path.starts_with('/')
        && !path.contains('\0')
        && !path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
}

fn page_limit(limit: Option<u32>) -> Option<u32> {
    let limit = limit.unwrap_or(DEFAULT_LIMIT);
    (limit > 0 && limit <= MAX_LIMIT).then_some(limit)
}

pub(crate) async fn lfs_create_lock(
    repo: LfsWriteRepository,
    State(state): State<Arc<AppState>>,
    anonymous: Option<axum::Extension<AnonymousLockNamespace>>,
    request: Result<Json<CreateRequest>, JsonRejection>,
) -> Result<Response, ServerError> {
    let request = match lock_request(request) {
        Ok(request) => request,
        Err(response) => return Ok(*response),
    };
    let auth = repo.capability();
    if !check_reference(
        auth,
        request
            .reference
            .as_ref()
            .map(|reference| reference.name.as_str()),
    ) {
        return Ok(response(
            StatusCode::FORBIDDEN,
            json!({"message":"revision scope mismatch"}),
        ));
    }
    if !valid_path(&request.path) {
        return Ok(response(
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"message":"invalid repository-relative lock path"}),
        ));
    }
    let (owner_key, owner_name) = identity(auth)?;
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random).map_err(|error| std::io::Error::other(error.to_string()))?;
    let lock = LfsLockRecord {
        id: hex::encode(random),
        path: request.path,
        owner_key,
        owner_name,
        locked_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    };
    Ok(
        match state
            .backend
            .create_lfs_lock(&lock_namespace(auth, anonymous.as_ref())?, &lock)
            .await?
        {
            CreateLfsLockOutcome::Created(lock) => {
                response(StatusCode::CREATED, json!({"lock":lock_json(&lock)}))
            }
            CreateLfsLockOutcome::Conflict(lock) => response(
                StatusCode::CONFLICT,
                json!({"message":"path is already locked","lock":lock_json(&lock)}),
            ),
        },
    )
}

pub(crate) async fn lfs_list_locks(
    repo: LfsRepository,
    State(state): State<Arc<AppState>>,
    anonymous: Option<axum::Extension<AnonymousLockNamespace>>,
    uri: Uri,
) -> Result<Response, ServerError> {
    let query = match parse_query_map(&uri) {
        Ok(query) => query,
        Err(ServerError::RequestQueryTooLarge) => {
            return Ok(response(
                StatusCode::URI_TOO_LONG,
                json!({"message":"lock query is too large"}),
            ));
        }
        Err(error) => return Err(error),
    };
    // Do not let a map silently select the last of conflicting filters or
    // authorization references. The shared parser bounds bytes first.
    if uri
        .query()
        .is_some_and(|raw| url::form_urlencoded::parse(raw.as_bytes()).count() != query.len())
    {
        return Ok(response(
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"message":"repeated lock query parameter"}),
        ));
    }
    let auth = repo.capability();
    if !check_reference(auth, query.get("refspec").map(String::as_str)) {
        return Ok(response(
            StatusCode::FORBIDDEN,
            json!({"message":"revision scope mismatch"}),
        ));
    }
    let limit = match query
        .get("limit")
        .map(|value| value.parse::<u32>())
        .transpose()
        .ok()
        .and_then(page_limit)
    {
        Some(limit) => limit,
        None => {
            return Ok(response(
                StatusCode::UNPROCESSABLE_ENTITY,
                json!({"message":"invalid lock page limit"}),
            ));
        }
    };
    let mut locks = state
        .backend
        .list_lfs_locks(
            &lock_namespace(auth, anonymous.as_ref())?,
            query.get("path").map(String::as_str),
            query.get("id").map(String::as_str),
            query.get("cursor").map(String::as_str),
            limit.saturating_add(1),
        )
        .await?;
    let has_more = locks.len() > limit as usize;
    locks.truncate(limit as usize);
    let mut body = json!({"locks":locks.iter().map(lock_json).collect::<Vec<_>>()});
    if has_more
        && let Some(last) = locks.last()
        && let Some(fields) = body.as_object_mut()
    {
        fields.insert("next_cursor".to_owned(), json!(last.id));
    }
    Ok(response(StatusCode::OK, body))
}

pub(crate) async fn lfs_verify_locks(
    repo: LfsWriteRepository,
    State(state): State<Arc<AppState>>,
    anonymous: Option<axum::Extension<AnonymousLockNamespace>>,
    request: Result<Json<VerifyRequest>, JsonRejection>,
) -> Result<Response, ServerError> {
    let request = match lock_request(request) {
        Ok(request) => request,
        Err(response) => return Ok(*response),
    };
    let auth = repo.capability();
    if !check_reference(
        auth,
        request
            .reference
            .as_ref()
            .map(|reference| reference.name.as_str()),
    ) {
        return Ok(response(
            StatusCode::FORBIDDEN,
            json!({"message":"revision scope mismatch"}),
        ));
    }
    let Some(limit) = page_limit(request.limit) else {
        return Ok(response(
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"message":"invalid lock page limit"}),
        ));
    };
    let (owner, _) = identity(auth)?;
    let mut locks = state
        .backend
        .list_lfs_locks(
            &lock_namespace(auth, anonymous.as_ref())?,
            None,
            None,
            request.cursor.as_deref(),
            limit.saturating_add(1),
        )
        .await?;
    let has_more = locks.len() > limit as usize;
    locks.truncate(limit as usize);
    let (ours, theirs): (Vec<_>, Vec<_>) = locks.iter().partition(|lock| lock.owner_key == owner);
    let mut body = json!({"ours":ours.into_iter().map(lock_json).collect::<Vec<_>>(),"theirs":theirs.into_iter().map(lock_json).collect::<Vec<_>>()});
    if has_more
        && let Some(last) = locks.last()
        && let Some(fields) = body.as_object_mut()
    {
        fields.insert("next_cursor".to_owned(), json!(last.id));
    }
    Ok(response(StatusCode::OK, body))
}

pub(crate) async fn lfs_unlock(
    repo: LfsWriteRepository,
    State(state): State<Arc<AppState>>,
    anonymous: Option<axum::Extension<AnonymousLockNamespace>>,
    Path(params): Path<std::collections::HashMap<String, String>>,
    request: Result<Json<UnlockRequest>, JsonRejection>,
) -> Result<Response, ServerError> {
    let request = match lock_request(request) {
        Ok(request) => request,
        Err(response) => return Ok(*response),
    };
    let id = params.get("id").ok_or(ServerError::InsufficientScope)?;
    let auth = repo.capability();
    if !check_reference(
        auth,
        request
            .reference
            .as_ref()
            .map(|reference| reference.name.as_str()),
    ) {
        return Ok(response(
            StatusCode::FORBIDDEN,
            json!({"message":"revision scope mismatch"}),
        ));
    }
    let (owner, _) = identity(auth)?;
    Ok(
        match state
            .backend
            .unlock_lfs_lock(
                &lock_namespace(auth, anonymous.as_ref())?,
                id,
                &owner,
                request.force,
            )
            .await?
        {
            UnlockLfsOutcome::Removed(lock) => {
                response(StatusCode::OK, json!({"lock":lock_json(&lock)}))
            }
            UnlockLfsOutcome::Missing => {
                response(StatusCode::NOT_FOUND, json!({"message":"lock not found"}))
            }
            UnlockLfsOutcome::Forbidden => response(
                StatusCode::FORBIDDEN,
                json!({"message":"lock belongs to another user"}),
            ),
        },
    )
}
