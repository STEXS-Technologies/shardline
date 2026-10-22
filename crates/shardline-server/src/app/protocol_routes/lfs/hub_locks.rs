//! Hub repository URLs use the same durable LFS lock handlers as standalone LFS.
use std::sync::Arc;

use axum::{
    Router,
    extract::{RawPathParams, State},
    http::Request,
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
};

use super::{authorize_repository, locks};
use crate::{ServerError, app::AppState};
use shardline_protocol::TokenScope;

pub(crate) fn register_hub_lock_routes(
    app: Router<Arc<AppState>>,
    state: Arc<AppState>,
) -> Router<Arc<AppState>> {
    let mut routes = Router::new();
    for prefix in ["/{ns}/{repo}/info/lfs", "/{repo_type}/{ns}/{repo}/info/lfs"] {
        routes = routes
            .route(
                &format!("{prefix}/locks"),
                get(locks::lfs_list_locks).post(locks::lfs_create_lock),
            )
            .route(
                &format!("{prefix}/locks/verify"),
                post(locks::lfs_verify_locks),
            )
            .route(
                &format!("{prefix}/locks/{{id}}/unlock"),
                post(locks::lfs_unlock),
            );
    }
    app.merge(routes.route_layer(middleware::from_fn_with_state(state, bind_repository)))
}

async fn bind_repository(
    State(state): State<Arc<AppState>>,
    params: RawPathParams,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ServerError> {
    let values: std::collections::HashMap<_, _> = params.iter().collect();
    if values
        .get("repo_type")
        .is_some_and(|kind| !matches!(*kind, "datasets" | "spaces"))
    {
        return Err(ServerError::InsufficientScope);
    }
    let owner = values.get("ns").ok_or(ServerError::InsufficientScope)?;
    let name = values.get("repo").ok_or(ServerError::InsufficientScope)?;
    let name = name.strip_suffix(".git").unwrap_or(name);
    let required = if request.method() == axum::http::Method::GET {
        TokenScope::Read
    } else {
        TokenScope::Write
    };
    let auth = authorize_repository(&state, request.headers(), required)?;
    if let Some(repository) = auth.repository() {
        if repository.owner() != *owner || repository.name() != name {
            return Err(ServerError::InsufficientScope);
        }
    } else {
        // Permissive deployments still isolate different URL repositories.
        request
            .extensions_mut()
            .insert(locks::AnonymousLockNamespace(format!(
                "hub-lfs:{}",
                serde_json::to_string(&(
                    values.get("repo_type").copied().unwrap_or("models"),
                    owner,
                    name
                ))?
            )));
    }
    Ok(next.run(request).await)
}
