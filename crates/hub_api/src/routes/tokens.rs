use axum::http::HeaderMap;
use axum::{
    Json,
    extract::{Path, State},
};

use crate::{error::HubApiError, models::TokenExchangeResponse};
use shardline_protocol::TokenScope;

use super::HubState;

// ---- Token exchange (requires Read scope) ----

pub(crate) async fn xet_read_token(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path((_repo_type, _ns, _repo, _rev)): Path<(String, String, String, String)>,
) -> Result<Json<TokenExchangeResponse>, HubApiError> {
    let ctx = if let Some(auth) = &state.auth {
        auth.authorize(&headers, TokenScope::Read)?
    } else {
        return Err(HubApiError::Unauthorized);
    };
    let token = state
        .auth
        .as_ref()
        .ok_or(HubApiError::Unauthorized)?
        .provider()
        .mint_token(ctx.claims())
        .map_err(|e| {
            tracing::debug!("failed to mint token: {e}");
            HubApiError::InvalidToken
        })?;
    shardline_metrics::record_hub_api_request("xet_read_token", "GET", 200);
    Ok(Json(TokenExchangeResponse { token }))
}

pub(crate) async fn xet_write_token(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path((_repo_type, _ns, _repo, _rev)): Path<(String, String, String, String)>,
) -> Result<Json<TokenExchangeResponse>, HubApiError> {
    let ctx = if let Some(auth) = &state.auth {
        auth.authorize(&headers, TokenScope::Write)?
    } else {
        return Err(HubApiError::Unauthorized);
    };
    let token = state
        .auth
        .as_ref()
        .ok_or(HubApiError::Unauthorized)?
        .provider()
        .mint_token(ctx.claims())
        .map_err(|e| {
            tracing::debug!("failed to mint token: {e}");
            HubApiError::InvalidToken
        })?;
    shardline_metrics::record_hub_api_request("xet_write_token", "GET", 200);
    Ok(Json(TokenExchangeResponse { token }))
}
