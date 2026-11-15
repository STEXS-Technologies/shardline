//! Authorization for protocols whose URLs do not carry a repository path.
//!
//! The verified token supplies the repository namespace. Keep read and write
//! extractors distinct, and borrow headers so handlers can still parse them.
use std::sync::Arc;

use axum::{extract::FromRequestParts, http::request::Parts};
use shardline_protocol::TokenScope;
use shardline_server_core::AuthorizedRepository;

use super::{AppState, authorize_repository};
use crate::ServerError;

#[derive(Debug)]
pub struct ReadRepository {
    auth: AuthorizedRepository,
}

#[derive(Debug)]
pub struct WriteRepository {
    auth: AuthorizedRepository,
}

impl ReadRepository {
    pub(crate) const fn capability(&self) -> &AuthorizedRepository {
        &self.auth
    }
}

impl WriteRepository {
    pub(crate) const fn capability(&self) -> &AuthorizedRepository {
        &self.auth
    }
}

impl FromRequestParts<Arc<AppState>> for ReadRepository {
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        Ok(Self {
            auth: authorize_repository(state, &parts.headers, TokenScope::Read)?,
        })
    }
}

impl FromRequestParts<Arc<AppState>> for WriteRepository {
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        Ok(Self {
            auth: authorize_repository(state, &parts.headers, TokenScope::Write)?,
        })
    }
}
