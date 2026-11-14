//! Convert verified external identity and resource claims through one policy.
use crate::{ServerConfig, ServerConfigError};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::AuthError;

#[derive(Clone, Default)]
pub(crate) struct ExternalJwtClaims {
    repository_claim: Option<String>,
    scope_claim: Option<String>,
}

impl ExternalJwtClaims {
    pub(crate) fn from_config(config: &ServerConfig) -> Result<Self, ServerConfigError> {
        let repository_claim = config.auth_jwt_repository_claim();
        let scope_claim = config.auth_jwt_scope_claim();
        if [repository_claim, scope_claim]
            .into_iter()
            .flatten()
            .any(|name| name.trim().is_empty())
        {
            return Err(ServerConfigError::ConfigFileError(
                "external JWT claim names must not be empty".into(),
            ));
        }
        Ok(Self {
            repository_claim: repository_claim.map(str::to_owned),
            scope_claim: scope_claim.map(str::to_owned),
        })
    }

    pub(crate) fn map(
        &self,
        payload: &serde_json::Value,
        issuer: &str,
        legacy_owner: &str,
    ) -> Result<TokenClaims, AuthError> {
        let now = shardline_protocol::unix_now_seconds_lossy();
        for claim in ["iat", "nbf"] {
            if payload
                .get(claim)
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|value| value > now)
            {
                return Err(AuthError::InvalidToken);
            }
        }
        let exp = payload
            .get("exp")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| AuthError::ProviderError("missing exp claim".into()))?;
        if exp < now {
            return Err(AuthError::ExpiredToken);
        }
        let subject = payload
            .get("sub")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or(AuthError::InvalidToken)?;
        let scope = payload
            .get(self.scope_claim.as_deref().unwrap_or("scope"))
            .and_then(serde_json::Value::as_str);
        let scope = match scope {
            Some("write" | "admin") => TokenScope::Write,
            Some("read") => TokenScope::Read,
            _ if self.repository_claim.is_some() || self.scope_claim.is_some() => {
                return Err(AuthError::InvalidToken);
            }
            _ => TokenScope::Read,
        };
        let repository = if let Some(claim) = &self.repository_claim {
            serde_json::from_value::<RepositoryScope>(
                payload.get(claim).cloned().ok_or(AuthError::InvalidToken)?,
            )
            .map_err(|_error| AuthError::InvalidToken)?
        } else {
            RepositoryScope::new(
                RepositoryProvider::Generic,
                legacy_owner,
                subject,
                Some("main"),
            )
            .map_err(|error| AuthError::ProviderError(error.to_string()))?
        };
        TokenClaims::new(issuer, subject, scope, repository, exp)
            .map_err(|error| AuthError::ProviderError(error.to_string()))
    }
}
