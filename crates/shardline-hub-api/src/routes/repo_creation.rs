//! Common repository creation and conflict handling for both Hub entrypoints.
use super::HubState;
use crate::error::HubApiError;
use axum::{Json, http::StatusCode};
use shardline_index::hub::HubRepoType;
use shardline_server_core::AuthorizedRepository;

/// Type-specific creation has its repository identity in the URL.
#[derive(serde::Deserialize)]
pub(super) struct CreationVisibility {
    #[serde(default)]
    pub private: bool,
    #[serde(default)]
    pub visibility: Option<String>,
}

/// Unsupported visibility must never silently create a public repository.
pub(super) fn private_visibility(
    private: bool,
    visibility: Option<&str>,
) -> Result<bool, HubApiError> {
    match visibility {
        None | Some("public") => Ok(private),
        Some("private") => Ok(true),
        Some(_) => Err(HubApiError::BadRequest(
            "repository visibility must be public or private".to_owned(),
        )),
    }
}

/// Call only after authorizing creation of the exact requested identity.
pub(super) fn create(
    state: &HubState,
    capability: &AuthorizedRepository,
    repo_type: HubRepoType,
    name: &str,
    private: bool,
) -> Result<(StatusCode, Json<serde_json::Value>), HubApiError> {
    let existing = state
        .store
        .get_repo(name)
        .map_err(|error| HubApiError::CasError(error.to_string()))?;
    let existing = if let Some(existing) = existing {
        existing
    } else {
        match state.store.create_repo(repo_type, name, private) {
            Ok(created) => {
                return Ok((
                    StatusCode::CREATED,
                    Json(serde_json::to_value(
                        super::repos::repo_response_for_public_base(
                            &state.public_base_url,
                            &created,
                        ),
                    )?),
                ));
            }
            Err(error) => {
                // Another replica may have won after our initial lookup. The
                // store's insert-only transaction preserves its type/visibility.
                // Re-read before classifying the failure; never suppress an
                // error when no durable repository exists.
                state
                    .store
                    .get_repo(name)
                    .map_err(|read_error| HubApiError::CasError(read_error.to_string()))?
                    .ok_or_else(|| HubApiError::CasError(error.to_string()))?
            }
        }
    };
    let owns_target = capability.claims().is_some_and(|claims| {
        let repository = claims.repository();
        name == format!("{}/{}", repository.owner(), repository.name())
    });
    let response = if owns_target {
        serde_json::to_value(super::repos::repo_response_for_public_base(
            &state.public_base_url,
            &existing,
        ))?
    } else {
        // A creation-only grant is not permission to inspect an existing repo.
        serde_json::json!({"name":name,"message":"repository already exists"})
    };
    Ok((StatusCode::CONFLICT, Json(response)))
}
