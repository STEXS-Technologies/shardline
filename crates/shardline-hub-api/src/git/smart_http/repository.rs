//! Resolve the conventional Git URL suffix before binding repository access.
use crate::{error::HubApiError, routes::HubState};
use shardline_server_core::VerifiedAuthContext;

pub(super) async fn canonical_name(
    state: &HubState,
    context: Option<&VerifiedAuthContext>,
    namespace: &str,
    name: &str,
) -> Result<String, HubApiError> {
    canonical_name_for_claims(
        state,
        context.map(VerifiedAuthContext::claims),
        namespace,
        name,
    )
    .await
}

pub(crate) async fn canonical_name_for_claims(
    state: &HubState,
    claims: Option<&shardline_protocol::TokenClaims>,
    namespace: &str,
    name: &str,
) -> Result<String, HubApiError> {
    let Some(without_suffix) = name.strip_suffix(".git") else {
        return Ok(name.to_owned());
    };
    if let Some(claims) = claims {
        // A credential for a literal `assets.git` repository retains its exact
        // meaning. Other credentials still pass the ordinary binding check on
        // the canonical name; suffix handling never grants additional scope.
        return Ok(if claims.repository().name() == name {
            name.to_owned()
        } else {
            without_suffix.to_owned()
        });
    }
    // Preserve existing literal names in permissive deployments too. Only an
    // absent exact repository allows the conventional suffix alias.
    let store = state.store.clone();
    let exact = format!("{namespace}/{name}");
    let exists = tokio::task::spawn_blocking(move || store.get_repo(&exact))
        .await
        .map_err(|error| HubApiError::CasError(error.to_string()))?
        .map_err(|error| HubApiError::CasError(error.to_string()))?
        .is_some();
    Ok(if exists { name } else { without_suffix }.to_owned())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
    use shardline_server_core::{AuthProvider, LocalHmacProvider};

    #[tokio::test]
    async fn suffix_aliases_preserve_literal_names_and_repository_binding() {
        let (_root, state) = super::super::tests::make_hub_state();
        for name in ["assets", "assets.git"] {
            state
                .store
                .create_repo(
                    shardline_index::hub::HubRepoType::Model,
                    &format!("alice/{name}"),
                    false,
                )
                .unwrap();
        }
        let provider = LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap();
        for (authorized, path, expected) in [
            ("assets", "assets.git", "assets"),
            ("assets.git", "assets.git", "assets.git"),
            ("assets.git", "assets.git.git", "assets.git"),
        ] {
            let claims = TokenClaims::new(
                "local",
                "writer",
                TokenScope::Write,
                RepositoryScope::new(RepositoryProvider::Generic, "alice", authorized, None)
                    .unwrap(),
                u64::MAX,
            )
            .unwrap();
            let credential = provider.mint_token(&claims).unwrap();
            let context = provider.verify_verified(&credential).unwrap();
            let resolved = canonical_name(&state, Some(&context), "alice", path)
                .await
                .unwrap();
            assert_eq!(resolved, expected);
            assert!(
                crate::routes::require_repository_binding(Some(&context), "alice", &resolved)
                    .is_ok()
            );
            let unrelated = canonical_name(&state, Some(&context), "alice", "other.git")
                .await
                .unwrap();
            assert!(
                crate::routes::require_repository_binding(Some(&context), "alice", &unrelated)
                    .is_err()
            );
            assert!(
                crate::routes::require_repository_binding(Some(&context), "bob", &resolved)
                    .is_err()
            );
        }
        assert_eq!(
            canonical_name(&state, None, "alice", "assets.git")
                .await
                .unwrap(),
            "assets.git"
        );
        assert_eq!(
            canonical_name(&state, None, "alice", "only.git")
                .await
                .unwrap(),
            "only"
        );
    }
}
