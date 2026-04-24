use std::{
    error::Error as StdError,
    fmt::Write,
    fs::write as write_file,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    time::Duration,
};

use axum::{
    body::Bytes,
    http::{
        HeaderMap,
        header::{AUTHORIZATION, HeaderValue},
    },
};
use futures_util::stream;
use hmac::Mac;
use reqwest::Client;
use serde_json::{json, to_vec};
use shardline_index::{IndexStore, LocalIndexStore, ProviderRepositoryState};
use shardline_protocol::{
    RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, TokenSigner,
};
use tokio::time::sleep;

use super::{
    AppState, MAX_BATCH_RECONSTRUCTION_FILE_IDS, MAX_BATCH_RECONSTRUCTION_QUERY_BYTES,
    MAX_PROVIDER_BASIC_AUTH_HEADER_BYTES, MAX_PROVIDER_NAME_BYTES, MAX_PROVIDER_SUBJECT_BYTES,
    MAX_PROVIDER_TOKEN_REQUEST_BODY_BYTES, MAX_PROVIDER_WEBHOOK_BODY_BYTES,
    acquire_chunk_transfer_permit, bounded_api_body_limit, extract_provider_subject,
    full_byte_stream_response, latest_lifecycle_signal_at, parse_batch_reconstruction_query,
    reconciled_provider_repository_state, serve_with_listener, validate_provider_name_path,
};
use crate::{
    ServerError,
    backend::{
        clear_repository_reference_probe_filter, lock_repository_reference_probe_test,
        repository_reference_probe_count, reset_repository_reference_probe_count_for_hash,
    },
    download_stream::{STREAM_READ_BUFFER_BYTES, ServerByteStream},
    local_backend::chunk_hash,
    test_fixtures::{single_chunk_xorb, single_file_shard},
    test_invariant_error::ServerTestInvariantError,
};

#[test]
fn provider_subject_extraction_rejects_oversized_query_subject() {
    let oversized = "s".repeat(MAX_PROVIDER_SUBJECT_BYTES + 1);
    let result = extract_provider_subject(&HeaderMap::new(), Some(&oversized));

    assert!(matches!(
        result,
        Err(ServerError::InvalidProviderTokenRequest)
    ));
}

#[test]
fn provider_subject_extraction_rejects_oversized_basic_auth_header_before_decode() {
    let oversized = "a".repeat(MAX_PROVIDER_BASIC_AUTH_HEADER_BYTES + 1);
    let header_value = HeaderValue::from_str(&format!("Basic {oversized}"));
    assert!(header_value.is_ok());
    let Ok(header_value) = header_value else {
        return;
    };
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, header_value);

    let result = extract_provider_subject(&headers, None);

    assert!(matches!(
        result,
        Err(ServerError::InvalidAuthorizationHeader)
    ));
}

#[test]
fn provider_api_body_limit_uses_stricter_configured_or_endpoint_ceiling() {
    let tighter = NonZeroUsize::new(32).unwrap_or(NonZeroUsize::MIN);
    let looser =
        NonZeroUsize::new(MAX_PROVIDER_WEBHOOK_BODY_BYTES + 1).unwrap_or(NonZeroUsize::MIN);

    assert_eq!(
        bounded_api_body_limit(tighter, MAX_PROVIDER_WEBHOOK_BODY_BYTES),
        tighter.get()
    );
    assert_eq!(
        bounded_api_body_limit(looser, MAX_PROVIDER_WEBHOOK_BODY_BYTES),
        MAX_PROVIDER_WEBHOOK_BODY_BYTES
    );
}

#[test]
fn provider_repository_reconciliation_marks_pending_lifecycle_signals() {
    let state = ProviderRepositoryState::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        Some(10),
        Some(12),
        Some("refs/heads/main".to_owned()),
    )
    .with_reconciliation(Some(11), None, None);

    assert_eq!(latest_lifecycle_signal_at(&state), Some(12));
    let reconciled = reconciled_provider_repository_state(&state, 20);

    assert_eq!(
        reconciled.last_cache_invalidated_at_unix_seconds(),
        Some(20)
    );
    assert_eq!(
        reconciled.last_authorization_rechecked_at_unix_seconds(),
        Some(20)
    );
    assert_eq!(reconciled.last_drift_checked_at_unix_seconds(), Some(20));
}

fn test_byte_stream(items: Vec<Result<Bytes, ServerError>>) -> ServerByteStream {
    Box::pin(stream::iter(items))
}

#[test]
fn batch_reconstruction_parser_deduplicates_file_ids() {
    let parsed = parse_batch_reconstruction_query(
        "file_id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&file_id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&ignored=value",
    );

    assert!(parsed.is_ok());
    let Ok(parsed) = parsed else {
        return;
    };
    assert_eq!(
        parsed,
        vec!["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()]
    );
}

#[test]
fn batch_reconstruction_parser_rejects_excessive_file_ids() {
    let mut query = String::new();
    for index in 0..=MAX_BATCH_RECONSTRUCTION_FILE_IDS {
        if !query.is_empty() {
            query.push('&');
        }
        query.push_str("file_id=");
        let written = write!(&mut query, "{index:064x}");
        assert!(written.is_ok());
    }

    let parsed = parse_batch_reconstruction_query(&query);

    assert!(matches!(
        parsed,
        Err(ServerError::TooManyBatchReconstructionFileIds)
    ));
}

#[test]
fn batch_reconstruction_parser_rejects_oversized_query_before_scanning() {
    let mut query = String::from("ignored=");
    query.push_str(&"a".repeat(MAX_BATCH_RECONSTRUCTION_QUERY_BYTES + 1));

    let parsed = parse_batch_reconstruction_query(&query);

    assert!(matches!(parsed, Err(ServerError::RequestQueryTooLarge)));
}

#[test]
fn provider_path_name_rejects_empty_or_oversized_values() {
    let empty = validate_provider_name_path("");
    let oversized = validate_provider_name_path(&"p".repeat(MAX_PROVIDER_NAME_BYTES + 1));
    let valid = validate_provider_name_path("github");

    assert!(matches!(
        empty,
        Err(ServerError::InvalidProviderTokenRequest)
    ));
    assert!(matches!(
        oversized,
        Err(ServerError::InvalidProviderTokenRequest)
    ));
    assert!(valid.is_ok());
}

mod transfer_http;

mod provider_http;

mod protocol_frontends_http;

fn write_provider_config(root: &Path) -> Result<PathBuf, Box<dyn StdError>> {
    let path = root.join("providers.json");
    let bytes = to_vec(&json!({
        "providers": [
            {
                "kind": "github",
                "integration_subject": "github-app",
                "webhook_secret": "secret",
                "repositories": [
                    {
                        "owner": "team",
                        "name": "assets",
                        "visibility": "private",
                        "default_revision": "main",
                        "clone_url": "https://github.example/team/assets.git",
                        "read_subjects": ["github-user-1"],
                        "write_subjects": ["github-user-1"]
                    }
                ]
            }
        ]
    }))?;
    write_file(&path, bytes)?;
    Ok(path)
}

fn write_gitlab_provider_config(root: &Path) -> Result<PathBuf, Box<dyn StdError>> {
    let path = root.join("providers-gitlab.json");
    let bytes = to_vec(&json!({
        "providers": [
            {
                "kind": "gitlab",
                "integration_subject": "gitlab-app",
                "webhook_secret": "secret",
                "repositories": [
                    {
                        "owner": "group",
                        "name": "assets",
                        "visibility": "private",
                        "default_revision": "main",
                        "clone_url": "https://gitlab.example/group/assets.git",
                        "read_subjects": ["gitlab-user-1"],
                        "write_subjects": ["gitlab-user-1"]
                    }
                ]
            }
        ]
    }))?;
    write_file(&path, bytes)?;
    Ok(path)
}

fn write_gitea_provider_config(root: &Path) -> Result<PathBuf, Box<dyn StdError>> {
    let path = root.join("providers-gitea.json");
    let bytes = to_vec(&json!({
        "providers": [
            {
                "kind": "gitea",
                "integration_subject": "gitea-app",
                "webhook_secret": "secret",
                "repositories": [
                    {
                        "owner": "team",
                        "name": "assets",
                        "visibility": "private",
                        "default_revision": "main",
                        "clone_url": "https://gitea.example/team/assets.git",
                        "read_subjects": ["gitea-user-1"],
                        "write_subjects": ["gitea-user-1"]
                    }
                ]
            }
        ]
    }))?;
    write_file(&path, bytes)?;
    Ok(path)
}

fn write_generic_provider_config(root: &Path) -> Result<PathBuf, Box<dyn StdError>> {
    let path = root.join("providers-generic.json");
    let bytes = to_vec(&json!({
        "providers": [
            {
                "kind": "generic",
                "integration_subject": "generic-bridge",
                "webhook_secret": "secret",
                "repositories": [
                    {
                        "owner": "team",
                        "name": "assets",
                        "visibility": "private",
                        "default_revision": "main",
                        "clone_url": "https://forge.example/team/assets.git",
                        "read_subjects": ["generic-user-1"],
                        "write_subjects": ["generic-user-1"]
                    }
                ]
            }
        ]
    }))?;
    write_file(&path, bytes)?;
    Ok(path)
}

fn seed_provider_repository_state(
    root: &Path,
    provider: RepositoryProvider,
    owner: &str,
    repo: &str,
) {
    let index_store = LocalIndexStore::new(root.to_path_buf());
    assert!(index_store.is_ok());
    let Ok(index_store) = index_store else {
        return;
    };
    let state = ProviderRepositoryState::new(
        provider,
        owner.to_owned(),
        repo.to_owned(),
        Some(7),
        Some(11),
        Some("refs/heads/seeded".to_owned()),
    );
    let persisted = IndexStore::upsert_provider_repository_state(&index_store, &state);
    assert!(persisted.is_ok());
}

fn assert_provider_repository_state_absent(
    root: &Path,
    provider: RepositoryProvider,
    owner: &str,
    repo: &str,
) {
    let state = provider_repository_state(root, provider, owner, repo);
    assert!(state.is_none());
}

fn assert_provider_repository_state_migrated(
    root: &Path,
    provider: RepositoryProvider,
    old_owner: &str,
    old_repo: &str,
    new_owner: &str,
    new_repo: &str,
) {
    assert_provider_repository_state_absent(root, provider, old_owner, old_repo);
    let state = provider_repository_state(root, provider, new_owner, new_repo);
    assert!(state.is_some());
    let Some(state) = state else {
        return;
    };
    assert_eq!(state.provider(), provider);
    assert_eq!(state.owner(), new_owner);
    assert_eq!(state.repo(), new_repo);
    assert_eq!(state.last_access_changed_at_unix_seconds(), Some(7));
    assert_eq!(state.last_revision_pushed_at_unix_seconds(), Some(11));
    assert_eq!(state.last_pushed_revision(), Some("refs/heads/seeded"));
}

fn assert_provider_repository_state_observed(
    root: &Path,
    provider: RepositoryProvider,
    owner: &str,
    repo: &str,
    access_changed: bool,
    revision: Option<&str>,
) {
    let state = provider_repository_state(root, provider, owner, repo);
    assert!(state.is_some());
    let Some(state) = state else {
        return;
    };
    assert_eq!(state.provider(), provider);
    assert_eq!(state.owner(), owner);
    assert_eq!(state.repo(), repo);
    assert_eq!(
        state.last_access_changed_at_unix_seconds().is_some(),
        access_changed
    );
    assert_eq!(
        state.last_revision_pushed_at_unix_seconds().is_some(),
        revision.is_some()
    );
    assert_eq!(state.last_pushed_revision(), revision);
}

fn provider_repository_state(
    root: &Path,
    provider: RepositoryProvider,
    owner: &str,
    repo: &str,
) -> Option<ProviderRepositoryState> {
    let index_store = LocalIndexStore::new(root.to_path_buf());
    assert!(index_store.is_ok());
    let Ok(index_store) = index_store else {
        return None;
    };
    let state = IndexStore::provider_repository_state(&index_store, provider, owner, repo);
    assert!(state.is_ok());
    let Ok(state) = state else {
        return None;
    };
    state
}

fn github_webhook_signature(body: &[u8]) -> Option<String> {
    let mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret");
    assert!(mac.is_ok());
    let Ok(mut mac) = mac else {
        return None;
    };
    mac.update(body);
    Some(format!(
        "sha256={}",
        hex::encode(mac.finalize().into_bytes())
    ))
}

fn gitea_webhook_signature(body: &[u8]) -> Option<String> {
    let mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret");
    assert!(mac.is_ok());
    let Ok(mut mac) = mac else {
        return None;
    };
    mac.update(body);
    Some(hex::encode(mac.finalize().into_bytes()))
}

fn generic_webhook_signature(body: &[u8]) -> Option<String> {
    let mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret");
    assert!(mac.is_ok());
    let Ok(mut mac) = mac else {
        return None;
    };
    mac.update(body);
    Some(format!(
        "sha256={}",
        hex::encode(mac.finalize().into_bytes())
    ))
}

fn bearer_token(
    subject: &str,
    scope: TokenScope,
    provider: RepositoryProvider,
    owner: &str,
    repo: &str,
    revision: Option<&str>,
) -> Result<String, Box<dyn StdError>> {
    let signer = TokenSigner::new(b"signing-key")?;
    let repository = RepositoryScope::new(provider, owner, repo, revision)?;
    let claims = TokenClaims::new("local", subject, scope, repository, u64::MAX)?;
    Ok(signer.sign(&claims)?)
}

async fn wait_for_health(base_url: &str) -> Result<(), Box<dyn StdError>> {
    let client = Client::new();
    for _attempt in 0..50 {
        let response = client.get(format!("{base_url}/healthz")).send().await;
        if let Ok(response) = response
            && response.status().is_success()
        {
            return Ok(());
        }
        sleep(Duration::from_millis(20)).await;
    }

    Err(ServerTestInvariantError::new("server did not become healthy").into())
}
