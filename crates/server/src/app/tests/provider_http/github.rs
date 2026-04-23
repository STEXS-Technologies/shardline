#![allow(clippy::panic_in_result_fn)]

use std::{error::Error as StdError, num::NonZeroU64};

use reqwest::{Client, StatusCode};
use shardline_protocol::{RepositoryProvider, TokenScope};

use super::super::{
    MAX_PROVIDER_NAME_BYTES, MAX_PROVIDER_SUBJECT_BYTES, MAX_PROVIDER_TOKEN_REQUEST_BODY_BYTES,
    MAX_PROVIDER_WEBHOOK_BODY_BYTES, assert_provider_repository_state_absent,
    assert_provider_repository_state_migrated, assert_provider_repository_state_observed,
    github_webhook_signature, provider_repository_state, seed_provider_repository_state,
};
use super::shared::{
    PROVIDER_BOOTSTRAP_KEY, invariant, retention_hold_count, start_github_provider_runtime,
    upload_repository_asset,
};
use crate::{
    GitLfsAuthenticateResponse, ProviderTokenIssueRequest, ProviderTokenIssueResponse,
    ProviderWebhookResponse, XetCasTokenResponse, test_fixtures::single_chunk_xorb,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_token_endpoint_issues_runtime_tokens_for_configured_provider()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_github_provider_runtime(NonZeroU64::MIN).await?;
    seed_provider_repository_state(
        runtime.storage_path(),
        RepositoryProvider::GitHub,
        "team",
        "assets",
    );

    let response = Client::new()
        .post(format!("{}/v1/providers/github/tokens", runtime.base_url()))
        .header("x-shardline-provider-key", PROVIDER_BOOTSTRAP_KEY)
        .json(&ProviderTokenIssueRequest {
            subject: "github-user-1".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: None,
            scope: TokenScope::Write,
        })
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let issued = response.json::<ProviderTokenIssueResponse>().await?;
    assert_eq!(issued.issuer, "github-app");
    assert_eq!(issued.revision.as_deref(), Some("refs/heads/main"));
    let state = provider_repository_state(
        runtime.storage_path(),
        RepositoryProvider::GitHub,
        "team",
        "assets",
    );
    let state = state.ok_or_else(|| invariant("provider state was not persisted"))?;
    assert!(state.last_cache_invalidated_at_unix_seconds().is_some());
    assert!(
        state
            .last_authorization_rechecked_at_unix_seconds()
            .is_some()
    );
    assert!(state.last_drift_checked_at_unix_seconds().is_some());

    let (xorb_body, xorb_hash) = single_chunk_xorb(b"aaaa");
    let upload = Client::new()
        .post(format!(
            "{}/v1/xorbs/default/{xorb_hash}",
            runtime.base_url()
        ))
        .bearer_auth(&issued.token)
        .body(xorb_body)
        .send()
        .await?;
    assert_eq!(upload.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_token_endpoint_returns_reference_client_token_shape() -> Result<(), Box<dyn StdError>>
{
    let token_ttl = NonZeroU64::new(300).unwrap_or(NonZeroU64::MIN);
    let runtime = start_github_provider_runtime(token_ttl).await?;

    let response = Client::new()
        .get(format!(
            "{}/api/github/team/assets/xet-write-token/main?subject=github-user-1",
            runtime.base_url()
        ))
        .header("x-shardline-provider-key", PROVIDER_BOOTSTRAP_KEY)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let issued = response.json::<XetCasTokenResponse>().await?;
    assert_eq!(issued.cas_url, runtime.base_url());
    assert!(!issued.access_token.is_empty());
    assert!(issued.exp > 0);

    let (xorb_body, xorb_hash) = single_chunk_xorb(b"aaaa");
    let upload = Client::new()
        .post(format!(
            "{}/v1/xorbs/default/{xorb_hash}",
            runtime.base_url()
        ))
        .bearer_auth(&issued.access_token)
        .body(xorb_body)
        .send()
        .await?;
    assert_eq!(upload.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_lfs_authenticate_response_carries_xet_bootstrap_headers()
-> Result<(), Box<dyn StdError>> {
    let token_ttl = NonZeroU64::new(300).unwrap_or(NonZeroU64::MIN);
    let runtime = start_github_provider_runtime(token_ttl).await?;

    let response = Client::new()
        .post(format!(
            "{}/v1/providers/github/git-lfs-authenticate",
            runtime.base_url()
        ))
        .header("x-shardline-provider-key", PROVIDER_BOOTSTRAP_KEY)
        .json(&ProviderTokenIssueRequest {
            subject: "github-user-1".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: Some("main".to_owned()),
            scope: TokenScope::Write,
        })
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let issued = response.json::<GitLfsAuthenticateResponse>().await?;
    assert_eq!(issued.href, runtime.base_url());
    assert_eq!(
        issued.header.get("X-Xet-Cas-Url").map(String::as_str),
        Some(runtime.base_url())
    );
    assert!(
        issued
            .header
            .get("X-Xet-Access-Token")
            .is_some_and(|value| !value.is_empty())
    );
    assert!(
        issued
            .header
            .get("X-Xet-Token-Expiration")
            .is_some_and(|value| value.parse::<u64>().is_ok())
    );
    assert!(issued.expires_in > 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_token_endpoint_denies_unauthorized_subject() -> Result<(), Box<dyn StdError>> {
    let runtime = start_github_provider_runtime(NonZeroU64::MIN).await?;

    let response = Client::new()
        .post(format!("{}/v1/providers/github/tokens", runtime.base_url()))
        .header("x-shardline-provider-key", PROVIDER_BOOTSTRAP_KEY)
        .json(&ProviderTokenIssueRequest {
            subject: "github-user-2".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: None,
            scope: TokenScope::Write,
        })
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_token_endpoint_rejects_oversized_json_before_parsing()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_github_provider_runtime(NonZeroU64::MIN).await?;

    let response = Client::new()
        .post(format!("{}/v1/providers/github/tokens", runtime.base_url()))
        .header("x-shardline-provider-key", PROVIDER_BOOTSTRAP_KEY)
        .header("content-type", "application/json")
        .body(vec![b'{'; MAX_PROVIDER_TOKEN_REQUEST_BODY_BYTES + 1])
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_token_endpoints_authenticate_before_json_parsing() -> Result<(), Box<dyn StdError>>
{
    let runtime = start_github_provider_runtime(NonZeroU64::MIN).await?;

    for path in [
        "/v1/providers/github/tokens",
        "/v1/providers/github/git-lfs-authenticate",
    ] {
        let response = Client::new()
            .post(format!("{}{path}", runtime.base_url()))
            .header("content-type", "application/json")
            .body("{")
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_token_endpoints_authenticate_before_path_and_subject_validation()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_github_provider_runtime(NonZeroU64::MIN).await?;

    let oversized_provider = "a".repeat(MAX_PROVIDER_NAME_BYTES + 1);
    let oversized_provider_response = Client::new()
        .post(format!(
            "{}/v1/providers/{oversized_provider}/tokens",
            runtime.base_url()
        ))
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await?;
    assert_eq!(
        oversized_provider_response.status(),
        StatusCode::UNAUTHORIZED
    );

    let oversized_subject = "a".repeat(MAX_PROVIDER_SUBJECT_BYTES + 1);
    let oversized_subject_response = Client::new()
        .get(format!(
            "{}/api/github/team/assets/xet-write-token/main?subject={oversized_subject}",
            runtime.base_url()
        ))
        .send()
        .await?;
    assert_eq!(
        oversized_subject_response.status(),
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_webhook_endpoint_rejects_oversized_body_before_authentication()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_github_provider_runtime(NonZeroU64::MIN).await?;

    let response = Client::new()
        .post(format!(
            "{}/v1/providers/github/webhooks",
            runtime.base_url()
        ))
        .header("x-github-event", "push")
        .header("x-github-delivery", "delivery-oversized")
        .header("x-hub-signature-256", "sha256=invalid")
        .body(vec![b'{'; MAX_PROVIDER_WEBHOOK_BODY_BYTES + 1])
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_webhook_endpoint_creates_repository_deletion_holds()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_github_provider_runtime(NonZeroU64::MIN).await?;
    seed_provider_repository_state(
        runtime.storage_path(),
        RepositoryProvider::GitHub,
        "team",
        "assets",
    );
    upload_repository_asset(&runtime, RepositoryProvider::GitHub, "team", "assets").await?;

    let webhook_body = br#"{
            "action":"deleted",
            "repository":{"full_name":"team/assets"}
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/github/webhooks",
            runtime.base_url()
        ))
        .header("x-github-event", "repository")
        .header("x-github-delivery", "delivery-1")
        .header(
            "x-hub-signature-256",
            github_webhook_signature(webhook_body).unwrap_or_else(|| "sha256=".to_owned()),
        )
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.event_kind, "repository_deleted");
    assert_eq!(outcome.owner, "team");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.affected_file_versions, 1);
    assert_eq!(outcome.affected_chunks, 2);
    assert_eq!(outcome.applied_holds, 4);

    assert_eq!(retention_hold_count(runtime.storage_path())?, 4);
    assert_provider_repository_state_absent(
        runtime.storage_path(),
        RepositoryProvider::GitHub,
        "team",
        "assets",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn github_provider_webhook_endpoint_reports_repository_rename()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_github_provider_runtime(NonZeroU64::MIN).await?;
    seed_provider_repository_state(
        runtime.storage_path(),
        RepositoryProvider::GitHub,
        "team",
        "assets",
    );

    let webhook_body = br#"{
            "action":"renamed",
            "repository":{"full_name":"team/new-assets"},
            "changes":{"repository":{"name":{"from":"assets"}}}
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/github/webhooks",
            runtime.base_url()
        ))
        .header("x-github-event", "repository")
        .header("x-github-delivery", "delivery-rename")
        .header(
            "x-hub-signature-256",
            github_webhook_signature(webhook_body).unwrap_or_else(|| "sha256=".to_owned()),
        )
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::GitHub);
    assert_eq!(outcome.event_kind, "repository_renamed");
    assert_eq!(outcome.owner, "team");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.new_owner.as_deref(), Some("team"));
    assert_eq!(outcome.new_repo.as_deref(), Some("new-assets"));
    assert_eq!(outcome.applied_holds, 0);
    assert_provider_repository_state_migrated(
        runtime.storage_path(),
        RepositoryProvider::GitHub,
        "team",
        "assets",
        "team",
        "new-assets",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn github_provider_webhook_endpoint_reports_access_changes() -> Result<(), Box<dyn StdError>>
{
    let runtime = start_github_provider_runtime(NonZeroU64::MIN).await?;

    let webhook_body = br#"{
            "action":"transferred",
            "repository":{"full_name":"team/assets"}
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/github/webhooks",
            runtime.base_url()
        ))
        .header("x-github-event", "repository")
        .header("x-github-delivery", "delivery-access")
        .header(
            "x-hub-signature-256",
            github_webhook_signature(webhook_body).unwrap_or_else(|| "sha256=".to_owned()),
        )
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::GitHub);
    assert_eq!(outcome.event_kind, "access_changed");
    assert_eq!(outcome.owner, "team");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.applied_holds, 0);
    assert_provider_repository_state_observed(
        runtime.storage_path(),
        RepositoryProvider::GitHub,
        "team",
        "assets",
        true,
        None,
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn github_provider_webhook_endpoint_reports_revision_pushes() -> Result<(), Box<dyn StdError>>
{
    let runtime = start_github_provider_runtime(NonZeroU64::MIN).await?;

    let webhook_body = br#"{
            "ref":"refs/heads/main",
            "repository":{"full_name":"team/assets"}
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/github/webhooks",
            runtime.base_url()
        ))
        .header("x-github-event", "push")
        .header("x-github-delivery", "delivery-push")
        .header(
            "x-hub-signature-256",
            github_webhook_signature(webhook_body).unwrap_or_else(|| "sha256=".to_owned()),
        )
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::GitHub);
    assert_eq!(outcome.event_kind, "revision_pushed");
    assert_eq!(outcome.owner, "team");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.delivery_id, "delivery-push");
    assert_eq!(outcome.revision.as_deref(), Some("refs/heads/main"));
    assert_eq!(outcome.applied_holds, 0);
    assert_provider_repository_state_observed(
        runtime.storage_path(),
        RepositoryProvider::GitHub,
        "team",
        "assets",
        false,
        Some("refs/heads/main"),
    );
    Ok(())
}
