#![allow(clippy::panic_in_result_fn)]

use std::{error::Error as StdError, num::NonZeroU64};

use reqwest::{Client, StatusCode};
use shardline_protocol::{RepositoryProvider, TokenScope};

use super::super::{
    assert_provider_repository_state_absent, assert_provider_repository_state_migrated,
    assert_provider_repository_state_observed, gitea_webhook_signature,
    seed_provider_repository_state, write_gitea_provider_config,
};
use super::shared::{
    PROVIDER_BOOTSTRAP_KEY, retention_hold_count, start_provider_runtime, upload_repository_asset,
};
use crate::{ProviderTokenIssueRequest, ProviderTokenIssueResponse, ProviderWebhookResponse};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gitea_provider_token_endpoint_issues_runtime_tokens() -> Result<(), Box<dyn StdError>> {
    let runtime = start_provider_runtime(
        write_gitea_provider_config,
        "gitea-runtime",
        NonZeroU64::MIN,
    )
    .await?;

    let response = Client::new()
        .post(format!("{}/v1/providers/gitea/tokens", runtime.base_url()))
        .header("x-shardline-provider-key", PROVIDER_BOOTSTRAP_KEY)
        .json(&ProviderTokenIssueRequest {
            subject: "gitea-user-1".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: None,
            scope: TokenScope::Write,
        })
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let issued = response.json::<ProviderTokenIssueResponse>().await?;
    assert_eq!(issued.issuer, "gitea-runtime");
    assert_eq!(issued.provider, RepositoryProvider::Gitea);
    assert_eq!(issued.owner, "team");
    assert_eq!(issued.repo, "assets");
    assert_eq!(issued.revision.as_deref(), Some("refs/heads/main"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gitea_provider_webhook_endpoint_creates_repository_deletion_holds()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_provider_runtime(
        write_gitea_provider_config,
        "gitea-runtime",
        NonZeroU64::MIN,
    )
    .await?;
    seed_provider_repository_state(
        runtime.storage_path(),
        RepositoryProvider::Gitea,
        "team",
        "assets",
    );
    upload_repository_asset(&runtime, RepositoryProvider::Gitea, "team", "assets").await?;

    let webhook_body = br#"{
            "action":"deleted",
            "repository":{"full_name":"team/assets"}
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/gitea/webhooks",
            runtime.base_url()
        ))
        .header("x-gitea-event", "repository")
        .header("x-gitea-delivery", "delivery-delete")
        .header(
            "x-gitea-signature",
            gitea_webhook_signature(webhook_body).unwrap_or_default(),
        )
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::Gitea);
    assert_eq!(outcome.event_kind, "repository_deleted");
    assert_eq!(outcome.owner, "team");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.delivery_id, "delivery-delete");
    assert_eq!(outcome.affected_file_versions, 1);
    assert_eq!(outcome.affected_chunks, 2);
    assert_eq!(outcome.applied_holds, 4);

    assert_eq!(retention_hold_count(runtime.storage_path())?, 4);
    assert_provider_repository_state_absent(
        runtime.storage_path(),
        RepositoryProvider::Gitea,
        "team",
        "assets",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gitea_provider_webhook_endpoint_reports_repository_rename() -> Result<(), Box<dyn StdError>>
{
    let runtime = start_provider_runtime(
        write_gitea_provider_config,
        "gitea-runtime",
        NonZeroU64::MIN,
    )
    .await?;
    seed_provider_repository_state(
        runtime.storage_path(),
        RepositoryProvider::Gitea,
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
            "{}/v1/providers/gitea/webhooks",
            runtime.base_url()
        ))
        .header("x-gitea-event", "repository")
        .header("x-gitea-delivery", "delivery-rename")
        .header(
            "x-gitea-signature",
            gitea_webhook_signature(webhook_body).unwrap_or_default(),
        )
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::Gitea);
    assert_eq!(outcome.event_kind, "repository_renamed");
    assert_eq!(outcome.owner, "team");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.new_owner.as_deref(), Some("team"));
    assert_eq!(outcome.new_repo.as_deref(), Some("new-assets"));
    assert_eq!(outcome.applied_holds, 0);
    assert_provider_repository_state_migrated(
        runtime.storage_path(),
        RepositoryProvider::Gitea,
        "team",
        "assets",
        "team",
        "new-assets",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gitea_provider_webhook_endpoint_reports_access_changes() -> Result<(), Box<dyn StdError>> {
    let runtime = start_provider_runtime(
        write_gitea_provider_config,
        "gitea-runtime",
        NonZeroU64::MIN,
    )
    .await?;

    let webhook_body = br#"{
            "action":"transferred",
            "repository":{"full_name":"team/assets"}
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/gitea/webhooks",
            runtime.base_url()
        ))
        .header("x-gitea-event", "repository")
        .header("x-gitea-delivery", "delivery-access")
        .header(
            "x-gitea-signature",
            gitea_webhook_signature(webhook_body).unwrap_or_default(),
        )
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::Gitea);
    assert_eq!(outcome.event_kind, "access_changed");
    assert_eq!(outcome.owner, "team");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.delivery_id, "delivery-access");
    assert_eq!(outcome.applied_holds, 0);
    assert_provider_repository_state_observed(
        runtime.storage_path(),
        RepositoryProvider::Gitea,
        "team",
        "assets",
        true,
        None,
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gitea_provider_webhook_endpoint_reports_revision_pushes() -> Result<(), Box<dyn StdError>>
{
    let runtime = start_provider_runtime(
        write_gitea_provider_config,
        "gitea-runtime",
        NonZeroU64::MIN,
    )
    .await?;

    let webhook_body = br#"{
            "ref":"refs/heads/main",
            "repository":{"full_name":"team/assets"}
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/gitea/webhooks",
            runtime.base_url()
        ))
        .header("x-gitea-event", "push")
        .header("x-gitea-delivery", "delivery-1")
        .header(
            "x-gitea-signature",
            gitea_webhook_signature(webhook_body).unwrap_or_default(),
        )
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::Gitea);
    assert_eq!(outcome.event_kind, "revision_pushed");
    assert_eq!(outcome.owner, "team");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.delivery_id, "delivery-1");
    assert_eq!(outcome.revision.as_deref(), Some("refs/heads/main"));
    assert_eq!(outcome.affected_file_versions, 0);
    assert_eq!(outcome.affected_chunks, 0);
    assert_eq!(outcome.applied_holds, 0);
    assert_provider_repository_state_observed(
        runtime.storage_path(),
        RepositoryProvider::Gitea,
        "team",
        "assets",
        false,
        Some("refs/heads/main"),
    );
    Ok(())
}
