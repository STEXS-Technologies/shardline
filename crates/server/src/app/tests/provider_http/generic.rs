#![allow(clippy::panic_in_result_fn)]

use std::{error::Error as StdError, num::NonZeroU64};

use reqwest::{Client, StatusCode};
use shardline_protocol::{RepositoryProvider, TokenScope};

use super::super::{
    assert_provider_repository_state_absent, assert_provider_repository_state_migrated,
    assert_provider_repository_state_observed, generic_webhook_signature,
    seed_provider_repository_state, write_generic_provider_config,
};
use super::shared::{
    PROVIDER_BOOTSTRAP_KEY, retention_hold_count, start_provider_runtime, upload_repository_asset,
};
use crate::{
    ProviderTokenIssueRequest, ProviderTokenIssueResponse, ProviderWebhookResponse,
    test_fixtures::single_chunk_xorb,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generic_provider_token_endpoint_issues_runtime_tokens() -> Result<(), Box<dyn StdError>> {
    let runtime = start_provider_runtime(
        write_generic_provider_config,
        "generic-bridge",
        NonZeroU64::MIN,
    )
    .await?;

    let response = Client::new()
        .post(format!(
            "{}/v1/providers/generic/tokens",
            runtime.base_url()
        ))
        .header("x-shardline-provider-key", PROVIDER_BOOTSTRAP_KEY)
        .json(&ProviderTokenIssueRequest {
            subject: "generic-user-1".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: None,
            scope: TokenScope::Write,
        })
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let issued = response.json::<ProviderTokenIssueResponse>().await?;
    assert_eq!(issued.issuer, "generic-bridge");
    assert_eq!(issued.provider, RepositoryProvider::Generic);
    assert_eq!(issued.revision.as_deref(), Some("refs/heads/main"));

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
async fn generic_provider_webhook_endpoint_creates_repository_deletion_holds()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_provider_runtime(
        write_generic_provider_config,
        "generic-bridge",
        NonZeroU64::MIN,
    )
    .await?;
    seed_provider_repository_state(
        runtime.storage_path(),
        RepositoryProvider::Generic,
        "team",
        "assets",
    );
    upload_repository_asset(&runtime, RepositoryProvider::Generic, "team", "assets").await?;

    let webhook_body = br#"{
            "kind":"repository_deleted",
            "repository":{"owner":"team","name":"assets"}
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/generic/webhooks",
            runtime.base_url()
        ))
        .header("x-shardline-event", "repository_deleted")
        .header("x-shardline-delivery", "delivery-1")
        .header(
            "x-shardline-signature",
            generic_webhook_signature(webhook_body).unwrap_or_else(|| "sha256=".to_owned()),
        )
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::Generic);
    assert_eq!(outcome.event_kind, "repository_deleted");
    assert_eq!(outcome.owner, "team");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.affected_file_versions, 1);
    assert_eq!(outcome.affected_chunks, 2);
    assert_eq!(outcome.applied_holds, 4);

    assert_eq!(retention_hold_count(runtime.storage_path())?, 4);
    assert_provider_repository_state_absent(
        runtime.storage_path(),
        RepositoryProvider::Generic,
        "team",
        "assets",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generic_provider_webhook_endpoint_reports_repository_rename()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_provider_runtime(
        write_generic_provider_config,
        "generic-bridge",
        NonZeroU64::MIN,
    )
    .await?;
    seed_provider_repository_state(
        runtime.storage_path(),
        RepositoryProvider::Generic,
        "team",
        "assets",
    );

    let webhook_body = br#"{
            "kind":"repository_renamed",
            "repository":{"owner":"team","name":"assets"},
            "new_repository":{"owner":"team","name":"new-assets"}
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/generic/webhooks",
            runtime.base_url()
        ))
        .header("x-shardline-event", "repository_renamed")
        .header("x-shardline-delivery", "delivery-rename")
        .header(
            "x-shardline-signature",
            generic_webhook_signature(webhook_body).unwrap_or_else(|| "sha256=".to_owned()),
        )
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::Generic);
    assert_eq!(outcome.event_kind, "repository_renamed");
    assert_eq!(outcome.owner, "team");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.new_owner.as_deref(), Some("team"));
    assert_eq!(outcome.new_repo.as_deref(), Some("new-assets"));
    assert_eq!(outcome.applied_holds, 0);
    assert_provider_repository_state_migrated(
        runtime.storage_path(),
        RepositoryProvider::Generic,
        "team",
        "assets",
        "team",
        "new-assets",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generic_provider_webhook_endpoint_reports_access_changes() -> Result<(), Box<dyn StdError>>
{
    let runtime = start_provider_runtime(
        write_generic_provider_config,
        "generic-bridge",
        NonZeroU64::MIN,
    )
    .await?;

    let webhook_body = br#"{
            "kind":"access_changed",
            "repository":{"owner":"team","name":"assets"}
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/generic/webhooks",
            runtime.base_url()
        ))
        .header("x-shardline-event", "access_changed")
        .header("x-shardline-delivery", "delivery-access")
        .header(
            "x-shardline-signature",
            generic_webhook_signature(webhook_body).unwrap_or_else(|| "sha256=".to_owned()),
        )
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::Generic);
    assert_eq!(outcome.event_kind, "access_changed");
    assert_eq!(outcome.owner, "team");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.delivery_id, "delivery-access");
    assert_eq!(outcome.applied_holds, 0);
    assert_provider_repository_state_observed(
        runtime.storage_path(),
        RepositoryProvider::Generic,
        "team",
        "assets",
        true,
        None,
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generic_provider_webhook_endpoint_reports_revision_pushes() -> Result<(), Box<dyn StdError>>
{
    let runtime = start_provider_runtime(
        write_generic_provider_config,
        "generic-bridge",
        NonZeroU64::MIN,
    )
    .await?;

    let webhook_body = br#"{
            "kind":"revision_pushed",
            "repository":{"owner":"team","name":"assets"},
            "revision":"refs/heads/main"
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/generic/webhooks",
            runtime.base_url()
        ))
        .header("x-shardline-event", "revision_pushed")
        .header("x-shardline-delivery", "delivery-push")
        .header(
            "x-shardline-signature",
            generic_webhook_signature(webhook_body).unwrap_or_else(|| "sha256=".to_owned()),
        )
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::Generic);
    assert_eq!(outcome.event_kind, "revision_pushed");
    assert_eq!(outcome.owner, "team");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.delivery_id, "delivery-push");
    assert_eq!(outcome.revision.as_deref(), Some("refs/heads/main"));
    assert_eq!(outcome.applied_holds, 0);
    assert_provider_repository_state_observed(
        runtime.storage_path(),
        RepositoryProvider::Generic,
        "team",
        "assets",
        false,
        Some("refs/heads/main"),
    );
    Ok(())
}
