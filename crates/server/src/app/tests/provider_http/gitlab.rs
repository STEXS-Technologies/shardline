#![allow(clippy::panic_in_result_fn)]

use std::{error::Error as StdError, num::NonZeroU64};

use reqwest::{Client, StatusCode};
use shardline_protocol::{RepositoryProvider, TokenScope};

use super::super::{
    assert_provider_repository_state_absent, assert_provider_repository_state_migrated,
    assert_provider_repository_state_observed, seed_provider_repository_state,
    write_gitlab_provider_config,
};
use super::shared::{
    PROVIDER_BOOTSTRAP_KEY, retention_hold_count, start_provider_runtime, upload_repository_asset,
};
use crate::{ProviderTokenIssueRequest, ProviderTokenIssueResponse, ProviderWebhookResponse};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gitlab_provider_token_endpoint_issues_runtime_tokens() -> Result<(), Box<dyn StdError>> {
    let runtime = start_provider_runtime(
        write_gitlab_provider_config,
        "gitlab-runtime",
        NonZeroU64::MIN,
    )
    .await?;

    let response = Client::new()
        .post(format!("{}/v1/providers/gitlab/tokens", runtime.base_url()))
        .header("x-shardline-provider-key", PROVIDER_BOOTSTRAP_KEY)
        .json(&ProviderTokenIssueRequest {
            subject: "gitlab-user-1".to_owned(),
            owner: "group".to_owned(),
            repo: "assets".to_owned(),
            revision: None,
            scope: TokenScope::Write,
        })
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let issued = response.json::<ProviderTokenIssueResponse>().await?;
    assert_eq!(issued.issuer, "gitlab-runtime");
    assert_eq!(issued.provider, RepositoryProvider::GitLab);
    assert_eq!(issued.owner, "group");
    assert_eq!(issued.repo, "assets");
    assert_eq!(issued.revision.as_deref(), Some("refs/heads/main"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gitlab_provider_webhook_endpoint_creates_repository_deletion_holds()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_provider_runtime(
        write_gitlab_provider_config,
        "gitlab-runtime",
        NonZeroU64::MIN,
    )
    .await?;
    seed_provider_repository_state(
        runtime.storage_path(),
        RepositoryProvider::GitLab,
        "group",
        "assets",
    );
    upload_repository_asset(&runtime, RepositoryProvider::GitLab, "group", "assets").await?;

    let webhook_body = br#"{
            "event_name":"project_destroy",
            "path_with_namespace":"group/assets"
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/gitlab/webhooks",
            runtime.base_url()
        ))
        .header("x-gitlab-event", "Project Hook")
        .header("x-gitlab-webhook-uuid", "delivery-1")
        .header("x-gitlab-token", "secret")
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::GitLab);
    assert_eq!(outcome.event_kind, "repository_deleted");
    assert_eq!(outcome.owner, "group");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.affected_file_versions, 1);
    assert_eq!(outcome.affected_chunks, 2);
    assert_eq!(outcome.applied_holds, 4);

    assert_eq!(retention_hold_count(runtime.storage_path())?, 4);
    assert_provider_repository_state_absent(
        runtime.storage_path(),
        RepositoryProvider::GitLab,
        "group",
        "assets",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gitlab_provider_webhook_endpoint_reports_repository_transfer()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_provider_runtime(
        write_gitlab_provider_config,
        "gitlab-runtime",
        NonZeroU64::MIN,
    )
    .await?;
    seed_provider_repository_state(
        runtime.storage_path(),
        RepositoryProvider::GitLab,
        "group",
        "assets",
    );

    let webhook_body = br#"{
            "event_name":"project_transfer",
            "old_path_with_namespace":"group/assets",
            "path_with_namespace":"new-group/assets"
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/gitlab/webhooks",
            runtime.base_url()
        ))
        .header("x-gitlab-event", "System Hook")
        .header("x-gitlab-webhook-uuid", "delivery-transfer")
        .header("x-gitlab-token", "secret")
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::GitLab);
    assert_eq!(outcome.event_kind, "repository_renamed");
    assert_eq!(outcome.owner, "group");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.new_owner.as_deref(), Some("new-group"));
    assert_eq!(outcome.new_repo.as_deref(), Some("assets"));
    assert_eq!(outcome.applied_holds, 0);
    assert_provider_repository_state_migrated(
        runtime.storage_path(),
        RepositoryProvider::GitLab,
        "group",
        "assets",
        "new-group",
        "assets",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gitlab_provider_webhook_endpoint_reports_project_updates_as_access_changes()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_provider_runtime(
        write_gitlab_provider_config,
        "gitlab-runtime",
        NonZeroU64::MIN,
    )
    .await?;

    let webhook_body = br#"{
            "event_name":"project_update",
            "path_with_namespace":"group/assets"
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/gitlab/webhooks",
            runtime.base_url()
        ))
        .header("x-gitlab-event", "System Hook")
        .header("x-gitlab-webhook-uuid", "delivery-update")
        .header("x-gitlab-token", "secret")
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::GitLab);
    assert_eq!(outcome.event_kind, "access_changed");
    assert_eq!(outcome.owner, "group");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.applied_holds, 0);
    assert_provider_repository_state_observed(
        runtime.storage_path(),
        RepositoryProvider::GitLab,
        "group",
        "assets",
        true,
        None,
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gitlab_provider_webhook_endpoint_reports_project_create_as_access_change()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_provider_runtime(
        write_gitlab_provider_config,
        "gitlab-runtime",
        NonZeroU64::MIN,
    )
    .await?;

    let webhook_body = br#"{
            "event_name":"project_create",
            "path_with_namespace":"group/assets"
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/gitlab/webhooks",
            runtime.base_url()
        ))
        .header("x-gitlab-event", "Project Hook")
        .header("x-gitlab-webhook-uuid", "delivery-create")
        .header("x-gitlab-token", "secret")
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::GitLab);
    assert_eq!(outcome.event_kind, "access_changed");
    assert_eq!(outcome.owner, "group");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.delivery_id, "delivery-create");
    assert_eq!(outcome.applied_holds, 0);
    assert_provider_repository_state_observed(
        runtime.storage_path(),
        RepositoryProvider::GitLab,
        "group",
        "assets",
        true,
        None,
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gitlab_provider_webhook_endpoint_reports_repository_update_revision_pushes()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_provider_runtime(
        write_gitlab_provider_config,
        "gitlab-runtime",
        NonZeroU64::MIN,
    )
    .await?;

    let webhook_body = br#"{
            "event_name":"repository_update",
            "project":{"path_with_namespace":"group/assets"},
            "refs":["refs/heads/main"]
        }"#;
    let response = Client::new()
        .post(format!(
            "{}/v1/providers/gitlab/webhooks",
            runtime.base_url()
        ))
        .header("x-gitlab-event", "System Hook")
        .header("x-gitlab-webhook-uuid", "delivery-repository-update")
        .header("x-gitlab-token", "secret")
        .body(webhook_body.to_vec())
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let outcome = response.json::<ProviderWebhookResponse>().await?;
    assert_eq!(outcome.provider, RepositoryProvider::GitLab);
    assert_eq!(outcome.event_kind, "revision_pushed");
    assert_eq!(outcome.owner, "group");
    assert_eq!(outcome.repo, "assets");
    assert_eq!(outcome.delivery_id, "delivery-repository-update");
    assert_eq!(outcome.revision.as_deref(), Some("refs/heads/main"));
    assert_eq!(outcome.applied_holds, 0);
    assert_provider_repository_state_observed(
        runtime.storage_path(),
        RepositoryProvider::GitLab,
        "group",
        "assets",
        false,
        Some("refs/heads/main"),
    );
    Ok(())
}
