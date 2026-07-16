use std::{env, error::Error, time::Duration};

use reqwest::{Client, StatusCode};
use serde_json::json;
use shardline_server::{ProviderTokenIssueResponse, ReadyResponse, test_fixtures};
use tokio::time::sleep;

type TestError = Box<dyn Error + Send + Sync>;

const PROVIDER_KEY: &str = "kind-smoke-provider-key";
const METRICS_TOKEN: &str = "kind-smoke-metrics-token";

#[derive(Debug)]
struct KindDeploymentConfig {
    api_url: String,
    transfer_url: String,
}

impl KindDeploymentConfig {
    fn from_env() -> Option<Self> {
        Some(Self {
            api_url: env::var("SHARDLINE_KIND_SMOKE_API_URL").ok()?,
            transfer_url: env::var("SHARDLINE_KIND_SMOKE_TRANSFER_URL").ok()?,
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the disposable kind deployment created by scripts/k8s/kind-smoke.sh"]
async fn kind_scaled_deployment_serves_split_roles_with_real_backends() -> Result<(), TestError> {
    let Some(config) = KindDeploymentConfig::from_env() else {
        return Ok(());
    };
    let client = Client::new();

    wait_for_health(&client, &config.api_url).await?;
    wait_for_health(&client, &config.transfer_url).await?;

    assert_ready(&client, &config.api_url, "api", "postgres", "s3", "redis").await?;
    assert_ready(
        &client,
        &config.transfer_url,
        "transfer",
        "postgres",
        "s3",
        "disabled",
    )
    .await?;

    let token = issue_write_token(&client, &config.api_url).await?;
    let (xorb, xorb_hash) = test_fixtures::single_chunk_xorb(b"kind deployment smoke xorb");
    let upload = client
        .post(format!(
            "{}/v1/xorbs/default/{xorb_hash}",
            config.transfer_url
        ))
        .bearer_auth(&token)
        .body(xorb.to_vec())
        .send()
        .await?;
    assert_eq!(
        upload.status(),
        StatusCode::OK,
        "transfer xorb upload failed"
    );

    let (shard, _) =
        test_fixtures::single_file_shard(&[(b"kind deployment smoke xorb", xorb_hash.as_str())]);
    let shard_upload = client
        .post(format!("{}/v1/shards", config.api_url))
        .bearer_auth(&token)
        .body(shard.to_vec())
        .send()
        .await?;
    assert_eq!(
        shard_upload.status(),
        StatusCode::OK,
        "api shard upload failed"
    );

    let transferred = client
        .get(format!(
            "{}/transfer/xorb/default/{xorb_hash}",
            config.transfer_url
        ))
        .bearer_auth(&token)
        .header("range", format!("bytes=0-{}", xorb.len() - 1))
        .send()
        .await?;
    assert_eq!(
        transferred.status(),
        StatusCode::PARTIAL_CONTENT,
        "transfer xorb download failed"
    );
    assert_eq!(transferred.bytes().await?.as_ref(), xorb.as_ref());

    let stats = client
        .get(format!("{}/v1/stats", config.api_url))
        .bearer_auth(&token)
        .send()
        .await?;
    assert_eq!(stats.status(), StatusCode::OK, "api stats route failed");

    let api_transfer_route = client
        .get(format!(
            "{}/transfer/xorb/default/{xorb_hash}",
            config.api_url
        ))
        .bearer_auth(&token)
        .send()
        .await?;
    assert_eq!(
        api_transfer_route.status(),
        StatusCode::NOT_FOUND,
        "api deployment unexpectedly served a transfer route"
    );

    let transfer_api_route = client
        .post(format!(
            "{}/v1/providers/generic/tokens",
            config.transfer_url
        ))
        .send()
        .await?;
    assert_eq!(
        transfer_api_route.status(),
        StatusCode::NOT_FOUND,
        "transfer deployment unexpectedly served an api route"
    );

    let metrics = client
        .get(format!("{}/metrics", config.api_url))
        .bearer_auth(METRICS_TOKEN)
        .send()
        .await?;
    assert_eq!(metrics.status(), StatusCode::OK, "metrics auth failed");
    assert!(metrics.text().await?.contains("shardline_up 1"));

    Ok(())
}

async fn wait_for_health(client: &Client, base_url: &str) -> Result<(), TestError> {
    for _attempt in 0..90 {
        if let Ok(response) = client.get(format!("{base_url}/healthz")).send().await
            && response.status() == StatusCode::OK
        {
            return Ok(());
        }
        sleep(Duration::from_millis(200)).await;
    }

    Err(format!("{base_url} did not become healthy").into())
}

async fn assert_ready(
    client: &Client,
    base_url: &str,
    expected_role: &str,
    expected_metadata: &str,
    expected_object: &str,
    expected_cache: &str,
) -> Result<(), TestError> {
    let response = client.get(format!("{base_url}/readyz")).send().await?;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{expected_role} is not ready"
    );
    let ready = response.json::<ReadyResponse>().await?;
    assert_eq!(ready.server_role, expected_role);
    assert_eq!(ready.metadata_backend, expected_metadata);
    assert_eq!(ready.object_backend, expected_object);
    assert_eq!(ready.cache_backend, expected_cache);
    Ok(())
}

async fn issue_write_token(client: &Client, api_url: &str) -> Result<String, TestError> {
    let response = client
        .post(format!("{api_url}/v1/providers/generic/tokens"))
        .header("x-shardline-provider-key", PROVIDER_KEY)
        .json(&json!({
            "subject": "kind-smoke-user",
            "owner": "kind-smoke",
            "repo": "assets",
            "revision": "main",
            "scope": "Write"
        }))
        .send()
        .await?;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "api provider token issuance failed"
    );
    Ok(response.json::<ProviderTokenIssueResponse>().await?.token)
}
